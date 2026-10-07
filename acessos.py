"""
Contador de acessos do site: visitantes únicos (por IP, anonimizado) e pessoas acessando ao mesmo tempo.

Como funciona
- A página envia um "estou aqui" (POST /api/presenca) a cada 60s enquanto a aba está visível.
- "Acessando agora" = IPs distintos que mandaram sinal nos últimos 150s.
- A cada minuto o servidor mede esse número. Se houver pouco movimento, grava um ponto a cada 10 minutos
  (com o pico do período); quando passa de LIMIAR_ALTO pessoas, grava minuto a minuto (e continua assim
  por mais 30 minutos depois que o movimento cai).
- IPs NUNCA são guardados: viram um código (HMAC-SHA256 com chave secreta) usado só para contar únicos.

Onde os dados ficam
- Com UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN definidos: no Upstash (sobrevive a reinícios do Render).
- Sem eles: no arquivo dados_acessos.json (ótimo para teste local; no Render é apagado a cada reinício).

Relatório por e-mail (diário, via Resend, porque o Render gratuito bloqueia SMTP)
- RESEND_API_KEY, RELATORIO_EMAIL_PARA (e opcionalmente RELATORIO_EMAIL_DE, RELATORIO_HORA, SITE_URL)
- RELATORIO_CHAVE: libera /api/acessos/relatorio?chave=...  (&preview=1 só mostra, sem enviar)
"""
import base64
import csv
import gzip
import hashlib
import hmac
import io
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

BRT = timezone(timedelta(hours=-3))      # Brasília (sem horário de verão desde 2019)

JANELA_ATIVO = 150                       # segundos sem sinal para a pessoa deixar de contar como "agora"
LIMIAR_ALTO = int(os.environ.get('ACESSOS_LIMIAR_ALTO', 20))   # a partir daqui grava minuto a minuto
MANTER_MODO_ALTO = 30 * 60               # continua minuto a minuto por 30 min após o último pico
INTERVALO_BAIXO = 10 * 60
MAX_PONTOS = 30000                       # ~20 dias em modo minuto; bem mais em modo 10 min
MAX_ATIVOS = 50000                       # proteção de memória contra abuso
ARQUIVO_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dados_acessos.json')

RESEND_URL = os.environ.get('RESEND_API_URL', 'https://api.resend.com/emails')
SITE_URL = os.environ.get('SITE_URL', 'https://eleicoesbrasil-hartmann.onrender.com').rstrip('/')
RELATORIO_HORA = int(os.environ.get('RELATORIO_HORA', 8))      # horário de Brasília


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [acessos] {msg}", flush=True)


# ===================== ARMAZENAMENTO =====================
class ArmazenamentoUpstash:
    persistente = True
    descricao = 'Upstash Redis'

    def __init__(self, url, token):
        self.url, self.headers = url.rstrip('/'), {'Authorization': f'Bearer {token}'}

    def _pipeline(self, comandos):
        r = requests.post(f'{self.url}/pipeline', headers=self.headers, json=comandos, timeout=10)
        r.raise_for_status()
        resultados = r.json()
        for item in resultados:
            if 'error' in item:
                raise RuntimeError(item['error'])
        return [item.get('result') for item in resultados]

    def carregar(self):
        sal, total, serie, ultimo = self._pipeline([
            ['GET', 'acessos:sal'], ['SCARD', 'acessos:ips'],
            ['LRANGE', 'acessos:serie', 0, -1], ['GET', 'acessos:ultimo_relatorio']])
        if not sal:
            sal = secrets.token_hex(32)
            self._pipeline([['SET', 'acessos:sal', sal, 'NX']])
            sal = self._pipeline([['GET', 'acessos:sal']])[0]   # se outro processo criou antes, usa o dele
        return sal, int(total or 0), [json.loads(p) for p in (serie or [])], ultimo

    def salvar(self, novos, ponto):
        comandos = []
        if novos:
            comandos.append(['SADD', 'acessos:ips', *novos])
        comandos.append(['SCARD', 'acessos:ips'])
        if ponto:
            comandos += [['RPUSH', 'acessos:serie', json.dumps(ponto, separators=(',', ':'))],
                         ['LTRIM', 'acessos:serie', -MAX_PONTOS, -1]]
        res = self._pipeline(comandos)
        return int(res[1 if novos else 0])

    def marcar_relatorio(self, quando):
        self._pipeline([['SET', 'acessos:ultimo_relatorio', quando]])


class ArmazenamentoArquivo:
    persistente = False
    descricao = 'arquivo local dados_acessos.json'

    def __init__(self, caminho):
        self.caminho = caminho
        self.dados = {'sal': None, 'ips': [], 'serie': [], 'ultimo_relatorio': None}
        if os.path.exists(caminho):
            with open(caminho, encoding='utf-8') as f:
                self.dados.update(json.load(f))
        self.ips = set(self.dados['ips'])

    def _gravar(self):
        self.dados['ips'] = sorted(self.ips)
        tmp = self.caminho + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.dados, f, separators=(',', ':'))
        os.replace(tmp, self.caminho)

    def carregar(self):
        if not self.dados['sal']:
            self.dados['sal'] = secrets.token_hex(32)
            self._gravar()
        return self.dados['sal'], len(self.ips), list(self.dados['serie']), self.dados['ultimo_relatorio']

    def salvar(self, novos, ponto):
        self.ips.update(novos)
        if ponto:
            self.dados['serie'] = (self.dados['serie'] + [ponto])[-MAX_PONTOS:]
        if novos or ponto:
            self._gravar()
        return len(self.ips)

    def marcar_relatorio(self, quando):
        self.dados['ultimo_relatorio'] = quando
        self._gravar()


def criar_armazenamento():
    url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
    if url and token:
        return ArmazenamentoUpstash(url, token)
    return ArmazenamentoArquivo(ARQUIVO_LOCAL)


# ===================== CONTADOR =====================
class Contador:
    def __init__(self, armazenamento):
        self.arm = armazenamento
        self.lock = threading.Lock()
        self.ativos = {}            # código do IP -> último sinal
        self.pendentes = set()      # códigos vistos desde a última gravação
        self.ja_salvos = set()      # códigos que este processo já gravou (evita regravar a cada minuto)
        self.pico_intervalo = 0
        self.agora = 0
        self.modo_alto_ate = 0
        self.ultimo_ponto = 0
        self.respostas = {}         # periodo -> JSON pronto, sem o "agora" (refeito a cada minuto)
        self.sal, self.total_unicos, self.serie, self.ultimo_relatorio = None, 0, [], None
        try:
            self.sal, self.total_unicos, self.serie, self.ultimo_relatorio = self.arm.carregar()
            if self.serie:
                self.ultimo_ponto = self.serie[-1]['t']
            log(f"usando {self.arm.descricao}: {self.total_unicos} visitantes únicos, {len(self.serie)} pontos")
            if not self.arm.persistente and os.environ.get('RENDER'):
                log("ATENÇÃO: sem Upstash configurado, os acessos são apagados a cada reinício do Render.")
        except Exception as e:
            self.sal = self.sal or secrets.token_hex(32)
            log(f"ERRO ao carregar dados de acesso ({e}); contando só em memória até o próximo reinício")

    def codigo(self, ip):
        return hmac.new(self.sal.encode(), ip.encode(), hashlib.sha256).hexdigest()[:20]

    def registrar(self, ip):
        h = self.codigo(ip)
        with self.lock:
            if h not in self.ativos and len(self.ativos) >= MAX_ATIVOS:
                return
            self.ativos[h] = time.time()
            self.pendentes.add(h)

    def contar_agora(self):
        limite = time.time() - JANELA_ATIVO
        with self.lock:
            for h in [h for h, t in self.ativos.items() if t < limite]:
                del self.ativos[h]
            return len(self.ativos)

    def modo_alto(self):
        return time.time() < self.modo_alto_ate

    def tique(self):
        """Roda a cada minuto: mede, decide se grava ponto e salva."""
        agora_ts = time.time()
        self.agora = self.contar_agora()
        self.pico_intervalo = max(self.pico_intervalo, self.agora)
        if self.agora >= LIMIAR_ALTO:
            if not self.modo_alto():
                log(f"movimento alto ({self.agora} simultâneos): gravando minuto a minuto")
            self.modo_alto_ate = agora_ts + MANTER_MODO_ALTO

        intervalo = 60 if self.modo_alto() else INTERVALO_BAIXO
        gravar_ponto = agora_ts - self.ultimo_ponto >= intervalo - 5

        with self.lock:
            novos, self.pendentes = self.pendentes - self.ja_salvos, set()
        ponto = None
        if gravar_ponto:
            ponto = {'t': int(agora_ts), 's': self.pico_intervalo, 'u': None, 'i': intervalo // 60}
        try:
            if novos:   # sem visitante novo no minuto, não gasta nenhum comando no armazenamento
                self.total_unicos = self.arm.salvar(sorted(novos), None)
                self.ja_salvos |= novos
            if ponto:
                ponto['u'] = self.total_unicos
                self.arm.salvar([], ponto)
        except Exception as e:
            log(f"ERRO ao salvar ({e}); tento de novo no próximo minuto")
            with self.lock:
                self.pendentes |= novos
            ponto = None
        if ponto:
            self.serie = (self.serie + [ponto])[-MAX_PONTOS:]
            self.ultimo_ponto = ponto['t']
            self.pico_intervalo = 0
        self.respostas = {}

    # ---------- dados públicos ----------
    def resumo(self, periodo):
        agora_ts = time.time()
        inicio = {'24h': agora_ts - 86400, '7d': agora_ts - 7 * 86400}.get(periodo)
        pontos = [p for p in self.serie if inicio is None or p['t'] >= inicio]
        serie_publica = agrupar(pontos, inicio or (pontos[0]['t'] if pontos else agora_ts), agora_ts, periodo)
        pico = max(self.serie, key=lambda p: p['s'], default=None)
        return {
            'unicos': self.total_unicos,
            'pico': {'s': pico['s'], 't': pico['t']} if pico else None,
            'modo': 'minuto' if self.modo_alto() else '10min',
            'limiar_alto': LIMIAR_ALTO,
            'desde': self.serie[0]['t'] if self.serie else None,
            'persistente': self.arm.persistente,
            'atualizado_em': int(agora_ts),
            'serie': serie_publica,
        }


def agrupar(pontos, inicio, fim, periodo):
    """Reduz a série para ~150-300 pontos: pico de simultâneos e último total de únicos por faixa de tempo.
    Faixa sem nenhum ponto por mais de 20 min = servidor dormindo = ninguém acessando (0)."""
    if not pontos:
        return []
    inicio, fim = int(inicio), int(fim)
    passo = {'24h': 600, '7d': 3600}.get(periodo) or max(600, -(-(fim - inicio) // 300) // 600 * 600)
    saida, i, ultimo_u, ultimo_t, ultimo_s = [], 0, 0, None, 0
    t = inicio - (inicio % passo)
    while t <= fim:
        s_max, u = None, None
        while i < len(pontos) and pontos[i]['t'] < t + passo:
            p = pontos[i]
            s_max = p['s'] if s_max is None else max(s_max, p['s'])
            u = p['u']
            ultimo_t, ultimo_s = p['t'], p['s']
            i += 1
        if u is not None:
            ultimo_u = u
        if s_max is None:
            dormindo = ultimo_t is None or (t + passo) - ultimo_t > 1200
            s_max = 0 if dormindo else ultimo_s
        if ultimo_t is not None:
            saida.append([t + passo // 2, s_max, ultimo_u])
        t += passo
    return saida


# ===================== RELATÓRIO =====================
def _barras_html(linhas, cor, sufixo=''):
    maximo = max((v for _, v, _ in linhas), default=0) or 1
    html = []
    for rotulo, valor, extra in linhas:
        largura = max(1, round(260 * valor / maximo)) if valor else 1
        html.append(
            f'<tr><td style="padding:2px 8px 2px 0;font-size:12px;color:#65676b;white-space:nowrap">{rotulo}</td>'
            f'<td style="padding:2px 0"><table cellpadding="0" cellspacing="0"><tr>'
            f'<td width="{largura}" height="14" bgcolor="{cor}" style="background:{cor};border-radius:3px"></td>'
            f'<td style="padding-left:6px;font-size:12px;color:#1c1e21;white-space:nowrap"><b>{valor:,}</b>{sufixo}{extra}</td>'
            f'</tr></table></td></tr>'.replace(',', '.'))
    return '<table cellpadding="0" cellspacing="0">' + ''.join(html) + '</table>'


def montar_relatorio(contador):
    agora_ts = time.time()
    agora = datetime.fromtimestamp(agora_ts, BRT)
    serie = contador.serie
    fmt = lambda n: f'{n:,}'.replace(',', '.')
    quando = lambda t: datetime.fromtimestamp(t, BRT).strftime('%d/%m %H:%M')

    def unicos_em(ts):
        anteriores = [p['u'] for p in serie if p['t'] <= ts]
        return anteriores[-1] if anteriores else 0

    ult24 = [p for p in serie if p['t'] >= agora_ts - 86400]
    pico24 = max(ult24, key=lambda p: p['s'], default=None)
    pico_total = max(serie, key=lambda p: p['s'], default=None)
    novos24 = contador.total_unicos - unicos_em(agora_ts - 86400)

    # Gráfico 1: únicos acumulados no fim de cada dia (14 dias)
    dias = []
    for d in range(13, -1, -1):
        dia = (agora - timedelta(days=d)).date()
        fim_dia = datetime(dia.year, dia.month, dia.day, 23, 59, 59, tzinfo=BRT).timestamp()
        acumulado = contador.total_unicos if d == 0 else unicos_em(fim_dia)
        anterior = unicos_em(fim_dia - 86400)
        if acumulado or dias:
            dias.append((dia.strftime('%d/%m'), acumulado, f' <span style="color:#28a745">(+{fmt(acumulado - anterior)})</span>'))
    # Gráfico 2: pico de simultâneos por hora (24h)
    horas = []
    for h in range(23, -1, -1):
        ini = agora_ts - (h + 1) * 3600
        bloco = [p['s'] for p in serie if ini <= p['t'] < ini + 3600]
        horas.append((datetime.fromtimestamp(ini + 3600, BRT).strftime('%Hh'), max(bloco, default=0), ''))

    card = lambda titulo, valor, sub='': (
        f'<td style="padding:10px 14px;background:#fff;border-left:4px solid #0056b3;border-radius:8px">'
        f'<div style="font-size:11px;color:#65676b;text-transform:uppercase;font-weight:600">{titulo}</div>'
        f'<div style="font-size:22px;font-weight:700;color:#050505">{valor}</div>'
        f'<div style="font-size:11px;color:#65676b">{sub}</div></td>')

    html = f"""<div style="font-family:Segoe UI,Arial,sans-serif;background:#f0f2f5;padding:18px;color:#1c1e21">
<h2 style="margin:0 0 4px">Relatório de acessos</h2>
<div style="font-size:13px;color:#65676b;margin-bottom:14px">{agora.strftime('%d/%m/%Y %H:%M')} (horário de Brasília)</div>
<table cellspacing="8" cellpadding="0" style="margin-left:-8px"><tr>
{card('Visitantes únicos', fmt(contador.total_unicos), f'+{fmt(novos24)} nas últimas 24h')}
{card('Pico 24h', fmt(pico24['s']) if pico24 else '0', quando(pico24['t']) if pico24 and pico24['s'] else 'sem acessos')}
{card('Maior pico', fmt(pico_total['s']) if pico_total else '0', quando(pico_total['t']) if pico_total and pico_total['s'] else '')}
</tr></table>
<div style="background:#fff;border-radius:10px;padding:14px 16px;margin-top:6px">
<h3 style="margin:0 0 8px;font-size:14px;color:#0056b3">Visitantes únicos acumulados (por dia)</h3>
{_barras_html(dias, '#0056b3') if dias else '<i style="font-size:12px">Ainda sem dados.</i>'}
</div>
<div style="background:#fff;border-radius:10px;padding:14px 16px;margin-top:12px">
<h3 style="margin:0 0 8px;font-size:14px;color:#0056b3">Pico de pessoas acessando ao mesmo tempo (últimas 24h, por hora)</h3>
{_barras_html(horas, '#28a745')}
</div>
<p style="font-size:12px;color:#65676b;margin-top:14px">Gráficos em tempo real: <a href="{SITE_URL}/#acessos">{SITE_URL}/#acessos</a><br>
A série completa vai em anexo (CSV). IPs não são armazenados: cada um vira um código anônimo só para contar únicos.</p>
</div>"""

    buffer = io.StringIO()
    escritor = csv.writer(buffer, delimiter=';')
    escritor.writerow(['data_hora_brasilia', 'pico_simultaneos', 'unicos_acumulados', 'intervalo_min'])
    for p in serie:
        escritor.writerow([datetime.fromtimestamp(p['t'], BRT).strftime('%d/%m/%Y %H:%M'), p['s'], p['u'], p['i']])
    assunto = (f"Acessos ao site: {fmt(contador.total_unicos)} únicos, pico de "
               f"{fmt(pico24['s']) if pico24 else 0} simultâneos em 24h")
    return assunto, html, buffer.getvalue()


def enviar_relatorio(contador):
    """True se enviou. Sem RESEND_API_KEY, só grava relatorio_acessos.html (útil no teste local)."""
    assunto, html, csv_txt = montar_relatorio(contador)
    chave, para = os.environ.get('RESEND_API_KEY'), os.environ.get('RELATORIO_EMAIL_PARA')
    if not (chave and para):
        caminho = os.path.join(os.path.dirname(ARQUIVO_LOCAL), 'relatorio_acessos.html')
        with open(caminho, 'w', encoding='utf-8') as f:
            f.write(html)
        log(f"e-mail não configurado: relatório salvo em {caminho}")
        return False
    r = requests.post(RESEND_URL, timeout=20, headers={'Authorization': f'Bearer {chave}'}, json={
        'from': os.environ.get('RELATORIO_EMAIL_DE', 'Eleições em tempo real <onboarding@resend.dev>'),
        'to': [e.strip() for e in para.split(',')],
        'subject': assunto,
        'html': html,
        'attachments': [{'filename': f"acessos_{datetime.now(BRT):%Y-%m-%d}.csv",
                         'content': base64.b64encode(csv_txt.encode('utf-8-sig')).decode()}],
    })
    if r.status_code >= 300:
        raise RuntimeError(f"Resend respondeu {r.status_code}: {r.text[:200]}")
    log(f"relatório enviado para {para}")
    return True


def relatorio_pendente(contador):
    agora = datetime.fromtimestamp(time.time(), BRT)
    hoje = agora.strftime('%Y-%m-%d')
    return agora.hour >= RELATORIO_HORA and (contador.ultimo_relatorio or '') < hoje


# ===================== INTEGRAÇÃO COM O FLASK =====================
def ip_do_cliente(request):
    for cab in ('CF-Connecting-IP', 'True-Client-IP'):
        if request.headers.get(cab):
            return request.headers[cab].strip()
    xff = request.headers.get('X-Forwarded-For')
    if xff:
        return xff.split(',')[0].strip()
    return request.remote_addr or 'desconhecido'


def iniciar(app, responder, comprimir):
    from flask import Response, jsonify, request

    contador = Contador(criar_armazenamento())

    def laco():
        while True:
            time.sleep(60)
            try:
                contador.tique()
                if relatorio_pendente(contador) and os.environ.get('RESEND_API_KEY'):
                    if enviar_relatorio(contador):
                        hoje = datetime.now(BRT).strftime('%Y-%m-%d')
                        contador.ultimo_relatorio = hoje
                        contador.arm.marcar_relatorio(hoje)
            except Exception as e:
                log(f"ERRO no ciclo de acessos: {e}")

    threading.Thread(target=laco, daemon=True).start()

    @app.route('/api/presenca', methods=['POST'])
    def api_presenca():
        contador.registrar(ip_do_cliente(request))
        # len(ativos) inclui quem acabou de chegar; saídas são limpas a cada minuto
        resp = jsonify({'agora': len(contador.ativos)})
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/api/acessos')
    def api_acessos():
        periodo = request.args.get('periodo', '24h')
        if periodo not in ('24h', '7d', 'tudo'):
            periodo = '24h'
        texto = contador.respostas.get(periodo)
        if texto is None:   # gráficos: calculados uma vez por minuto
            texto = json.dumps(contador.resumo(periodo), ensure_ascii=False, separators=(',', ':'))
            contador.respostas[periodo] = texto
        # "agora" sempre ao vivo (o mesmo número do contador na aba)
        corpo = '{"agora":%d,%s' % (len(contador.ativos), texto[1:])
        return responder(gzip.compress(corpo.encode('utf-8'), compresslevel=5))

    @app.route('/api/acessos/relatorio')
    def api_relatorio():
        chave = os.environ.get('RELATORIO_CHAVE')
        if not chave or not hmac.compare_digest(request.args.get('chave', ''), chave):
            return Response('Não encontrado', status=404)
        if request.args.get('preview'):
            return Response(montar_relatorio(contador)[1], mimetype='text/html')
        try:
            enviado = enviar_relatorio(contador)
            return {'enviado': enviado, 'para': os.environ.get('RELATORIO_EMAIL_PARA')}
        except Exception as e:
            return {'enviado': False, 'erro': str(e)}, 502

    return contador
