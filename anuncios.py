"""
"Anuncie aqui": recebe interessados em espaço publicitário no site.

- POST /api/anuncie                 formulário do site (nome, marca, contato, investimento, mensagem)
- GET  /admin/anuncios?chave=...    lista de interessados (protegida pela mesma senha do relatório)
- GET  /admin/anuncios.csv?chave=.. planilha para abrir no Excel/Sheets

Onde fica guardado: Upstash (lista "anuncios:interessados") ou, sem Upstash, o arquivo dados_anuncios.json.
Aviso por e-mail a cada interessado: via Resend (mesmas variáveis do relatório de acessos).
Senha da página: ANUNCIOS_CHAVE, ou RELATORIO_CHAVE se a primeira não existir.
"""
import csv
import hmac
import html
import io
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

BRT = timezone(timedelta(hours=-3))
ARQUIVO_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dados_anuncios.json')
CHAVE_LISTA = 'anuncios:interessados'
FAIXAS = ['Até R$ 500', 'R$ 500 a R$ 2.000', 'R$ 2.000 a R$ 5.000', 'Acima de R$ 5.000', 'Prefiro conversar']
LIMITE_POR_IP_HORA = 3
LIMITE_POR_DIA = 200          # teto geral contra ataques
RESEND_URL = os.environ.get('RESEND_API_URL', 'https://api.resend.com/emails')


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [anuncios] {msg}", flush=True)


# ===================== ARMAZENAMENTO =====================
class Guarda:
    def __init__(self):
        url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
        self.banco = None
        if url and token:
            from acessos import ArmazenamentoUpstash
            self.banco = ArmazenamentoUpstash(url, token)
        self.lock = threading.Lock()

    def adicionar(self, item):
        texto = json.dumps(item, ensure_ascii=False, separators=(',', ':'))
        if self.banco:
            self.banco._pipeline([['RPUSH', CHAVE_LISTA, texto]])
            return
        with self.lock:
            dados = self._ler_arquivo()
            dados.append(item)
            with open(ARQUIVO_LOCAL, 'w', encoding='utf-8') as f:
                json.dump(dados, f, ensure_ascii=False)

    def listar(self):
        if self.banco:
            return [json.loads(x) for x in (self.banco._pipeline([['LRANGE', CHAVE_LISTA, 0, -1]])[0] or [])]
        with self.lock:
            return self._ler_arquivo()

    def _ler_arquivo(self):
        if not os.path.exists(ARQUIVO_LOCAL):
            return []
        with open(ARQUIVO_LOCAL, encoding='utf-8') as f:
            return json.load(f)


# ===================== VALIDAÇÃO =====================
def limpar(valor, maximo):
    valor = re.sub(r'\s+', ' ', str(valor or '')).strip()
    return valor[:maximo]


def tipo_contato(contato):
    if re.fullmatch(r'[^@\s]+@[^@\s]+\.[^@\s]+', contato):
        return 'email'
    digitos = re.sub(r'\D', '', contato)
    if 10 <= len(digitos) <= 13:
        return 'telefone'
    return None


def link_contato(item):
    """Link pronto para falar com o interessado (e-mail ou WhatsApp)."""
    c = item.get('contato', '')
    if item.get('tipo_contato') == 'email':
        assunto = requests.utils.quote(f"Anúncio no site Eleições em tempo real – {item.get('marca', '')}")
        return f"mailto:{c}?subject={assunto}", 'E-mail'
    digitos = re.sub(r'\D', '', c)
    if len(digitos) in (10, 11):
        digitos = '55' + digitos
    texto = requests.utils.quote(f"Olá, {item.get('nome', '')}! Aqui é do site Eleições em tempo real, sobre o anúncio da {item.get('marca', '')}.")
    return f"https://wa.me/{digitos}?text={texto}", 'WhatsApp'


# ===================== AVISO POR E-MAIL =====================
def avisar_por_email(item):
    chave, para = os.environ.get('RESEND_API_KEY'), os.environ.get('RELATORIO_EMAIL_PARA')
    if not (chave and para):
        return
    e = {k: html.escape(str(item.get(k, ''))) for k in ('nome', 'marca', 'contato', 'investimento', 'mensagem', 'quando')}
    link, rotulo = link_contato(item)
    corpo = f"""<div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px;color:#1c1e21">
<h2 style="margin:0 0 10px">📣 Novo interessado em anunciar</h2>
<table cellpadding="6" style="border-collapse:collapse">
<tr><td><b>Nome</b></td><td>{e['nome']}</td></tr>
<tr><td><b>Marca</b></td><td>{e['marca']}</td></tr>
<tr><td><b>Contato</b></td><td>{e['contato']} – <a href="{html.escape(link)}">abrir {rotulo}</a></td></tr>
<tr><td><b>Investimento</b></td><td>{e['investimento']}</td></tr>
<tr><td><b>Mensagem</b></td><td>{e['mensagem'] or '—'}</td></tr>
<tr><td><b>Quando</b></td><td>{e['quando']}</td></tr>
</table>
<p style="color:#65676b;font-size:12px">Todos os interessados: {html.escape(os.environ.get('SITE_URL', 'https://eleicoesbrasil-hartmann.onrender.com'))}/admin/anuncios?chave=SUA_SENHA</p></div>"""
    payload = {
        'from': os.environ.get('RELATORIO_EMAIL_DE', 'Eleições em tempo real <onboarding@resend.dev>'),
        'to': [x.strip() for x in para.split(',')],
        'subject': f"📣 Novo anunciante: {item.get('marca')} ({item.get('investimento')})",
        'html': corpo,
    }
    if item.get('tipo_contato') == 'email':
        payload['reply_to'] = item['contato']   # responder o e-mail já fala com o interessado
    try:
        r = requests.post(RESEND_URL, json=payload, timeout=15, headers={'Authorization': f'Bearer {chave}'})
        if r.status_code >= 300:
            log(f"aviso por e-mail falhou: {r.status_code} {r.text[:150]}")
    except Exception as ex:
        log(f"aviso por e-mail falhou: {ex}")


# ===================== PÁGINA DE ADMINISTRAÇÃO =====================
def pagina_admin(itens, chave):
    linhas = []
    for it in reversed(itens):   # mais recentes primeiro
        link, rotulo = link_contato(it)
        e = {k: html.escape(str(it.get(k, ''))) for k in ('quando', 'nome', 'marca', 'contato', 'investimento', 'mensagem')}
        linhas.append(f"<tr><td>{e['quando']}</td><td>{e['nome']}</td><td><b>{e['marca']}</b></td>"
                      f"<td>{e['contato']}<br><a class='bt' href='{html.escape(link)}' target='_blank' rel='noopener'>Falar por {rotulo}</a></td>"
                      f"<td>{e['investimento']}</td><td class='msg'>{e['mensagem'] or '—'}</td></tr>")
    contagem = {f: sum(1 for i in itens if i.get('investimento') == f) for f in FAIXAS}
    resumo = ' · '.join(f"{f}: <b>{n}</b>" for f, n in contagem.items() if n) or 'nenhum ainda'
    return f"""<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Interessados em anunciar</title>
<style>body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f0f2f5;color:#1c1e21;margin:0;padding:18px}}
.caixa{{background:#fff;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,.05);padding:16px;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{text-align:left;padding:9px 10px;border-bottom:1px solid #e4e6eb;vertical-align:top}}
th{{font-size:11px;text-transform:uppercase;color:#65676b}}.msg{{max-width:320px}}
.bt{{display:inline-block;margin-top:5px;background:#0056b3;color:#fff;text-decoration:none;font-weight:700;font-size:12px;padding:5px 10px;border-radius:6px}}
.topo{{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:12px}}</style></head><body>
<div class="topo"><h2 style="margin:0">📣 Interessados em anunciar ({len(itens)})</h2>
<a class="bt" href="/admin/anuncios.csv?chave={html.escape(chave)}">Baixar planilha (CSV)</a></div>
<p style="font-size:13px;color:#65676b">Por faixa de investimento: {resumo}</p>
<div class="caixa"><table><tr><th>Quando</th><th>Nome</th><th>Marca</th><th>Contato</th><th>Investimento</th><th>Mensagem</th></tr>
{''.join(linhas) or '<tr><td colspan="6">Nenhum interessado ainda.</td></tr>'}</table></div>
<p style="font-size:12px;color:#65676b">Dados pessoais (LGPD): use só para falar sobre anúncios e apague quem pedir para sair.</p></body></html>"""


# ===================== ROTAS =====================
def iniciar(app):
    from flask import Response, jsonify, request
    from acessos import ip_do_cliente

    guarda = Guarda()
    envios_por_ip, envios_hoje = {}, {'dia': '', 'n': 0}
    lock = threading.Lock()

    def senha_ok():
        chave = os.environ.get('ANUNCIOS_CHAVE') or os.environ.get('RELATORIO_CHAVE')
        return bool(chave) and hmac.compare_digest(request.args.get('chave', ''), chave)

    @app.route('/api/anuncie', methods=['POST'])
    def api_anuncie():
        d = request.get_json(silent=True) or {}
        if d.get('site'):   # campo invisível: só robôs preenchem
            return jsonify({'ok': True})
        item = {
            'nome': limpar(d.get('nome'), 80),
            'marca': limpar(d.get('marca'), 80),
            'contato': limpar(d.get('contato'), 120),
            'investimento': limpar(d.get('investimento'), 40),
            'mensagem': limpar(d.get('mensagem'), 500),
        }
        erros = []
        if len(item['nome']) < 2: erros.append('Informe seu nome.')
        if len(item['marca']) < 2: erros.append('Informe sua marca ou empresa.')
        item['tipo_contato'] = tipo_contato(item['contato'])
        if not item['tipo_contato']: erros.append('Informe um e-mail ou um telefone com DDD.')
        if item['investimento'] not in FAIXAS: erros.append('Escolha uma faixa de investimento.')
        if d.get('consentimento') is not True: erros.append('É preciso autorizar o contato.')
        if erros:
            return jsonify({'ok': False, 'erros': erros}), 400

        ip, agora = ip_do_cliente(request), time.time()
        hoje = datetime.now(BRT).strftime('%Y-%m-%d')
        with lock:
            recentes = [t for t in envios_por_ip.get(ip, []) if agora - t < 3600]
            if envios_hoje['dia'] != hoje:
                envios_hoje.update(dia=hoje, n=0)
            if len(recentes) >= LIMITE_POR_IP_HORA or envios_hoje['n'] >= LIMITE_POR_DIA:
                return jsonify({'ok': False, 'erros': ['Recebemos muitos envios agora. Tente de novo mais tarde.']}), 429
            envios_por_ip[ip] = recentes + [agora]
            envios_hoje['n'] += 1

        item['quando'] = datetime.now(BRT).strftime('%d/%m/%Y %H:%M')
        item['consentimento'] = True
        try:
            guarda.adicionar(item)
        except Exception as ex:
            log(f"erro ao guardar interessado: {ex}")
            return jsonify({'ok': False, 'erros': ['Não foi possível enviar agora. Tente de novo em instantes.']}), 502
        threading.Thread(target=avisar_por_email, args=(item,), daemon=True).start()
        log(f"novo interessado: {item['marca']} ({item['investimento']})")
        return jsonify({'ok': True})

    @app.route('/admin/anuncios')
    def admin_anuncios():
        if not senha_ok():
            return Response('Não encontrado', status=404)
        resp = Response(pagina_admin(guarda.listar(), request.args.get('chave', '')), mimetype='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/admin/anuncios.csv')
    def admin_anuncios_csv():
        if not senha_ok():
            return Response('Não encontrado', status=404)
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=';')
        w.writerow(['quando', 'nome', 'marca', 'contato', 'tipo_contato', 'investimento', 'mensagem'])
        for it in guarda.listar():
            w.writerow([it.get(k, '') for k in ('quando', 'nome', 'marca', 'contato', 'tipo_contato', 'investimento', 'mensagem')])
        return Response(buf.getvalue().encode('utf-8-sig'), mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment; filename=interessados_anuncios.csv', 'Cache-Control': 'no-store'})
