"""
Coletor da apuração ao vivo: a cada 5 minutos baixa do TSE os resultados de TODOS os níveis
(Brasil, 27 UFs, 5.570 municípios e as zonas eleitorais de cada município) e grava no Upstash.
O servidor no Render só LÊ do Upstash: nenhum clique de visitante gera consulta ao TSE.

Rode no SEU computador durante a apuração, dentro da pasta do projeto:
    caffeinate -i python coletor_ao_vivo.py 2026 2

("caffeinate -i" impede o Mac de dormir enquanto o coletor roda.)
Precisa das mesmas variáveis do Upstash que estão no Render:
    export UPSTASH_REDIS_REST_URL="https://....upstash.io"
    export UPSTASH_REDIS_REST_TOKEN="...."

Ele para sozinho quando Brasil e todas as UFs chegarem a 100%. Depois rode congelar_ao_vivo.py.

Economia:
- Pede ao TSE só o que mudou (If-None-Match / If-Modified-Since): arquivo igual volta "304" sem corpo.
- No Upstash, a "foto" atual de cada UF é sobrescrita a cada ciclo (tamanho constante, ~2-3 MB no total)
  e o histórico guarda só Brasil e UFs (1 ponto a cada 5 min, ~150 bytes cada).
"""
import base64
import gzip
import json
import math
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

from acessos import ArmazenamentoUpstash
from congelar_ao_vivo import CARGOS, ESTADOS, TSE_BASE, descobrir_codigos, normalizar, processar

BRT = timezone(timedelta(hours=-3))
INTERVALO = int(os.environ.get('COLETOR_INTERVALO_SEG', 300))
WORKERS = int(os.environ.get('COLETOR_WORKERS', 8))
RPS_INICIAL = float(os.environ.get('COLETOR_RPS', 10))       # frente rápida (cards): pedidos por segundo ao TSE
RPS_MAXIMO = float(os.environ.get('COLETOR_RPS_MAX', 20))
RPS_COMPLETO = float(os.environ.get('COLETOR_RPS_COMPLETO', 4))   # varredura das cidades: bem mais devagar
RPS_COMPLETO_MAX = float(os.environ.get('COLETOR_RPS_COMPLETO_MAX', 8))
TOLERANCIA_FALHAS_UF = 0.05   # UF com mais de 5% de falhas não é regravada (mantém a versão anterior)
MAX_LOTE_BYTES = 700_000        # cada chamada ao Upstash fica bem abaixo do limite de 1 MB
EXPIRA_SEG = 60 * 86400         # tudo expira sozinho em 60 dias (não acumula lixo)
MAX_PONTOS_HISTORICO = 2000


def chave(ano, turno, *partes):
    return ':'.join(['ao_vivo', str(ano), str(turno), *partes])


def empacotar(obj):
    """JSON -> gzip -> base64 (o Upstash REST trabalha com texto)."""
    bruto = json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return base64.b64encode(gzip.compress(bruto, compresslevel=6)).decode('ascii')


# ===================== DOWNLOAD COM CACHE CONDICIONAL =====================
sessao = requests.Session()
sessao.mount('https://', requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS))
sessao.mount('http://', requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS))
cache_http = {}      # url -> (etag, last_modified, resultado_processado)


class Limitador:
    """Limita os pedidos ao TSE por segundo (todas as threads juntas). Em 429, pausa todo mundo e reduz a velocidade."""

    def __init__(self, rps, maximo, nome):
        self.rps, self.maximo, self.nome = rps, maximo, nome
        self.proximo = time.monotonic()
        self.pausa_ate = 0.0
        self.recusas = 0
        self.lock = threading.Lock()

    def esperar(self):
        with self.lock:
            inicio = max(time.monotonic(), self.proximo, self.pausa_ate)
            self.proximo = inicio + 1.0 / self.rps
        espera = inicio - time.monotonic()
        if espera > 0:
            time.sleep(espera)

    def recusado(self, segundos):
        with self.lock:
            self.recusas += 1
            novo = time.monotonic() + segundos
            if novo > self.pausa_ate + 1:   # um aviso e uma redução por rajada de recusas, não por pedido
                self.rps = max(2.0, self.rps / 2)
                print(f"  ⚠ TSE pediu para ir mais devagar (429, {self.nome}): pausa de {segundos}s, agora {self.rps:.0f} pedidos/s", flush=True)
            self.pausa_ate = max(self.pausa_ate, novo)

    def fim_de_ciclo(self):
        with self.lock:
            if self.recusas == 0 and self.rps < self.maximo:
                self.rps = min(self.maximo, self.rps * 1.25)   # ciclo limpo: acelera aos poucos
            self.recusas = 0


limitador = Limitador(RPS_INICIAL, RPS_MAXIMO, 'cards')
limitador_completo = Limitador(RPS_COMPLETO, RPS_COMPLETO_MAX, 'cidades')


def baixar(url, tentativas=4, lim=None):
    """Devolve (resultado, status) com status 'novo', 'igual', 'ausente' (404) ou 'falha'.
    Em falha devolve o último resultado conhecido, para nunca 'zerar' um município."""
    anterior = cache_http.get(url)
    if anterior and anterior[2] and anterior[2].get('apurado') == '100,00' and '/zz/zz-' not in url \
            and not re.search(r'/(br|[a-z]{2})-c\d{4}-', url):
        return anterior[2], 'concluido'   # município/zona 100% apurado não muda mais: nem pergunta ao TSE
    cab = {'User-Agent': 'Mozilla/5.0 (coletor eleicoes)'}
    if anterior:
        if anterior[0]:
            cab['If-None-Match'] = anterior[0]
        if anterior[1]:
            cab['If-Modified-Since'] = anterior[1]
    lim = lim or limitador
    for t in range(tentativas):
        lim.esperar()
        try:
            r = sessao.get(url, headers=cab, timeout=15)
            if r.status_code == 429:
                try:
                    pausa = min(120, max(5, int(r.headers.get('Retry-After', 30))))
                except ValueError:
                    pausa = 30
                lim.recusado(pausa)
                continue
            if r.status_code == 304 and anterior:
                return anterior[2], 'igual'
            if r.status_code == 200:
                res = processar(r.json())
                cache_http[url] = (r.headers.get('ETag'), r.headers.get('Last-Modified'), res)
                return res, 'novo'
            if r.status_code == 404:
                return None, 'ausente'
        except (requests.RequestException, ValueError):
            pass
        time.sleep(1 + t)
    return (anterior[2] if anterior else None), 'falha'


# ===================== MUNICÍPIOS E ZONAS =====================
def carregar_municipios(ano, cod):
    """{UF: [{'codigo', 'ibge', 'nome', 'zonas': ['0001', ...]}]} a partir do config do TSE."""
    for tipo in ('fed', 'est'):
        url = f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/config/mun-e{int(cod[tipo]):06d}-cm.json"
        try:
            r = sessao.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                cfg = r.json()
                break
        except requests.RequestException:
            pass
    else:
        sys.exit("Não consegui baixar a lista de municípios do TSE.")

    dir_geo = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'municipios')
    municipios = {}
    for abr in cfg.get('abr', []):
        uf = abr.get('cd', '').upper()
        if uf == 'ZZ':   # exterior: cidades do mundo (só presidente)
            municipios['ZZ'] = [{'codigo': str(m['cd']).zfill(5), 'nome': m.get('nm', '').upper()} for m in abr.get('mu', [])]
            continue
        if uf not in ESTADOS:
            continue
        nomes_geo = {}
        caminho = os.path.join(dir_geo, f'{uf}.json')
        if os.path.exists(caminho):
            with open(caminho, encoding='utf-8') as f:
                nomes_geo = {normalizar(ft['properties']['name']): str(ft['properties']['id'])
                             for ft in json.load(f)['features']}
        lista = []
        for m in abr.get('mu', []):
            zonas = []
            for z in m.get('z', []) or []:
                cz = z.get('cd') if isinstance(z, dict) else z
                if cz not in (None, ''):
                    zonas.append(str(cz).zfill(4))
            lista.append({'codigo': str(m['cd']).zfill(5),
                          'ibge': str(m.get('cdi') or '') or nomes_geo.get(normalizar(m.get('nm', ''))),
                          'nome': m.get('nm', '').upper(), 'zonas': sorted(set(zonas))})
        municipios[uf] = lista
    n_mun = sum(len(v) for k, v in municipios.items() if k != 'ZZ')
    n_zon = sum(len(m['zonas']) for k, v in municipios.items() if k != 'ZZ' for m in v)
    n = lambda x: f"{x:,}".replace(',', '.')
    print(f"  {n(n_mun)} municípios, {n(n_zon)} pares município/zona e {len(municipios.get('ZZ', []))} cidades no exterior")
    return municipios


# ===================== EXTERIOR =====================
_tabela_exterior = None


def tabela_exterior():
    global _tabela_exterior
    if _tabela_exterior is None:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'exterior.json')
        with open(caminho, encoding='utf-8') as f:
            _tabela_exterior = json.load(f)
    return _tabela_exterior


def consolidar_exterior(res, cidades):
    """Soma as cidades do exterior por país. % apurado do país = média dos % das suas cidades."""
    tab = tabela_exterior()
    paises = {}
    for m in cidades:
        iso = tab['locais'].get(m['codigo'], 'OUTROS')
        info = tab['paises'].get(iso, {'nome': 'Outros locais', 'continente': 'Outros'})
        p = paises.setdefault(iso, {'nome': info['nome'], 'continente': info['continente'], 'votos': {}, 'partido': {},
                                    'apurados': [], 'cidades': [], 'cidades_total': 0, 'cidades_concluidas': 0})
        p['cidades_total'] += 1
        r = res.get(('ZZ', m['codigo'], 'presidente'))
        if not r:
            continue
        p['apurados'].append(float(r['apurado'].replace(',', '.') or 0))
        p['cidades_concluidas'] += r['apurado'] == '100,00'
        for c in r['candidatos']:
            p['votos'][c['nome']] = p['votos'].get(c['nome'], 0) + int(c['votos'].replace('.', '') or 0)
            p['partido'][c['nome']] = c['partido']
        p['cidades'].append({'nome': m['nome'], 'apurado': r['apurado'], 'candidatos': r['candidatos'][:2]})
    saida = {}
    for iso, p in paises.items():
        total = sum(p['votos'].values())
        if not p['apurados']:
            continue
        cands = sorted(p['votos'].items(), key=lambda kv: kv[1], reverse=True)
        saida[iso] = {
            'nome': p['nome'], 'continente': p['continente'],
            'apurado': f"{sum(p['apurados']) / len(p['apurados']):.2f}".replace('.', ','),
            'cidades_total': p['cidades_total'], 'cidades_concluidas': p['cidades_concluidas'],
            'candidatos': [{'nome': n, 'partido': p['partido'][n], 'votos': f"{v:,}".replace(',', '.'),
                            'percentual': f"{100 * v / total if total else 0:.2f}".replace('.', ','), 'eleito': False}
                           for n, v in cands[:5]],
            'cidades': sorted(p['cidades'], key=lambda c: c['nome']),
        }
    return {'total': res.get(('ZZ', 'presidente'), {'apurado': '0,00', 'candidatos': []}), 'paises': saida}


# ===================== UM CICLO =====================
def ciclo(ano, turno, cod, municipios, banco, ufs_governador, completo=True, publicar_resumo=True):
    """completo=False (modo leve, para o Render gratuito): sem zonas e sem governador por município."""
    inicio = time.time()

    def url(abr, cargo, uf):
        tipo, c = CARGOS[cargo]
        return f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/dados/{uf.lower()}/{abr}-c{c:04d}-e{int(cod[tipo]):06d}-u.json"

    def cargo_gov_ou_dep(cargo, uf):
        return 'dep_distrital' if cargo == 'dep_estadual' and uf == 'DF' else cargo

    cargos_mun = ['presidente'] + (['governador'] if turno == 2 else ['governador', 'senador'])

    res, contagem = {}, {'novo': 0, 'igual': 0, 'ausente': 0, 'falha': 0, 'concluido': 0}

    lim = limitador if publicar_resumo else limitador_completo
    tarefas_uf, falhas_uf = {}, {}

    def executar(tarefas, rotulo):
        feitas, ultimo_aviso = 0, time.time()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for (k, _), (r, st) in zip(tarefas, ex.map(lambda t: baixar(t[1], lim=lim), tarefas)):
                contagem[st] += 1
                feitas += 1
                tarefas_uf[k[0]] = tarefas_uf.get(k[0], 0) + 1
                if st == 'falha' and not r:
                    falhas_uf[k[0]] = falhas_uf.get(k[0], 0) + 1
                if r:
                    res[k] = r
                if time.time() - ultimo_aviso > 20:   # progresso a cada 20s, para nunca parecer travado
                    ultimo_aviso = time.time()
                    n = lambda x: f"{x:,}".replace(',', '.')
                    print(f"    … {rotulo}: {n(feitas)}/{n(len(tarefas))} arquivos ({lim.rps:.0f} pedidos/s)", flush=True)

    # Fase 1: Brasil e UFs (inclui governador da UF, para saber onde há 2º turno de governador)
    fase1 = [(('br', 'presidente'), url('br', 'presidente', 'br'))]
    for uf in ESTADOS:
        fase1.append(((uf, 'presidente'), url(uf.lower(), 'presidente', uf)))
        fase1.append(((uf, 'governador'), url(uf.lower(), 'governador', uf)))
    fase1.append((('ZZ', 'presidente'), url('zz', 'presidente', 'zz')))
    executar(fase1, 'Brasil e UFs')
    for uf in ESTADOS:
        if res.get((uf, 'governador'), {}).get('candidatos'):
            ufs_governador.add(uf)

    # Fase 2: municípios e zonas
    fase2 = []
    for uf in ESTADOS:
        u = uf.lower()
        for m in municipios.get(uf, []):
            for cargo in cargos_mun:
                if cargo != 'presidente' and (not completo or (cargo == 'governador' and uf not in ufs_governador)):
                    continue
                fase2.append(((uf, m['codigo'], cargo), url(f"{u}{m['codigo']}", cargo_gov_ou_dep(cargo, uf), uf)))
                for z in (m['zonas'] if completo and len(m['zonas']) > 1 else []):   # cidade de 1 zona: a zona é a própria cidade
                    fase2.append(((uf, m['codigo'], z, cargo), url(f"{u}{m['codigo']}-z{z}", cargo, uf)))
    for m in municipios.get('ZZ', []):   # ~186 cidades: entra também no modo leve
        fase2.append((('ZZ', m['codigo'], 'presidente'), url(f"zz{m['codigo']}", 'presidente', 'zz')))
    executar(fase2, 'municípios, zonas e exterior')
    tarefas = fase1 + fase2

    agora_brt = datetime.now(BRT)
    brasil = res.get(('br', 'presidente'), {'apurado': '0,00', 'candidatos': []})
    estados = {uf: res[(uf, 'presidente')] for uf in ESTADOS if (uf, 'presidente') in res}
    finalizado = brasil.get('apurado') == '100,00' and all(estados.get(uf, {}).get('apurado') == '100,00' for uf in ESTADOS)
    resumo = {'ultima_atualizacao': agora_brt.strftime('%H:%M:%S'), 'gerado_em': int(time.time()),
              'finalizado': finalizado, 'brasil': brasil, 'estados': estados, 'mapa_municipal': True}

    # --- foto de cada UF
    fotos = {}
    for uf in ESTADOS:
        mapa, det_mun, zonas = {}, {}, {}
        for m in municipios.get(uf, []):
            cargos = {c: res[(uf, m['codigo'], c)] for c in cargos_mun if (uf, m['codigo'], c) in res}
            det_mun[m['codigo']] = {'cargos': cargos}
            p = cargos.get('presidente')
            if p and m['ibge']:
                mapa[m['ibge']] = {'tse': m['codigo'], 'nome': m['nome'], 'apurado': p['apurado'], 'candidatos': p['candidatos'][:2]}
            zs = {}
            for z in (m['zonas'] if len(m['zonas']) > 1 else []):
                cz = {c: res[(uf, m['codigo'], z, c)] for c in cargos_mun if (uf, m['codigo'], z, c) in res}
                if cz:
                    zs[z] = {'cargos': cz}
            if zs:
                zonas[m['codigo']] = zs
        estado = {'governador': res[(uf, 'governador')]} if uf in ufs_governador else {}
        fotos[uf] = {'mapa': mapa, 'estado': {'cargos': estado}, 'municipios': det_mun, 'zonas': zonas}

    # --- exterior consolidado por país (o TSE publica por cidade; o país vem de static/exterior.json)
    exterior = consolidar_exterior(res, municipios.get('ZZ', []))
    resumo['exterior_disponivel'] = bool(exterior['paises'])

    # --- histórico (Brasil + UFs + exterior): [hora, % apurado, [[nome, partido, votos, %], ...]]
    def ponto(r):
        return [int(time.time()), r.get('apurado', '0,00'),
                [[c['nome'], c['partido'], int(c['votos'].replace('.', '') or 0), c['percentual']] for c in r.get('candidatos', [])[:5]]]

    # --- gravação no Upstash, em lotes
    estado_exterior['disponivel'] = resumo['exterior_disponivel']
    comandos = [['SET', chave(ano, turno, 'resumo'), empacotar(resumo), 'EX', EXPIRA_SEG]] if publicar_resumo else []
    def uf_confiavel(uf):
        return falhas_uf.get(uf, 0) <= TOLERANCIA_FALHAS_UF * max(1, tarefas_uf.get(uf, 0))
    puladas = [uf for uf in fotos if not uf_confiavel(uf)]
    if puladas:
        print(f"  ⚠ muitas falhas em {', '.join(puladas)}: mantida a versão anterior dessas UFs", flush=True)
    comandos += [['SET', chave(ano, turno, 'uf', uf), empacotar(f), 'EX', EXPIRA_SEG] for uf, f in fotos.items() if uf_confiavel(uf)]
    if exterior['total'].get('candidatos') and uf_confiavel('ZZ'):
        comandos.append(['SET', chave(ano, turno, 'exterior'), empacotar(exterior), 'EX', EXPIRA_SEG])
    serie = [('BR', brasil)] + list(estados.items()) + ([('ZZ', exterior['total'])] if exterior['total'].get('candidatos') else [])
    for abr, r in (serie if publicar_resumo else []):
        k = chave(ano, turno, 'hist', abr)
        comandos += [['RPUSH', k, json.dumps(ponto(r), ensure_ascii=False, separators=(',', ':'))],
                     ['LTRIM', k, -MAX_PONTOS_HISTORICO, -1], ['EXPIRE', k, EXPIRA_SEG]]
    comandos.append(['SET', chave(ano, turno, 'versao'), str(int(time.time())), 'EX', EXPIRA_SEG])  # por último: avisa o Render

    lote, tamanho = [], 0
    for cmd in comandos:
        t = sum(len(str(x)) for x in cmd)
        if lote and tamanho + t > MAX_LOTE_BYTES:
            banco._pipeline(lote)
            lote, tamanho = [], 0
        lote.append(cmd)
        tamanho += t
    if lote:
        banco._pipeline(lote)

    n = lambda x: f"{x:,}".replace(',', '.')
    lideres = ' x '.join(f"{c['nome']} {c['percentual']}%" for c in brasil.get('candidatos', [])[:2]) or 'sem votos'
    print(f"  {agora_brt:%H:%M:%S} | Brasil {brasil.get('apurado')}% apurado | {lideres} | "
          f"{n(len(tarefas))} arquivos ({n(contagem['igual'])} sem mudança, {n(contagem['concluido'])} já em 100%, "
          f"{contagem['falha']} falhas) em {time.time() - inicio:.0f}s", flush=True)
    lim.fim_de_ciclo()
    return finalizado


estado_exterior = {'disponivel': False}
ultimo_bom = {}   # (abrangência, cargo) -> último resultado com candidatos lido com sucesso


def semear_ultimo_bom(ano, turno, banco):
    """Ao iniciar, recupera do Upstash o último resumo com votos, para nunca começar publicando zeros."""
    try:
        bruto = banco._pipeline([['GET', chave(ano, turno, 'resumo')]])[0]
        if not bruto:
            return
        r = json.loads(gzip.decompress(base64.b64decode(bruto)))
        if r.get('brasil', {}).get('candidatos'):
            ultimo_bom[('br', 'presidente')] = r['brasil']
        for uf, v in (r.get('estados') or {}).items():
            if v.get('candidatos'):
                ultimo_bom[(uf, 'presidente')] = v
    except Exception as e:
        print(f"  [aviso] não consegui recuperar o último resumo: {e}", flush=True)


ufs_governador_rapido = None   # UFs com 2º turno para governador (descobertas na 1ª leitura rápida)


def ciclo_rapido(ano, turno, cod, banco):
    """Brasil, UFs (presidente e governador) e total do exterior: grava os cards, o painel de cada estado e um ponto do gráfico."""
    global ufs_governador_rapido
    def url(abr, cargo, uf):
        tipo, c = CARGOS[cargo]
        return f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/dados/{uf.lower()}/{abr}-c{c:04d}-e{int(cod[tipo]):06d}-u.json"   # o TSE só aceita minúsculas
    tarefas = [(('br', 'presidente'), url('br', 'presidente', 'br')), (('ZZ', 'presidente'), url('zz', 'presidente', 'zz'))]
    tarefas += [((uf, 'presidente'), url(uf.lower(), 'presidente', uf)) for uf in ESTADOS]
    if turno == 2:   # governador: na 1ª leitura testa as 27 UFs; depois, só as que têm 2º turno para governador
        ufs_gov = ufs_governador_rapido if ufs_governador_rapido is not None else ESTADOS
        tarefas += [((uf, 'governador'), url(uf.lower(), 'governador', uf)) for uf in ufs_gov]
    if not ultimo_bom:
        semear_ultimo_bom(ano, turno, banco)
    res, frescos, falhas, falhou = {}, set(), 0, set()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for (k, _), (r, _st) in zip(tarefas, ex.map(lambda t: baixar(t[1]), tarefas)):
            if _st == 'falha' and not r:
                falhou.add(k)
            if r:
                res[k] = r
                frescos.add(k)
                if r.get('candidatos'):
                    ultimo_bom[k] = r
            else:
                if _st == 'falha':
                    falhas += 1   # arquivo que não existe (ex.: governador em UF sem 2º turno) não é falha
                if k in ultimo_bom:
                    res[k] = ultimo_bom[k]   # TSE falhou agora: mantém o último valor bom, nunca zera
    if ('br', 'presidente') not in res:
        print(f"  {datetime.now(BRT):%H:%M:%S} ⚡ TSE não respondeu ({falhas} falhas): nada publicado, o site mantém os últimos dados", flush=True)
        limitador.fim_de_ciclo()
        return False
    brasil = res[('br', 'presidente')]
    if turno == 2 and ufs_governador_rapido is None and not any(c == 'governador' for _, c in falhou):
        ufs_governador_rapido = sorted(uf for uf in ESTADOS if res.get((uf, 'governador'), {}).get('candidatos') is not None
                                       and (uf, 'governador') in frescos)
    estadual = {}
    for uf in ESTADOS:
        cargos = {c: res[(uf, c)] for c in ('presidente', 'governador') if (uf, c) in res}
        if cargos:
            estadual[uf] = cargos
    estados = {uf: res[(uf, 'presidente')] for uf in ESTADOS if (uf, 'presidente') in res}
    exterior = res.get(('ZZ', 'presidente'))
    finalizado = brasil.get('apurado') == '100,00' and all(estados.get(uf, {}).get('apurado') == '100,00' for uf in ESTADOS)
    resumo = {'ultima_atualizacao': datetime.now(BRT).strftime('%H:%M:%S'), 'gerado_em': int(time.time()), 'finalizado': finalizado,
              'brasil': brasil, 'estados': estados, 'mapa_municipal': True, 'exterior_disponivel': estado_exterior['disponivel']}

    def ponto(r):
        return [int(time.time()), r.get('apurado', '0,00'),
                [[c['nome'], c['partido'], int(c['votos'].replace('.', '') or 0), c['percentual']] for c in r.get('candidatos', [])[:5]]]
    comandos = [['SET', chave(ano, turno, 'resumo'), empacotar(resumo), 'EX', EXPIRA_SEG],
                ['SET', chave(ano, turno, 'estadual'), empacotar(estadual), 'EX', EXPIRA_SEG]]
    serie = [('BR', brasil)] + list(estados.items()) + ([('ZZ', exterior)] if exterior and exterior.get('candidatos') else [])
    chaves_tse = {'BR': ('br', 'presidente'), 'ZZ': ('ZZ', 'presidente')}
    for abr, r in serie:
        if chaves_tse.get(abr, (abr, 'presidente')) not in frescos:
            continue   # ponto repetido do último valor bom não entra no gráfico
        k = chave(ano, turno, 'hist', abr)
        comandos += [['RPUSH', k, json.dumps(ponto(r), ensure_ascii=False, separators=(',', ':'))],
                     ['LTRIM', k, -MAX_PONTOS_HISTORICO, -1], ['EXPIRE', k, EXPIRA_SEG]]
    comandos.append(['SET', chave(ano, turno, 'versao'), str(int(time.time())), 'EX', EXPIRA_SEG])
    banco._pipeline(comandos)
    lideres = ' x '.join(f"{c['nome']} {c['percentual']}%" for c in brasil.get('candidatos', [])[:2]) or 'sem votos'
    aviso = f" | {falhas} falhas (mantido o último valor)" if falhas else ''
    print(f"  {datetime.now(BRT):%H:%M:%S} ⚡ Brasil {brasil.get('apurado')}% apurado | {lideres}{aviso}", flush=True)
    limitador.fim_de_ciclo()
    return finalizado


def main(ano, turno):
    url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
    if not (url and token):
        sys.exit("Defina UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN (os mesmos do Render).")
    banco = ArmazenamentoUpstash(url, token)
    print(f"\n=== Coletor ao vivo {ano} – {turno}º turno (a cada {INTERVALO}s, até {RPS_INICIAL:.0f}-{RPS_MAXIMO:.0f} pedidos/s ao TSE) ===")
    cod = descobrir_codigos(ano, turno)
    municipios = carregar_municipios(ano, cod)
    ufs_governador = set()
    RAPIDO = int(os.environ.get('COLETOR_RAPIDO_SEG', 60))
    parar = threading.Event()

    def frente_completa():
        while not parar.is_set():
            inicio = time.time()
            try:
                ciclo(ano, turno, cod, municipios, banco, ufs_governador, publicar_resumo=False)
            except Exception as e:
                print(f"  ✘ erro no ciclo completo: {e} (tento de novo no próximo)", flush=True)
            parar.wait(max(5, INTERVALO - (time.time() - inicio)))   # ciclo longo: emenda; curto: espera o intervalo

    print(f"  ⚡ Brasil, UFs e exterior a cada {RAPIDO}s | 🗺️ municípios, zonas e cidades do exterior em seguida, no ritmo do TSE", flush=True)
    threading.Thread(target=frente_completa, daemon=True).start()
    while True:
        try:
            if ciclo_rapido(ano, turno, cod, banco):
                print("\n  ✔ Apuração encerrada (100% em todas as UFs). Fazendo uma última leitura completa...", flush=True)
                parar.set()
                ciclo(ano, turno, cod, municipios, banco, ufs_governador)
                print(f"  ✔ Coletor parado. Agora rode: python congelar_ao_vivo.py {ano} {turno}  e faça o git push.", flush=True)
                return
        except Exception as e:
            print(f"  ✘ erro na leitura rápida: {e}", flush=True)
        time.sleep(max(5, RAPIDO - (time.time() % RAPIDO)))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(int(sys.argv[1]), int(sys.argv[2]))
