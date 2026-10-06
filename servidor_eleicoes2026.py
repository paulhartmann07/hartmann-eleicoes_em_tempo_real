import gzip
import json
import os
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from functools import lru_cache

import requests
from flask import Flask, Response, jsonify, request, send_from_directory
from flask_cors import CORS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_DADOS = os.path.join(BASE_DIR, 'dados')
DIR_GEO_MUN = os.path.join(BASE_DIR, 'static', 'municipios')

app = Flask(__name__, static_folder=os.path.join(BASE_DIR, 'static'), static_url_path='/static')
CORS(app)

# Permite apontar para um TSE simulado em testes locais (ex.: TSE_BASE=http://localhost:9000/oficial)
TSE_BASE = os.environ.get('TSE_BASE', 'https://resultados.tse.jus.br/oficial')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Cache-Control': 'no-cache, no-store, must-revalidate'
}

ESTADOS = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
           'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
           'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

# ===================== CATÁLOGO DE ELEIÇÕES =====================
# Anos passados vêm de dados/{ano}/t{turno}/ (gerados por gerar_historico.py).
# Anos com 'ao_vivo' consultam o TSE em tempo real enquanto não houver arquivos estáticos.
ELEICOES = {
    2018: {'tipo': 'geral',     'turnos': {1: '2018-10-07', 2: '2018-10-28'}},
    2020: {'tipo': 'municipal', 'turnos': {1: '2020-11-15', 2: '2020-11-29'}},
    2022: {'tipo': 'geral',     'turnos': {1: '2022-10-02', 2: '2022-10-30'}},
    2024: {'tipo': 'municipal', 'turnos': {1: '2024-10-06', 2: '2024-10-27'}},
    2026: {'tipo': 'geral',     'turnos': {1: '2026-10-04', 2: '2026-10-25'}, 'ao_vivo': True,
           # Usados só se o ele-c.json do TSE não responder. Os do 2º turno seguem o padrão de 2022
           # (federal 1T, federal 2T, estadual 1T, estadual 2T em sequência) e NÃO foram confirmados.
           'codigos_reserva': {1: {'fed': '6257', 'est': '6259'}, 2: {'fed': '6258', 'est': '6260'}}},
}
ANO_PADRAO_LEGADO, TURNO_PADRAO_LEGADO = 2026, 1

CARGOS_LIVE = {  # cargo -> (tipo de eleição, código do cargo)
    'presidente': ('fed', 1), 'governador': ('est', 3), 'senador': ('est', 5),
    'dep_federal': ('est', 6), 'dep_estadual': ('est', 7), 'dep_distrital': ('est', 8),
}
INTERVALO_AO_VIVO = 60          # segundos entre leituras durante a apuração
INTERVALO_FINALIZADA = 1800     # depois de 100% apurado (o ideal é congelar com congelar_ao_vivo.py)
TTL_MAPA_AO_VIVO = 180          # cache do mapa municipal ao vivo
TTL_DETALHES_AO_VIVO = 60       # cache do painel de cargos ao vivo
MAX_AGE_HISTORICO = 3600        # navegador guarda dados históricos por 1h
WORKERS_MUNICIPIOS = 8          # requisições paralelas ao TSE (plano gratuito do Render tem 0,1 CPU)


# ===================== UTILITÁRIOS =====================
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def normalizar(s):
    s = unicodedata.normalize('NFD', s or '').encode('ascii', 'ignore').decode()
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def caminho_estatico(ano, turno, *partes):
    return os.path.join(DIR_DADOS, str(ano), f't{turno}', *partes)


def estatico(ano, turno, *partes, padrao=None):
    caminho = caminho_estatico(ano, turno, *partes)
    if not os.path.exists(caminho):
        return padrao
    with open(caminho, encoding='utf-8') as f:
        return json.load(f)


# ===================== RESPOSTAS PRONTAS EM MEMÓRIA =====================
# Cada resposta é montada UMA vez e guardada já comprimida (gzip). Depois disso, atender uma
# requisição é só enviar bytes: quase zero de CPU, que é o recurso mais escasso no plano gratuito.
_respostas = {}                 # chave -> (momento, bytes gzip)
_detalhes_prontos = {}          # (ano, turno, uf) -> {'estado': gz, 'municipios': {cd: gz}}
_lock_respostas = threading.Lock()


def comprimir(obj):
    return gzip.compress(json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8'), compresslevel=6)


def responder(gz, max_age=0, status=200):
    if 'gzip' in request.headers.get('Accept-Encoding', ''):
        resp = Response(gz, status=status, mimetype='application/json')
        resp.headers['Content-Encoding'] = 'gzip'
    else:
        resp = Response(gzip.decompress(gz), status=status, mimetype='application/json')
    resp.headers['Vary'] = 'Accept-Encoding'
    resp.headers['Cache-Control'] = f'public, max-age={max_age}' if max_age else 'no-cache'
    return resp


def resposta_em_cache(chave, gerar, ttl=None):
    """Devolve os bytes comprimidos de `gerar()`, calculando só na primeira vez (ou quando o ttl vence)."""
    item = _respostas.get(chave)
    if item and (ttl is None or time.time() - item[0] < ttl):
        return item[1]
    gz = comprimir(gerar())
    with _lock_respostas:
        _respostas[chave] = (time.time(), gz)
    return gz


def resposta_arquivo(chave, ano, turno, *partes, padrao):
    """Arquivo estático comprimido direto dos bytes do disco, sem converter para objetos Python."""
    item = _respostas.get(chave)
    if item:
        return item[1]
    caminho = caminho_estatico(ano, turno, *partes)
    if os.path.exists(caminho):
        with open(caminho, 'rb') as f:
            gz = gzip.compress(f.read(), compresslevel=6)
    else:
        gz = comprimir(padrao)
    with _lock_respostas:
        _respostas[chave] = (time.time(), gz)
    return gz


def detalhes_historicos(ano, turno, uf):
    """Lê o arquivo de detalhes da UF uma única vez e guarda a resposta de cada município já pronta."""
    chave = (ano, turno, uf)
    if chave not in _detalhes_prontos:
        with _lock_respostas:
            if chave not in _detalhes_prontos:
                det = estatico(ano, turno, 'detalhes', f'{uf}.json', padrao={})
                _detalhes_prontos[chave] = {
                    'estado': comprimir({'uf': uf, 'cargos': det.get('estado', {}).get('cargos', {})}),
                    'municipios': {cd: comprimir({'uf': uf, 'municipio_codigo': cd, 'cargos': m.get('cargos', {})})
                                   for cd, m in det.get('municipios', {}).items()}
                }
                del det  # libera os objetos Python; ficam só os bytes comprimidos
    return _detalhes_prontos[chave]


def turno_ja_comecou(ano, turno):
    if os.environ.get('FORCAR_TURNOS_AO_VIVO') == '1':
        return True
    return date.today().isoformat() >= ELEICOES[ano]['turnos'][turno]


def fonte(ano, turno):
    """'historico', 'ao_vivo' ou None (indisponível)."""
    cfg = ELEICOES.get(ano)
    if not cfg or turno not in cfg['turnos']:
        return None
    if os.path.exists(caminho_estatico(ano, turno, 'brasil.json')):
        return 'historico'
    if cfg.get('ao_vivo') and turno_ja_comecou(ano, turno):
        return 'ao_vivo'
    return None


def ler_params():
    try:
        ano = int(request.args.get('ano', ANO_PADRAO_LEGADO))
        turno = int(request.args.get('turno', TURNO_PADRAO_LEGADO))
    except ValueError:
        ano, turno = ANO_PADRAO_LEGADO, TURNO_PADRAO_LEGADO
    return ano, turno


def vazio():
    return {'apurado': '0,00', 'candidatos': []}


# ===================== TSE AO VIVO =====================
_codigos = {}
_lock_codigos = threading.Lock()


def codigos_ao_vivo(ano, turno):
    """Descobre os códigos de eleição no ele-c.json do TSE (cache de 1h); cai nos códigos de reserva se falhar."""
    with _lock_codigos:  # trava a consulta inteira: só uma thread vai ao TSE, as outras usam o resultado
        em_cache = _codigos.get((ano, turno))
        if em_cache and time.time() - em_cache[1] < 3600:
            return em_cache[0]
        return _descobrir_codigos(ano, turno)


def _descobrir_codigos(ano, turno):
    cod = dict(ELEICOES[ano].get('codigos_reserva', {}).get(turno, {}))
    try:
        resp = requests.get(f"{TSE_BASE}/comum/config/ele-c.json", headers=HEADERS, timeout=5)
        if resp.ok:
            achados = {}
            for pleito in resp.json().get('pl', []):
                for e in pleito.get('e', []):
                    nm = normalizar(e.get('nm', ''))
                    if str(ano) not in nm or 'SUPLEMENTAR' in nm or str(e.get('t', '')) != str(turno):
                        continue
                    if 'FEDERAL' in nm:
                        achados['fed'] = str(e['cd'])
                    elif 'ESTADUAL' in nm:
                        achados['est'] = str(e['cd'])
            if achados:
                cod.update(achados)
                log(f"Códigos {ano} {turno}º turno descobertos no ele-c.json: {achados}")
    except Exception as e:
        log(f"[ERRO ele-c.json] {e}")
    _codigos[(ano, turno)] = (cod, time.time())
    return cod


def url_tse(ano, codigo, abrangencia, cargo):
    uf = abrangencia[:2]
    return (f"{TSE_BASE}/ele{ano}/{int(codigo)}/dados/{uf}/{abrangencia}-c{cargo:04d}-e{int(codigo):06d}-u.json"
            f"?t={int(time.time() * 1000)}")


quarentena_404 = {}


def buscar_tse(url_sem_ts, chave_quarentena=None, timeout=5):
    agora = time.time()
    if chave_quarentena and quarentena_404.get(chave_quarentena, 0) > agora:
        return None
    try:
        resp = requests.get(url_sem_ts, headers=HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404 and chave_quarentena:
            quarentena_404[chave_quarentena] = agora + 60
    except Exception as e:
        log(f"[ERRO TSE] {url_sem_ts.split('?')[0]}: {e}")
    return None


def processar_json_tse(data, limit=5):
    pst = data.get('s', {}).get('pst', '0,00')
    todos = []
    cargos = data.get('carg', [])
    if cargos:
        for agr in cargos[0].get('agr', []):
            for par in agr.get('par', []):
                for c in par.get('cand', []):
                    votos_int = int(c.get('vap', 0) or 0)
                    todos.append({
                        'nome': c.get('nmu', c.get('nm', 'Desconhecido')),
                        'partido': par.get('sg', ''),
                        'percentual': c.get('pvap', '0,00'),
                        'votos': f"{votos_int:,}".replace(",", "."),
                        'votos_raw': votos_int,
                        'eleito': c.get('e', 'n') == 's'
                    })
    todos.sort(key=lambda x: x['votos_raw'], reverse=True)
    for c in todos:
        del c['votos_raw']
    return pst, todos[:limit]


def resultado_tse(ano, turno, abrangencia, cargo_nome, limit=5):
    tipo, cargo_cd = CARGOS_LIVE[cargo_nome]
    codigo = codigos_ao_vivo(ano, turno).get(tipo)
    if not codigo:
        return vazio()
    data = buscar_tse(url_tse(ano, codigo, abrangencia, cargo_cd), chave_quarentena=f"{ano}-{turno}-{abrangencia}-{cargo_cd}")
    if not data:
        return vazio()
    pst, cands = processar_json_tse(data, limit)
    return {'apurado': pst, 'candidatos': cands}


# --- Cache do resultado nacional/estadual ao vivo (um por eleição) ---
caches_ao_vivo = {}
_lock_cache = threading.Lock()


def arquivo_cache(ano, turno):
    return os.path.join(BASE_DIR, f'dados_cache_{ano}_t{turno}.json')


def cache_ao_vivo(ano, turno):
    with _lock_cache:
        if (ano, turno) not in caches_ao_vivo:
            dados = {'ultima_atualizacao': None, 'brasil': vazio(), 'estados': {}}
            try:
                with open(arquivo_cache(ano, turno), encoding='utf-8') as f:
                    salvo = json.load(f)
                if 'brasil' in salvo and 'estados' in salvo:
                    dados = salvo
                    log(f"[CACHE] {ano} {turno}º turno carregado do disco")
            except FileNotFoundError:
                pass
            except Exception as e:
                log(f"[CACHE ERRO] {e}")
            caches_ao_vivo[(ano, turno)] = dados
        return caches_ao_vivo[(ano, turno)]


def atualizar_ao_vivo(ano, turno):
    anterior = cache_ao_vivo(ano, turno)
    dados = {'ultima_atualizacao': None, 'brasil': anterior['brasil'], 'estados': dict(anterior['estados'])}
    hora = time.strftime("%H:%M:%S")
    locais = ['br'] + [uf.lower() for uf in ESTADOS]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futuros = {ex.submit(resultado_tse, ano, turno, loc, 'presidente', 5): loc for loc in locais}
        for fut in as_completed(futuros):
            loc = futuros[fut]
            try:
                res = fut.result()
                if loc == 'br':
                    dados['brasil'] = res
                else:
                    dados['estados'][loc.upper()] = res
            except Exception as e:
                log(f"[ERRO EXECUTOR] {loc}: {e}")
    dados['ultima_atualizacao'] = hora
    with _lock_cache:
        caches_ao_vivo[(ano, turno)] = dados
    with _lock_respostas:
        _respostas.pop(('votacao', ano, turno), None)   # a próxima requisição comprime a versão nova
    try:
        with open(arquivo_cache(ano, turno), 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False)
    except Exception:
        pass
    return dados


def eleicoes_ao_vivo_ativas():
    return [(ano, t) for ano, cfg in ELEICOES.items() if cfg.get('ao_vivo')
            for t in cfg['turnos'] if fonte(ano, t) == 'ao_vivo']


def laco_ao_vivo():
    proxima = {}
    while True:
        for ano, turno in eleicoes_ao_vivo_ativas():
            if time.time() < proxima.get((ano, turno), 0):
                continue
            dados = atualizar_ao_vivo(ano, turno)
            finalizada = dados['brasil'].get('apurado') == '100,00'
            proxima[(ano, turno)] = time.time() + (INTERVALO_FINALIZADA if finalizada else INTERVALO_AO_VIVO)
        time.sleep(5)


threading.Thread(target=laco_ao_vivo, daemon=True).start()


# --- Municípios ao vivo (config do TSE, com código IBGE no campo 'cdi') ---
cache_municipios_ao_vivo = {}


@lru_cache(maxsize=27)
def nomes_geometria(uf):
    caminho = os.path.join(DIR_GEO_MUN, f'{uf}.json')
    if not os.path.exists(caminho):
        return {}
    with open(caminho, encoding='utf-8') as f:
        return {normalizar(ft['properties'].get('name', '')): str(ft['properties']['id'])
                for ft in json.load(f)['features']}


def municipios_ao_vivo(ano, turno, uf):
    chave = (ano, turno)
    if chave not in cache_municipios_ao_vivo:
        cod = codigos_ao_vivo(ano, turno)
        ts = int(time.time() * 1000)
        urls = [f"{TSE_BASE}/ele{ano}/{int(cod[t])}/config/mun-e{int(cod[t]):06d}-cm.json?t={ts}" for t in ('fed', 'est') if cod.get(t)]
        if cod.get('fed'):
            urls.insert(1, f"{TSE_BASE}/ele{ano}/comum/config/mun-e{int(cod['fed']):06d}-cm.json?t={ts}")
        for url in urls:
            try:
                resp = requests.get(url, headers=HEADERS, timeout=8)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                por_uf = {}
                for abr in data.get('abr', []) or data.get('ufs', []):
                    sigla = (abr.get('cd', '') or abr.get('sg', '')).upper()
                    lista = []
                    for m in abr.get('mu', []) or abr.get('muns', []):
                        codigo = str(m.get('cd', '') or m.get('c', '')).zfill(5)
                        nome = m.get('nm', '') or m.get('n', '')
                        if codigo and nome:
                            lista.append({'codigo': codigo, 'ibge': str(m.get('cdi', '') or '') or None, 'nome': nome.upper()})
                    lista.sort(key=lambda x: normalizar(x['nome']))
                    if sigla:
                        por_uf[sigla] = lista
                if por_uf:
                    cache_municipios_ao_vivo[chave] = por_uf
                    break
            except Exception as e:
                log(f"[ERRO MUNICÍPIOS] {url.split('?')[0]}: {e}")
    lista = cache_municipios_ao_vivo.get(chave, {}).get(uf, [])
    # Se o TSE não trouxer o código IBGE, casa pelo nome com a geometria
    nomes = nomes_geometria(uf)
    for m in lista:
        if not m.get('ibge'):
            m['ibge'] = nomes.get(normalizar(m['nome']))
    return lista


cache_mapas = {}            # (ano, turno, uf) -> (momento, dados)
_locks_mapas = {}
_lock_locks = threading.Lock()


def mapa_ao_vivo(ano, turno, uf):
    chave = (ano, turno, uf)
    with _lock_locks:
        lock = _locks_mapas.setdefault(chave, threading.Lock())
    with lock:  # duas pessoas clicando em MG ao mesmo tempo disparam só uma rodada de consultas
        em_cache = cache_mapas.get(chave)
        if em_cache and time.time() - em_cache[0] < TTL_MAPA_AO_VIVO:
            return em_cache[1]  # bytes já comprimidos
        muns = municipios_ao_vivo(ano, turno, uf)
        resultado = {}

        def um(m):
            return m, resultado_tse(ano, turno, f"{uf.lower()}{m['codigo']}", 'presidente', 2)

        with ThreadPoolExecutor(max_workers=WORKERS_MUNICIPIOS) as ex:
            for m, res in ex.map(um, muns):
                if m.get('ibge'):
                    resultado[m['ibge']] = {'tse': m['codigo'], 'nome': m['nome'], **res}
        gz = comprimir(resultado)
        cache_mapas[chave] = (time.time(), gz)
        return gz


def detalhes_ao_vivo(ano, turno, uf, cd_mun=None):
    uf_l = uf.lower()
    cargo_dep_est = 'dep_distrital' if uf.upper() == 'DF' else 'dep_estadual'
    if turno == 2:
        cargos = ['governador'] if cd_mun is None else ['presidente', 'governador']
    else:
        base = ['governador', 'senador', 'dep_federal', cargo_dep_est]
        cargos = base if cd_mun is None else ['presidente'] + base
    abr = uf_l if cd_mun is None else f"{uf_l}{str(cd_mun).zfill(5)}"
    with ThreadPoolExecutor(max_workers=5) as ex:
        res = dict(zip(cargos, ex.map(lambda c: resultado_tse(ano, turno, abr, c, 5), cargos)))
    if 'dep_distrital' in res:
        res['dep_estadual'] = res.pop('dep_distrital')  # o front usa a mesma chave e troca o rótulo no DF
    return {'cargos': res}


# ===================== ROTAS =====================
@app.route('/')
def index():
    return send_from_directory(BASE_DIR, 'eleicoes2026.html')


@app.route('/api/eleicoes')
def api_eleicoes():
    lista, padrao = [], None
    for ano in sorted(ELEICOES, reverse=True):
        cfg = ELEICOES[ano]
        turnos = []
        for t, data_eleicao in sorted(cfg['turnos'].items()):
            f = fonte(ano, t)
            turnos.append({'turno': t, 'data': data_eleicao, 'disponivel': f is not None, 'ao_vivo': f == 'ao_vivo'})
            if f and (padrao is None or (ano, t) > (padrao['ano'], padrao['turno'])):
                padrao = {'ano': ano, 'turno': t}
        lista.append({'ano': ano, 'tipo': cfg['tipo'], 'turnos': turnos})
    return jsonify({'eleicoes': lista, 'padrao': padrao})


@app.route('/api/votacao')
def api_votacao():
    ano, turno = ler_params()
    f = fonte(ano, turno)
    base = {'ano': ano, 'turno': turno, 'tipo': ELEICOES.get(ano, {}).get('tipo', 'geral'), 'fonte': f}
    if f == 'historico':
        def gerar():
            info = estatico(ano, turno, 'info.json', padrao={})
            return {**base, 'ultima_atualizacao': 'Resultado final',
                    'fonte_descricao': info.get('fonte_curta', 'Portal de Dados Abertos do TSE'),
                    'brasil': estatico(ano, turno, 'brasil.json', padrao=vazio()),
                    'estados': estatico(ano, turno, 'estados.json', padrao={})}
        return responder(resposta_em_cache(('votacao', ano, turno), gerar), MAX_AGE_HISTORICO)
    if f == 'ao_vivo':
        return responder(resposta_em_cache(('votacao', ano, turno), lambda: {**base, **cache_ao_vivo(ano, turno)}))
    return responder(comprimir({**base, 'ultima_atualizacao': None, 'brasil': vazio(), 'estados': {}}), status=404)


@app.route('/api/municipios/<uf>')
def api_municipios(uf):
    ano, turno = ler_params()
    uf = uf.upper()
    f = fonte(ano, turno)
    if f == 'historico':
        return responder(resposta_arquivo(('mun', ano, turno, uf), ano, turno, 'municipios', f'{uf}.json', padrao=[]),
                         MAX_AGE_HISTORICO)
    if f == 'ao_vivo':
        return responder(resposta_em_cache(('mun', ano, turno, uf), lambda: municipios_ao_vivo(ano, turno, uf), ttl=3600))
    return responder(comprimir([]))


@app.route('/api/mapa/<uf>')
def api_mapa(uf):
    ano, turno = ler_params()
    uf = uf.upper()
    f = fonte(ano, turno)
    if f == 'historico':
        return responder(resposta_arquivo(('mapa', ano, turno, uf), ano, turno, 'mapa', f'{uf}.json', padrao={}),
                         MAX_AGE_HISTORICO)
    if f == 'ao_vivo' and uf in ESTADOS:
        return responder(mapa_ao_vivo(ano, turno, uf))
    return responder(comprimir({}))


@app.route('/api/detalhes/<uf>')
def api_detalhes_estado(uf):
    ano, turno = ler_params()
    uf = uf.upper()
    f = fonte(ano, turno)
    if f == 'historico':
        return responder(detalhes_historicos(ano, turno, uf)['estado'], MAX_AGE_HISTORICO)
    if f == 'ao_vivo':
        return responder(resposta_em_cache(('det', ano, turno, uf, None),
                                           lambda: {'uf': uf, **detalhes_ao_vivo(ano, turno, uf)}, ttl=TTL_DETALHES_AO_VIVO))
    return responder(comprimir({'uf': uf, 'cargos': {}}))


@app.route('/api/detalhes/<uf>/<cd_mun>')
def api_detalhes_municipio(uf, cd_mun):
    ano, turno = ler_params()
    uf, cd_mun = uf.upper(), str(cd_mun).zfill(5)
    f = fonte(ano, turno)
    vazio_mun = {'uf': uf, 'municipio_codigo': cd_mun, 'cargos': {}}
    if f == 'historico':
        gz = detalhes_historicos(ano, turno, uf)['municipios'].get(cd_mun)
        return responder(gz or comprimir(vazio_mun), MAX_AGE_HISTORICO)
    if f == 'ao_vivo':
        return responder(resposta_em_cache(('det', ano, turno, uf, cd_mun),
                                           lambda: {**vazio_mun, **detalhes_ao_vivo(ano, turno, uf, cd_mun)},
                                           ttl=TTL_DETALHES_AO_VIVO))
    return responder(comprimir(vazio_mun))


if __name__ == '__main__':
    porta = int(os.environ.get("PORT", 5001))
    app.run(host='0.0.0.0', port=porta)
