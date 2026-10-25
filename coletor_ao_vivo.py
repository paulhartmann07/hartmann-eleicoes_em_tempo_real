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
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

from acessos import ArmazenamentoUpstash
from congelar_ao_vivo import CARGOS, ESTADOS, TSE_BASE, descobrir_codigos, normalizar, processar

BRT = timezone(timedelta(hours=-3))
INTERVALO = int(os.environ.get('COLETOR_INTERVALO_SEG', 300))
WORKERS = int(os.environ.get('COLETOR_WORKERS', 32))
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


def baixar(url, tentativas=3):
    """Devolve (resultado, status) com status 'novo', 'igual', 'ausente' (404) ou 'falha'.
    Em falha devolve o último resultado conhecido, para nunca 'zerar' um município."""
    anterior = cache_http.get(url)
    cab = {'User-Agent': 'Mozilla/5.0 (coletor eleicoes)'}
    if anterior:
        if anterior[0]:
            cab['If-None-Match'] = anterior[0]
        if anterior[1]:
            cab['If-Modified-Since'] = anterior[1]
    for t in range(tentativas):
        try:
            r = sessao.get(url, headers=cab, timeout=15)
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
    n_mun = sum(len(v) for v in municipios.values())
    n_zon = sum(len(m['zonas']) for v in municipios.values() for m in v)
    print(f"  {n_mun:,} municípios e {n_zon:,} pares município/zona".replace(',', '.'))
    return municipios


# ===================== UM CICLO =====================
def ciclo(ano, turno, cod, municipios, banco, ufs_governador, completo=True):
    """completo=False (modo leve, para o Render gratuito): sem zonas e sem governador por município."""
    inicio = time.time()

    def url(abr, cargo, uf):
        tipo, c = CARGOS[cargo]
        return f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/dados/{uf.lower()}/{abr}-c{c:04d}-e{int(cod[tipo]):06d}-u.json"

    def cargo_gov_ou_dep(cargo, uf):
        return 'dep_distrital' if cargo == 'dep_estadual' and uf == 'DF' else cargo

    cargos_mun = ['presidente'] + (['governador'] if turno == 2 else ['governador', 'senador'])

    res, contagem = {}, {'novo': 0, 'igual': 0, 'ausente': 0, 'falha': 0}

    def executar(tarefas):
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for (k, _), (r, st) in zip(tarefas, ex.map(lambda t: baixar(t[1]), tarefas)):
                contagem[st] += 1
                if r:
                    res[k] = r

    # Fase 1: Brasil e UFs (inclui governador da UF, para saber onde há 2º turno de governador)
    fase1 = [(('br', 'presidente'), url('br', 'presidente', 'br'))]
    for uf in ESTADOS:
        fase1.append(((uf, 'presidente'), url(uf.lower(), 'presidente', uf)))
        fase1.append(((uf, 'governador'), url(uf.lower(), 'governador', uf)))
    executar(fase1)
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
    executar(fase2)
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

    # --- histórico (Brasil + UFs): [hora, % apurado, [[nome, partido, votos, %], ...]]
    def ponto(r):
        return [int(time.time()), r.get('apurado', '0,00'),
                [[c['nome'], c['partido'], int(c['votos'].replace('.', '') or 0), c['percentual']] for c in r.get('candidatos', [])[:5]]]

    # --- gravação no Upstash, em lotes
    comandos = [['SET', chave(ano, turno, 'resumo'), empacotar(resumo), 'EX', EXPIRA_SEG]]
    comandos += [['SET', chave(ano, turno, 'uf', uf), empacotar(f), 'EX', EXPIRA_SEG] for uf, f in fotos.items()]
    for abr, r in [('BR', brasil)] + list(estados.items()):
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
          f"{n(len(tarefas))} arquivos ({n(contagem['igual'])} sem mudança, {contagem['falha']} falhas) "
          f"em {time.time() - inicio:.0f}s")
    return finalizado


def main(ano, turno):
    url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
    if not (url and token):
        sys.exit("Defina UPSTASH_REDIS_REST_URL e UPSTASH_REDIS_REST_TOKEN (os mesmos do Render).")
    banco = ArmazenamentoUpstash(url, token)
    print(f"\n=== Coletor ao vivo {ano} – {turno}º turno (a cada {INTERVALO // 60} min) ===")
    cod = descobrir_codigos(ano, turno)
    municipios = carregar_municipios(ano, cod)
    ufs_governador = set()
    while True:
        try:
            if ciclo(ano, turno, cod, municipios, banco, ufs_governador):
                print("\n  ✔ Apuração encerrada (100% em todas as UFs). Coletor parado.")
                print(f"  Agora rode: python congelar_ao_vivo.py {ano} {turno}  e faça o git push.")
                return
        except Exception as e:
            print(f"  ✘ erro no ciclo: {e} (tento de novo no próximo)")
        # alinha nos múltiplos de 5 min (17:00, 17:05...) para o gráfico ficar regular
        espera = INTERVALO - (time.time() % INTERVALO)
        time.sleep(max(5, espera))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(int(sys.argv[1]), int(sys.argv[2]))
