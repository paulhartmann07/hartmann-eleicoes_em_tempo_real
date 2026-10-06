"""
Congela uma eleição que já terminou de ser apurada, salvando o resultado final do TSE em arquivos estáticos
(mesmo formato do gerar_historico.py). Depois do push, o servidor para de consultar o TSE para esse turno.

Rode no SEU computador (não no Render), dentro da pasta do projeto:
    python congelar_ao_vivo.py 2026 1        # 1º turno de 2026
    python congelar_ao_vivo.py 2026 2        # depois que o 2º turno chegar a 100%

Faz cerca de 28 mil consultas ao TSE no 1º turno (5 cargos x 5.570 municípios) e ~11 mil no 2º turno.
Leva poucos minutos; o limite do TSE é 300 requisições por segundo por IP e o script usa bem menos.
"""
import json
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_DADOS = os.path.join(BASE_DIR, 'dados')
DIR_GEO_MUN = os.path.join(BASE_DIR, 'static', 'municipios')
TSE_BASE = os.environ.get('TSE_BASE', 'https://resultados.tse.jus.br/oficial')
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache'}
WORKERS = 24
TOLERANCIA_FALHAS = 0.005    # aborta sem salvar se mais de 0,5% das consultas falharem

ESTADOS = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA',
           'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
CODIGOS_RESERVA = {(2026, 1): {'fed': '6257', 'est': '6259'}, (2026, 2): {'fed': '6258', 'est': '6260'}}
CARGOS = {'presidente': ('fed', 1), 'governador': ('est', 3), 'senador': ('est', 5),
          'dep_federal': ('est', 6), 'dep_estadual': ('est', 7), 'dep_distrital': ('est', 8)}


def normalizar(s):
    s = unicodedata.normalize('NFD', s or '').encode('ascii', 'ignore').decode()
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def salvar(caminho, dados):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, separators=(',', ':'))


sessao = requests.Session()
sessao.mount('https://', requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS))


def baixar_json(url, tentativas=4):
    """None = arquivo não existe (404, ex.: cargo sem disputa); levanta exceção se falhar de verdade."""
    for t in range(tentativas):
        try:
            r = sessao.get(f"{url}?t={int(time.time() * 1000)}", headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
        except requests.RequestException:
            pass
        time.sleep(1.5 * (t + 1))
    raise RuntimeError(f"falhou após {tentativas} tentativas: {url}")


def descobrir_codigos(ano, turno):
    cod = dict(CODIGOS_RESERVA.get((ano, turno), {}))
    try:
        for pleito in baixar_json(f"{TSE_BASE}/comum/config/ele-c.json").get('pl', []):
            for e in pleito.get('e', []):
                nm = normalizar(e.get('nm', ''))
                if str(ano) in nm and 'SUPLEMENTAR' not in nm and str(e.get('t', '')) == str(turno):
                    if 'FEDERAL' in nm:
                        cod['fed'] = str(e['cd'])
                    elif 'ESTADUAL' in nm:
                        cod['est'] = str(e['cd'])
    except Exception as e:
        print(f"  [aviso] ele-c.json indisponível ({e}); usando códigos de reserva")
    if not cod:
        sys.exit(f"Não encontrei os códigos da eleição {ano} {turno}º turno.")
    print(f"  códigos da eleição: {cod}")
    return cod


def processar(data, limit=5):
    pst = data.get('s', {}).get('pst', '0,00')
    todos = []
    for agr in (data.get('carg') or [{}])[0].get('agr', []):
        for par in agr.get('par', []):
            for c in par.get('cand', []):
                v = int(c.get('vap', 0) or 0)
                todos.append((v, {'nome': c.get('nmu', c.get('nm', 'Desconhecido')), 'partido': par.get('sg', ''),
                                  'percentual': c.get('pvap', '0,00'), 'votos': f"{v:,}".replace(',', '.'),
                                  'eleito': c.get('e', 'n') == 's'}))
    todos.sort(key=lambda x: x[0], reverse=True)
    return {'apurado': pst, 'candidatos': [c for _, c in todos[:limit]]}


def main(ano, turno):
    print(f"\n=== Congelando {ano} – {turno}º turno ===")
    cod = descobrir_codigos(ano, turno)

    def url(abr, cargo):
        tipo, c = CARGOS[cargo]
        return f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/dados/{abr[:2]}/{abr}-c{c:04d}-e{int(cod[tipo]):06d}-u.json"

    # --- municípios (com código IBGE no campo 'cdi')
    cfg = None
    for tipo in ('fed', 'est'):
        cfg = baixar_json(f"{TSE_BASE}/ele{ano}/{int(cod[tipo])}/config/mun-e{int(cod[tipo]):06d}-cm.json")
        if cfg:
            break
    if not cfg:
        sys.exit("Não consegui baixar a lista de municípios do TSE.")
    municipios = {}
    for abr in cfg.get('abr', []):
        uf = abr.get('cd', '').upper()
        if uf not in ESTADOS:
            continue  # exterior (ZZ) entra só no total Brasil
        nomes_geo = {}
        caminho_geo = os.path.join(DIR_GEO_MUN, f'{uf}.json')
        if os.path.exists(caminho_geo):
            with open(caminho_geo, encoding='utf-8') as f:
                nomes_geo = {normalizar(ft['properties']['name']): str(ft['properties']['id']) for ft in json.load(f)['features']}
        municipios[uf] = [{'codigo': str(m['cd']).zfill(5),
                           'ibge': str(m.get('cdi') or '') or nomes_geo.get(normalizar(m.get('nm', ''))),
                           'nome': m.get('nm', '').upper()} for m in abr.get('mu', [])]
    total_mun = sum(len(v) for v in municipios.values())
    print(f"  {total_mun:,} municípios em {len(municipios)} UFs".replace(',', '.'))

    cargos_estado = ['governador'] if turno == 2 else ['governador', 'senador', 'dep_federal', 'dep_estadual']
    cargos_mun = ['presidente'] + cargos_estado

    def cargo_real(cargo, uf):
        return 'dep_distrital' if cargo == 'dep_estadual' and uf == 'DF' else cargo

    # --- lista de todas as consultas: (chave, url)
    tarefas = [(('br', 'presidente'), url('br', 'presidente'))]
    for uf in ESTADOS:
        u = uf.lower()
        tarefas.append(((uf, 'presidente'), url(u, 'presidente')))
        tarefas += [((uf, c), url(u, cargo_real(c, uf))) for c in cargos_estado]
        for m in municipios.get(uf, []):
            tarefas += [((uf, m['codigo'], c), url(f"{u}{m['codigo']}", cargo_real(c, uf))) for c in cargos_mun]

    print(f"  {len(tarefas):,} consultas ao TSE...".replace(',', '.'))
    resultados, falhas, feitas, inicio = {}, [], 0, time.time()

    def uma(t):
        chave, u = t
        try:
            data = baixar_json(u)
            return chave, (processar(data) if data else None), None
        except Exception as e:
            return chave, None, str(e)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for chave, res, erro in ex.map(uma, tarefas):
            feitas += 1
            if erro:
                falhas.append(erro)
            elif res:
                resultados[chave] = res
            if feitas % 1000 == 0 or feitas == len(tarefas):
                n = lambda x: f"{x:,}".replace(',', '.')
                print(f"\r    {n(feitas)}/{n(len(tarefas))}  ({time.time() - inicio:.0f}s, {len(falhas)} falhas)", end='', flush=True)
    print()

    if len(falhas) > TOLERANCIA_FALHAS * len(tarefas):
        print(f"  ✘ {len(falhas)} consultas falharam (ex.: {falhas[:3]}). NADA foi salvo. Rode de novo daqui a pouco.")
        sys.exit(1)

    brasil = resultados.get(('br', 'presidente'))
    if not brasil:
        sys.exit("  ✘ Resultado Brasil indisponível. NADA foi salvo.")
    if brasil['apurado'] != '100,00':
        print(f"  [ATENÇÃO] a apuração nacional está em {brasil['apurado']}%, não 100%. "
              f"Se congelar agora, o site vai mostrar esse resultado parcial como final.")
        if input("  Continuar mesmo assim? (s/N) ").strip().lower() != 's':
            sys.exit(0)

    base = os.path.join(DIR_DADOS, str(ano), f't{turno}')
    salvar(os.path.join(base, 'brasil.json'), {**brasil, 'metrica': 'votos'})
    salvar(os.path.join(base, 'estados.json'), {uf: resultados[(uf, 'presidente')] for uf in ESTADOS if (uf, 'presidente') in resultados})

    sem_ibge = 0
    for uf in ESTADOS:
        mapa, det_muns = {}, {}
        for m in municipios.get(uf, []):
            cargos = {c: resultados[(uf, m['codigo'], c)] for c in cargos_mun if (uf, m['codigo'], c) in resultados}
            det_muns[m['codigo']] = {'cargos': cargos}
            if 'presidente' in cargos:
                if m['ibge']:
                    p = cargos['presidente']
                    mapa[m['ibge']] = {'tse': m['codigo'], 'nome': m['nome'], 'apurado': p['apurado'], 'candidatos': p['candidatos'][:2]}
                else:
                    sem_ibge += 1
        estado = {c: resultados[(uf, c)] for c in cargos_estado if (uf, c) in resultados}
        salvar(os.path.join(base, 'mapa', f'{uf}.json'), mapa)
        salvar(os.path.join(base, 'detalhes', f'{uf}.json'), {'estado': {'cargos': estado}, 'municipios': det_muns})
        salvar(os.path.join(base, 'municipios', f'{uf}.json'),
               sorted(municipios.get(uf, []), key=lambda m: normalizar(m['nome'])))

    salvar(os.path.join(base, 'info.json'), {
        'ano': ano, 'turno': turno, 'tipo': 'geral', 'fonte_curta': 'divulgação oficial do TSE',
        'fonte': f'resultados.tse.jus.br (eleições {cod})', 'apurado_brasil': brasil['apurado'],
        'gerado_em': time.strftime('%Y-%m-%d %H:%M')
    })
    print(f"  ✔ dados/{ano}/t{turno} salvo ({len(falhas)} falhas toleradas, {sem_ibge} municípios sem código IBGE)")
    print("  Agora: git add dados && git commit -m \"Congela resultado\" && git push")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(int(sys.argv[1]), int(sys.argv[2]))
