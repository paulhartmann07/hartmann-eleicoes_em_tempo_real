"""
Gera os arquivos estáticos das eleições passadas a partir do Portal de Dados Abertos do TSE.

Rode UMA VEZ no seu computador e suba a pasta `dados/` para o GitHub. O servidor no Render
só lê esses arquivos (resultados finais não mudam mais).

Uso:
    python gerar_historico.py 2018 2020 2022 2024      # gera os anos pedidos
    python gerar_historico.py 2026                     # (quando o TSE publicar os dados abertos de 2026)
    python gerar_historico.py --geometrias             # opcional: baixa a malha municipal ATUAL do IBGE

Os .zip do TSE são grandes (centenas de MB). Ficam guardados em .cache_tse/ para não baixar de novo;
essa pasta NÃO deve ir para o GitHub (já está no .gitignore sugerido no README).

Estrutura gerada para cada ano/turno em dados/{ano}/t{turno}/:
    brasil.json             cards do topo (geral: presidente | municipal: partidos por municípios liderados)
    estados.json            resultado por UF usado no mapa do Brasil
    mapa/{UF}.json          resultado por município (chave = código IBGE) usado no mapa do estado
    detalhes/{UF}.json      todos os cargos do estado e de cada município (painel inferior)
    municipios/{UF}.json    lista de municípios para a busca
    info.json               metadados
"""
import csv
import io
import json
import os
import re
import sys
import time
import unicodedata
import zipfile
from collections import defaultdict

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_CACHE = os.path.join(BASE_DIR, ".cache_tse")
DIR_DADOS = os.path.join(BASE_DIR, "dados")
DIR_GEO_MUN = os.path.join(BASE_DIR, "static", "municipios")

URL_VOTACAO = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_{ano}.zip"
URL_TSE_IBGE = "https://cdn.tse.jus.br/estatistica/sead/odsele/municipio_tse_ibge/municipio_tse_ibge.zip"

ESTADOS = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA',
           'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

TIPO_ANO = {2018: 'geral', 2020: 'municipal', 2022: 'geral', 2024: 'municipal', 2026: 'geral'}

# Códigos de cargo do TSE
CARGOS_GERAL = {1: 'presidente', 3: 'governador', 5: 'senador', 6: 'dep_federal', 7: 'dep_estadual', 8: 'dep_estadual'}
CARGOS_MUNICIPAL = {11: 'prefeito', 13: 'vereador'}

TOP_DETALHE = 5     # candidatos por cargo no painel inferior
TOP_MAPA = 2        # candidatos por município no mapa (tooltip mostra 1º e 2º)
TOP_PARTIDOS_UF = 10


# ===================== UTILITÁRIOS =====================
def normalizar(s):
    s = unicodedata.normalize('NFD', s or '').encode('ascii', 'ignore').decode()
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def fmt_pct(v):
    return f"{v:.2f}".replace('.', ',')


def fmt_int(n):
    return f"{n:,}".replace(',', '.')


def baixar(url, destino):
    if os.path.exists(destino):
        print(f"  [cache] {os.path.basename(destino)}")
        return destino
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    print(f"  baixando {url}")
    tmp = destino + ".parcial"
    with requests.get(url, stream=True, timeout=60, headers={'User-Agent': 'Mozilla/5.0'}) as r:
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        feito, inicio = 0, time.time()
        with open(tmp, 'wb') as f:
            for bloco in r.iter_content(chunk_size=1 << 20):
                f.write(bloco)
                feito += len(bloco)
                if total:
                    print(f"\r    {feito / 1e6:,.0f} / {total / 1e6:,.0f} MB", end='', flush=True)
        print(f"\r    concluído: {feito / 1e6:,.0f} MB em {time.time() - inicio:.0f}s")
    os.replace(tmp, destino)
    return destino


def salvar(caminho, dados):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, separators=(',', ':'))


def ler_csv_zip(zf, nome):
    """Lê um CSV do TSE (latin-1, separado por ';') e devolve (cabeçalho, iterador de linhas)."""
    bruto = zf.open(nome)
    texto = io.TextIOWrapper(bruto, encoding='latin-1', newline='')
    leitor = csv.reader(texto, delimiter=';', quotechar='"')
    cab = [c.strip().upper() for c in next(leitor)]
    return cab, leitor


# ===================== MAPEAMENTO TSE -> IBGE =====================
def carregar_mapa_tse_ibge():
    """Tenta o arquivo oficial do TSE; se falhar, retorna {} e o script usa casamento por nome."""
    try:
        caminho = baixar(URL_TSE_IBGE, os.path.join(DIR_CACHE, 'municipio_tse_ibge.zip'))
        mapa = {}
        with zipfile.ZipFile(caminho) as zf:
            nome = next(n for n in zf.namelist() if n.lower().endswith('.csv'))
            cab, linhas = ler_csv_zip(zf, nome)
            col_tse = next(i for i, c in enumerate(cab) if 'TSE' in c and c.startswith('CD'))
            col_ibge = next(i for i, c in enumerate(cab) if 'IBGE' in c and c.startswith('CD'))
            for l in linhas:
                if len(l) > max(col_tse, col_ibge):
                    mapa[l[col_tse].strip().zfill(5)] = l[col_ibge].strip()
        print(f"  mapeamento TSE→IBGE: {len(mapa)} municípios")
        return mapa
    except Exception as e:
        print(f"  [aviso] não consegui usar o arquivo TSE→IBGE ({e}); vou casar por nome.")
        return {}


def carregar_nomes_geometria():
    """{UF: {nome_normalizado: codigo_ibge}} a partir de static/municipios/*.json (fallback)."""
    res = {}
    for uf in ESTADOS:
        caminho = os.path.join(DIR_GEO_MUN, f'{uf}.json')
        if not os.path.exists(caminho):
            continue
        with open(caminho, encoding='utf-8') as f:
            geo = json.load(f)
        res[uf] = {normalizar(ft['properties'].get('name', '')): str(ft['properties']['id']) for ft in geo['features']}
    return res


# ===================== AGREGAÇÃO =====================
def processar_ano(ano):
    tipo = TIPO_ANO.get(ano)
    if not tipo:
        sys.exit(f"Ano {ano} não suportado. Use: {sorted(TIPO_ANO)}")
    cargos = CARGOS_GERAL if tipo == 'geral' else CARGOS_MUNICIPAL

    print(f"\n=== {ano} ({tipo}) ===")
    zip_path = baixar(URL_VOTACAO.format(ano=ano), os.path.join(DIR_CACHE, f'votacao_candidato_munzona_{ano}.zip'))
    tse_ibge = carregar_mapa_tse_ibge()
    nomes_geo = carregar_nomes_geometria()

    # votos[turno][cargo][uf][mun_tse][sq] = votos      (uf 'ZZ' = exterior; entra só no total Brasil)
    votos = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))))
    candidatos = {}         # sq -> {nome, partido}
    situacao = {}           # (turno, sq) -> texto
    nomes_mun = {}          # mun_tse -> (uf, nome)

    with zipfile.ZipFile(zip_path) as zf:
        arquivos = sorted(n for n in zf.namelist() if n.lower().endswith('.csv'))
        # O arquivo _BR (quando existe) repete presidente de todas as UFs: processa por último e só usa o que faltar
        arquivos.sort(key=lambda n: n.upper().endswith('_BR.CSV'))
        ufs_com_presidente = set()

        for nome_arq in arquivos:
            eh_br = nome_arq.upper().endswith('_BR.CSV')
            cab, linhas = ler_csv_zip(zf, nome_arq)
            idx = {c: i for i, c in enumerate(cab)}
            col_votos = idx.get('QT_VOTOS_NOMINAIS_VALIDOS', idx.get('QT_VOTOS_NOMINAIS'))
            obrig = ['NR_TURNO', 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'CD_CARGO', 'SQ_CANDIDATO',
                     'NM_URNA_CANDIDATO', 'SG_PARTIDO', 'DS_SIT_TOT_TURNO']
            if col_votos is None or any(c not in idx for c in obrig):
                print(f"  [pulando] {nome_arq}: colunas inesperadas")
                continue
            i_tipo = [idx[c] for c in ('NM_TIPO_ELEICAO', 'DS_ELEICAO') if c in idx]
            i = {c: idx[c] for c in obrig}
            n_linhas, inicio = 0, time.time()

            for l in linhas:
                try:
                    cargo_cd = int(l[i['CD_CARGO']])
                except (ValueError, IndexError):
                    continue
                cargo = cargos.get(cargo_cd)
                if not cargo:
                    continue
                if any('SUPLEMENTAR' in l[k].upper() for k in i_tipo):
                    continue
                uf = l[i['SG_UF']].strip().upper()
                if cargo_cd == 1:
                    if eh_br and uf in ufs_com_presidente:
                        continue
                    if not eh_br:
                        ufs_com_presidente.add(uf)
                try:
                    v = int(l[col_votos] or 0)
                    turno = int(l[i['NR_TURNO']])
                except ValueError:
                    continue
                mun = l[i['CD_MUNICIPIO']].strip().zfill(5)
                sq = l[i['SQ_CANDIDATO']].strip()
                votos[turno][cargo][uf][mun][sq] += v
                if sq not in candidatos:
                    candidatos[sq] = {'nome': l[i['NM_URNA_CANDIDATO']].strip(), 'partido': l[i['SG_PARTIDO']].strip()}
                situacao[(turno, sq)] = l[i['DS_SIT_TOT_TURNO']].strip().upper()
                nomes_mun.setdefault(mun, (uf, l[i['NM_MUNICIPIO']].strip()))
                n_linhas += 1
            print(f"  {nome_arq}: {n_linhas:,} linhas úteis ({time.time() - inicio:.0f}s)")

    def ranking(contagem, turno, top):
        total = sum(contagem.values())
        ordenado = sorted(contagem.items(), key=lambda kv: kv[1], reverse=True)[:top]
        return [{
            'nome': candidatos[sq]['nome'],
            'partido': candidatos[sq]['partido'],
            'percentual': fmt_pct(100 * v / total if total else 0),
            'votos': fmt_int(v),
            'eleito': situacao.get((turno, sq), '').startswith('ELEITO')
        } for sq, v in ordenado]

    def ranking_partidos(contagem_partidos, top):
        total = sum(contagem_partidos.values())
        ordenado = sorted(contagem_partidos.items(), key=lambda kv: kv[1], reverse=True)[:top]
        return [{'nome': p, 'partido': p, 'percentual': fmt_pct(100 * n / total if total else 0),
                 'votos': fmt_int(n), 'eleito': False} for p, n in ordenado]

    def somar(dic_muns):
        tot = defaultdict(int)
        for por_cand in dic_muns.values():
            for sq, v in por_cand.items():
                tot[sq] += v
        return tot

    def codigo_ibge(mun, uf, nome):
        cod = tse_ibge.get(mun)
        if cod:
            return cod
        return nomes_geo.get(uf, {}).get(normalizar(nome))

    sem_ibge = set()
    for turno in sorted(votos):
        base = os.path.join(DIR_DADOS, str(ano), f't{turno}')
        por_cargo = votos[turno]
        cargo_mapa = 'presidente' if tipo == 'geral' else 'prefeito'
        dados_mapa = por_cargo.get(cargo_mapa, {})

        estados_json, brasil = {}, {}
        lideres_br = defaultdict(int)
        total_muns_br = 0

        if tipo == 'geral':
            brasil = {'apurado': '100,00', 'metrica': 'votos',
                      'candidatos': ranking(somar({f'{uf}{m}': c for uf, ms in dados_mapa.items() for m, c in ms.items()}), turno, 5)}
            for uf in ESTADOS:
                if uf in dados_mapa:
                    estados_json[uf] = {'apurado': '100,00', 'candidatos': ranking(somar(dados_mapa[uf]), turno, 5)}
        else:
            for uf in ESTADOS:
                lideres_uf = defaultdict(int)
                for mun, por_cand in dados_mapa.get(uf, {}).items():
                    if not por_cand:
                        continue
                    sq_lider = max(por_cand, key=por_cand.get)
                    partido = candidatos[sq_lider]['partido']
                    lideres_uf[partido] += 1
                    lideres_br[partido] += 1
                    total_muns_br += 1
                if lideres_uf:
                    estados_json[uf] = {'apurado': '100,00', 'candidatos': ranking_partidos(lideres_uf, 5)}
            brasil = {'apurado': '100,00', 'metrica': 'municipios', 'total_municipios': total_muns_br,
                      'candidatos': ranking_partidos(lideres_br, 5)}

        salvar(os.path.join(base, 'brasil.json'), brasil)
        salvar(os.path.join(base, 'estados.json'), estados_json)

        for uf in ESTADOS:
            # --- mapa municipal (chave IBGE)
            mapa_uf = {}
            for mun, por_cand in dados_mapa.get(uf, {}).items():
                nome = nomes_mun.get(mun, (uf, ''))[1]
                ibge = codigo_ibge(mun, uf, nome)
                if not ibge:
                    sem_ibge.add(f'{uf}/{nome}')
                    continue
                mapa_uf[ibge] = {'tse': mun, 'nome': nome, 'apurado': '100,00',
                                 'candidatos': ranking(por_cand, turno, TOP_MAPA)}
            if mapa_uf:
                salvar(os.path.join(base, 'mapa', f'{uf}.json'), mapa_uf)

            # --- detalhes (estado + cada município)
            muns_uf = set()
            for cargo, por_uf in por_cargo.items():
                muns_uf.update(por_uf.get(uf, {}).keys())
            if not muns_uf:
                continue

            det_estado = {}
            if tipo == 'geral':
                for cargo in ('governador', 'senador', 'dep_federal', 'dep_estadual'):
                    if uf in por_cargo.get(cargo, {}):
                        det_estado[cargo] = {'apurado': '100,00',
                                             'candidatos': ranking(somar(por_cargo[cargo][uf]), turno, TOP_DETALHE)}
            elif uf in estados_json:
                todos = defaultdict(int)
                for por_cand in dados_mapa.get(uf, {}).values():
                    if por_cand:
                        todos[candidatos[max(por_cand, key=por_cand.get)]['partido']] += 1
                det_estado['partidos'] = {'apurado': '100,00', 'candidatos': ranking_partidos(todos, TOP_PARTIDOS_UF)}

            det_muns = {}
            for mun in muns_uf:
                cargos_mun = {}
                for cargo, por_uf in por_cargo.items():
                    por_cand = por_uf.get(uf, {}).get(mun)
                    if por_cand:
                        cargos_mun[cargo] = {'apurado': '100,00', 'candidatos': ranking(por_cand, turno, TOP_DETALHE)}
                det_muns[mun] = {'cargos': cargos_mun}
            salvar(os.path.join(base, 'detalhes', f'{uf}.json'), {'estado': {'cargos': det_estado}, 'municipios': det_muns})

            # --- lista para a busca
            lista = []
            for mun in muns_uf:
                nome = nomes_mun.get(mun, (uf, ''))[1]
                lista.append({'codigo': mun, 'ibge': codigo_ibge(mun, uf, nome), 'nome': nome.upper()})
            lista.sort(key=lambda m: normalizar(m['nome']))
            salvar(os.path.join(base, 'municipios', f'{uf}.json'), lista)

        salvar(os.path.join(base, 'info.json'), {
            'ano': ano, 'turno': turno, 'tipo': tipo,
            'fonte': 'Portal de Dados Abertos do TSE (votacao_candidato_munzona)',
            'gerado_em': time.strftime('%Y-%m-%d %H:%M')
        })
        print(f"  ✔ dados/{ano}/t{turno} gerado")

    if sem_ibge:
        print(f"  [aviso] {len(sem_ibge)} municípios sem código IBGE (ficam fora do mapa, mas aparecem na busca): "
              f"{sorted(sem_ibge)[:10]}{' ...' if len(sem_ibge) > 10 else ''}")


# ===================== GEOMETRIAS ATUAIS DO IBGE (opcional) =====================
def baixar_geometrias_ibge():
    """Substitui static/municipios/*.json pela malha municipal atual do IBGE (inclui municípios criados após 2010)."""
    os.makedirs(DIR_GEO_MUN, exist_ok=True)
    for uf in ESTADOS:
        nomes = requests.get(f'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios', timeout=30).json()
        nomes = {str(m['id']): m['nome'] for m in nomes}
        geo = requests.get(f'https://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}',
                           params={'formato': 'application/vnd.geo+json', 'intrarregiao': 'municipio', 'qualidade': 'minima'},
                           timeout=60).json()

        def arred(c):
            return [round(c[0], 4), round(c[1], 4)] if isinstance(c[0], (int, float)) else [arred(x) for x in c]

        feats = []
        for ft in geo['features']:
            cod = str(ft['properties'].get('codarea'))
            feats.append({'type': 'Feature', 'properties': {'id': cod, 'name': nomes.get(cod, cod)},
                          'geometry': {'type': ft['geometry']['type'], 'coordinates': arred(ft['geometry']['coordinates'])}})
        salvar(os.path.join(DIR_GEO_MUN, f'{uf}.json'), {'type': 'FeatureCollection', 'features': feats})
        print(f"  {uf}: {len(feats)} municípios")


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    if '--geometrias' in args:
        baixar_geometrias_ibge()
        args.remove('--geometrias')
    for a in args:
        processar_ano(int(a))