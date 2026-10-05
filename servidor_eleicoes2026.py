import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
CORS(app)

ESTADOS = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
           'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 
           'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

CODIGO_PASTA_FED = "6257"
CODIGO_ELEICAO_FED = "e006257"

CODIGO_PASTA_EST = "6259"
CODIGO_ELEICAO_EST = "e006259"

ARQUIVO_CACHE = "dados_cache.json"

# Inicialização do Cache: Tenta ler o arquivo local se ele já existir
dados_cache = {
    "ultima_atualizacao": "Inicializando...",
    "brasil": {"apurado": "0,00", "candidatos": []},
    "estados": {}
}

if os.path.exists(ARQUIVO_CACHE):
    try:
        with open(ARQUIVO_CACHE, 'r', encoding='utf-8') as f:
            dados_carregados = json.load(f)
            if "brasil" in dados_carregados and "estados" in dados_carregados:
                dados_cache = dados_carregados
                print("  [CACHE] Dados anteriores do disco carregados com sucesso!")
    except Exception as e:
        print(f"  [CACHE ERRO] Falha ao ler arquivo local: {e}")

quarentena_404 = {}
cache_municipios = {}

def processar_json_tse(data, limit=5):
    pst = data.get('s', {}).get('pst', '0,00')
    todos_candidatos = []
    
    cargos = data.get('carg', [])
    if cargos:
        for agr in cargos[0].get('agr', []):
            for par in agr.get('par', []):
                for c in par.get('cand', []):
                    votos_int = int(c.get('vap', 0))
                    todos_candidatos.append({
                        'nome': c.get('nmu', c.get('nm', 'Desconhecido')),
                        'partido': par.get('sg', ''),
                        'percentual': c.get('pvap', '0,00'),
                        'votos': f"{votos_int:,}".replace(",", "."),
                        'votos_raw': votos_int,
                        'eleito': c.get('e', 'n') == 's'
                    })
    
    todos_candidatos.sort(key=lambda x: x['votos_raw'], reverse=True)
    for c in todos_candidatos:
        del c['votos_raw']
        
    return pst, todos_candidatos[:limit]

def buscar_abrangencia(local):
    sigla = local.lower()
    agora = time.time()
    
    if local in quarentena_404 and agora < quarentena_404[local]:
        return local, {'apurado': '0,00', 'candidatos': []}

    timestamp_ms = int(agora * 1000)
    url = f"https://resultados.tse.jus.br/oficial/ele2026/{CODIGO_PASTA_FED}/dados/{sigla}/{sigla}-c0001-{CODIGO_ELEICAO_FED}-u.json?t={timestamp_ms}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cache-Control': 'no-cache, no-store, must-revalidate'
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            pst, candidatos = processar_json_tse(resp.json(), limit=5)
            return local, {'apurado': pst, 'candidatos': candidatos}
        elif resp.status_code == 404:
            quarentena_404[local] = agora + 60
    except Exception as e:
        print(f"  [ERRO] {local}: {e}")
        
    return local, {'apurado': '0,00', 'candidatos': []}

def recolher_dados():
    global dados_cache
    while True:
        hora_atual = time.strftime("%H:%M:%S")
        locais = ['BR'] + ESTADOS
        
        # Consulta de forma concorrente e atualiza o cache progressivamente
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_local = {executor.submit(buscar_abrangencia, loc): loc for loc in locais}
            
            for future in as_completed(future_to_local):
                local = future_to_local[future]
                try:
                    local_res, dados = future.result()
                    if local_res == 'BR':
                        dados_cache["brasil"] = dados
                    else:
                        dados_cache["estados"][local_res] = dados
                    
                    dados_cache["ultima_atualizacao"] = hora_atual
                except Exception as e:
                    print(f"  [ERRO EXECUTOR] {local}: {e}")

        # Salva o arquivo de cache no disco para próximos deploys/restarts
        try:
            with open(ARQUIVO_CACHE, 'w', encoding='utf-8') as f:
                json.dump(dados_cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        time.sleep(60)

thread = threading.Thread(target=recolher_dados, daemon=True)
thread.start()

# --- ROTAS FLASK ---

@app.route('/')
def index():
    return send_from_directory('.', 'eleicoes2026.html')

@app.route('/api/votacao', methods=['GET'])
def obter_votacao():
    return jsonify(dados_cache)

# ROTA MUNICÍPIOS
@app.route('/api/municipios/<uf>', methods=['GET'])
def obter_municipios(uf):
    uf_upper = uf.upper()
    if uf_upper in cache_municipios:
        return jsonify(cache_municipios[uf_upper])
        
    timestamp_ms = int(time.time() * 1000)
    urls = [
        f"https://resultados.tse.jus.br/oficial/ele2026/{CODIGO_PASTA_FED}/config/mun-{CODIGO_ELEICAO_FED}-cm.json?t={timestamp_ms}",
        f"https://resultados.tse.jus.br/oficial/ele2026/comum/config/mun-{CODIGO_ELEICAO_FED}-cm.json?t={timestamp_ms}",
        f"https://resultados.tse.jus.br/oficial/ele2026/{CODIGO_PASTA_EST}/config/mun-{CODIGO_ELEICAO_EST}-cm.json?t={timestamp_ms}"
    ]
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                abrangencias = data.get('abr', []) or data.get('ufs', [])
                for abr in abrangencias:
                    sigla = abr.get('cd', '') or abr.get('sg', '')
                    muns_raw = abr.get('mu', []) or abr.get('muns', [])
                    
                    lista_muns = []
                    for m in muns_raw:
                        codigo = str(m.get('cd', '') or m.get('c', '')).zfill(5)
                        nome = m.get('nm', '') or m.get('n', '')
                        if codigo and nome:
                            lista_muns.append({'codigo': codigo, 'nome': nome.upper()})
                    
                    lista_muns.sort(key=lambda x: x['nome'])
                    if sigla:
                        cache_municipios[sigla.upper()] = lista_muns
                        
                if uf_upper in cache_municipios:
                    return jsonify(cache_municipios[uf_upper])
        except Exception as e:
            print(f"  [ERRO MUNICÍPIOS] {url}: {e}")
            
    return jsonify([])

# ENDPOINT ESTADO COMPLETO
@app.route('/api/detalhes/<uf>', methods=['GET'])
def obter_detalhes_estado(uf):
    sigla = uf.lower()
    timestamp_ms = int(time.time() * 1000)
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    cargos_codigos = {
        'governador': ('6259', 'c0003', 'e006259'),
        'senador': ('6259', 'c0005', 'e006259'),
        'dep_federal': ('6259', 'c0006', 'e006259'),
        'dep_estadual': ('6259', 'c0008' if uf.upper() == 'DF' else 'c0007', 'e006259')
    }
    
    resultado_detalhado = {'uf': uf.upper(), 'cargos': {}}
    
    for cargo_nome, (pasta, codigo_c, eleicao_c) in cargos_codigos.items():
        url = f"https://resultados.tse.jus.br/oficial/ele2026/{pasta}/dados/{sigla}/{sigla}-{codigo_c}-{eleicao_c}-u.json?t={timestamp_ms}"
        try:
            resp = requests.get(url, headers=headers, timeout=4)
            if resp.status_code == 200:
                pst, cands = processar_json_tse(resp.json(), limit=5)
                resultado_detalhado['cargos'][cargo_nome] = {'apurado': pst, 'candidatos': cands}
            else:
                resultado_detalhado['cargos'][cargo_nome] = {'apurado': '0,00', 'candidatos': []}
        except Exception:
            resultado_detalhado['cargos'][cargo_nome] = {'apurado': '0,00', 'candidatos': []}
            
    return jsonify(resultado_detalhado)

# ENDPOINT MUNICÍPIO ESPECÍFICO
@app.route('/api/detalhes/<uf>/<cd_mun>', methods=['GET'])
def obter_detalhes_municipio(uf, cd_mun):
    sigla = uf.lower()
    cd_mun_5 = str(cd_mun).zfill(5)
    timestamp_ms = int(time.time() * 1000)
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    cargos_codigos = {
        'presidente': ('6257', 'c0001', 'e006257'),
        'governador': ('6259', 'c0003', 'e006259'),
        'senador': ('6259', 'c0005', 'e006259'),
        'dep_federal': ('6259', 'c0006', 'e006259'),
        'dep_estadual': ('6259', 'c0008' if uf.upper() == 'DF' else 'c0007', 'e006259')
    }
    
    resultado_detalhado = {'uf': uf.upper(), 'municipio_codigo': cd_mun_5, 'cargos': {}}
    
    for cargo_nome, (pasta, codigo_c, eleicao_c) in cargos_codigos.items():
        url = f"https://resultados.tse.jus.br/oficial/ele2026/{pasta}/dados/{sigla}/{sigla}{cd_mun_5}-{codigo_c}-{eleicao_c}-u.json?t={timestamp_ms}"
        try:
            resp = requests.get(url, headers=headers, timeout=4)
            if resp.status_code == 200:
                pst, cands = processar_json_tse(resp.json(), limit=5)
                resultado_detalhado['cargos'][cargo_nome] = {'apurado': pst, 'candidatos': cands}
            else:
                resultado_detalhado['cargos'][cargo_nome] = {'apurado': '0,00', 'candidatos': []}
        except Exception:
            resultado_detalhado['cargos'][cargo_nome] = {'apurado': '0,00', 'candidatos': []}
            
    return jsonify(resultado_detalhado)

if __name__ == '__main__':
    porta = int(os.environ.get("PORT", 5001))
    app.run(host='0.0.0.0', port=porta)
