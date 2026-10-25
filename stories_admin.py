"""
Stories editáveis pelo navegador, sem deploy.

Para você (senha = ANUNCIOS_CHAVE ou RELATORIO_CHAVE):
    /admin/stories?chave=...      criar, editar, reordenar, pausar e apagar stories

Para o site:
    GET /api/stories              pacote de stories ativos (mesmo formato do antigo static/stories.json)

Armazenamento: Upstash (chave "stories:pacote") ou, sem Upstash, dados_stories.json.
Na primeira leitura, importa static/stories.json (o arquivo continua como reserva e para restaurar).
"""
import hmac
import html
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse

BRT = timezone(timedelta(hours=-3))
PASTA = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_ORIGINAL = os.path.join(PASTA, 'static', 'stories.json')
ARQUIVO_LOCAL = os.path.join(PASTA, 'dados_stories.json')
CHAVE = 'stories:pacote'
MAX_TITULO, MAX_TEXTO, MAX_CATEGORIA = 60, 420, 24
PALETA = {
    'verde-azulado': ['#0f766e', '#134e4a'], 'indigo': ['#4338ca', '#312e81'], 'violeta': ['#6d28d9', '#4c1d95'],
    'ardosia': ['#475569', '#1e293b'], 'ambar': ['#b45309', '#78350f'], 'mostarda': ['#a16207', '#713f12'],
    'azul-ceu': ['#0369a1', '#0c4a6e'], 'ciano': ['#0e7490', '#164e63'], 'verde': ['#15803d', '#14532d'], 'grafite': ['#334155', '#0f172a'],
}
NOMES_CORES = {'verde-azulado': 'Verde-azulado', 'indigo': 'Índigo', 'violeta': 'Violeta', 'ardosia': 'Ardósia', 'ambar': 'Âmbar',
               'mostarda': 'Mostarda', 'azul-ceu': 'Azul-céu', 'ciano': 'Ciano', 'verde': 'Verde', 'grafite': 'Grafite'}


def agora_texto():
    d = datetime.now(BRT)
    return d.strftime('%d/%m/%Y, %Hh%M'), d.strftime('%b %d, %Y, %H:%M')


class Banco:
    def __init__(self):
        url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
        self.up = None
        if url and token:
            from acessos import ArmazenamentoUpstash
            self.up = ArmazenamentoUpstash(url, token)
        self.lock = threading.Lock()

    def ler(self):
        if self.up:
            bruto = self.up._pipeline([['GET', CHAVE]])[0]
            return json.loads(bruto) if bruto else None
        with self.lock:
            if os.path.exists(ARQUIVO_LOCAL):
                with open(ARQUIVO_LOCAL, encoding='utf-8') as f:
                    return json.load(f)
        return None

    def gravar(self, pacote):
        texto = json.dumps(pacote, ensure_ascii=False)
        if self.up:
            self.up._pipeline([['SET', CHAVE, texto]])
            return
        with self.lock:
            with open(ARQUIVO_LOCAL, 'w', encoding='utf-8') as f:
                f.write(texto)


def original():
    with open(ARQUIVO_ORIGINAL, encoding='utf-8') as f:
        p = json.load(f)
    for st in p['stories']:
        st.setdefault('ativo', True)
    return p


def url_valida(u):
    if not u:
        return True
    if u.startswith('#'):
        return u in ('#apuracao', '#analises', '#evolucao', '#acessos')
    try:
        r = urlparse(u)
        return r.scheme in ('http', 'https') and bool(r.netloc)
    except Exception:
        return False


def chave_cor(cor):
    return next((k for k, v in PALETA.items() if v == cor), 'grafite')


# ===================== PÁGINA =====================
def pagina(p, chave, editar=None, aviso=''):
    e = html.escape
    ch = e(chave)
    st_ed = next((s for s in p['stories'] if s['id'] == editar), None) if editar else None
    v = lambda campo: e(str((st_ed or {}).get(campo, '') or ''))
    cor_atual = chave_cor(st_ed['cor']) if st_ed else 'verde-azulado'
    opcoes_cor = ''.join(f'<option value="{k}"{" selected" if k == cor_atual else ""}>{NOMES_CORES[k]}</option>' for k in PALETA)
    tipo_auto = st_ed and st_ed.get('tipo') == 'primeiro_turno'

    cards = []
    total = len(p['stories'])
    for i, s in enumerate(p['stories']):
        ativo = s.get('ativo', True)
        partes = []
        for acao, rot, titulo, desab in [('subir', '↑', 'Subir', i == 0), ('descer', '↓', 'Descer', i == total - 1),
                                          ('alternar', 'Pausar' if ativo else 'Ativar', '', False), ('remover', 'Apagar', '', False)]:
            extra = ' class="perigo" onclick="return confirm(&quot;Apagar este story?&quot;)"' if acao == 'remover' else ''
            partes.append(f'<form method="post" action="/admin/stories/{e(s["id"])}/{acao}" style="display:inline">'
                          f'<input type="hidden" name="chave" value="{ch}"><button {"disabled" if desab else ""} title="{titulo}"{extra}>{rot}</button></form>')
        botoes = ''.join(partes)
        cards.append(f"""<div class="card{'' if ativo else ' pausado'}" style="background:linear-gradient(160deg,{e(s['cor'][0])},{e(s['cor'][1])})">
<div class="cat">{e(s.get('emoji', ''))} {e(s.get('categoria', ''))} · {i + 1}/{total}{'' if ativo else ' · PAUSADO'}</div>
<div class="tit">{e(s.get('titulo', ''))}</div><div class="txt">{e(s.get('texto', ''))}</div>
<div class="fonte">Fonte: {e(s.get('fonte', ''))}{' · <b>com inglês</b>' if s.get('titulo_en') else ' · sem inglês'}{' · números automáticos do 1º turno' if s.get('tipo') == 'primeiro_turno' else ''}</div>
<div class="acoes"><a class="bt" href="/admin/stories?chave={ch}&editar={e(s['id'])}#form">Editar</a>{botoes}</div></div>""")

    return f"""<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Stories – administração</title>
<style>body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f0f2f5;color:#1c1e21;margin:0;padding:18px}}
.caixa{{background:#fff;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,.05);padding:16px;margin-bottom:16px}}
.grade{{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:14px}}
.card{{color:#fff;border-radius:14px;padding:16px;display:flex;flex-direction:column;gap:6px;min-height:220px}}
.card.pausado{{opacity:.45}}.cat{{font-size:11px;font-weight:700;text-transform:uppercase;opacity:.9}}
.tit{{font-size:17px;font-weight:800}}.txt{{font-size:12.5px;line-height:1.45;white-space:pre-line;flex:1}}.fonte{{font-size:11px;opacity:.85}}
.acoes{{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}}
button,a.bt{{border:none;background:rgba(255,255,255,.92);color:#1c1e21;border-radius:6px;padding:5px 10px;font-weight:700;font-size:12px;cursor:pointer;text-decoration:none}}
button:disabled{{opacity:.4;cursor:default}}button.perigo{{background:#fde2e2;color:#b91c1c}}
label{{display:block;font-size:12px;font-weight:700;margin:10px 0 4px}}input,select,textarea{{padding:8px 10px;border:1px solid #ced0d4;border-radius:6px;font-size:14px;width:100%;box-sizing:border-box;font-family:inherit}}
textarea{{min-height:110px;resize:vertical}}.cont{{font-size:11px;color:#65676b;text-align:right}}.cont.passou{{color:#b91c1c;font-weight:700}}
.duas{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:0 14px}}
.principal{{background:#0056b3!important;color:#fff!important;padding:10px 18px!important;font-size:14px!important;margin-top:14px}}
.aviso{{background:#e7f6ec;border:1px solid #9bd5ae;border-radius:8px;padding:10px 14px;margin-bottom:14px}}
.nav a{{font-size:13px;margin-right:14px}}details summary{{cursor:pointer;font-weight:700;margin-top:12px}}</style></head><body>
<h2 style="margin:0 0 6px">📰 Stories do site</h2>
<p class="nav"><a href="/?ano={p['eleicao']['ano']}&turno={p['eleicao']['turno']}#analises" target="_blank">Ver no site →</a>
<a href="/admin/publicidade?chave={ch}">Publicidade</a><a href="/admin/anuncios?chave={ch}">Interessados em anunciar</a></p>
{f'<div class="aviso">{e(aviso)}</div>' if aviso else ''}
<div class="caixa"><b>{total}</b> stories ({sum(1 for s in p['stories'] if s.get('ativo', True))} ativos) · aparecem na eleição <b>{p['eleicao']['ano']}, {p['eleicao']['turno']}º turno</b> ·
atualizado em <b>{e(p.get('atualizado', ''))}</b></div>
<div class="grade">{''.join(cards) or '<p>Nenhum story.</p>'}</div>

<div class="caixa" id="form" style="margin-top:16px"><h3 style="margin-top:0">{'✏️ Editar story' if st_ed else '➕ Novo story'}</h3>
<form method="post" action="/admin/stories/salvar" onsubmit="return conferir(this)"><input type="hidden" name="chave" value="{ch}"><input type="hidden" name="id" value="{v('id')}">
<div class="duas">
<div><label>Categoria (rótulo da bolinha)</label><input name="categoria" maxlength="{MAX_CATEGORIA}" required value="{v('categoria')}" placeholder="Ex.: Pesquisas"></div>
<div><label>Emoji</label><input name="emoji" maxlength="8" required value="{v('emoji') or '📰'}"></div>
<div><label>Cor</label><select name="cor">{opcoes_cor}</select></div>
<div><label>Tipo</label><select name="tipo"><option value="">Texto normal</option><option value="primeiro_turno"{' selected' if tipo_auto else ''}>Texto + números automáticos do 1º turno</option></select></div>
</div>
<label>Título</label><input name="titulo" maxlength="{MAX_TITULO}" required value="{v('titulo')}" oninput="contar(this)"><div class="cont"></div>
<label>Texto (use Enter para quebrar linha)</label><textarea name="texto" maxlength="{MAX_TEXTO}" required oninput="contar(this)">{v('texto')}</textarea><div class="cont"></div>
<div class="duas">
<div><label>Fonte (nome do veículo)</label><input name="fonte" maxlength="60" required value="{v('fonte')}" placeholder="Ex.: Agência Brasil"></div>
<div><label>Link da matéria (https://...) ou aba do site (#evolucao)</label><input name="url" maxlength="400" value="{v('url')}" placeholder="https://"></div>
</div>
<details{' open' if st_ed and st_ed.get('titulo_en') else ''}><summary>🇺🇸 Versão em inglês (opcional — sem ela, estrangeiros veem o texto em português)</summary>
<div class="duas"><div><label>Categoria em inglês</label><input name="categoria_en" maxlength="{MAX_CATEGORIA}" value="{v('categoria_en')}"></div>
<div><label>Título em inglês</label><input name="titulo_en" maxlength="{MAX_TITULO}" value="{v('titulo_en')}" oninput="contar(this)"><div class="cont"></div></div></div>
<label>Texto em inglês</label><textarea name="texto_en" maxlength="{MAX_TEXTO}" oninput="contar(this)">{v('texto_en')}</textarea><div class="cont"></div></details>
<button class="principal">{'Salvar alterações' if st_ed else 'Publicar story'}</button>
{f'<a class="bt" href="/admin/stories?chave={ch}" style="background:#e4e6eb;margin-left:8px">Cancelar edição</a>' if st_ed else ''}
</form><p style="font-size:12px;color:#65676b">Entra no ar em até 30 segundos. Mantenha a neutralidade: resumo próprio, sem copiar trechos da matéria, e sempre com a fonte.</p></div>

<div class="caixa"><h3 style="margin-top:0">⚙️ Configurações</h3>
<form method="post" action="/admin/stories/config"><input type="hidden" name="chave" value="{ch}"><div class="duas">
<div><label>Ano da eleição em que os stories aparecem</label><input type="number" name="ano" value="{p['eleicao']['ano']}" min="2018" max="2100"></div>
<div><label>Turno</label><select name="turno"><option value="1"{' selected' if p['eleicao']['turno'] == 1 else ''}>1º turno</option><option value="2"{' selected' if p['eleicao']['turno'] == 2 else ''}>2º turno</option></select></div>
</div><label>Aviso no fim da aba Análises</label><input name="aviso" maxlength="200" value="{e(p.get('aviso', ''))}">
<button class="principal">Salvar configurações</button></form>
<form method="post" action="/admin/stories/restaurar" onsubmit="return confirm('Substituir todos os stories pelos do arquivo static/stories.json?')" style="margin-top:12px">
<input type="hidden" name="chave" value="{ch}"><button class="perigo">Restaurar stories do arquivo original</button></form></div>
<script>
function contar(el) {{
  const c = el.nextElementSibling; if (!c || !c.classList.contains('cont')) return;
  const max = Number(el.getAttribute('maxlength')); c.textContent = el.value.length + ' / ' + max; c.classList.toggle('passou', el.value.length > max * 0.9);
}}
document.querySelectorAll('[oninput]').forEach(contar);
function conferir(f) {{
  const u = f.url.value.trim();
  if (u && !/^https?:\\/\\/./.test(u) && !['#apuracao','#analises','#evolucao','#acessos'].includes(u)) {{ alert('O link precisa começar com https:// ou ser uma aba do site (#apuracao, #analises, #evolucao, #acessos).'); return false; }}
  return true;
}}
</script></body></html>"""


# ===================== ROTAS =====================
def iniciar(app, responder, comprimir):
    from flask import Response, redirect, request

    banco = Banco()
    cache = {'momento': 0, 'gz': None}

    def senha_ok(valor):
        chave = os.environ.get('ANUNCIOS_CHAVE') or os.environ.get('RELATORIO_CHAVE')
        return bool(chave) and hmac.compare_digest(valor or '', chave)

    def pacote():
        try:
            p = banco.ler()
        except Exception as ex:
            print(f"[stories] erro ao ler: {ex}", flush=True)
            p = None
        if not p:
            p = original()
            try:
                banco.gravar(p)   # primeira vez: importa o arquivo
            except Exception:
                pass
        return p

    def salvar(p):
        p['atualizado'], p['atualizado_en'] = agora_texto()
        banco.gravar(p)
        cache['momento'] = 0

    def voltar(chave, aviso, editar=''):
        extra = f"&editar={quote(editar)}#form" if editar else ''
        return redirect(f"/admin/stories?chave={quote(chave)}&aviso={quote(aviso)}{extra}", code=303)

    @app.route('/api/stories')
    def api_stories():
        if time.time() - cache['momento'] > 30 or cache['gz'] is None:
            p = pacote()
            publico = {k: v for k, v in p.items() if k != 'stories'}
            publico['stories'] = [s for s in p['stories'] if s.get('ativo', True)]
            cache.update(momento=time.time(), gz=comprimir(publico))
        resp = responder(cache['gz'])
        resp.headers['Cache-Control'] = 'public, max-age=30'
        return resp

    @app.route('/admin/stories')
    def admin_stories():
        chave = request.args.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        resp = Response(pagina(pacote(), chave, request.args.get('editar'), request.args.get('aviso', '')), mimetype='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/admin/stories/salvar', methods=['POST'])
    def admin_salvar():
        f = request.form
        chave = f.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        campos = {k: (f.get(k) or '').strip() for k in ('categoria', 'emoji', 'titulo', 'texto', 'fonte', 'url', 'categoria_en', 'titulo_en', 'texto_en')}
        campos['texto'] = campos['texto'].replace('\r\n', '\n')
        campos['texto_en'] = campos['texto_en'].replace('\r\n', '\n')
        if not (campos['categoria'] and campos['titulo'] and campos['texto'] and campos['fonte']):
            return voltar(chave, 'Preencha categoria, título, texto e fonte.', f.get('id', ''))
        if not url_valida(campos['url']):
            return voltar(chave, 'O link precisa começar com https:// ou ser uma aba do site (#evolucao, #analises...).', f.get('id', ''))
        campos['categoria'] = campos['categoria'][:MAX_CATEGORIA]; campos['titulo'] = campos['titulo'][:MAX_TITULO]; campos['texto'] = campos['texto'][:MAX_TEXTO]
        campos['cor'] = PALETA.get(f.get('cor'), PALETA['grafite'])
        p = pacote()
        sid = f.get('id') or ''
        existente = next((s for s in p['stories'] if s['id'] == sid), None)
        if existente:
            for k in ('categoria_en', 'titulo_en', 'texto_en'):
                existente.pop(k, None)
            existente.update({k: v for k, v in campos.items() if v or not k.endswith('_en')})
            existente['tipo'] = f.get('tipo') or None
            if not existente['tipo']:
                existente.pop('tipo')
            msg = f"Story \"{campos['titulo']}\" atualizado."
        else:
            novo = {'id': secrets.token_hex(4), 'ativo': True, **{k: v for k, v in campos.items() if v or not k.endswith('_en')}}
            if f.get('tipo'):
                novo['tipo'] = f.get('tipo')
            p['stories'].append(novo)
            msg = f"Story \"{campos['titulo']}\" publicado (último da fila; use ↑ para subir)."
        salvar(p)
        return voltar(chave, msg)

    @app.route('/admin/stories/<sid>/<acao>', methods=['POST'])
    def admin_acao(sid, acao):
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        p = pacote()
        lista = p['stories']
        i = next((k for k, s in enumerate(lista) if s['id'] == sid), None)
        if i is None:
            return voltar(chave, 'Story não encontrado.')
        if acao == 'subir' and i > 0:
            lista[i - 1], lista[i] = lista[i], lista[i - 1]
        elif acao == 'descer' and i < len(lista) - 1:
            lista[i + 1], lista[i] = lista[i], lista[i + 1]
        elif acao == 'alternar':
            lista[i]['ativo'] = not lista[i].get('ativo', True)
        elif acao == 'remover':
            lista.pop(i)
        else:
            return voltar(chave, 'Ação desconhecida.')
        salvar(p)
        return voltar(chave, {'subir': 'Story movido para cima.', 'descer': 'Story movido para baixo.', 'remover': 'Story apagado.',
                              'alternar': 'Story atualizado.'}[acao])

    @app.route('/admin/stories/config', methods=['POST'])
    def admin_config():
        f = request.form
        chave = f.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        p = pacote()
        try:
            p['eleicao'] = {'ano': int(f.get('ano')), 'turno': 2 if f.get('turno') == '2' else 1}
        except (TypeError, ValueError):
            return voltar(chave, 'Ano inválido.')
        p['aviso'] = (f.get('aviso') or '').strip()[:200]
        salvar(p)
        return voltar(chave, 'Configurações salvas.')

    @app.route('/admin/stories/restaurar', methods=['POST'])
    def admin_restaurar():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        salvar(original())
        return voltar(chave, 'Stories restaurados a partir do arquivo original.')
