"""
Espaços publicitários do site: topo, abertura (popup), laterais e rodapé.

Para você (protegido por senha = ANUNCIOS_CHAVE ou RELATORIO_CHAVE):
    /admin/publicidade?chave=...   sobe imagem, escolhe espaço, link e período; pausa/remove; vê impressões e cliques

Para o site:
    GET  /api/publicidade          anúncios ativos por espaço (sem as imagens)
    GET  /pub/img/<id>             imagem do anúncio (em cache)
    GET  /pub/clique/<id>          conta o clique e redireciona para o site do anunciante
    POST /api/pub/vistas           conta impressões (somadas em memória, gravadas 1x por minuto)

Armazenamento: Upstash (imagens em base64, até 600 KB cada) ou, sem Upstash, o arquivo dados_publicidade.json.
"""
import base64
import hmac
import html
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

BRT = timezone(timedelta(hours=-3))
ARQUIVO_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dados_publicidade.json')
ESPACOS = {
    'topo':     {'nome': 'Topo', 'tamanho': '970 × 90 px (aparece abaixo das abas, em todas as seções)'},
    'abertura': {'nome': 'Abertura (popup)', 'tamanho': '600 × 500 px (aparece ao abrir o site, no máximo 1x a cada 12h por pessoa)'},
    'lateral':  {'nome': 'Laterais', 'tamanho': '160 × 600 px (colunas fixas, só em telas largas)'},
    'rodape':   {'nome': 'Rodapé', 'tamanho': '970 × 90 px (fim de todas as páginas)'},
}
TIPOS_IMAGEM = {'image/png', 'image/jpeg', 'image/webp', 'image/gif'}
MAX_IMAGEM = 600 * 1024


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] [publicidade] {msg}", flush=True)


def hoje():
    return datetime.now(BRT).strftime('%Y-%m-%d')


# ===================== ARMAZENAMENTO =====================
class Banco:
    """Mesma interface para Upstash e para o arquivo local (teste)."""

    def __init__(self):
        url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
        self.up = None
        if url and token:
            from acessos import ArmazenamentoUpstash
            self.up = ArmazenamentoUpstash(url, token)
        self.lock = threading.Lock()

    # --- arquivo local
    def _ler(self):
        if os.path.exists(ARQUIVO_LOCAL):
            with open(ARQUIVO_LOCAL, encoding='utf-8') as f:
                return json.load(f)
        return {'criativos': {}, 'imagens': {}, 'stats': {}}

    def _gravar(self, d):
        with open(ARQUIVO_LOCAL, 'w', encoding='utf-8') as f:
            json.dump(d, f)

    # --- operações
    def criativos(self):
        if self.up:
            bruto = self.up._pipeline([['HGETALL', 'pub:criativos']])[0] or []
            if isinstance(bruto, dict):
                return {k: json.loads(v) for k, v in bruto.items()}
            return {bruto[i]: json.loads(bruto[i + 1]) for i in range(0, len(bruto), 2)}
        with self.lock:
            return self._ler()['criativos']

    def salvar_criativo(self, c, imagem_b64=None):
        if self.up:
            cmds = [['HSET', 'pub:criativos', c['id'], json.dumps(c, ensure_ascii=False)]]
            if imagem_b64:
                cmds.append(['SET', f"pub:img:{c['id']}", imagem_b64])
            self.up._pipeline(cmds)
            return
        with self.lock:
            d = self._ler()
            d['criativos'][c['id']] = c
            if imagem_b64:
                d['imagens'][c['id']] = imagem_b64
            self._gravar(d)

    def remover(self, cid):
        if self.up:
            self.up._pipeline([['HDEL', 'pub:criativos', cid], ['DEL', f'pub:img:{cid}'], ['DEL', f'pub:stats:{cid}']])
            return
        with self.lock:
            d = self._ler()
            for k in ('criativos', 'imagens', 'stats'):
                d[k].pop(cid, None)
            self._gravar(d)

    def imagem(self, cid):
        if self.up:
            return self.up._pipeline([['GET', f'pub:img:{cid}']])[0]
        with self.lock:
            return self._ler()['imagens'].get(cid)

    def somar_stats(self, contagens):
        """contagens: {id: {'imp': n, 'cli': n}}"""
        if not contagens:
            return
        if self.up:
            cmds = [['HINCRBY', f'pub:stats:{cid}', campo, n] for cid, c in contagens.items() for campo, n in c.items() if n]
            if cmds:
                self.up._pipeline(cmds)
            return
        with self.lock:
            d = self._ler()
            for cid, c in contagens.items():
                s = d['stats'].setdefault(cid, {'imp': 0, 'cli': 0})
                for campo, n in c.items():
                    s[campo] = s.get(campo, 0) + n
            self._gravar(d)

    def stats(self, ids):
        if not ids:
            return {}
        if self.up:
            res = self.up._pipeline([['HGETALL', f'pub:stats:{cid}'] for cid in ids])
            saida = {}
            for cid, bruto in zip(ids, res):
                bruto = bruto or []
                par = bruto if isinstance(bruto, dict) else {bruto[i]: bruto[i + 1] for i in range(0, len(bruto), 2)}
                saida[cid] = {'imp': int(par.get('imp', 0)), 'cli': int(par.get('cli', 0))}
            return saida
        with self.lock:
            st = self._ler()['stats']
        return {cid: {'imp': st.get(cid, {}).get('imp', 0), 'cli': st.get(cid, {}).get('cli', 0)} for cid in ids}


def ativo_hoje(c):
    d = hoje()
    return c.get('ativo') and (not c.get('inicio') or c['inicio'] <= d) and (not c.get('fim') or d <= c['fim'])


def link_valido(link):
    try:
        u = urlparse(link)
        return u.scheme in ('http', 'https') and bool(u.netloc)
    except Exception:
        return False


# ===================== PÁGINA DE ADMINISTRAÇÃO =====================
def pagina_admin(criativos, stats, chave, aviso=''):
    e = html.escape
    ch = e(chave)
    blocos = []
    for slot, info in ESPACOS.items():
        itens = sorted([c for c in criativos.values() if c['slot'] == slot], key=lambda c: c.get('criado', ''), reverse=True)
        linhas = []
        for c in itens:
            st = stats.get(c['id'], {'imp': 0, 'cli': 0})
            ctr = f"{100 * st['cli'] / st['imp']:.2f}%".replace('.', ',') if st['imp'] else '—'
            estado = 'no ar' if ativo_hoje(c) else ('pausado' if not c.get('ativo') else 'fora do período')
            cor = {'no ar': '#28a745', 'pausado': '#65676b', 'fora do período': '#b45309'}[estado]
            periodo = f"{e(c.get('inicio') or 'já')} → {e(c.get('fim') or 'sem fim')}"
            n = lambda x: f"{x:,}".replace(',', '.')
            linhas.append(f"""<tr>
<td><img src="/pub/img/{e(c['id'])}?v={e(c.get('criado', ''))}" alt="" style="max-width:180px;max-height:90px;border:1px solid #e4e6eb;border-radius:4px"></td>
<td><b>{e(c.get('anunciante', ''))}</b><br><a href="{e(c['link'])}" target="_blank" rel="noopener" style="font-size:12px">{e(c['link'][:60])}</a></td>
<td>{periodo}<br><b style="color:{cor}">{estado}</b></td>
<td style="text-align:right">{n(st['imp'])}<br><small>impressões</small></td>
<td style="text-align:right">{n(st['cli'])}<br><small>cliques · {ctr}</small></td>
<td><form method="post" action="/admin/publicidade/{e(c['id'])}/alternar" style="display:inline"><input type="hidden" name="chave" value="{ch}">
<button>{'Pausar' if c.get('ativo') else 'Ativar'}</button></form>
<form method="post" action="/admin/publicidade/{e(c['id'])}/remover" style="display:inline" onsubmit="return confirm('Remover este anúncio?')">
<input type="hidden" name="chave" value="{ch}"><button class="perigo">Remover</button></form></td></tr>""")
        corpo = ''.join(linhas) or '<tr><td colspan="6" style="color:#65676b">Nenhum anúncio. Enquanto estiver vazio, este espaço mostra o convite "Anuncie aqui".</td></tr>'
        blocos.append(f"""<div class="caixa"><h3>{e(info['nome'])} <small>· tamanho sugerido: {e(info['tamanho'])}</small></h3>
<table><tr><th>Imagem</th><th>Anunciante / link</th><th>Período</th><th>Vistas</th><th>Cliques</th><th></th></tr>{corpo}</table></div>""")
    opcoes = ''.join(f'<option value="{k}">{e(v["nome"])} – {e(v["tamanho"].split(" (")[0])}</option>' for k, v in ESPACOS.items())
    return f"""<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Publicidade – administração</title>
<style>body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f0f2f5;color:#1c1e21;margin:0;padding:18px}}
.caixa{{background:#fff;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,.05);padding:16px;margin-bottom:16px;overflow-x:auto}}
h3 small{{font-weight:400;color:#65676b;font-size:12px}}table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{text-align:left;padding:8px;border-bottom:1px solid #e4e6eb;vertical-align:middle}}th{{font-size:11px;text-transform:uppercase;color:#65676b}}
label{{display:block;font-size:12px;font-weight:700;margin:10px 0 4px}}input,select{{padding:8px 10px;border:1px solid #ced0d4;border-radius:6px;font-size:14px;width:100%;box-sizing:border-box}}
.grade{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:0 14px}}
button{{border:none;background:#e4e6eb;border-radius:6px;padding:6px 10px;font-weight:700;cursor:pointer;margin:2px}}
button.principal{{background:#0056b3;color:#fff;padding:10px 18px;margin-top:14px}}button.perigo{{background:#fde2e2;color:#b91c1c}}
.aviso{{background:#e7f6ec;border:1px solid #9bd5ae;border-radius:8px;padding:10px 14px;margin-bottom:14px}}a.bt{{font-size:13px}}</style></head><body>
<h2 style="margin:0 0 6px">📢 Espaços publicitários</h2>
<p style="margin:0 0 14px;font-size:13px"><a class="bt" href="/admin/anuncios?chave={ch}">Ver interessados em anunciar →</a></p>
{f'<div class="aviso">{e(aviso)}</div>' if aviso else ''}
<div class="caixa"><h3>➕ Novo anúncio</h3>
<form method="post" action="/admin/publicidade/novo" enctype="multipart/form-data" onsubmit="return conferirImagem(this)"><input type="hidden" name="chave" value="{ch}">
<div class="grade">
<div><label>Espaço</label><select name="slot" required>{opcoes}</select></div>
<div><label>Anunciante</label><input name="anunciante" maxlength="80" required placeholder="Ex.: Padaria Pão Quente"></div>
<div><label>Link de destino (https://...)</label><input name="link" maxlength="400" required placeholder="https://"></div>
<div><label>Texto alternativo (acessibilidade)</label><input name="alt" maxlength="120" placeholder="Ex.: Promoção de pão de queijo"></div>
<div><label>Início (opcional)</label><input type="date" name="inicio"></div>
<div><label>Fim (opcional)</label><input type="date" name="fim"></div>
<div><label>Imagem (PNG, JPG, WEBP ou GIF, até 600 KB)</label><input type="file" name="imagem" accept="image/png,image/jpeg,image/webp,image/gif" required></div>
</div><button class="principal">Publicar anúncio</button></form>
<p style="font-size:12px;color:#65676b">Entra no ar em até 1 minuto, sem deploy. Com mais de um anúncio no mesmo espaço, o site sorteia um a cada visita.</p></div>
{''.join(blocos)}
<p style="font-size:12px;color:#65676b">"Vistas" = vezes que o anúncio apareceu na tela; cliques passam por /pub/clique para serem contados. Números atualizam a cada minuto.</p>
<script>
function conferirImagem(form) {{
  const f = form.imagem.files[0];
  if (f && f.size > {MAX_IMAGEM}) {{ alert('Imagem grande demais (' + Math.round(f.size / 1024) + ' KB). O limite é 600 KB. Reduza em squoosh.app ou tinypng.com.'); return false; }}
  return true;
}}
</script>
</body></html>"""


# ===================== ROTAS =====================
def iniciar(app, responder, comprimir):
    from flask import Response, jsonify, redirect, request

    banco = Banco()
    cache = {'momento': 0, 'gz': None, 'criativos': {}}
    imagens = {}                       # id -> (bytes, tipo)
    pendentes, lock = {}, threading.Lock()
    ultima_gravacao = {'t': time.time()}
    app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024

    def senha_ok(valor):
        chave = os.environ.get('ANUNCIOS_CHAVE') or os.environ.get('RELATORIO_CHAVE')
        return bool(chave) and hmac.compare_digest(valor or '', chave)

    def recarregar():
        cache.update(momento=0)

    def ativos():
        if time.time() - cache['momento'] > 60 or cache['gz'] is None:
            try:
                todos = banco.criativos()
            except Exception as ex:
                log(f"erro ao ler anúncios: {ex}")
                todos = cache['criativos']
            por_slot = {s: [] for s in ESPACOS}
            for c in todos.values():
                if c.get('slot') in por_slot and ativo_hoje(c):
                    por_slot[c['slot']].append({'id': c['id'], 'alt': c.get('alt') or c.get('anunciante', ''),
                                                'anunciante': c.get('anunciante', ''), 'v': c.get('criado', '')})
            cache.update(momento=time.time(), criativos=todos, gz=comprimir({'espacos': por_slot}))
        return cache['gz']

    def contar(cid, campo):
        with lock:
            pendentes.setdefault(cid, {'imp': 0, 'cli': 0})[campo] += 1
        if time.time() - ultima_gravacao['t'] > 60:
            ultima_gravacao['t'] = time.time()
            threading.Thread(target=gravar_contagens, daemon=True).start()

    def gravar_contagens():
        with lock:
            lote = dict(pendentes)
            pendentes.clear()
        try:
            banco.somar_stats(lote)
        except Exception as ex:
            log(f"erro ao gravar contagens: {ex}")
            with lock:
                for cid, c in lote.items():
                    p = pendentes.setdefault(cid, {'imp': 0, 'cli': 0})
                    p['imp'] += c['imp']; p['cli'] += c['cli']

    @app.errorhandler(413)
    def grande_demais(_):
        return Response('<meta charset="utf-8"><body style="font-family:sans-serif;padding:24px">'
                        '<h3>Imagem grande demais</h3><p>O limite é 600 KB. Reduza a imagem (por exemplo em squoosh.app ou tinypng.com) e envie de novo.</p>'
                        '<button onclick="history.back()">← Voltar</button></body>', status=413, mimetype='text/html')

    @app.route('/api/publicidade')
    def api_publicidade():
        resp = responder(ativos())
        resp.headers['Cache-Control'] = 'public, max-age=60'
        return resp

    @app.route('/pub/img/<cid>')
    def pub_img(cid):
        if cid not in imagens:
            try:
                b64 = banco.imagem(cid)
            except Exception:
                b64 = None
            c = cache['criativos'].get(cid) or banco.criativos().get(cid)
            if not b64 or not c:
                return Response(status=404)
            imagens[cid] = (base64.b64decode(b64), c.get('tipo', 'image/png'))
        dados, tipo = imagens[cid]
        return Response(dados, mimetype=tipo, headers={'Cache-Control': 'public, max-age=86400'})

    @app.route('/pub/clique/<cid>')
    def pub_clique(cid):
        ativos()
        c = cache['criativos'].get(cid)
        if not c or not link_valido(c.get('link', '')):
            return redirect('/')
        contar(cid, 'cli')
        return redirect(c['link'], code=302)

    @app.route('/api/pub/vistas', methods=['POST'])
    def api_pub_vistas():
        ids = (request.get_json(silent=True) or {}).get('ids', [])[:6]
        ativos()
        for cid in ids:
            if isinstance(cid, str) and cid in cache['criativos']:
                contar(cid, 'imp')
        return Response(status=204)

    @app.route('/admin/publicidade')
    def admin_publicidade():
        chave = request.args.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        gravar_contagens()   # mostra os números mais recentes
        todos = banco.criativos()
        resp = Response(pagina_admin(todos, banco.stats(list(todos)), chave, request.args.get('aviso', '')), mimetype='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    def voltar(chave, aviso):
        from urllib.parse import quote
        return redirect(f"/admin/publicidade?chave={quote(chave)}&aviso={quote(aviso)}", code=303)

    @app.route('/admin/publicidade/novo', methods=['POST'])
    def admin_novo():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        slot, link = request.form.get('slot'), (request.form.get('link') or '').strip()
        arquivo = request.files.get('imagem')
        if slot not in ESPACOS:
            return voltar(chave, 'Escolha um espaço válido.')
        if not link_valido(link):
            return voltar(chave, 'O link precisa começar com https:// (ou http://).')
        if not arquivo or arquivo.mimetype not in TIPOS_IMAGEM:
            return voltar(chave, 'Envie uma imagem PNG, JPG, WEBP ou GIF.')
        dados = arquivo.read()
        if len(dados) > MAX_IMAGEM:
            return voltar(chave, f'Imagem grande demais ({len(dados) // 1024} KB). O limite é 600 KB.')
        c = {'id': secrets.token_hex(6), 'slot': slot, 'link': link,
             'anunciante': (request.form.get('anunciante') or '').strip()[:80],
             'alt': (request.form.get('alt') or '').strip()[:120],
             'inicio': request.form.get('inicio') or '', 'fim': request.form.get('fim') or '',
             'ativo': True, 'tipo': arquivo.mimetype, 'criado': datetime.now(BRT).strftime('%Y%m%d%H%M%S')}
        try:
            banco.salvar_criativo(c, base64.b64encode(dados).decode('ascii'))
        except Exception as ex:
            return voltar(chave, f'Não consegui salvar: {ex}')
        recarregar()
        log(f"novo anúncio: {c['anunciante']} ({slot})")
        return voltar(chave, f"Anúncio de {c['anunciante'] or 'anunciante'} publicado no espaço {ESPACOS[slot]['nome']}.")

    @app.route('/admin/publicidade/<cid>/alternar', methods=['POST'])
    def admin_alternar(cid):
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        c = banco.criativos().get(cid)
        if c:
            c['ativo'] = not c.get('ativo')
            banco.salvar_criativo(c)
            recarregar()
        return voltar(chave, 'Anúncio ativado.' if c and c['ativo'] else 'Anúncio pausado.')

    @app.route('/admin/publicidade/<cid>/remover', methods=['POST'])
    def admin_remover(cid):
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        banco.remover(cid)
        imagens.pop(cid, None)
        recarregar()
        return voltar(chave, 'Anúncio removido.')
