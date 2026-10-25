"""
Chat ao vivo com reações e moderação.

Para o site:
    GET  /api/chat?desde=<t>        mensagens novas desde <t> (a resposta é a mesma para todos, montada a cada 2s)
    POST /api/chat                  {"apelido", "texto"}  envia mensagem
    POST /api/chat/reacao           {"emoji"}             soma uma reação
    POST /api/chat/denunciar        {"id"}                denuncia uma mensagem (3 denúncias = some até revisão)
Para você (senha = ANUNCIOS_CHAVE ou RELATORIO_CHAVE):
    /admin/chat?chave=...           apagar, bloquear, modo lento, palavras proibidas, ligar/desligar

Estado principal em memória (o servidor roda 1 processo); mensagens, bloqueios e configuração
também vão para o Upstash, para sobreviver a reinícios. Guarda só as últimas 200 mensagens.
"""
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import threading
import time
import unicodedata
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

BRT = timezone(timedelta(hours=-3))
MAX_MSGS = 200
ENVIO_POR_RESPOSTA = 60
EMOJIS = ['🎉', '😮', '😬', '👏', '😂', '🔥']
DENUNCIAS_PARA_OCULTAR = 3
ARQUIVO_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dados_chat.json')
PALAVRAS_PADRAO = ['porra', 'caralho', 'merda', 'puta', 'puto', 'foda', 'fodase', 'fdp', 'vsf', 'vtnc', 'tnc', 'pqp', 'arrombado',
                   'arrombada', 'cuzao', 'buceta', 'piroca', 'viado', 'bicha', 'macaco', 'retardado', 'corno', 'vagabundo', 'vagabunda',
                   'desgracado', 'filhodaputa', 'nazista']
RE_LINK = re.compile(r'(https?://|www\.|\b[\w-]+\.(com|br|net|org|io|ly|me|tv|gg|xyz|info|site|online|app)\b)', re.I)


def normalizar(t):
    t = unicodedata.normalize('NFD', t.lower()).encode('ascii', 'ignore').decode()
    t = t.translate(str.maketrans('013457@$', 'oieastas'))   # 0->o, 1->i, 3->e, 4->a, 5->s, 7->t, @->a, $->s
    return re.sub(r'(.)\1{2,}', r'\1\1', t)                    # "porrrraaa" -> "porraa"


class Banco:
    def __init__(self):
        url, token = os.environ.get('UPSTASH_REDIS_REST_URL'), os.environ.get('UPSTASH_REDIS_REST_TOKEN')
        self.up = None
        if url and token:
            from acessos import ArmazenamentoUpstash
            self.up = ArmazenamentoUpstash(url, token)
        self.lock = threading.Lock()

    def carregar(self):
        if self.up:
            msgs, cfg, reac = self.up._pipeline([['LRANGE', 'chat:msgs', 0, MAX_MSGS - 1], ['GET', 'chat:config'], ['HGETALL', 'chat:reacoes']])
            reac = reac or []
            reac = reac if isinstance(reac, dict) else {reac[i]: reac[i + 1] for i in range(0, len(reac), 2)}
            return [json.loads(m) for m in reversed(msgs or [])], (json.loads(cfg) if cfg else None), {k: int(v) for k, v in reac.items()}
        if os.path.exists(ARQUIVO_LOCAL):
            with open(ARQUIVO_LOCAL, encoding='utf-8') as f:
                d = json.load(f)
            return d.get('msgs', []), d.get('config'), d.get('reacoes', {})
        return [], None, {}

    def _local(self, msgs, cfg, reac):
        with self.lock:
            with open(ARQUIVO_LOCAL, 'w', encoding='utf-8') as f:
                json.dump({'msgs': msgs, 'config': cfg, 'reacoes': reac}, f, ensure_ascii=False)

    def nova_msg(self, m, todas, cfg, reac):
        if self.up:
            self.up._pipeline([['LPUSH', 'chat:msgs', json.dumps(m, ensure_ascii=False)], ['LTRIM', 'chat:msgs', 0, MAX_MSGS - 1]])
        else:
            self._local(todas, cfg, reac)

    def regravar_msgs(self, todas, cfg, reac):
        if self.up:
            cmds = [['DEL', 'chat:msgs']]
            if todas:
                cmds.append(['RPUSH', 'chat:msgs', *[json.dumps(m, ensure_ascii=False) for m in reversed(todas)]])
            self.up._pipeline(cmds)
        else:
            self._local(todas, cfg, reac)

    def salvar_config(self, cfg, todas, reac):
        if self.up:
            self.up._pipeline([['SET', 'chat:config', json.dumps(cfg, ensure_ascii=False)]])
        else:
            self._local(todas, cfg, reac)

    def somar_reacoes(self, lote, todas, cfg, reac):
        if not lote:
            return
        if self.up:
            self.up._pipeline([['HINCRBY', 'chat:reacoes', e, n] for e, n in lote.items()])
        else:
            self._local(todas, cfg, reac)


def iniciar(app, responder, comprimir):
    from flask import Response, jsonify, redirect, request
    from acessos import ip_do_cliente

    banco = Banco()
    lock = threading.Lock()
    sal = (os.environ.get('UPSTASH_REDIS_REST_TOKEN') or os.environ.get('RELATORIO_CHAVE') or 'chat') + ':chat'
    try:
        msgs_salvas, cfg_salva, reac_salvas = banco.carregar()
    except Exception as ex:
        print(f"[chat] erro ao carregar: {ex}", flush=True)
        msgs_salvas, cfg_salva, reac_salvas = [], None, {}
    est = {
        'msgs': deque(msgs_salvas, maxlen=MAX_MSGS),
        'cfg': {'ativo': True, 'lento': 10, 'palavras': PALAVRAS_PADRAO, 'bloqueados': {}, **(cfg_salva or {})},
        'reacoes': {e: int(reac_salvas.get(e, 0)) for e in EMOJIS},
        'reacoes_pendentes': {}, 'onda': deque(maxlen=400),
        'ultimo_envio': {}, 'envios_10min': {}, 'ultima_reacao': {},
        'resposta': None, 'resposta_t': 0, 'reac_gravadas_t': time.time(),
    }

    def codigo_ip():
        return hmac.new(sal.encode(), ip_do_cliente(request).encode(), hashlib.sha256).hexdigest()[:16]

    def senha_ok(valor):
        chave = os.environ.get('ANUNCIOS_CHAVE') or os.environ.get('RELATORIO_CHAVE')
        return bool(chave) and hmac.compare_digest(valor or '', chave)

    def bloqueado_por_palavra(texto):
        n = normalizar(texto)
        compacto = re.sub(r'[^a-z]', '', n)
        for p in est['cfg']['palavras']:
            p = normalizar(p).strip()
            if p and (re.search(rf'\b{re.escape(p)}\b', n) or (len(p) >= 5 and p in compacto)):
                return True
        return False

    def montar_resposta():
        """Uma resposta pronta para todos, refeita no máximo a cada 2s (poupa CPU no plano gratuito)."""
        agora = time.time()
        if est['resposta'] is not None and agora - est['resposta_t'] < 2:
            return est['resposta']
        with lock:
            visiveis = [{'id': m['id'], 't': m['t'], 'apelido': m['apelido'], 'texto': m['texto']}
                        for m in est['msgs'] if not m.get('oculta')][-ENVIO_POR_RESPOSTA:]
            onda = {}
            for t, e in est['onda']:
                if agora - t <= 6:
                    onda[e] = onda.get(e, 0) + 1
            corpo = {'ativo': est['cfg']['ativo'], 'lento': est['cfg']['lento'], 'msgs': visiveis,
                     'reacoes': dict(est['reacoes']), 'onda': onda, 'agora': agora, 'emojis': EMOJIS}
            est['resposta'], est['resposta_t'] = corpo, agora
        if agora - est['reac_gravadas_t'] > 60:
            est['reac_gravadas_t'] = agora
            with lock:
                lote, est['reacoes_pendentes'] = est['reacoes_pendentes'], {}
            threading.Thread(target=lambda: banco.somar_reacoes(lote, list(est['msgs']), est['cfg'], est['reacoes']), daemon=True).start()
        return corpo

    def invalidar():
        est['resposta'] = None

    @app.route('/api/chat')
    def api_chat_ler():
        corpo = montar_resposta()
        try:
            desde = float(request.args.get('desde', 0))
        except ValueError:
            desde = 0
        if desde:
            corpo = {**corpo, 'msgs': [m for m in corpo['msgs'] if m['t'] > desde]}
        resp = jsonify(corpo)
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/api/chat', methods=['POST'])
    def api_chat_enviar():
        d = request.get_json(silent=True) or {}
        apelido = re.sub(r'\s+', ' ', str(d.get('apelido') or '')).strip()[:20]
        texto = re.sub(r'\s+', ' ', str(d.get('texto') or '')).strip()[:200]
        cfg = est['cfg']
        if not cfg['ativo']:
            return jsonify({'ok': False, 'erro': 'O chat está pausado no momento.'}), 403
        quem, agora = codigo_ip(), time.time()
        if quem in cfg['bloqueados']:
            return jsonify({'ok': False, 'erro': 'Você não pode enviar mensagens neste chat.'}), 403
        if len(apelido) < 2:
            return jsonify({'ok': False, 'erro': 'Escolha um apelido com pelo menos 2 letras.'}), 400
        if not texto:
            return jsonify({'ok': False, 'erro': 'Escreva uma mensagem.'}), 400
        if RE_LINK.search(texto) or RE_LINK.search(apelido):
            return jsonify({'ok': False, 'erro': 'Links não são permitidos no chat.'}), 400
        if bloqueado_por_palavra(texto) or bloqueado_por_palavra(apelido):
            return jsonify({'ok': False, 'erro': 'Mensagem bloqueada pelo filtro do chat. Mantenha o respeito.'}), 400
        with lock:
            espera = cfg['lento'] - (agora - est['ultimo_envio'].get(quem, 0))
            if espera > 0:
                return jsonify({'ok': False, 'erro': f'Modo lento: aguarde {int(espera) + 1}s para enviar outra mensagem.', 'espera': int(espera) + 1}), 429
            recentes = [t for t in est['envios_10min'].get(quem, []) if agora - t < 600]
            if len(recentes) >= 20:
                return jsonify({'ok': False, 'erro': 'Você enviou muitas mensagens. Faça uma pausa.'}), 429
            ultimas = [m for m in est['msgs'] if m.get('quem') == quem][-1:]
            if ultimas and normalizar(ultimas[0]['texto']) == normalizar(texto):
                return jsonify({'ok': False, 'erro': 'Mensagem repetida.'}), 400
            m = {'id': secrets.token_hex(6), 't': agora, 'apelido': apelido, 'texto': texto, 'quem': quem, 'denuncias': [],
                 'hora': datetime.now(BRT).strftime('%d/%m %H:%M:%S')}
            est['msgs'].append(m)
            est['ultimo_envio'][quem] = agora
            est['envios_10min'][quem] = recentes + [agora]
            invalidar()
        try:
            banco.nova_msg(m, list(est['msgs']), cfg, est['reacoes'])
        except Exception as ex:
            print(f"[chat] erro ao gravar: {ex}", flush=True)
        return jsonify({'ok': True, 'msg': {k: m[k] for k in ('id', 't', 'apelido', 'texto')}})

    @app.route('/api/chat/reacao', methods=['POST'])
    def api_chat_reacao():
        e = (request.get_json(silent=True) or {}).get('emoji')
        if e not in EMOJIS or not est['cfg']['ativo']:
            return Response(status=204)
        quem, agora = codigo_ip(), time.time()
        with lock:
            if agora - est['ultima_reacao'].get(quem, 0) < 0.8:   # no máximo ~1 reação por segundo por pessoa
                return Response(status=204)
            est['ultima_reacao'][quem] = agora
            est['reacoes'][e] += 1
            est['reacoes_pendentes'][e] = est['reacoes_pendentes'].get(e, 0) + 1
            est['onda'].append((agora, e))
        return Response(status=204)

    @app.route('/api/chat/denunciar', methods=['POST'])
    def api_chat_denunciar():
        mid = (request.get_json(silent=True) or {}).get('id')
        quem = codigo_ip()
        with lock:
            m = next((x for x in est['msgs'] if x['id'] == mid), None)
            if m and quem not in m['denuncias'] and m.get('quem') != quem:
                m['denuncias'].append(quem)
                if len(m['denuncias']) >= DENUNCIAS_PARA_OCULTAR and not m.get('revisada'):
                    m['oculta'] = True
                    invalidar()
        if m:
            try:
                banco.regravar_msgs(list(est['msgs']), est['cfg'], est['reacoes'])
            except Exception:
                pass
        return jsonify({'ok': True})

    # ===================== ADMINISTRAÇÃO =====================
    def pagina(chave, aviso=''):
        e = html.escape
        ch = e(chave)
        cfg = est['cfg']
        linhas = []
        for m in reversed(est['msgs']):
            estado = '🚫 oculta' if m.get('oculta') else ('⚠ ' + str(len(m['denuncias'])) + ' denúncia(s)' if m['denuncias'] else '')
            bloq = m.get('quem') in cfg['bloqueados']
            acoes = (f'<form method="post" action="/admin/chat/msg/{e(m["id"])}/apagar" style="display:inline"><input type="hidden" name="chave" value="{ch}"><button class="perigo">Apagar</button></form>'
                     + (f'<form method="post" action="/admin/chat/msg/{e(m["id"])}/liberar" style="display:inline"><input type="hidden" name="chave" value="{ch}"><button>Liberar</button></form>' if m.get('oculta') else '')
                     + ('' if bloq else f'<form method="post" action="/admin/chat/msg/{e(m["id"])}/bloquear" style="display:inline" onsubmit="return confirm(&quot;Bloquear quem enviou e apagar todas as mensagens dessa pessoa?&quot;)"><input type="hidden" name="chave" value="{ch}"><button class="perigo">Bloquear autor</button></form>'))
            linhas.append(f"<tr class='{'oculta' if m.get('oculta') else ''}'><td>{e(m.get('hora', ''))}</td><td><b>{e(m['apelido'])}</b>{' <small>(bloqueado)</small>' if bloq else ''}</td>"
                          f"<td>{e(m['texto'])}</td><td>{estado}</td><td>{acoes}</td></tr>")
        reac = ' '.join(f'{em} {n:,}'.replace(',', '.') for em, n in est['reacoes'].items())
        return f"""<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Chat – moderação</title><meta http-equiv="refresh" content="30">
<style>body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f0f2f5;color:#1c1e21;margin:0;padding:18px}}
.caixa{{background:#fff;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,.05);padding:16px;margin-bottom:16px;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{text-align:left;padding:7px 8px;border-bottom:1px solid #e4e6eb;vertical-align:top}}
th{{font-size:11px;text-transform:uppercase;color:#65676b}}tr.oculta td{{background:#fff4f4}}
button{{border:none;background:#e4e6eb;border-radius:6px;padding:5px 10px;font-weight:700;cursor:pointer;margin:2px}}
button.perigo{{background:#fde2e2;color:#b91c1c}}button.principal{{background:#0056b3;color:#fff}}button.grande{{font-size:15px;padding:10px 16px}}
.aviso{{background:#e7f6ec;border:1px solid #9bd5ae;border-radius:8px;padding:10px 14px;margin-bottom:14px}}
textarea{{width:100%;min-height:70px;box-sizing:border-box;font-family:inherit;padding:8px;border:1px solid #ced0d4;border-radius:6px}}
input[type=number]{{width:80px;padding:6px;border:1px solid #ced0d4;border-radius:6px}}.nav a{{font-size:13px;margin-right:14px}}</style></head><body>
<h2 style="margin:0 0 6px">💬 Chat – moderação</h2>
<p class="nav"><a href="/admin/publicidade?chave={ch}">Publicidade</a><a href="/admin/stories?chave={ch}">Stories</a><a href="/admin/anuncios?chave={ch}">Interessados</a></p>
{f'<div class="aviso">{e(aviso)}</div>' if aviso else ''}
<div class="caixa">
<form method="post" action="/admin/chat/alternar" style="display:inline"><input type="hidden" name="chave" value="{ch}">
<button class="grande {'perigo' if cfg['ativo'] else 'principal'}">{'⏸ Pausar o chat agora' if cfg['ativo'] else '▶ Reabrir o chat'}</button></form>
&nbsp; Chat <b>{'aberto' if cfg['ativo'] else 'PAUSADO'}</b> · {len(est['msgs'])} mensagens guardadas · {len(cfg['bloqueados'])} bloqueados · reações: {reac}
<p style="font-size:12px;color:#65676b;margin:8px 0 0">Esta página se atualiza sozinha a cada 30s. Mensagens com {DENUNCIAS_PARA_OCULTAR} denúncias somem do site até você liberar ou apagar.</p></div>
<div class="caixa"><table><tr><th>Hora</th><th>Apelido</th><th>Mensagem</th><th>Situação</th><th></th></tr>{''.join(linhas) or '<tr><td colspan=5>Nenhuma mensagem.</td></tr>'}</table></div>
<div class="caixa"><h3 style="margin-top:0">⚙️ Regras</h3>
<form method="post" action="/admin/chat/config"><input type="hidden" name="chave" value="{ch}">
Modo lento: 1 mensagem a cada <input type="number" name="lento" min="0" max="300" value="{cfg['lento']}"> segundos por pessoa<br><br>
Palavras proibidas (separadas por vírgula; acentos e maiúsculas são ignorados):
<textarea name="palavras">{e(', '.join(cfg['palavras']))}</textarea><button class="principal">Salvar regras</button></form>
{('<form method="post" action="/admin/chat/desbloquear-todos" style="margin-top:10px"><input type="hidden" name="chave" value="' + ch + '"><button>Desbloquear todos (' + str(len(cfg['bloqueados'])) + ')</button></form>') if cfg['bloqueados'] else ''}
<form method="post" action="/admin/chat/limpar" style="margin-top:10px" onsubmit="return confirm('Apagar TODAS as mensagens do chat?')"><input type="hidden" name="chave" value="{ch}"><button class="perigo">Apagar todas as mensagens</button></form>
</div></body></html>"""

    def voltar(chave, aviso):
        return redirect(f"/admin/chat?chave={quote(chave)}&aviso={quote(aviso)}", code=303)

    def persistir_tudo():
        try:
            banco.regravar_msgs(list(est['msgs']), est['cfg'], est['reacoes'])
            banco.salvar_config(est['cfg'], list(est['msgs']), est['reacoes'])
        except Exception as ex:
            print(f"[chat] erro ao gravar: {ex}", flush=True)

    @app.route('/admin/chat')
    def chat_admin_chat():
        chave = request.args.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        r = Response(pagina(chave, request.args.get('aviso', '')), mimetype='text/html')
        r.headers['Cache-Control'] = 'no-store'
        return r

    @app.route('/admin/chat/msg/<mid>/<acao>', methods=['POST'])
    def chat_admin_msg(mid, acao):
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        with lock:
            m = next((x for x in est['msgs'] if x['id'] == mid), None)
            if not m:
                return voltar(chave, 'Mensagem não encontrada.')
            if acao == 'apagar':
                est['msgs'].remove(m); aviso = 'Mensagem apagada.'
            elif acao == 'liberar':
                m['oculta'] = False; m['revisada'] = True; aviso = 'Mensagem liberada.'
            elif acao == 'bloquear':
                est['cfg']['bloqueados'][m['quem']] = {'apelido': m['apelido'], 'quando': datetime.now(BRT).strftime('%d/%m %H:%M')}
                for x in [x for x in est['msgs'] if x.get('quem') == m['quem']]:
                    est['msgs'].remove(x)
                aviso = f"{m['apelido']} bloqueado e mensagens apagadas."
            else:
                return voltar(chave, 'Ação desconhecida.')
            invalidar()
        persistir_tudo()
        return voltar(chave, aviso)

    @app.route('/admin/chat/alternar', methods=['POST'])
    def chat_admin_alternar():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        est['cfg']['ativo'] = not est['cfg']['ativo']
        invalidar(); persistir_tudo()
        return voltar(chave, 'Chat reaberto.' if est['cfg']['ativo'] else 'Chat pausado: ninguém consegue enviar mensagens.')

    @app.route('/admin/chat/config', methods=['POST'])
    def chat_admin_config():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        try:
            est['cfg']['lento'] = max(0, min(300, int(request.form.get('lento', 10))))
        except ValueError:
            pass
        est['cfg']['palavras'] = [p.strip() for p in (request.form.get('palavras') or '').split(',') if p.strip()][:500]
        invalidar(); persistir_tudo()
        return voltar(chave, 'Regras salvas.')

    @app.route('/admin/chat/desbloquear-todos', methods=['POST'])
    def chat_admin_desbloquear():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        est['cfg']['bloqueados'] = {}
        persistir_tudo()
        return voltar(chave, 'Todos desbloqueados.')

    @app.route('/admin/chat/limpar', methods=['POST'])
    def chat_admin_limpar():
        chave = request.form.get('chave', '')
        if not senha_ok(chave):
            return Response('Não encontrado', status=404)
        with lock:
            est['msgs'].clear(); invalidar()
        persistir_tudo()
        return voltar(chave, 'Todas as mensagens foram apagadas.')
