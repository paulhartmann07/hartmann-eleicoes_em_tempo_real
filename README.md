# hartmann-eleicoes_em_tempo_real

Este repositório cataloga a criação de um site com servidor para atualizar as eleições em tempo real, permitindo ao usuário uma aplicação em tempo real que não dependa das grandes mídias e tendo a possibilidade de alterar como quer ver as informações!

## Eleições disponíveis

| Ano  | Tipo       | Fonte                                                  |
|------|------------|--------------------------------------------------------|
| 2026 | Gerais     | 1º turno congelado em `dados/`; 2º turno ao vivo (TSE)  |
| 2024 | Municipais | Portal de Dados Abertos do TSE (arquivos em `dados/`)  |
| 2022 | Gerais     | Portal de Dados Abertos do TSE                         |
| 2020 | Municipais | Portal de Dados Abertos do TSE                         |
| 2018 | Gerais     | Portal de Dados Abertos do TSE                         |

## Gerar os dados históricos (uma vez, no seu computador)

```bash
pip install -r requirements.txt
python gerar_historico.py 2018 2020 2022 2024
git add dados/ && git commit -m "Dados históricos" && git push
```

O script baixa os arquivos `votacao_candidato_munzona_{ano}.zip` do TSE (centenas de MB cada, guardados em `.cache_tse/`, que não vai para o Git) e gera JSONs compactos em `dados/{ano}/t{turno}/`.

Quando o TSE publicar os dados abertos de 2026, rode `python gerar_historico.py 2026`: a partir daí o servidor usa os arquivos estáticos e para de consultar o TSE.

## Apuração ao vivo

Durante a apuração o servidor consulta o TSE a cada 60s, só os resultados de Brasil e das 27 UFs.
Para poupar CPU (o plano gratuito do Render tem 0,1 CPU), **não há mapa por município ao vivo**:
clicar num estado mostra o painel de cargos, e cidades são vistas pela busca.

Quando Brasil e as 27 UFs chegam a 100%, o servidor **para sozinho de consultar o TSE**
(a marca fica salva em `dados_cache_{ano}_t{turno}.json`; depois de reiniciar ele faz no máximo
uma leitura para confirmar). Como rede de segurança, também para 3 dias após a eleição.

## Coletor ao vivo (todos os níveis a cada 5 min)

Durante a apuração, rode no seu computador (o Render só lê os dados prontos do Upstash):

```bash
export UPSTASH_REDIS_REST_URL="..."   # os mesmos valores configurados no Render
export UPSTASH_REDIS_REST_TOKEN="..."
caffeinate -i python coletor_ao_vivo.py 2026 2
```

A cada 5 minutos ele baixa Brasil, UFs, municípios e zonas eleitorais (só o que mudou), grava uma "foto"
por UF e um ponto de histórico (Brasil e UFs) para a aba "Evolução da apuração". Para sozinho em 100%.
Se o coletor parar, depois de 15 minutos o site volta sozinho ao modo antigo (Brasil e UFs consultados direto no TSE).

**Sem depender do computador:** defina `COLETOR_NO_RENDER=1` no Render e o próprio servidor roda o coletor.
No plano gratuito ele roda no modo leve (Brasil, UFs e municípios; sem zonas e sem governador por município).
Com `COLETOR_COMPLETO=1` inclui zonas e governador por município; use só com plano pago (mais CPU).

### Exterior (2º turno de 2026)

O coletor também busca o total do exterior (`zz`) e as 186 cidades do exterior (`zz<código>`), soma por país e grava
no Upstash. No site: botão 🇧🇷 Brasil / 🌎 Mundo no mapa e, na aba de evolução, "Total", "Só no Brasil" e "Exterior".
A tabela cidade→país (`static/exterior.json`) vem das planilhas de seções do exterior do TRE-DF, compiladas em
github.com/jonatasbferreira/apuramn (exterior-countries.json). Mapa-múndi: johan/world.geo.json (simplificado).

## Congelar um turno encerrado (libera o mapa por município)

```bash
python congelar_ao_vivo.py 2026 2      # depois que o 2º turno chegar a 100%
git add dados && git commit -m "Congela resultado" && git push
```

O script só salva se quase todas as consultas derem certo (tolerância de 0,5%) e avisa se a apuração nacional não estiver em 100%.

### Malha municipal atual (opcional)

`static/municipios/` vem com a malha do IBGE de 2010 simplificada (5.564 municípios). Para incluir os 6 municípios criados depois:

```bash
python gerar_historico.py --geometrias
```

## Contador de acessos (aba "Acessos ao site")

A página avisa o servidor a cada minuto enquanto está visível. O servidor mede quantas pessoas estão com o site
aberto e grava um ponto a cada 10 minutos (movimento normal) ou a cada minuto (a partir de 20 pessoas simultâneas,
ajustável com `ACESSOS_LIMIAR_ALTO`). IPs viram um código anônimo (HMAC) e nunca são gravados.

Variáveis de ambiente no Render (Environment):

| Variável | Para quê |
|---|---|
| `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN` | Guardar os acessos (sem isso, o Render apaga tudo a cada reinício) |
| `RESEND_API_KEY`, `RELATORIO_EMAIL_PARA` | Relatório diário por e-mail (o Render gratuito bloqueia SMTP) |
| `RELATORIO_HORA` | Hora do relatório, horário de Brasília (padrão: 8) |
| `RELATORIO_CHAVE` | Senha para `/api/acessos/relatorio?chave=...` (envia na hora; com `&preview=1` só mostra) |

O processo precisa ser **único** (os contadores ficam na memória). Start Command no Render:
`gunicorn --workers 1 --threads 4 servidor_eleicoes2026:app`

## Idiomas (PT / EN)

Navegador em português → português; qualquer outro idioma → inglês. Botões 🇧🇷 PT / 🇺🇸 EN no topo (a escolha fica salva)
e links diretos com `?lang=en` ou `?lang=pt`. A tradução fica no `<head>` do `eleicoes2026.html` (`I18N_EXATOS`,
`I18N_PADROES`, `I18N_PALAVRAS`): para mudar um texto em inglês, edite ali. Os stories têm campos `_en` no `static/stories.json`.

## Anuncie aqui

Banner "Quero anunciar" na apuração e na aba Análises. Cada envio fica guardado (Upstash, ou `dados_anuncios.json`
localmente) e gera um e-mail de aviso pelo Resend. Para ver todos os interessados:
`/admin/anuncios?chave=SUA_SENHA` (planilha em `/admin/anuncios.csv?chave=...`).
A senha é `ANUNCIOS_CHAVE` ou, se ela não existir, a `RELATORIO_CHAVE`.
Proteções: consentimento obrigatório (LGPD), campo invisível contra robôs e no máximo 3 envios por hora por IP.

## Espaços publicitários

Quatro espaços: **topo** (970×90), **abertura** (popup 600×500, no máximo 1x a cada 12h por pessoa), **laterais**
(160×600, só em telas a partir de 1500px) e **rodapé** (970×90). Espaço vazio mostra o convite "Anuncie aqui".

Para subir, pausar ou remover anúncios, sem deploy: `/admin/publicidade?chave=SUA_SENHA`.
Imagens até 600 KB (PNG, JPG, WEBP ou GIF), guardadas no Upstash. Vistas contam quando ao menos metade do
anúncio aparece na tela; cliques passam por `/pub/clique/<id>`. Os números são gravados a cada minuto.

## Rodar localmente

```bash
python servidor_eleicoes2026.py      # http://localhost:5001
```

Variáveis úteis para teste:
- `FORCAR_TURNOS_AO_VIVO=1` libera o 2º turno de 2026 antes do dia 25/10.
- `TSE_BASE=http://localhost:9000/oficial` aponta para um TSE simulado.

Links diretos funcionam: `/?ano=2022&turno=2`.
