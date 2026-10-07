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

## Rodar localmente

```bash
python servidor_eleicoes2026.py      # http://localhost:5001
```

Variáveis úteis para teste:
- `FORCAR_TURNOS_AO_VIVO=1` libera o 2º turno de 2026 antes do dia 25/10.
- `TSE_BASE=http://localhost:9000/oficial` aponta para um TSE simulado.

Links diretos funcionam: `/?ano=2022&turno=2`.
