# hartmann-eleicoes_em_tempo_real

Este repositório cataloga a criação de um site com servidor para atualizar as eleições em tempo real, permitindo ao usuário uma aplicação em tempo real que não dependa das grandes mídias e tendo a possibilidade de alterar como quer ver as informações!

## Eleições disponíveis

| Ano  | Tipo       | Fonte                                                  |
|------|------------|--------------------------------------------------------|
| 2026 | Gerais     | 1º turno congelado em `dados/` · 2º turno ao vivo (TSE) |
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

## Congelar um turno que terminou de apurar

Enquanto um turno está "ao vivo", o servidor consulta o TSE o tempo todo (e o mapa municipal faz uma consulta por município).
Quando chegar a 100%, salve o resultado final em arquivos e o servidor para de consultar o TSE para aquele turno:

```bash
python congelar_ao_vivo.py 2026 1      # já pode rodar
python congelar_ao_vivo.py 2026 2      # depois que o 2º turno chegar a 100%
git add dados && git commit -m "Congela resultado" && git push
```

O script só salva se quase todas as consultas derem certo (tolerância de 0,5%) e avisa se a apuração nacional não estiver em 100%.

### Malha municipal atual (opcional)

`static/municipios/` vem com a malha do IBGE de 2010 simplificada (5.564 municípios). Para incluir os 6 municípios criados depois:

```bash
python gerar_historico.py --geometrias
```

## Rodar localmente

```bash
python servidor_eleicoes2026.py      # http://localhost:5001
```

Variáveis úteis para teste:
- `FORCAR_TURNOS_AO_VIVO=1` libera o 2º turno de 2026 antes do dia 25/10.
- `TSE_BASE=http://localhost:9000/oficial` aponta para um TSE simulado.

Links diretos funcionam: `/?ano=2022&turno=2`.
