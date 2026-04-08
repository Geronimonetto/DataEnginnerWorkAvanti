# 📊 Pipeline de Indicadores Econômicos — BCB

Fala pessoal! Esse aqui é o repositório do pipeline de dados que consome as APIs do Banco Central do Brasil pra consolidar indicadores econômicos (SELIC, Dólar, Euro) numa tabela fato pronta pra análise.

A ideia é simples: buscar os dados todo dia, tratar os dados faltantes de finais de semana e feriados via **Forward Fill**, manter o histórico de alterações com **SCD2** e entregar tudo em um CSV.

## 🚀 Como rodar

> **Pré-requisito:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando.

```bash
# 1. Sobe a infra (PostgreSQL + Airflow)
docker compose up -d

# 2. Espera uns 2 min pro Airflow inicializar

# 3. Acessa http://localhost:8080
#    Login: admin
#    Senha: admin123

# 4. Dá um Trigger na DAG "pipeline_medalhao_bcb"

# 5. Os CSVs de saída caem em data/
```

Pra derrubar tudo:
```bash
docker compose down -v
```

---

## 🏛️ Arquitetura — Medallion

Organizei a solução usando **Medallion Architecture**, que basicamente separa o dado em 3 camadas com responsabilidades diferentes:

```
APIs BCB (SGS + PTAX)
       │
       ▼
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ 🥉 BRONZE   │────▶│ 🥈 SILVER        │────▶│ 🥇 GOLD         │
│  JSON bruto │     │  Limpo + Fill    │     │  Tabela Fato     │
│  aiohttp    │     │  dbt Core        │     │  fct_indicador   │
│  + backoff  │     │  + Forward Fill  │     │  + dim_indicador │
│             │     │  + SCD2 Snapshot │     │                  │
└─────────────┘     └──────────────────┘     └─────────────────┘
       │                    │                        │
       └────────────────────┴────────────────────────┘
                     PostgreSQL 13
```

**Orquestrador:** Apache Airflow 2.8  
**Infra:** Docker Compose (dois containers — PostgreSQL + Airflow)

### Fluxo da DAG

```
extrair_selic ─┐
extrair_usd  ──┼──▶ dbt seed ──▶ dbt silver ──▶ dbt snapshot ──▶ dbt gold ──▶ export CSV
extrair_eur  ──┘
```

As 3 extrações rodam em paralelo, depois vem o dbt fazendo a transformação em sequência.

---

## 📐 Decisões Técnicas

### 1. Bronze — Ingestão crua

A Bronze salva o payload **exatamente como veio** da API, sem mexer em nada. Uso `aiohttp` pra fazer as chamadas de forma assíncrona com **backoff exponencial** — se a API do BCB travar, o script espera e tenta de novo automaticamente em vez de simplesmente morrer.

A DAG também tem uma lógica de **carga inteligente**: na primeira execução (tabela bronze vazia), ela puxa o histórico completo desde `01/01/2024`. Nas próximas, busca somente o dia corrente. Isso deixa o pipeline eficiente sem sacrificar a carga inicial.

### 2. Tratamento das diferenças entre APIs

Esse é um ponto que deu trabalho. As duas APIs do BCB têm contratos bem diferentes entre si:

| | SGS (SELIC) | PTAX (USD/EUR) |
|---|---|---|
| **Data de entrada** | `dd/MM/yyyy` | `MM-dd-yyyy` |
| **Data no retorno** | `"dd/MM/yyyy"` (string) | `"yyyy-MM-dd HH:mm:ss"` (string) |
| **Tipo do valor** | `string` ("10.65") | `number` (5.1393) |
| **Fins de semana/feriados** | Não retorna | Não retorna |
| **Protocolo** | REST simples | OData |

A saída? Guardar tudo na Bronze e deixar a Silver resolver. Cada `stg_*.sql` no dbt faz o `TO_DATE` e `CAST` correto pro seu indicador. Se amanhã a API mudar o formato, é só ajustar um model SQL sem mexer na ingestão.

### 3. Silver — dbt Core

Aqui é onde a mágica acontece:

- **Forward Fill**: Criei um calendário recorrente e uso `FIRST_VALUE` + window function pra propagar o último valor válido pros dias sem dado (sábados, domingos, feriados). Esses registros levam `is_filled = true`.
- **Deduplicação**: Coluna `_inserted_at` com timestamp na ingestão + `ROW_NUMBER() PARTITION BY data ORDER BY _inserted_at DESC` — sempre pega o registro mais recente. Isso garante idempotência: pode rodar 10 vezes no mesmo dia que não duplica nada.

### 4. SCD2 — Rastreamento de alterações

Uso **dbt snapshot** com `strategy='check'` nos campos `valor` e `is_filled`. Se o BCB corrigir um valor retroativo (acontece), o snapshot detect a mudança, fecha o registro antigo com `valid_to` e abre um novo com `valid_from` atualizado. A flag `is_current` facilita pegar só a versão vigente.

### 5. Gold — Tabela Fato

A `fct_indicador_diario` é a consolidação final. Schema:

| Campo | Tipo | Descrição |
|---|---|---|
| `sk_fato` | integer | Surrogate key |
| `data_referencia` | date | Data (sem lacunas!) |
| `sk_indicador` | integer | FK → `dim_indicador` |
| `valor` | decimal(18,6) | Valor do indicador |
| `is_filled` | boolean | `true` se veio do forward fill |
| `valid_from` | timestamp | Início da validade (SCD2) |
| `valid_to` | timestamp | Fim da validade (SCD2) |
| `is_current` | boolean | `true` se é a versão atual |

Materializada como `table` — recriada a cada execução, então o CSV final é sempre um snapshot limpo.

---

## 📂 Estrutura do Projeto

```
WorkAvanti/
├── docker-compose.yml          # Infra (PostgreSQL + Airflow)
├── README.md
│
├── airflow_project/
│   └── dags/
│       └── dag_medallion.py    # DAG principal
│
├── workavanti/                 # Código Python
│   ├── extract_data.py         # Extrator assíncrono (aiohttp)
│   ├── runtimer.py             # Motor de execução + contratos
│   ├── auditor.py              # Log de pipeline_runs
│   └── export_csv.py           # Exportação dos entregáveis
│
├── dbt_bcb/                    # Projeto dbt
│   ├── models/
│   │   ├── silver/             # stg_selic, stg_usd, stg_eur
│   │   └── gold/               # fct_indicador_diario
│   ├── snapshots/              # SCD2
│   └── seeds/
│       └── dim_indicador.csv   # Dimensão de suporte
│
├── data/                       # CSVs gerados pelo pipeline
│   ├── fct_indicador_diario.csv
│   ├── dim_indicador.csv
│   └── pipeline_runs.csv
│
└── docs/
    └── Desafio.md
```

---

## 📊 Entregáveis

| Arquivo | O que tem dentro |
|---|---|
| `fct_indicador_diario.csv` | Tabela fato com todos os indicadores, calendário recorrente, forward fill e SCD2 |
| `dim_indicador.csv` | Dimensão com os 3 indicadores (SELIC, USD, EUR) |
| `pipeline_runs.csv` | Log de auditoria — cada execução com status, duração e erros |

---

## 🛠️ Stack

| Componente | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.8 |
| Data Warehouse | PostgreSQL 13 |
| Transformação | dbt Core |
| Ingestão | Python 3.11 + aiohttp |
| Infra | Docker Compose |
# DataEnginnerWorkAvanti
