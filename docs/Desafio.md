# 📊 Nivelamento – Engenheiro de Dados  
**“Let’s work data”**  
> Material Confidencial  

---

# 1. 📌 Contexto

O Banco Central do Brasil (BCB) disponibiliza, de forma pública e gratuita, séries históricas de indicadores econômicos via APIs REST.

Os dados utilizados neste projeto incluem:

- Taxa **SELIC diária**
- Cotações de câmbio (**USD** e **EUR**) via boletim PTAX

## 🎯 Objetivo

Construir um **pipeline de dados completo** que:

- Consuma APIs públicas do BCB  
- Processe os dados em camadas (**Medallion Architecture**)  
- Entregue uma tabela fato:

✔ Histórica  
✔ Contínua (sem lacunas)  
✔ Confiável  
✔ Pronta para consumo analítico  

### 🧠 Regra de negócio

**"Quero saber o valor de cada indicador em qualquer data, sem lacunas."**

- Fins de semana e feriados devem existir  
- Revisões retroativas devem ser tratadas  
- Dados devem ser rastreáveis  

---

# 2. 🌐 Fontes de Dados

## 📈 SGS / BCB – SELIC diária (código 11)

GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={dd/MM/yyyy}

---

## 💱 PTAX – Catálogo de moedas

GET https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas?$format=json

---

## 💵 PTAX – Cotação USD / EUR (fechamento)

GET https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(...)  
?@moeda='USD'  
&@dataInicial='MM-dd-yyyy'  
&@dataFinalCotacao='MM-dd-yyyy'  
&$filter=tipoBoletim eq 'Fechamento'  
&$format=json  

---

## ⚖️ Diferenças entre APIs

| Característica | SGS (SELIC) | PTAX (USD / EUR) |
|------|--------|--------|
| Formato de data (entrada) | dd/MM/yyyy | MM-dd-yyyy |
| Formato de data (retorno) | dd/MM/yyyy (string) | yyyy-MM-dd HH:mm:ss |
| Tipo do valor | string ("10.65") | number (5.1393) |
| Retorna fins de semana | ❌ | ❌ |
| Retorna feriados | ❌ | ❌ |
| Limite de período | 10 anos | Não documentado |
| Protocolo | REST simples | OData |

---

# 3. 🏗️ Desafio Técnico

Pipeline estruturado em três camadas:

- 🥉 Bronze  
- 🥈 Silver  
- 🥇 Gold  

---

## 🥉 Camada Bronze – Ingestão Bruta

### Responsabilidades

- Consumir APIs (SELIC, USD, EUR)  
- Persistir payload **sem transformação**  
- Registrar metadados  

### Requisitos

- Retry com **backoff exponencial**  
- Idempotência (sem duplicação)  

### Metadados

| Campo | Descrição |
|------|--------|
| endpoint | URL chamada |
| timestamp | Execução |
| status_http | Status |
| rows_returned | Nº registros |

---

## 🥈 Camada Silver – Limpeza e Padronização

### Responsabilidades

- Padronizar schemas  
- Criar calendário contínuo  
- Tratar lacunas  

### Regras

- Datas → `date`  
- Valores → `decimal`  
- Gerar calendário completo  

---

### 🔁 Forward Fill

- Preencher dias sem valor com último valor válido  
- Criar flag:

`is_filled = true`

---

### 🧬 SCD2

Se houver alteração:

- Registro antigo → `is_current = false`, `valid_to preenchido`  
- Novo registro → `is_current = true`, `valid_from atualizado`  

---

## 🥇 Camada Gold – Tabela Fato

### Tabela final

`fct_indicador_diario`

---

## 📊 Estrutura

| Campo | Tipo | Descrição |
|------|------|--------|
| sk_fato | integer | Chave surrogate |
| data_referencia | date | Data contínua |
| sk_indicador | integer | FK |
| valor | decimal(18,6) | Valor |
| is_filled | boolean | Forward fill |
| valid_from | timestamp | Início |
| valid_to | timestamp | Fim |
| is_current | boolean | Atual |

---

## 📘 Dimensão – dim_indicador

| sk_indicador | codigo | nome | periodicidade | fonte |
|------|--------|--------|--------|--------|
| 1 | 11 | SELIC | diária | SGS/BCB |
| 2 | USD | Dólar comercial | diária | PTAX/BCB |
| 3 | EUR | Euro comercial | diária | PTAX/BCB |

---

## 📋 pipeline_runs

| Campo | Descrição |
|------|--------|
| id_run | UUID |
| label | Execução |
| etapa | bronze/silver/gold |
| status | success/error |
| rows_returned | Registros |
| duration_s | Tempo |
| executed_at | Timestamp |
| error_msg | Erro |

---

# 4. 📈 Exemplo

| Data | Indicador | Valor | is_filled |
|------|--------|--------|--------|
| 2024-01-04 | SELIC | 10.65 | false |
| 2024-01-05 | SELIC | 10.65 | true ⭐ |
| 2024-01-06 | SELIC | 10.65 | true ⭐ |

⭐ = forward fill  

---

# 5. 🧭 Arquitetura

## Padrão

**Medallion Architecture**

---

## Fluxo

APIs BCB  
↓  
Bronze (raw)  
↓  
Silver (clean + fill + SCD2)  
↓  
Gold (analítico)  

---

## Decisões Técnicas

### Armazenamento
- Bronze → JSON bruto  
- Silver → estruturado  
- Gold → analítico  

### Idempotência
- Chave: data + indicador  

### Forward Fill
- Último valor válido propagado  

### SCD2
- Controle por versão + comparação  

### Rastreabilidade
- pipeline_runs  

---

# 6. 📦 Entregáveis

- Código organizado:
  - bronze/  
  - silver/  
  - gold/  

- Arquivos:
  - fct_indicador_diario.csv  
  - dim_indicador.csv  
  - pipeline_runs.csv  

- Arquitetura documentada  
- README com decisões  

---

# 7. ✅ Critérios de Avaliação

| Critério | Avaliação |
|------|--------|
| Ingestão | Retry + idempotência |
| Schemas | Padronização |
| Forward fill | Correto |
| SCD2 | Versionamento |
| Modelagem | Separação |
| Rastreabilidade | pipeline_runs |
| Arquitetura | Clareza |
| Documentação | Decisões |