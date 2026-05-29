# 🚕 NYC Taxi Trips — Pipeline de Dados no Databricks

Pipeline de engenharia de dados para análise das viagens de táxi amarelo em Nova York (dezembro/2019), construído com a arquitetura **Medallion (Bronze → Silver → Gold)** no Databricks usando Delta Lake e PySpark.

-----

## 🏗️ Arquitetura

```
Bronze (Raw)  →  Silver (Cleaned)  →  Gold (KPIs)
```

|Camada    |Descrição                                                                                                              |
|----------|-----------------------------------------------------------------------------------------------------------------------|
|**Bronze**|Ingestão bruta dos dados CSV do dataset público do Databricks (10.000 registros)                                       |
|**Silver**|Limpeza, renomeação de colunas, cálculo de duração da viagem, extração de data/hora e normalização do tipo de pagamento|
|**Gold**  |Tabela analítica com KPIs de comportamento de pagamento por tipo                                                       |

-----

## 📁 Estrutura do Projeto

```
NYCTaxiTrips/
│
├── setup.py                    # Configuração inicial do ambiente/catálogo
│
├── ingest_bronze.py            # Lê o CSV e salva na camada Bronze (Delta)
│
├── transform_silver.py         # Limpa e transforma os dados para a camada Silver
│
├── goldKPI.py                  # Agrega KPIs e salva na camada Gold
│
└── PaymentBehaviorJob.yml      # Job do Databricks para orquestrar o pipeline
```

-----

## 📊 KPIs Gerados (Gold)

Tabela `workspace.gold.payment_behavior`:

- **Total de corridas** por tipo de pagamento
- **Custo médio** (USD) por tipo
- **Distância média** (milhas) por tipo

> ⚠️ Corridas do tipo `dispute` apresentam valor médio negativo — possível indicativo de estornos ou cobranças contestadas.

-----

## 🛠️ Stack

- **Databricks** (notebooks + Jobs)
- **PySpark** + **Delta Lake**
- **Dataset:** `dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz`

-----

## ▶️ Como Executar

1. Importe os notebooks no Databricks na ordem: `setup.py` → `ingest_bronze.py` → `transform_silver.py` → `goldKPI.py`
1. Ou use o `PaymentBehaviorJob.yml` para criar um Job automatizado no Databricks.