# Databricks notebook source
df = spark.sql(
    """
    SELECT *
    FROM workspace.silver.nyctaxi_cleaned
    """
)

# COMMAND ----------

# Fazendo uma tabela gold que mostre o tipo de pagamento mais comum , quantas corridas por tipo de pagamento, media do custo total por tipo de pagamento e a media da distancia percorrida por tipo de pagamento

df_payment_behavior = spark.sql(
    """
    SELECT payment_type,
           COUNT(*) AS total_trips,
           ROUND(AVG(total_amount_usd), 2) AS avg_cost_usd,
           ROUND(AVG(trip_distance_miles), 2) AS avg_trip_distance

    FROM workspace.silver.nyctaxi_cleaned

    GROUP BY payment_type

    ORDER BY total_trips DESC
    """
)

# Identificado valor médio negativo para corridas do tipo dispute, indicando possíveis estornos ou cobranças contestadas

# COMMAND ----------

# Salvando essa tabela no camada gold
path = "workspace.gold"
table_name = "payment_behavior"

df_payment_behavior.write.format("delta").mode("overwrite").saveAsTable(f"{path}.{table_name}")
