# Databricks notebook source
# Fazendo uma tabela gold que mostre o tipo de pagamento mais comum , quantas corridas por tipo de pagamento, media do custo total por tipo de pagamento

df_payment_behavior = spark.sql(
    """
    SELECT payment_type,
           COUNT(*) AS total_trips,
           ROUND(AVG(total_amount_usd), 2) AS avg_cost_usd

    FROM workspace.silver.nyctaxi_cleaned

    GROUP BY payment_type

    ORDER BY total_trips DESC
    """
)

# COMMAND ----------

# Salvando essa tabela no camada gold
path = "workspace.gold"
table_name = "payment_behavior"


df_payment_behavior.write.format("delta").mode("overwrite").saveAsTable(f"{path}.{table_name}")