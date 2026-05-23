# Databricks notebook source
# Lendo o dataset do databricks dos dados das viagens de taxis em NY no mes de dezembro de 2019

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz")
).limit(10000)

# COMMAND ----------

# Salvando o dataset como uma tabela delta na camada bronze

path = "workspace.bronze"
table_name = "nyctaxitrips"
df.write.format("delta").mode("overwrite").saveAsTable(f"{path}.{table_name}")
