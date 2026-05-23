# Databricks notebook source
df_temp = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz")
).limit(10000)

# COMMAND ----------

display(df_temp)

# COMMAND ----------

df = df_temp
df.write.format("delta").mode("overwrite").saveAsTable("nyctaxi_db.df_bronze")