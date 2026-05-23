# Databricks notebook source
from pyspark.sql.functions import col, unix_timestamp, round, date_format, year, month, dayofmonth, when

# Lendo a tabela bronze
df = spark.read.table("workspace.bronze.nyctaxitrips")

# COMMAND ----------

# Renomeando as tabelas e dropando outras

df = (
    df
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("trip_distance", "trip_distance_miles")
    .withColumnRenamed("fare_amount", "fare_amount_usd")
    .withColumnRenamed("tip_amount", "tip_amount_usd")
    .withColumnRenamed("total_amount", "total_amount_usd")
    .withColumnRenamed("extra", "extra_usd")
)

df = df.drop(
    "RatecodeID",
    "store_and_fwd_flag", 
    "PULocationID", 
    "DOLocationID", 
    "mta_tax"
)

# COMMAND ----------

# Calculando o tempo de viagem

df = df.withColumn(
    "trip_duration_minutes",
    round((((unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)),2)
)

# COMMAND ----------

# Formatando as datas

df = df.withColumn(
    "pickup_datetime",
    date_format(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
)

df = df.withColumn(
    "dropoff_datetime",
    date_format(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")
)

# COMMAND ----------

# Calculando o ano, mês e dia

df = df.withColumn(
    "pickup_year",
    year(col("pickup_datetime"))
)

df = df.withColumn(
    "pickup_month",
    month(col("pickup_datetime"))
)

df = df.withColumn(
    "pickup_day",
    dayofmonth(col("pickup_datetime"))
)

# COMMAND ----------

# Calculando o tipo de pagamento

df = (
    df.withColumn("payment_type", when(df.payment_type == 1, "credit_card")
    .when(df.payment_type == 2, "cash")
    .when(df.payment_type == 3, "no_charge")
    .when(df.payment_type == 4, "dispute")
    .otherwise("unknown"))
)

# COMMAND ----------

# Salvando o dataframe limpo na camada silver

path = "workspace.silver"
table_name = "nyctaxi_cleaned"

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{path}.{table_name}")
