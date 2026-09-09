# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02 - Taxi Zone Reference Ingestion

# COMMAND ----------
import requests
from pyspark.sql.functions import current_timestamp, lit

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
local_path = "/tmp/taxi_zone_lookup.csv"
raw_path = "abfss://landing@projecttaxidata.dfs.core.windows.net/raw/taxi_zone/taxi_zone_lookup.csv"

r = requests.get(url, timeout=60)
r.raise_for_status()
open(local_path, "wb").write(r.content)
dbutils.fs.cp("file:" + local_path, raw_path, True)

zone_df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
zone_bronze = (zone_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("NYC_TLC"))
    .withColumn("_source_file", lit(raw_path)))

(zone_bronze.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("nyc_taxi.bronze.taxi_zone_lookup"))
