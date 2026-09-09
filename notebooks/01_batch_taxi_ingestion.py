# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Batch Taxi Ingestion
# MAGIC Downloads one NYC TLC Yellow Taxi monthly Parquet file, lands it in ADLS raw storage, then incrementally ingests new raw files into the Bronze Delta table with Auto Loader.

# COMMAND ----------
import requests
from pathlib import Path
from pyspark.sql.functions import col, current_timestamp, lit

dbutils.widgets.text("year_month", "2026-05", "Taxi month (YYYY-MM)")
year_month = dbutils.widgets.get("year_month")

source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
local_path = f"/tmp/yellow_tripdata_{year_month}.parquet"
target_path = f"abfss://landing@projecttaxidata.dfs.core.windows.net/raw/taxi/yellow_tripdata_{year_month}.parquet"

with requests.get(source_url, stream=True, timeout=120) as r:
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

dbutils.fs.cp("file:" + local_path, target_path, True)
print(f"Landed {year_month} taxi file to ADLS")

# COMMAND ----------
source_path = "abfss://landing@projecttaxidata.dfs.core.windows.net/raw/taxi/"
schema_path = "abfss://landing@projecttaxidata.dfs.core.windows.net/autoloader/schema/taxi/"
checkpoint_path = "abfss://landing@projecttaxidata.dfs.core.windows.net/autoloader/checkpoints/taxi/"

taxi_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
)

taxi_bronze = (
    taxi_stream
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_source_file_name", col("_metadata.file_name"))
    .withColumn("_source_system", lit("NYC_TLC"))
)

q = (
    taxi_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("nyc_taxi.bronze.yellow_taxi")
)
q.awaitTermination()

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT _source_file_name, COUNT(*) AS row_count, MAX(_ingestion_timestamp) AS last_ingested_at
# MAGIC FROM nyc_taxi.bronze.yellow_taxi
# MAGIC GROUP BY _source_file_name
# MAGIC ORDER BY _source_file_name;
