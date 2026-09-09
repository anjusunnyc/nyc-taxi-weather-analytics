# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 06 - Silver Taxi Zone Dimension

# COMMAND ----------
from pyspark.sql.functions import col, trim, current_timestamp
zone=(spark.table("nyc_taxi.bronze.taxi_zone_lookup")
 .select(col("LocationID").cast("int").alias("location_id"), trim(col("Borough")).alias("borough"),
         trim(col("Zone")).alias("zone_name"), trim(col("service_zone")).alias("service_zone"))
 .filter(col("location_id").isNotNull()).dropDuplicates(["location_id"])
 .withColumn("_silver_processed_timestamp", current_timestamp()))
zone.createOrReplaceTempView("zone_updates")

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi.silver.dim_taxi_zone (
# MAGIC  location_id INT, borough STRING, zone_name STRING, service_zone STRING, _silver_processed_timestamp TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC MERGE INTO nyc_taxi.silver.dim_taxi_zone t USING zone_updates s ON t.location_id=s.location_id
# MAGIC WHEN MATCHED THEN UPDATE SET t.borough=s.borough,t.zone_name=s.zone_name,t.service_zone=s.service_zone,t._silver_processed_timestamp=s._silver_processed_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT *;
