# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 05 - Silver Taxi Transformation
# MAGIC Cleans taxi facts, derives analytical fields and incrementally inserts previously unseen trips with Delta MERGE.

# COMMAND ----------
from pyspark.sql.functions import *

dbutils.widgets.text("year_month", "", "Optional taxi month YYYY-MM; blank = all Bronze")
year_month = dbutils.widgets.get("year_month").strip()
bronze = spark.table("nyc_taxi.bronze.yellow_taxi")
if year_month:
    bronze = bronze.filter(col("_source_file_name") == lit(f"yellow_tripdata_{year_month}.parquet"))

base=(bronze
 .filter(col("tpep_pickup_datetime").isNotNull() & col("tpep_dropoff_datetime").isNotNull())
 .filter(col("PULocationID").isNotNull() & col("DOLocationID").isNotNull())
 .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
 .filter(col("trip_distance") > 0).filter(col("fare_amount") >= 0)
 .withColumn("trip_duration_minutes", (unix_timestamp("tpep_dropoff_datetime")-unix_timestamp("tpep_pickup_datetime"))/60.0)
 .withColumn("average_speed_mph", try_divide(col("trip_distance"), col("trip_duration_minutes")/lit(60.0)))
 .withColumn("fare_per_mile", try_divide(col("fare_amount"), col("trip_distance")))
 .withColumn("tip_percentage", when((col("fare_amount")>0)&(col("tip_amount")>=0), try_divide(col("tip_amount"),col("fare_amount"))*100))
 .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
 .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
 .withColumn("pickup_hour_timestamp", date_trunc("hour", col("tpep_pickup_datetime")))
 .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
 .withColumn("pickup_month", month("tpep_pickup_datetime")).withColumn("pickup_year", year("tpep_pickup_datetime"))
 .withColumn("is_weekend", when(dayofweek("tpep_pickup_datetime").isin(1,7),1).otherwise(0))
 .withColumn("_silver_processed_timestamp", current_timestamp()))

clean=(base.filter(col("trip_duration_minutes")>0)
 .filter((col("average_speed_mph")>0)&(col("average_speed_mph")<=100))
 .filter((col("fare_per_mile")>=0)&(col("fare_per_mile")<=100))
 .filter(col("tip_amount").isNull() | (col("tip_amount")>=0)))

final=(clean.withColumn("trip_id", sha2(concat_ws("||",
 col("VendorID").cast("string"), col("tpep_pickup_datetime").cast("string"), col("tpep_dropoff_datetime").cast("string"),
 col("PULocationID").cast("string"), col("DOLocationID").cast("string"), col("trip_distance").cast("string"),
 col("fare_amount").cast("string"), col("tip_amount").cast("string"), col("total_amount").cast("string")),256))
 .dropDuplicates(["trip_id"]))
final.createOrReplaceTempView("taxi_silver_updates")

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS nyc_taxi.silver.taxi_trips
# MAGIC USING DELTA AS SELECT * FROM taxi_silver_updates WHERE 1=0;
# MAGIC
# MAGIC MERGE INTO nyc_taxi.silver.taxi_trips AS t
# MAGIC USING taxi_silver_updates AS s
# MAGIC ON t.trip_id = s.trip_id
# MAGIC WHEN NOT MATCHED THEN INSERT *;
