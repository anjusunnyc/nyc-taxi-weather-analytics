# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 09 - Silver Enrichment

# COMMAND ----------
from pyspark.sql.functions import col
t=spark.table("nyc_taxi.silver.taxi_trips"); z=spark.table("nyc_taxi.silver.dim_taxi_zone"); w=spark.table("nyc_taxi.silver.weather_history")
tz=(t.alias("t").join(z.alias("z"),col("t.PULocationID")==col("z.location_id"),"left")
    .select(col("t.*"),col("z.borough").alias("pickup_borough"),col("z.zone_name").alias("pickup_zone"),col("z.service_zone").alias("pickup_service_zone")))
e=(tz.alias("t").join(w.alias("w"),col("t.pickup_hour_timestamp")==col("w.weather_hour"),"left")
 .select(col("t.trip_id"),col("t.VendorID"),col("t.tpep_pickup_datetime"),col("t.tpep_dropoff_datetime"),col("t.pickup_date"),col("t.pickup_hour"),
         col("t.pickup_hour_timestamp"),col("t.pickup_day_of_week"),col("t.is_weekend"),col("t.PULocationID").alias("pickup_location_id"),
         col("t.DOLocationID").alias("dropoff_location_id"),col("t.pickup_borough"),col("t.pickup_zone"),col("t.pickup_service_zone"),
         col("t.trip_distance"),col("t.trip_duration_minutes"),col("t.average_speed_mph"),col("t.fare_amount"),col("t.total_amount"),col("t.fare_per_mile"),
         col("t.tip_amount"),col("t.tip_percentage"),col("t.payment_type"),col("w.weather_hour"),col("w.temperature_c"),
         col("w.precipitation").alias("precipitation_mm"),col("w.relative_humidity"),col("w.weather_code"),col("w.weather_condition_category"),
         col("w.cloud_cover_pct"),col("w.wind_speed_kph"),col("w.wind_gust_kph"),col("w.is_adverse_weather")))
print(f"Enriched rows: {e.count()}")
(e.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("nyc_taxi.silver.taxi_weather_enriched"))
