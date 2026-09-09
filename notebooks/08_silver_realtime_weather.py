# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 08 - Silver Realtime Weather

# COMMAND ----------
from pyspark.sql.functions import col, date_trunc, when, current_timestamp
stream=spark.readStream.table("nyc_taxi.bronze.weather_realtime")
s=(stream
 .withColumn("weather_timestamp",col("weather_observation_time").cast("timestamp"))
 .withColumn("weather_hour",date_trunc("hour",col("weather_observation_time").cast("timestamp")))
 .withColumn("weather_condition_category",
   when(col("weather_condition").rlike("(?i)rain|drizzle"),"Rain")
   .when(col("weather_condition").rlike("(?i)snow|sleet|ice"),"Snow")
   .when(col("weather_condition").rlike("(?i)fog|mist"),"Fog")
   .when(col("weather_condition").rlike("(?i)thunder"),"Thunderstorm")
   .when(col("weather_condition").rlike("(?i)cloud|overcast"),"Cloudy")
   .when(col("weather_condition").rlike("(?i)clear|sunny"),"Clear").otherwise("Other"))
 .withColumn("is_adverse_weather",when(col("weather_condition_category").isin("Rain","Snow","Fog","Thunderstorm"),1).otherwise(0))
 .withColumn("_silver_processed_timestamp",current_timestamp())
 .select("weather_timestamp","weather_hour","location_name","latitude","longitude","temperature_c","precipitation_mm",
         "relative_humidity","weather_code","weather_condition","weather_condition_category","cloud_cover",
         "wind_speed_kph","wind_gust_kph","visibility_km","is_adverse_weather","_ingestion_timestamp","_silver_processed_timestamp"))
checkpoint="abfss://landing@projecttaxidata.dfs.core.windows.net/autoloader/checkpoints/silver/weather_realtime/"
q=(s.writeStream.format("delta").option("checkpointLocation",checkpoint).trigger(availableNow=True).toTable("nyc_taxi.silver.weather_realtime"))
q.awaitTermination()
