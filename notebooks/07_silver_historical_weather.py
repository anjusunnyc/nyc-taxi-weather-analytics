# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 07 - Silver Historical Weather

# COMMAND ----------
from pyspark.sql.functions import col, date_trunc, when, current_timestamp
b=spark.table("nyc_taxi.bronze.weather_history")
s=(b.withColumn("weather_hour",date_trunc("hour",col("weather_timestamp")))
 .withColumnRenamed("temperature_2m","temperature_c")
 .withColumnRenamed("relative_humidity_2m","relative_humidity")
 .withColumnRenamed("wind_speed_10m","wind_speed_kph")
 .withColumnRenamed("wind_gusts_10m","wind_gust_kph")
 .withColumnRenamed("cloud_cover","cloud_cover_pct")
 .withColumn("weather_condition_category",
   when(col("weather_code")==0,"Clear").when(col("weather_code").isin(1,2,3),"Cloudy")
   .when(col("weather_code").isin(45,48),"Fog").when(col("weather_code").between(51,67),"Rain")
   .when(col("weather_code").between(71,77),"Snow").when(col("weather_code").between(80,82),"Rain")
   .when(col("weather_code").between(85,86),"Snow").when(col("weather_code").between(95,99),"Thunderstorm").otherwise("Other"))
 .withColumn("is_adverse_weather",when(col("weather_condition_category").isin("Rain","Snow","Fog","Thunderstorm"),1).otherwise(0))
 .withColumn("_silver_processed_timestamp",current_timestamp())
 .select("weather_hour","temperature_c","precipitation","rain","relative_humidity","weather_code",
         "weather_condition_category","cloud_cover_pct","wind_speed_kph","wind_gust_kph","is_adverse_weather","_silver_processed_timestamp"))
(s.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("nyc_taxi.silver.weather_history"))
