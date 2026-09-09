# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 04 - Realtime Weather Ingestion
# MAGIC Scheduled every 15 minutes. WeatherAPI -> timestamped raw JSON -> Auto Loader -> Bronze Delta.

# COMMAND ----------
import requests, json
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, current_timestamp, lit

api_key = dbutils.secrets.get(scope="weather-kv-scope", key="databricks-weatherapikey")
r = requests.get("https://api.weatherapi.com/v1/current.json", params={"key": api_key, "q": "40.7128,-74.0060", "aqi": "no"}, timeout=60)
r.raise_for_status()
weather_json = r.json()

ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
local = f"/tmp/weather_{ts}.json"
target = f"abfss://landing@projecttaxidata.dfs.core.windows.net/raw/weather_realtime/weather_{ts}.json"
with open(local, "w") as f: json.dump(weather_json, f)
dbutils.fs.cp("file:" + local, target)
print(f"Realtime weather landed: {target}")

# COMMAND ----------
weather_schema = StructType([
    StructField("location", StructType([
        StructField("name", StringType()), StructField("region", StringType()), StructField("country", StringType()),
        StructField("lat", DoubleType()), StructField("lon", DoubleType()), StructField("tz_id", StringType()),
        StructField("localtime_epoch", LongType()), StructField("localtime", StringType())
    ])),
    StructField("current", StructType([
        StructField("last_updated_epoch", LongType()), StructField("last_updated", StringType()),
        StructField("temp_c", DoubleType()), StructField("is_day", LongType()),
        StructField("condition", StructType([StructField("text", StringType()), StructField("icon", StringType()), StructField("code", LongType())])),
        StructField("wind_kph", DoubleType()), StructField("wind_degree", LongType()), StructField("wind_dir", StringType()),
        StructField("precip_mm", DoubleType()), StructField("humidity", LongType()), StructField("cloud", LongType()),
        StructField("feelslike_c", DoubleType()), StructField("gust_kph", DoubleType()), StructField("vis_km", DoubleType())
    ]))
])

source = "abfss://landing@projecttaxidata.dfs.core.windows.net/raw/weather_realtime/"
checkpoint = "abfss://landing@projecttaxidata.dfs.core.windows.net/autoloader/checkpoints/weather_realtime/"
stream = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(weather_schema).load(source)

bronze = stream.select(
    col("location.name").alias("location_name"), col("location.region").alias("region"),
    col("location.country").alias("country"), col("location.lat").alias("latitude"), col("location.lon").alias("longitude"),
    col("current.last_updated").alias("weather_observation_time"), col("current.temp_c").alias("temperature_c"),
    col("current.is_day").alias("is_day"), col("current.condition.text").alias("weather_condition"),
    col("current.condition.code").alias("weather_code"), col("current.wind_kph").alias("wind_speed_kph"),
    col("current.wind_degree").alias("wind_degree"), col("current.wind_dir").alias("wind_direction"),
    col("current.precip_mm").alias("precipitation_mm"), col("current.humidity").alias("relative_humidity"),
    col("current.cloud").alias("cloud_cover"), col("current.feelslike_c").alias("feels_like_c"),
    col("current.gust_kph").alias("wind_gust_kph"), col("current.vis_km").alias("visibility_km"),
    col("_metadata.file_path").alias("_source_file"), col("_metadata.file_name").alias("_source_file_name"),
    current_timestamp().alias("_ingestion_timestamp"), lit("WeatherAPI").alias("_source_system")
)

q=(bronze.writeStream.format("delta").option("checkpointLocation", checkpoint).trigger(availableNow=True).toTable("nyc_taxi.bronze.weather_realtime"))
q.awaitTermination()
