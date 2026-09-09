# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03 - Historical Weather Ingestion

# COMMAND ----------
import requests, json
from datetime import datetime
from pyspark.sql.functions import arrays_zip, explode, col, current_timestamp, lit, to_timestamp

dbutils.widgets.text("start_date", "2026-04-01", "Start date")
dbutils.widgets.text("end_date", "2026-05-31", "End date")
dbutils.widgets.text("batch_label", "2026-04_2026-05", "Batch label")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
batch_label = dbutils.widgets.get("batch_label")

params = {
    "latitude": 40.7128, "longitude": -74.0060,
    "start_date": start_date, "end_date": end_date,
    "hourly": ",".join(["temperature_2m","precipitation","rain","relative_humidity_2m","weather_code","cloud_cover","wind_speed_10m","wind_gusts_10m"]),
    "timezone": "America/New_York"
}
r = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=120)
r.raise_for_status()
raw = r.json()
local = f"/tmp/weather_history_{batch_label}.json"
with open(local, "w") as f: json.dump(raw, f)
raw_target = f"abfss://landing@projecttaxidata.dfs.core.windows.net/raw/weather/weather_history_{batch_label}.json"
dbutils.fs.cp("file:" + local, raw_target, True)
print(f"Historical weather landed: {raw_target}")

# COMMAND ----------
weather_raw_path = "abfss://landing@projecttaxidata.dfs.core.windows.net/raw/weather/"
weather_raw_df = spark.read.option("multiLine", "true").json(weather_raw_path)

weather_flat = weather_raw_df.select(explode(arrays_zip(
    col("hourly.time"), col("hourly.temperature_2m"), col("hourly.precipitation"),
    col("hourly.rain"), col("hourly.relative_humidity_2m"), col("hourly.weather_code"),
    col("hourly.cloud_cover"), col("hourly.wind_speed_10m"), col("hourly.wind_gusts_10m")
)).alias("weather"))

weather_bronze = weather_flat.select(
    to_timestamp(col("weather.time")).alias("weather_timestamp"),
    col("weather.temperature_2m").alias("temperature_2m"),
    col("weather.precipitation").alias("precipitation"),
    col("weather.rain").alias("rain"),
    col("weather.relative_humidity_2m").alias("relative_humidity_2m"),
    col("weather.weather_code").alias("weather_code"),
    col("weather.cloud_cover").alias("cloud_cover"),
    col("weather.wind_speed_10m").alias("wind_speed_10m"),
    col("weather.wind_gusts_10m").alias("wind_gusts_10m"),
    current_timestamp().alias("_ingestion_timestamp"), lit("Open-Meteo").alias("_source_system")
).dropDuplicates(["weather_timestamp"])

(weather_bronze.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("nyc_taxi.bronze.weather_history"))
