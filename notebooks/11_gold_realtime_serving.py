# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 11 - Gold Realtime Serving Views

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_current_weather AS
# MAGIC SELECT * FROM (SELECT weather_timestamp,weather_hour,location_name,temperature_c,precipitation_mm,relative_humidity,weather_condition,weather_condition_category,wind_speed_kph,wind_gust_kph,visibility_km,is_adverse_weather,ROW_NUMBER() OVER (ORDER BY weather_timestamp DESC) rn FROM nyc_taxi.silver.weather_realtime) WHERE rn=1;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_current_weather_impact AS
# MAGIC SELECT cw.weather_timestamp,cw.location_name,cw.temperature_c,cw.precipitation_mm,cw.relative_humidity,cw.weather_condition,cw.weather_condition_category,cw.wind_speed_kph,cw.is_adverse_weather,
# MAGIC s.avg_speed_mph historical_avg_speed_mph,s.speed_change_pct,d.avg_actual_trips_per_hour historical_avg_trips_per_hour,d.avg_expected_clear_demand expected_clear_trips_per_hour,d.demand_change_pct,f.avg_fare_per_mile,f.fare_change_pct,t.avg_tip_percentage,t.tip_change_pct
# MAGIC FROM nyc_taxi.gold.vw_current_weather cw
# MAGIC LEFT JOIN nyc_taxi.gold.weather_speed_impact s USING (weather_condition_category)
# MAGIC LEFT JOIN nyc_taxi.gold.weather_demand_impact d USING (weather_condition_category)
# MAGIC LEFT JOIN nyc_taxi.gold.weather_fare_impact f USING (weather_condition_category)
# MAGIC LEFT JOIN nyc_taxi.gold.weather_tip_impact t USING (weather_condition_category);

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_realtime_speed_impact_history AS
# MAGIC WITH realtime_dedup AS (
# MAGIC   SELECT weather_timestamp,weather_hour,location_name,temperature_c,precipitation_mm,relative_humidity,weather_condition,weather_condition_category,wind_speed_kph,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY weather_timestamp,weather_condition ORDER BY weather_timestamp DESC) rn
# MAGIC   FROM nyc_taxi.silver.weather_realtime
# MAGIC )
# MAGIC SELECT r.weather_timestamp,r.weather_hour,r.location_name,r.temperature_c,r.precipitation_mm,r.relative_humidity,r.weather_condition,r.weather_condition_category,r.wind_speed_kph,s.speed_change_pct
# MAGIC FROM realtime_dedup r
# MAGIC LEFT JOIN nyc_taxi.gold.weather_speed_impact s ON r.weather_condition_category=s.weather_condition_category
# MAGIC WHERE r.rn=1;
