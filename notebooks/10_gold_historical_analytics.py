# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 10 - Gold Historical Analytics

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_weather_speed_impact AS
# MAGIC WITH x AS (SELECT weather_condition_category,is_adverse_weather,COUNT(*) trip_count,AVG(average_speed_mph) avg_speed_mph FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category IS NOT NULL AND average_speed_mph IS NOT NULL GROUP BY 1,2),
# MAGIC b AS (SELECT AVG(average_speed_mph) clear_avg_speed_mph FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category='Clear' AND average_speed_mph IS NOT NULL)
# MAGIC SELECT x.weather_condition_category,x.is_adverse_weather,x.trip_count,ROUND(x.avg_speed_mph,2) avg_speed_mph,ROUND(b.clear_avg_speed_mph,2) clear_avg_speed_mph,ROUND((x.avg_speed_mph-b.clear_avg_speed_mph)/b.clear_avg_speed_mph*100,2) speed_change_pct FROM x CROSS JOIN b;
# MAGIC CREATE OR REPLACE TABLE nyc_taxi.gold.weather_speed_impact USING DELTA AS SELECT * FROM nyc_taxi.gold.vw_weather_speed_impact;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_demand_vs_baseline AS
# MAGIC WITH hourly AS (SELECT pickup_hour_timestamp,pickup_day_of_week,pickup_hour,weather_condition_category,is_adverse_weather,COUNT(*) actual_trip_count FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category IS NOT NULL GROUP BY 1,2,3,4,5),
# MAGIC base AS (SELECT pickup_day_of_week,pickup_hour,AVG(actual_trip_count) expected_clear_demand FROM hourly WHERE weather_condition_category='Clear' GROUP BY 1,2)
# MAGIC SELECT h.*,b.expected_clear_demand,ROUND((h.actual_trip_count-b.expected_clear_demand)/b.expected_clear_demand*100,2) hourly_demand_change_pct FROM hourly h LEFT JOIN base b USING (pickup_day_of_week,pickup_hour);
# MAGIC
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_weather_demand_impact_adjusted AS
# MAGIC SELECT weather_condition_category,is_adverse_weather,COUNT(*) weather_hours,ROUND(AVG(actual_trip_count),2) avg_actual_trips_per_hour,ROUND(AVG(expected_clear_demand),2) avg_expected_clear_demand,ROUND((AVG(actual_trip_count)-AVG(expected_clear_demand))/AVG(expected_clear_demand)*100,2) demand_change_pct
# MAGIC FROM nyc_taxi.gold.vw_demand_vs_baseline WHERE expected_clear_demand IS NOT NULL GROUP BY 1,2;
# MAGIC CREATE OR REPLACE TABLE nyc_taxi.gold.weather_demand_impact USING DELTA AS SELECT * FROM nyc_taxi.gold.vw_weather_demand_impact_adjusted;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_weather_fare_impact AS
# MAGIC WITH x AS (SELECT weather_condition_category,is_adverse_weather,COUNT(*) trip_count,AVG(fare_per_mile) avg_fare_per_mile FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category IS NOT NULL AND fare_per_mile>0 GROUP BY 1,2),
# MAGIC b AS (SELECT AVG(fare_per_mile) clear_avg_fare_per_mile FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category='Clear' AND fare_per_mile>0)
# MAGIC SELECT x.weather_condition_category,x.is_adverse_weather,x.trip_count,ROUND(x.avg_fare_per_mile,2) avg_fare_per_mile,ROUND(b.clear_avg_fare_per_mile,2) clear_avg_fare_per_mile,ROUND((x.avg_fare_per_mile-b.clear_avg_fare_per_mile)/b.clear_avg_fare_per_mile*100,2) fare_change_pct FROM x CROSS JOIN b;
# MAGIC CREATE OR REPLACE TABLE nyc_taxi.gold.weather_fare_impact USING DELTA AS SELECT * FROM nyc_taxi.gold.vw_weather_fare_impact;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_weather_tip_impact AS
# MAGIC WITH x AS (SELECT weather_condition_category,is_adverse_weather,COUNT(*) trip_count,AVG(tip_percentage) avg_tip_percentage FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category IS NOT NULL AND tip_percentage>=0 AND payment_type=1 GROUP BY 1,2),
# MAGIC b AS (SELECT AVG(tip_percentage) clear_avg_tip_percentage FROM nyc_taxi.silver.taxi_weather_enriched WHERE weather_condition_category='Clear' AND tip_percentage>=0 AND payment_type=1)
# MAGIC SELECT x.weather_condition_category,x.is_adverse_weather,x.trip_count,ROUND(x.avg_tip_percentage,2) avg_tip_percentage,ROUND(b.clear_avg_tip_percentage,2) clear_avg_tip_percentage,ROUND((x.avg_tip_percentage-b.clear_avg_tip_percentage)/b.clear_avg_tip_percentage*100,2) tip_change_pct FROM x CROSS JOIN b;
# MAGIC CREATE OR REPLACE TABLE nyc_taxi.gold.weather_tip_impact USING DELTA AS SELECT * FROM nyc_taxi.gold.vw_weather_tip_impact;

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_zone_hourly_demand AS
# MAGIC SELECT pickup_hour_timestamp,pickup_day_of_week,pickup_hour,pickup_location_id,pickup_borough,pickup_zone,weather_condition_category,is_adverse_weather,COUNT(*) trip_count FROM nyc_taxi.silver.taxi_weather_enriched WHERE pickup_zone IS NOT NULL AND weather_condition_category IS NOT NULL GROUP BY 1,2,3,4,5,6,7,8;
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_zone_clear_baseline AS
# MAGIC SELECT pickup_location_id,pickup_borough,pickup_zone,pickup_day_of_week,pickup_hour,AVG(trip_count) expected_clear_demand FROM nyc_taxi.gold.vw_zone_hourly_demand WHERE weather_condition_category='Clear' GROUP BY 1,2,3,4,5;
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_zone_adverse_impact AS
# MAGIC SELECT a.pickup_hour_timestamp,a.pickup_location_id,a.pickup_borough,a.pickup_zone,a.weather_condition_category,a.trip_count actual_trip_count,b.expected_clear_demand,ROUND((a.trip_count-b.expected_clear_demand)/b.expected_clear_demand*100,2) demand_change_pct FROM nyc_taxi.gold.vw_zone_hourly_demand a JOIN nyc_taxi.gold.vw_zone_clear_baseline b ON a.pickup_location_id=b.pickup_location_id AND a.pickup_day_of_week=b.pickup_day_of_week AND a.pickup_hour=b.pickup_hour WHERE a.is_adverse_weather=1 AND b.expected_clear_demand>0;
# MAGIC CREATE OR REPLACE VIEW nyc_taxi.gold.vw_zone_weather_sensitivity AS
# MAGIC SELECT pickup_location_id,pickup_borough,pickup_zone,COUNT(*) adverse_weather_hours,SUM(actual_trip_count) actual_adverse_trips,ROUND(SUM(expected_clear_demand),2) expected_clear_trips,ROUND((SUM(actual_trip_count)-SUM(expected_clear_demand))/SUM(expected_clear_demand)*100,2) demand_change_pct FROM nyc_taxi.gold.vw_zone_adverse_impact GROUP BY 1,2,3 HAVING COUNT(*)>=20 AND SUM(expected_clear_demand)>=100;
# MAGIC CREATE OR REPLACE TABLE nyc_taxi.gold.zone_weather_sensitivity USING DELTA AS SELECT * FROM nyc_taxi.gold.vw_zone_weather_sensitivity;
