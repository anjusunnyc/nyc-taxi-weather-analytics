# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 13 - Security and Governance
# MAGIC Run as a one-time deployment/configuration notebook, not on every pipeline execution.

# COMMAND ----------
admin_user = spark.sql("SELECT current_user() AS u").first()[0]
admin_sql = admin_user.replace("'", "''")

spark.sql("""CREATE OR REPLACE TABLE nyc_taxi.gold.secure_zone_weather_analysis USING DELTA AS SELECT * FROM nyc_taxi.gold.zone_weather_sensitivity""")
spark.sql(f"""CREATE OR REPLACE FUNCTION nyc_taxi.gold.fn_zone_row_filter(borough STRING) RETURNS BOOLEAN RETURN CASE WHEN current_user()='{admin_sql}' THEN TRUE WHEN borough='Manhattan' THEN TRUE ELSE FALSE END""")
spark.sql("""ALTER TABLE nyc_taxi.gold.secure_zone_weather_analysis SET ROW FILTER nyc_taxi.gold.fn_zone_row_filter ON (pickup_borough)""")

spark.sql("""CREATE OR REPLACE TABLE nyc_taxi.gold.secure_weather_financial_analysis USING DELTA AS
SELECT f.weather_condition_category,f.is_adverse_weather,f.avg_fare_per_mile,f.fare_change_pct,t.avg_tip_percentage,t.tip_change_pct
FROM nyc_taxi.gold.weather_fare_impact f LEFT JOIN nyc_taxi.gold.weather_tip_impact t USING (weather_condition_category)""")
spark.sql(f"""CREATE OR REPLACE FUNCTION nyc_taxi.gold.fn_financial_mask(value DOUBLE) RETURNS DOUBLE RETURN CASE WHEN current_user()='{admin_sql}' THEN value ELSE NULL END""")
for c in ["avg_fare_per_mile","fare_change_pct","avg_tip_percentage","tip_change_pct"]:
    spark.sql(f"ALTER TABLE nyc_taxi.gold.secure_weather_financial_analysis ALTER COLUMN {c} SET MASK nyc_taxi.gold.fn_financial_mask")
print("Row filter and financial masks configured")

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED nyc_taxi.gold.secure_zone_weather_analysis;
# MAGIC DESCRIBE EXTENDED nyc_taxi.gold.secure_weather_financial_analysis;
