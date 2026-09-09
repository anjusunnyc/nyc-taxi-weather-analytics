# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 12 - Delta Features and Maintenance
# MAGIC Use the first section for demonstrations. Do not schedule schema-change demonstrations in a recurring production workflow. The maintenance section can be scheduled separately.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Version history / Time Travel
# MAGIC DESCRIBE HISTORY nyc_taxi.gold.weather_demand_impact;

# COMMAND ----------
# MAGIC %md
# MAGIC For Time Travel, query a version that exists in DESCRIBE HISTORY, for example:
# MAGIC `SELECT * FROM nyc_taxi.gold.weather_demand_impact VERSION AS OF 0;`

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Physical maintenance
# MAGIC OPTIMIZE nyc_taxi.silver.taxi_trips;
# MAGIC VACUUM nyc_taxi.silver.taxi_trips RETAIN 168 HOURS DRY RUN;
