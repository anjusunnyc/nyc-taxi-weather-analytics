# Architecture

Store the current NYC Taxi & Weather Lakehouse architecture diagram and supporting architecture documentation here.

Current design: NYC TLC batch data, taxi-zone reference data and historical Open-Meteo weather land in ADLS Gen2 and flow through Databricks Bronze, Silver and Gold layers. WeatherAPI current observations are ingested every 15 minutes by Databricks Workflows, stored as timestamped JSON in ADLS, processed with Auto Loader and Structured Streaming, and served through realtime Gold views. Power BI consumes Gold data through a Databricks SQL Warehouse.
