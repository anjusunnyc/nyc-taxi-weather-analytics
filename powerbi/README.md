# Power BI

Power BI connects to the Databricks Gold layer through a Databricks SQL Warehouse.

## Report pages

1. **Current Weather and Speed Intelligence** — current weather KPI cards, expected speed/demand/fare/tip impacts, historical speed impact, near-real-time expected speed-impact history, and expected current taxi speed gauge.
2. **Historical Weather Impact Analysis** — actual vs expected demand, fare impact, tipping behavior, zone sensitivity, and operational recommendations.

## Semantic model

Shared dimensions include `DimWeatherCondition` and `DimZone`, connected to the Gold analytical tables with single-direction filtering where appropriate.

Do not commit credentials or connection secrets. If the PBIX file becomes too large for normal Git usage, store report screenshots and model documentation here instead.
