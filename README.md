# NYC Taxi & Weather Impact Analytics

## End-to-End Data Engineering Project in Azure Databricks

This is an end-to-end data engineering project built on **Azure Databricks** to ingest, process, govern, analyze, and visualize NYC Yellow Taxi and weather data using a modern **Lakehouse architecture**.

The solution combines historical NYC taxi trip data, taxi-zone reference data, historical weather observations, and near-real-time weather data. It implements batch and near-real-time pipelines across **Bronze, Silver, and Gold layers**, applies data-quality and governance controls, orchestrates workloads with **Databricks Workflows**, and serves curated analytical datasets to **Power BI**.

The project was designed around two complementary objectives: a **Data Engineering Objective**, focused on building a production-style data platform, and a **Business Analytics Objective**, focused on understanding how weather conditions affect NYC taxi operations.

---

## 1. Project Objectives

### 1.1 Data Engineering Objective

Design and implement an end-to-end Azure Databricks Lakehouse solution that demonstrates the major components of a modern data engineering workflow:

- Ingest batch and near-real-time data from multiple external sources.
- Land source data in **Azure Data Lake Storage Gen2 (ADLS Gen2)**.
- Govern storage and tables using **Unity Catalog** and External Locations.
- Implement the **Medallion Architecture: Bronze -> Silver -> Gold**.
- Use **Databricks Auto Loader** for incremental file ingestion.
- Use **Spark Structured Streaming** and checkpoints for incremental near-real-time processing.
- Perform cleansing, validation, schema standardization, deduplication, joins, and derived-column creation using **Apache Spark**.
- Use **Delta Lake MERGE** for idempotent and incremental loading.
- Demonstrate Delta Lake functionality including **ACID tables, schema evolution, version history, Time Travel, OPTIMIZE, and VACUUM**.
- Secure API credentials with **Azure Key Vault** and a Databricks Key Vault-backed secret scope.
- Apply **Unity Catalog row filters and column masks** for data governance.
- Orchestrate modular batch and near-real-time pipelines using **Databricks Workflows**.
- Build Gold-layer business metrics and serving views.
- Connect the Gold layer to **Power BI** through a Databricks SQL Warehouse.
- Build a **Power BI semantic model** using shared Weather Condition and Zone dimensions.
- Create interactive dashboards for historical analysis and near-real-time weather intelligence.
- Organize project notebooks and assets for **GitHub source control and reproducibility**.

### 1.2 Business Analytics Objectives

The platform answers five business questions:

1. **Speed Impact** - Analyze the expected change in average taxi speed by weather condition and map the current weather to historical patterns to estimate the expected current pace.
2. **Demand Impact** - Measure the expected change in trip volume by weather condition using a normalized clear-weather baseline and use current weather as an indicator of expected demand behavior.
3. **Fare Impact** - Determine whether fare per mile changes during adverse weather compared with clear conditions.
4. **Tipping Behavior** - Determine whether average passenger tipping behavior changes during adverse weather compared with clear conditions.
5. **Zone Sensitivity** - Identify pickup zones showing the largest change in taxi demand during adverse weather.

---

## 2. Business Scenario

A data engineering solution was developed for an NYC ground transportation use case to understand how weather affects taxi operations.

The engineering scope includes platform architecture, ingestion, transformation, orchestration, security, governance, analytical serving, semantic modeling, and dashboard development. The resulting Power BI solution is intended to support operational planning by combining validated historical patterns with near-real-time weather observations.

---

## 3. Technology Stack

| Area | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Lakehouse / Processing | Azure Databricks |
| Data Lake | Azure Data Lake Storage Gen2 |
| Processing Engine | Apache Spark / PySpark / Spark SQL |
| Storage Format | Delta Lake |
| Incremental File Ingestion | Databricks Auto Loader |
| Near-Real-Time Processing | Spark Structured Streaming |
| Orchestration | Databricks Workflows / Jobs |
| Governance | Unity Catalog |
| Secret Management | Azure Key Vault |
| Historical Weather | Open-Meteo Historical Weather API |
| Current Weather | WeatherAPI.com |
| Taxi Data | NYC Taxi & Limousine Commission |
| BI / Semantic Layer | Power BI |
| BI Compute | Databricks SQL Warehouse |
| Version Control | GitHub |

---

## 4. Data Sources

### NYC Yellow Taxi Trip Data

- **Source:** NYC Taxi & Limousine Commission (TLC)
- **Format:** Parquet
- **Data used:** April and May 2026
- **Bronze volume:** approximately **7.92 million trip records**
- Key fields include pickup/drop-off timestamps, trip distance, pickup/drop-off location IDs, passenger count, fare amount, total amount, payment type, and tip amount.

### NYC Taxi Zone Lookup

- **Source:** NYC TLC
- **Format:** CSV
- Static reference data containing:
  - `LocationID`
  - `Borough`
  - `Zone`
  - `service_zone`

### Historical Weather

- **Source:** Open-Meteo Historical Weather API
- **Format:** JSON
- **Granularity:** Hourly
- **Period used:** April and May 2026
- **Location:** New York City
- Variables include temperature, precipitation, rain, relative humidity, weather code, cloud cover, wind speed, and wind gusts.

### Near-Real-Time Weather

- **Source:** WeatherAPI.com Current Weather API
- **Format:** JSON
- **Polling interval:** Every 15 minutes through a Databricks Workflow
- Variables include observation time, temperature, precipitation, humidity, condition, wind speed, visibility, and cloud information.

> The API credential is not stored in source code. It is retrieved securely from Azure Key Vault through a Databricks Key Vault-backed secret scope.

---

## 5. Solution Architecture

```text
                         DATA SOURCES
        +---------------------+----------------------+
        |                     |                      |
 NYC Yellow Taxi       Taxi Zone Lookup      Historical Weather
     Parquet                 CSV                Open-Meteo API
        |                     |                      |
        +---------------------+----------------------+
                              |
                         ADLS Gen2 RAW
                              |
                      Databricks Auto Loader
                              |
                         BRONZE DELTA
                              |
                     PySpark / Spark SQL
                              |
                         SILVER DELTA
                  Cleaning | Quality | Joins
                              |
                         GOLD DELTA
                   Business-ready analytics
                              |
                   Databricks SQL Warehouse
                              |
                            Power BI


 WeatherAPI.com
       |
 Databricks Workflow (15-minute schedule)
       |
 ADLS Gen2 timestamped JSON
       |
 Auto Loader -> Bronze
       |
 Structured Streaming -> Silver
       |
 Current/Realtime Gold Views
       |
 Power BI
```

### Architecture Principles

- **ADLS Gen2** provides persistent cloud storage.
- **Unity Catalog** governs access to storage, catalogs, schemas, tables, views, and security policies.
- **Delta Lake** provides transactional storage and table history.
- **Databricks Workflows** controls pipeline execution and dependencies.
- **Power BI** consumes business-ready Gold data through a Databricks SQL Warehouse.

---

## 6. Azure and Unity Catalog Setup

The platform was configured with an **Azure Databricks Access Connector** using a managed identity.

A Unity Catalog **External Location** was created to provide governed access to the ADLS Gen2 landing container.

The project uses:

```text
Catalog: nyc_taxi

Schemas:
nyc_taxi.bronze
nyc_taxi.silver
nyc_taxi.gold
```

This separates raw, curated, and business-serving datasets while maintaining centralized governance through Unity Catalog.

---

## 7. Bronze Layer - Raw and Incremental Ingestion

The Bronze layer preserves source-level data while adding ingestion metadata needed for traceability and incremental processing.

### Taxi Ingestion

NYC TLC Parquet files are first landed in ADLS Gen2 and then processed using **Databricks Auto Loader**.

Auto Loader provides:

- Incremental file discovery
- Persistent checkpoints
- Schema tracking
- Source-file metadata
- Idempotent processing of newly arrived files

Bronze taxi ingestion includes metadata such as:

- ingestion timestamp
- source filename
- source file size
- source file modification timestamp

Bronze table:

```text
nyc_taxi.bronze.yellow_taxi
```

April and May taxi files produced approximately **7,922,076 Bronze records**.

### Taxi Zone Reference Data

The static TLC taxi-zone lookup is loaded into:

```text
nyc_taxi.bronze.taxi_zone_lookup
```

### Historical Weather

Historical Open-Meteo JSON is landed in ADLS and transformed from its nested API structure into hourly records before being stored in Bronze.

```text
Open-Meteo API
      ->
ADLS Raw JSON
      ->
Flatten Hourly Data
      ->
Bronze Delta
```

Bronze table:

```text
nyc_taxi.bronze.weather_history
```

April and May produced **1,464 hourly weather observations**.

### Near-Real-Time Weather

WeatherAPI is called by a Databricks notebook scheduled every 15 minutes.

Each API response is stored as a timestamped JSON file in ADLS. Auto Loader incrementally discovers these files and writes them to:

```text
nyc_taxi.bronze.weather_realtime
```

An **explicit nested JSON schema** was implemented after schema inference initially interpreted the `location` and `current` objects incorrectly as strings rather than nested structures.

This is a **scheduled near-real-time micro-batch architecture**, rather than an event-broker-based streaming architecture.

---

## 8. Silver Layer - Cleaning, Quality and Standardization

The Silver layer converts raw data into validated, standardized, analytics-ready datasets.

Main Silver tables:

```text
nyc_taxi.silver.taxi_trips
nyc_taxi.silver.dim_taxi_zone
nyc_taxi.silver.weather_history
nyc_taxi.silver.weather_realtime
nyc_taxi.silver.taxi_weather_enriched
```

### Taxi Data Quality

Taxi records were validated against business and analytical rules including:

- Required pickup/drop-off timestamps
- Required pickup/drop-off locations
- Positive trip distance
- Non-negative fare
- Valid trip duration
- Plausible average speed
- Plausible fare per mile
- Non-negative tip values
- Duplicate inspection

Quality diagnostics identified:

| Quality Check | Records |
|---|---:|
| Distance <= 0 | 206,546 |
| Negative fare | 28,559 |
| Duration <= 0 | 101,574 |
| Speed > 100 mph | 1,745 |
| Fare/mile > $100 | 51,785 |
| Negative tip | 91 |

The rules overlap, so these diagnostic counts should not be added together.

Overall:

```text
Bronze records : 7,922,076
Silver records : 7,537,429
Removed        :   384,647
Removed %      :      4.86%
Retained %     :     95.14%
```

### Derived Taxi Metrics

The Silver transformation creates analytical fields including:

- `trip_duration_minutes`
- `average_speed_mph`
- `fare_per_mile`
- `tip_percentage`
- `pickup_date`
- `pickup_hour`
- `pickup_day_of_week`
- `pickup_month`
- `pickup_year`
- weekend indicator
- hourly pickup timestamp

Safe division logic is used for metrics where a zero denominator could otherwise cause failures.

### Deterministic Trip Identifier

The TLC source does not provide a single unique trip identifier suitable for the pipeline.

A deterministic **SHA-256 `trip_id`** is generated from selected trip attributes and used as a business key for deduplication and incremental processing.

### Delta MERGE

The cleaned taxi dataset is loaded into Silver using a Delta Lake `MERGE`.

For this immutable trip fact, the production pipeline uses an **insert-only MERGE**:

```sql
MERGE INTO nyc_taxi.silver.taxi_trips AS t
USING taxi_silver_updates AS s
ON t.trip_id = s.trip_id
WHEN NOT MATCHED THEN INSERT *;
```

This prevents already-processed historical taxi records from being unnecessarily rewritten while preserving idempotent incremental loading.

---

## 9. Weather Standardization

Historical Open-Meteo weather and WeatherAPI current weather use different source coding systems.

The Silver layer therefore standardizes both sources into a common analytical weather classification such as:

```text
Clear
Cloudy
Rain
Snow
Fog
Thunderstorm
Other
```

An `is_adverse_weather` indicator is also created.

This canonical classification allows historical taxi behavior and current weather observations to be compared consistently.

### Structured Streaming

Near-real-time Bronze weather records are processed incrementally using **Spark Structured Streaming**.

Checkpointing ensures that previously processed source records are not repeatedly processed.

The pipeline uses an available-now micro-batch pattern: process currently available unprocessed records and stop while retaining checkpoint state for the next scheduled run.

---

## 10. Silver Enrichment

Taxi trips are enriched with two key datasets.

### Taxi Zone Join

`PULocationID` is joined to the taxi-zone dimension to add:

- Pickup zone
- Pickup borough
- Service zone

All **7,537,429 valid Silver taxi trips** successfully matched a pickup zone.

### Historical Weather Join

Taxi pickup timestamps are truncated to hourly granularity and joined to historical hourly weather.

Results:

```text
Valid taxi trips       : 7,537,429
Weather matched        : 7,537,415
Weather not matched    :        14
```

The resulting enriched dataset supports all five analytical objectives:

```text
nyc_taxi.silver.taxi_weather_enriched
```

---

## 11. Gold Layer - Business Analytics

The Gold layer contains business-ready aggregations and serving views that answer the five analytical objectives.

### Objective 1 - Speed Impact

Average taxi speed was calculated by weather condition and compared with the clear-weather baseline.

Historical results:

| Weather | Avg Speed (mph) | Change vs Clear |
|---|---:|---:|
| Clear | 10.83 | 0.00% |
| Cloudy | 10.54 | -2.73% |
| Rain | 10.21 | -5.71% |
| Snow | 9.15 | -15.56% |

The current weather condition can then be mapped to this historical rule to provide an **expected current speed impact**.

This is an expected impact based on historical behavior; it is not a measurement of live taxi speed.

### Objective 2 - Demand Impact

Raw demand comparisons can be misleading because taxi demand naturally varies by hour and day of week.

Demand was therefore normalized by comparing each weather period against **clear-weather demand for the corresponding day-of-week and hour-of-day**.

Results:

| Weather | Demand Change vs Expected Clear |
|---|---:|
| Clear | 0.00% |
| Cloudy | -1.05% |
| Rain | -0.31% |
| Snow | +8.27%* |

\* Snow had only one comparable weather hour and is therefore treated as **inconclusive / low sample**.

### Objective 3 - Fare Impact

Fare impact was evaluated using **fare per mile**, rather than raw fare, to reduce the effect of different trip distances.

| Weather | Avg Fare/Mile | Change vs Clear |
|---|---:|---:|
| Clear | $8.41 | 0.00% |
| Cloudy | $8.53 | +1.44% |
| Snow | $8.91 | +5.87% |
| Rain | $9.10 | +8.18% |

The results show an association between adverse weather and higher observed fare per mile. They should not be interpreted as proof that weather itself caused the increase.

### Objective 4 - Tipping Behavior

Tipping analysis was restricted to **credit-card trips (`payment_type = 1`)**, where recorded tip amounts provide a meaningful comparison.

| Weather | Avg Tip % | Change vs Clear |
|---|---:|---:|
| Clear | 24.31% | 0.00% |
| Cloudy | 25.01% | +2.87% |
| Rain | 24.61% | +1.22% |
| Snow | 23.96% | -1.44% |

### Objective 5 - Zone Sensitivity

Pickup-zone demand during adverse weather was compared with expected clear-weather demand for equivalent day-of-week and hour-of-day periods.

Examples of larger demand decreases include:

- Belmont: approximately -31.74%
- Battery Park: approximately -29.87%
- Red Hook: approximately -25.68%
- Middle Village: approximately -22.64%
- LaGuardia Airport: approximately -12.24%

Examples of larger increases include:

- Hunts Point: approximately +36.55%
- Brooklyn Navy Yard: approximately +25.27%
- Maspeth: approximately +23.94%
- Long Island City / Hunters Point: approximately +23.66%
- Elmhurst: approximately +18.21%

The analysis applies minimum-support thresholds to reduce the effect of extremely sparse zone/weather combinations.

---

## 12. Near-Real-Time Serving Layer

The near-real-time pipeline maps the latest WeatherAPI observation to validated historical Gold metrics.

Serving views include:

```text
nyc_taxi.gold.vw_current_weather
nyc_taxi.gold.vw_current_weather_impact
nyc_taxi.gold.vw_realtime_speed_impact_history
```

`vw_current_weather` identifies the latest available Silver weather observation.

`vw_current_weather_impact` maps the current canonical weather category to historical Gold metrics for:

- Speed
- Demand
- Fare
- Tipping

The realtime speed-impact history view allows Power BI to show how the **expected historical speed-impact rule** changes as new weather observations arrive.

Again, this represents expected impact based on historical rules, not measured realtime taxi speed.

---

## 13. Databricks Workflows

### Batch Workflow

The batch workflow was split into modular tasks with explicit dependencies.

```text
batch_taxi_ingestion ----------> silver_taxi -------------------+
                                                                 |
batch_reference_ingestion -----> silver_zone -------------------+--> silver_enrichment
                                                                 |          |
batch_historical_weather ------> silver_historical_weather -----+          |
                                                                            v
                                                               gold_historical_analytics
```

Independent ingestion branches can run in parallel before converging at Silver enrichment.

This demonstrates:

- Task dependencies
- Parallel processing
- Modular notebook execution
- Failure isolation
- Repeatable orchestration

### Near-Real-Time Workflow

A separate workflow handles current weather:

```text
realtime_weather_ingestion
           |
           v
silver_realtime_weather
```

It is scheduled every **15 minutes** and can run on Databricks serverless compute without keeping the development cluster continuously active.

---

## 14. Delta Lake Functionality

The project demonstrates several Delta Lake capabilities.

### ACID Transactions

Bronze, Silver, and Gold physical tables use Delta Lake, providing transactional reliability for writes and updates.

### MERGE / Incremental Loading

Delta MERGE is used to make taxi Silver loading idempotent and incremental.

### Schema Evolution

A Gold demand table was altered to add an analytical note column, demonstrating controlled schema evolution.

### Version History

Delta table history was inspected to identify operations such as:

- Table creation
- Schema changes
- Updates

### Time Travel

Earlier table versions can be queried to inspect or reproduce historical table states.

### OPTIMIZE

`OPTIMIZE` was executed on the Silver taxi table to demonstrate Delta file-layout maintenance.

### VACUUM

A safe seven-day retention check was performed:

```sql
VACUUM nyc_taxi.silver.taxi_trips
RETAIN 168 HOURS
DRY RUN;
```

No obsolete files were eligible for deletion at the time of the dry run.

---

## 15. Security and Governance

### Azure Key Vault

The WeatherAPI key is stored in **Azure Key Vault** and accessed through a Databricks Key Vault-backed secret scope.

This avoids hard-coding credentials in notebooks or GitHub.

### Unity Catalog Row Filtering

A Unity Catalog row-filter function was applied to a secure Gold serving table to demonstrate identity-based geographic access.

The implementation allows the project administrator full access while demonstrating how restricted users could be limited to authorized borough-level data.

### Unity Catalog Column Masking

Column masks were demonstrated on business-sensitive analytical measures such as fare metrics.

Authorized users can see the original values, while unauthorized users receive masked values.

This demonstrates centralized governance without creating separate copies of the same dataset.

---

## 16. Power BI Semantic Model

Power BI connects to the Databricks Gold layer through a **Databricks SQL Warehouse**.

A semantic model was created using shared dimensions rather than treating every analytical table as an isolated dataset.

### Weather Dimension

```text
DimWeatherCondition
        1
        |
        +---- * weather_speed_impact
        |
        +---- * weather_demand_impact
        |
        +---- * weather_fare_impact
        |
        +---- * weather_tip_impact
```

### Zone Dimension

```text
DimZone
   1
   |
   *
zone_weather_sensitivity
```

Relationships use a **one-to-many (1:*)** pattern with single-direction filtering from dimensions to analytical fact tables.

This supports consistent slicers and business logic across report visuals.

---

## 17. Power BI Dashboard

The report combines historical analytics with near-real-time weather intelligence.

### Page 1 - Current Weather and Speed Intelligence

Key components include:

- Current Weather KPI
- Current Expected Speed Impact
- Current Expected Demand Impact
- Current Fare Impact
- Current Tip Impact
- Historical speed impact by weather
- Near-real-time expected speed-impact history
- Expected current taxi speed gauge

### Page 2 - Business Impact Analysis

The second page covers:

- Actual vs expected taxi demand by weather
- Fare impact analysis
- Tipping behavior
- Zone sensitivity
- Interactive zone/borough filtering
- Operational recommendations

---

## 18. Key Recommendations

Based on the five analyses:

1. **Optimize driver allocation during adverse weather** by repositioning taxis toward zones where demand historically increases.
2. **Account for reduced travel speeds during poor weather**, particularly rain and snow, when planning trip times and fleet availability.
3. **Use weather conditions in pricing and revenue planning**, as fare per mile was higher during adverse-weather periods, with rain showing the largest observed increase.
4. **Integrate near-real-time weather with historical patterns** to anticipate changes in speed, demand, fares, and passenger behavior before operational decisions are made.

---

## 19. Suggested Repository Structure

```text
nyc-taxi-weather-analytics/
|
+-- README.md
|
+-- notebooks/
|   +-- 01_batch_taxi_ingestion.py
|   +-- 02_batch_reference_ingestion.py
|   +-- 03_batch_historical_weather.py
|   +-- 04_realtime_weather_ingestion.py
|   +-- 05_silver_taxi.py
|   +-- 06_silver_zone.py
|   +-- 07_silver_historical_weather.py
|   +-- 08_silver_realtime_weather.py
|   +-- 09_silver_enrichment.py
|   +-- 10_gold_historical_analytics.py
|   +-- 11_gold_realtime_serving.py
|   +-- 12_delta_features_and_maintenance.py
|   +-- 13_security_governance.py
|
+-- docs/
|   +-- architecture/
|   +-- screenshots/
|   +-- project-report/
|
+-- powerbi/
|   +-- README.md
|
+-- sql/
|   +-- optional-serving-queries.sql
|
+-- .gitignore
```

Do **not** commit:

- API keys
- Azure credentials
- Databricks tokens
- `.env` files containing secrets
- Power BI or Databricks credentials
- local temporary files
- raw multi-million-row source datasets unless intentionally required

---

## 20. Project Highlights

This project demonstrates an end-to-end data engineering lifecycle rather than only a set of analytical notebooks:

- Multi-source ingestion
- Batch + near-real-time processing
- ADLS Gen2 storage
- Unity Catalog governance
- Auto Loader
- Spark Structured Streaming
- Delta Lake
- Incremental MERGE processing
- Data-quality validation
- Historical and realtime data standardization
- Fact/reference enrichment
- Medallion architecture
- Gold analytical modeling
- Delta versioning and maintenance
- Azure Key Vault secret management
- Row-level filtering
- Column masking
- Databricks Workflows
- SQL Warehouse serving
- Power BI semantic modeling
- Interactive operational analytics
- GitHub-ready modular notebook organization

---

## 21. Important Analytical Notes

- Near-real-time weather is collected on a **15-minute schedule**. This is scheduled micro-batch processing, not event-by-event streaming.
- The current weather does not directly measure current taxi speed or demand. It is mapped to **historical weather-impact rules**.
- Snow results should be interpreted cautiously where sample sizes are small.
- Fare and demand findings represent observed historical associations and should not automatically be interpreted as causal effects.
- Tipping analysis is restricted to credit-card trips.
- Zone sensitivity represents adverse-weather behavior collectively unless a weather-condition-specific zone analysis is explicitly created.

---

## 22. Future Enhancements

Potential extensions include:

- Additional months or years of NYC TLC data
- More robust seasonal and holiday normalization
- Weather forecasts rather than only current observations
- Machine-learning demand forecasting
- Feature engineering using weather severity
- Databricks Asset Bundles for CI/CD
- Automated testing and data-quality expectations
- Power BI Service scheduled refresh or DirectQuery where appropriate
- More granular weather-by-zone demand analysis
- Monitoring and alerting for pipeline failures and data-quality thresholds

---

## Conclusion

The project demonstrates how **Azure Databricks, ADLS Gen2, Delta Lake, Unity Catalog, Spark, Azure Key Vault, Databricks Workflows, and Power BI** can be combined to create a governed end-to-end Lakehouse solution.

It moves beyond raw ingestion by implementing incremental processing, data-quality controls, standardized Silver datasets, historical/realtime enrichment, business-ready Gold metrics, governance policies, workflow orchestration, semantic modeling, and operational visualization.

The final result provides both a reusable **data engineering architecture** and a practical **weather-impact analytics solution for NYC taxi operations**.
