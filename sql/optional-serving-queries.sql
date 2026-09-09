-- NYC Taxi & Weather Analytics
-- Optional validation / serving queries.

-- Latest realtime weather impact
SELECT *
FROM nyc_taxi.gold.vw_current_weather_impact;

-- Historical Gold metrics
SELECT * FROM nyc_taxi.gold.weather_speed_impact;
SELECT * FROM nyc_taxi.gold.weather_demand_impact;
SELECT * FROM nyc_taxi.gold.weather_fare_impact;
SELECT * FROM nyc_taxi.gold.weather_tip_impact;
SELECT * FROM nyc_taxi.gold.zone_weather_sensitivity;

-- Near-real-time expected speed impact history
SELECT *
FROM nyc_taxi.gold.vw_realtime_speed_impact_history
ORDER BY weather_timestamp DESC;
