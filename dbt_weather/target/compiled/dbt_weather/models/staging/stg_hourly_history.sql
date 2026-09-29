WITH hourly_history_source AS (
    --  FIXED: We use the explicit source() macro. 
    -- dbt handles the backticks and hyphenated project string IDs automatically behind the scenes!
    SELECT * FROM `weather-data-ingestion-506305`.`weather_data`.`raw_hourly_history`
)

SELECT
    --  The columns are explicitly read out of your synchronized source map view
    CAST(target_latitude AS FLOAT64) AS latitude,
    CAST(target_longitude AS FLOAT64) AS longitude,
    timezone_name,
    CAST(timezone_offset AS INT64) AS timezone_offset,
    CAST(temperature AS FLOAT64) AS temperature_celsius,
    CAST(feels_like AS FLOAT64) AS feels_like_celsius,
    CAST(humidity AS INT64) AS relative_humidity_percentage,
    CAST(cloud_cover AS INT64) AS cloud_cover_percentage,
    CAST(wind_speed AS FLOAT64) AS wind_speed_kmh,
    CAST(weather_code AS INT64) AS weather_code,
    CAST(dew_point AS FLOAT64) AS dew_point_celsius,
    CAST(pressure AS INT64) AS atmospheric_pressure_hpa,
    CAST(uv_index AS FLOAT64) AS uv_index,
    CAST(visibility_meters AS INT64) AS visibility_meters,
    observation_time AS observation_timestamp,
    sunrise_time AS sunrise_timestamp,
    sunset_time AS sunset_timestamp,
    CAST(ingested_at AS TIMESTAMP) AS ingested_at_timestamp,
    CAST(airflow_dag_id AS STRING) AS airflow_dag_id,
    CAST(airflow_run_id AS STRING) AS airflow_run_id

FROM hourly_history_source
WHERE observation_time IS NOT NULL