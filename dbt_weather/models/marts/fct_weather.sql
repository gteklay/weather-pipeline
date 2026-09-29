WITH staging_data AS (
    SELECT * FROM {{ ref('stg_hourly_history') }}
)

SELECT
    latitude,
    longitude,
    timezone_name,
    timezone_offset,
    temperature_celsius,
    feels_like_celsius,
    relative_humidity_percentage,
    cloud_cover_percentage,
    wind_speed_kmh,
    weather_code,
    dew_point_celsius,
    atmospheric_pressure_hpa,
    uv_index,
    visibility_meters,
    observation_timestamp,
    ingested_at_timestamp,
    airflow_dag_id,
    airflow_run_id,
    
    --  THE DYNAMIC LOCAL TIME CONVERTER: 
    -- Automatically maps the time zone straight from your table's data stream row metrics!
    DATETIME(observation_timestamp, timezone_name) AS observation_local_time,
    
    CASE 
        WHEN observation_timestamp BETWEEN sunrise_timestamp AND sunset_timestamp THEN TRUE 
        ELSE FALSE 
    END AS is_daylight,
    TIMESTAMP_DIFF(sunset_timestamp, observation_timestamp, HOUR) AS hours_until_sunset
FROM staging_data
