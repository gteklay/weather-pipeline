-- THE HARDENED PRODUCTION DATA SINK
CREATE TABLE IF NOT EXISTS `weather-data-ingestion-506305.weather_data.raw_hourly_history` (
    -- Geographic Anchor Metrics
    target_latitude FLOAT64 OPTIONS(description="Geographic latitude coordinate decimal."),
    target_longitude FLOAT64 OPTIONS(description="Geographic longitude coordinate decimal."),
    
    -- Geographic Time Identity Matrices
    timezone_name STRING OPTIONS(description="The text identifier name string of the regional timezone zone."),
    timezone_offset INT64 OPTIONS(description="The mathematical timezone offset shift value calculated in seconds."),
    
    -- Core Ambient & Apparent Atmospheric Metrics
    temperature FLOAT64 OPTIONS(description="The recorded ambient surface air temperature metric measured in Celsius."),
    feels_like FLOAT64 OPTIONS(description="The apparent thermal sensation parameter variable calculated in Celsius."),
    humidity INT64 OPTIONS(description="The relative humidity percentage concentration value range."),
    cloud_cover INT64 OPTIONS(description="The total cloud cover percentage distribution scale."),
    wind_speed FLOAT64 OPTIONS(description="The calculated average wind vector tracking speed measured in km/h."),
    weather_code INT64 OPTIONS(description="The OpenWeather static condition classification lookup key ID code."),
    
    --4 EXPANDED ADVANCED METEOROLOGICAL VECTORS
    dew_point FLOAT64 OPTIONS(description="The precise calculated dew point saturation temperature value in Celsius."),
    pressure INT64 OPTIONS(description="The surface barometric atmospheric pressure mapping vector measured in hPa."),
    uv_index FLOAT64 OPTIONS(description="The absolute calculated Ultraviolet Index exposure radiation severity scale."),
    visibility_meters INT64 OPTIONS(description="The maximum horizontal visibility vector distance clear to the eye in metres."),
    
    -- Chronological Timeline Anchors
    observation_time TIMESTAMP OPTIONS(description="The strict UTC timestamp tracking of the meteorological lookup loop."),
    sunrise_time TIMESTAMP OPTIONS(description="The absolute UTC solar sunrise boundary timeline constraint coordinate."),
    sunset_time TIMESTAMP OPTIONS(description="The absolute UTC solar sunset boundary timeline constraint coordinate."),
    ingested_at_timestamp TIMESTAMP OPTIONS(description="The timestamp indicating when the data was ingested into the system."),
    airflow_dag_id STRING OPTIONS(description="The unique identifier for the Airflow DAG that processed this data."),
    airflow_run_id STRING OPTIONS(description="The unique identifier for the specific Airflow run that processed
)
OPTIONS(
    description="The unified, join-free incremental raw logging stream housing high-resolution weather variables for target locations."
);
