
    
    



select ingested_at_timestamp
from `weather-data-ingestion-506305`.`weather_data`.`stg_hourly_history`
where ingested_at_timestamp is null


