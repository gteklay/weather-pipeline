
    
    



select airflow_dag_id
from `weather-data-ingestion-506305`.`weather_data`.`stg_hourly_history`
where airflow_dag_id is null


