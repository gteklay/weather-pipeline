
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select airflow_run_id
from `weather-data-ingestion-506305`.`weather_data`.`stg_hourly_history`
where airflow_run_id is null



  
  
      
    ) dbt_internal_test