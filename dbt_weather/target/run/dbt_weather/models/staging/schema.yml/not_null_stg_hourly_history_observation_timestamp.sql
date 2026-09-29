
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observation_timestamp
from `weather-data-ingestion-506305`.`weather_data`.`stg_hourly_history`
where observation_timestamp is null



  
  
      
    ) dbt_internal_test