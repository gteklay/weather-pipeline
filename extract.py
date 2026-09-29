import io
import requests
import polars as pl
import pendulum
from decimal import Decimal
from datetime import timedelta
import json

#  MODERN AIRFLOW 3 CORE SDK SUITE
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

GCP_PROJECT_ID = "weather-data-ingestion-506305"  # ⚠️ Update with your actual unique GCP Project ID
DATASET_ID = "weather_data"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ️ INTERNAL PARSING ENGINE: Normalizes types securely using Polars and Decimal math
import io
import json
import polars as pl

import json
import polars as pl

def flatten_weather_data(raw_bytes: bytes) -> dict:
    # 1. Parse raw bytes safely into a native Python dictionary array 
    json_data = json.loads(raw_bytes)
    df_raw = pl.DataFrame([json_data])

    #  FIXED: We extract index 0 out of the "data" list BEFORE pulling fields!
    df_flat = df_raw.select([
        # Root-level scalar fields
        pl.col("lat").cast(pl.Float64).alias("target_latitude"),
        pl.col("lon").cast(pl.Float64).alias("target_longitude"),
        pl.col("timezone").cast(pl.String).alias("timezone_name"),
        pl.col("timezone_offset").cast(pl.Int64),
        
        # ⚡ THE CORRECTION: Extract list index 0 first, THEN use .struct.field()
        pl.col("data").list.get(0).struct.field("temp").cast(pl.Float64).alias("temperature"),
        pl.col("data").list.get(0).struct.field("feels_like").cast(pl.Float64).alias("feels_like"),
        pl.col("data").list.get(0).struct.field("humidity").cast(pl.Int64).alias("humidity"),
        pl.col("data").list.get(0).struct.field("clouds").cast(pl.Int64).alias("cloud_cover"),
        pl.col("data").list.get(0).struct.field("wind_speed").cast(pl.Float64).alias("wind_speed"),
        pl.col("data").list.get(0).struct.field("dew_point").cast(pl.Float64).alias("dew_point"),
        pl.col("data").list.get(0).struct.field("pressure").cast(pl.Int64).alias("pressure"),
        pl.col("data").list.get(0).struct.field("uvi").cast(pl.Float64).alias("uv_index"),
        pl.col("data").list.get(0).struct.field("visibility").cast(pl.Int64).alias("visibility_meters"),
        
        # Deep extraction: Drill down into data list ➔ get weather array ➔ get index 0 ➔ get id field
        pl.col("data")
          .list.get(0)
          .struct.field("weather")
          .list.get(0)
          .struct.field("id")
          .cast(pl.Int64)
          .alias("weather_code"),
        
        # Chronological epoch parsing executed straight inside the indexed data structure
        pl.from_epoch(pl.col("data").list.get(0).struct.field("dt")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("observation_time"),
        pl.from_epoch(pl.col("data").list.get(0).struct.field("sunrise")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("sunrise_time"),
        pl.from_epoch(pl.col("data").list.get(0).struct.field("sunset")).dt.strftime("%Y-%m-%d %H:%M:%S").alias("sunset_time")
    ])
    
    # Extract the unwrapped row dictionary from index 0
    return df_flat.to_dicts()[0]

# ️ AIRFLOW 3 DECORATED MASTER BOUNDARY
@dag(
    dag_id='extract_weather_data',
    default_args=default_args,
    #  HARDENED AIRFLOW 3 PATTERN: Zero hardcoded history parameters
    start_date=None,
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'production', 'polars', 'taskflow']
)
def weather_pipeline():

    @task(task_id='extract_and_flatten_with_polars')
    def extract_and_flatten() -> dict:
        try:
            api_key = Variable.get("openweather_api_key")
        except KeyError:
            raise RuntimeError("❌ Set 'openweather_api_key' in Airflow Variables UI.")

        # 滋 PRECISION DECIMAL SAFEGUARD: Prevents rounding inaccuracies before network execution
        lat = Decimal(str(Variable.get("target_latitude", default="49.1900")))
        lon = Decimal(str(Variable.get("target_longitude", default="-122.8400")))
        
        weather_url = "https://api.openweathermap.org/data/4.0/onecall/current"
        weather_params = {
            "lat": float(lat), 
            "lon": float(lon), 
            "appid": api_key, 
            "units": "metric", 
            "lang": "en"
        }

        # Request using strict execution timeout safety limits
        response = requests.get(weather_url, params=weather_params, timeout=(5, 10))
        response.raise_for_status() 
        
        return flatten_weather_data(response.content)


    # TaskFlow API Object Capture Link
    weather_record = extract_and_flatten()


   
    load_history_to_gcp = BigQueryInsertJobOperator(
        task_id='stream_records_to_gcp',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                # ⚡ THE PERFECTION FIX: Raw plain text string query. 
                # Airflow natively extracts the fields from XCom at runtime without syntax errors!
                "query": """
                    INSERT INTO `weather-data-ingestion-506305.weather_data.raw_hourly_history` 
                    VALUES (
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['target_latitude'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['target_longitude'] }}, 
                        '{{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['timezone_name'] }}', 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['timezone_offset'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['temperature'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['feels_like'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['humidity'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['cloud_cover'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['wind_speed'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['weather_code'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['dew_point'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['pressure'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['uv_index'] }}, 
                        {{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['visibility_meters'] }},
                        TIMESTAMP('{{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['observation_time'] }}'), 
                        TIMESTAMP('{{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['sunrise_time'] }}'), 
                        TIMESTAMP('{{ ti.xcom_pull(task_ids="extract_and_flatten_with_polars")['sunset_time'] }}')
                    );
                """,
                "useLegacySql": False,
            }
        }
    )





    #  TRANSFORMATION OPERATOR: Executes downstream dbt compiles and automated quality tests [1.12]
    run_dbt_transformations = BashOperator(
        task_id='execute_isolated_dbt_transformations',
        bash_command=(
            '/home/airflow/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt_weather --profiles-dir /opt/airflow/dbt_weather && '
            '/home/airflow/dbt_venv/bin/dbt test --project-dir /opt/airflow/dbt_weather --profiles-dir /opt/airflow/dbt_weather'
        )
    )

    #  TASKFLOW LINEAR PIPELINE LINK
    weather_record >> load_history_to_gcp >> run_dbt_transformations

# Instantiate the pipeline
dag_instance = weather_pipeline()
