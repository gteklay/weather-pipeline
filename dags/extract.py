# dags/extract.py
import requests
import pendulum
from decimal import Decimal
from datetime import datetime, timedelta

#  IMPORT YOUR AGNOSTIC UTILITY ENGINE COMPONENT:
from utils import SchemaCreator

# Unified Next-Gen Airflow 3 Task SDK Suites
from airflow.sdk import Variable, dag, task

GCP_PROJECT_ID = "weather-data-ingestion-506305"
DATASET_ID = "weather_data"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='extract_weather_data',
    default_args=default_args,
    start_date=None,
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'production', 'polars', 'taskflow', 'pydantic']
)
def weather_pipeline():

    # TASK 1: API Extraction and Polars Parsing Core
    @task(task_id='extract_and_flatten_with_polars')
    def extract_and_flatten() -> dict:
        try:
            api_key = Variable.get("openweather_api_key")
        except KeyError:
            raise RuntimeError("❌ Set 'openweather_api_key' in Airflow Variables UI.")

        # Precision Decimal pattern safeguarding variables from binary float errors
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
        
        # Executes the payload schema parsing logic from utils.py
        return SchemaCreator.from_raw_payload(response.content)


    # TASK 2: Cloud Load Step utilizing the optimized cloud JSON streaming pipeline
    @task(task_id='stream_records_to_gcp')
    def stream_to_bigquery(record: dict, **context) -> None:
        """
        Natively accepts the validated dictionary via Airflow 3 TaskFlow.
        Streams the row straight into BigQuery with zero serialization overhead.
        """
        from google.cloud import bigquery
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        # Fetch cloud credentials using Airflow's secure connections pool
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        credentials = hook.get_credentials()
        
        # Instantiate the optimized cloud client gateway
        client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
        table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.raw_hourly_history"
        
        # Map values directly into BigQuery parameter row schema
        formatted_row = {
            "target_latitude": float(record["target_latitude"]),
            "target_longitude": float(record["target_longitude"]),
            "timezone_name": str(record["timezone_name"]),
            "timezone_offset": int(record["timezone_offset"]),
            "temperature": float(record["temperature"]),
            "feels_like": float(record["feels_like"]),
            "humidity": int(record["humidity"]),
            "cloud_cover": int(record["cloud_cover"]),
            "wind_speed": float(record["wind_speed"]),
            "weather_code": int(record["weather_code"]),
            "dew_point": float(record["dew_point"]),
            "pressure": int(record["pressure"]),
            "uv_index": float(record["uv_index"]),
            "visibility_meters": int(record["visibility_meters"]),
            "observation_time": str(record["observation_time"]),
            "sunrise_time": str(record["sunrise_time"]),
            "sunset_time": str(record["sunset_time"]),
            "ingested_at": pendulum.now("UTC").to_datetime_string(),  # Timestamp for ingestion tracking
            "airflow_dag_id": str(context['dag_run'].dag_id),
            "airflow_run_id": str(context['dag_run'].run_id)
        }

        # Stream row directly to the cloud data warehouse
        errors = client.insert_rows_json(table_ref, [formatted_row])
        
        if errors:
            raise RuntimeError(f"❌ BigQuery Streaming Insertion Failed: {errors}")
        print(f" Successfully streamed row to {table_ref} with total type safety.")


    #  CLEAN DECOUPLING WORKSPACE DEPENDENCIES:
    # Airflow 3 tracks and routes the data flow objects automatically across your containers!
    weather_data_record = extract_and_flatten()
    stream_to_bigquery(weather_data_record)

dag_instance = weather_pipeline()
