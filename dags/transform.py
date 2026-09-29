# dags/transform.py
import pendulum
from datetime import timedelta

# Unified Next-Gen Airflow 3 Task SDK Authoring Suite
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': 'analytics_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='transform_weather_data',
    default_args=default_args,
    start_date=None,
    schedule='0 0 * * 0',  # Weekly on Sundays at midnight UTC
    catchup=False,
    tags=['analytics', 'dbt', 'production', 'zero-lock']
)
def analytics_transform_pipeline():

    #  TASK 1: Compiles models into BigQuery using safe /tmp/ directories
    run_dbt_models = BashOperator(
        task_id='execute_dbt_run_transformations',
        # ⚡ THE PRODUCTION FIX: We redirect log-path and target-path directly into /tmp/
        # This completely stops dbt from touching your restricted laptop host folders!
        bash_command=(
            '/home/airflow/dbt_venv/bin/dbt run '
            '--project-dir /opt/airflow/dbt_weather '
            '--profiles-dir /opt/airflow/dbt_weather '
            '--log-path /tmp/dbt_logs '
            '--target-path /tmp/dbt_target'
        )
    )

    #  TASK 2: Executes 5-of-5 QA test firewalls using identical /tmp/ tracking maps
    execute_dbt_tests = BashOperator(
        task_id='execute_dbt_test_firewalls',
        # ⚡ THE PRODUCTION FIX: Point tests to the exact same /tmp/ target cache map
        bash_command=(
            '/home/airflow/dbt_venv/bin/dbt test '
            '--project-dir /opt/airflow/dbt_weather '
            '--profiles-dir /opt/airflow/dbt_weather '
            '--log-path /tmp/dbt_logs '
            '--target-path /tmp/dbt_target'
        )
    )

    #  STABLE LINEAR PIPELINE DEPENDENCY TRACKING
    run_dbt_models >> execute_dbt_tests

# Instantiate the pipeline object footprint
dag_instance = analytics_transform_pipeline()
