FROM apache/airflow:3.3.0-python3.13

USER root
# Install system utilities and fetch the Astral uv engine binaries
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow
# 1. Install ingestion tools straight into the active image environment layer
RUN pip install --no-cache-dir --break-system-packages requests polars==1.43.0

# 2. Establish the isolated virtual environment bubble for dbt transformation models
RUN uv venv /home/airflow/dbt_venv

# 3. ⚡ THE ABSOLUTE FIX: Explicitly assign the environmental target variable path.
# This forces uv to cleanly compile dbt right inside the designated environment.
ENV VIRTUAL_ENV=/home/airflow/dbt_venv
RUN uv pip install --no-cache-dir \
    dbt-core==1.12.3 \
    dbt-bigquery==1.12.0

