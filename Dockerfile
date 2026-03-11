# Dockerfile
FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y git libpq-dev && apt-get clean

USER airflow

# Install dbt in a virtual environment to avoid conflicts with Airflow
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install dbt-postgres==1.8.2

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
