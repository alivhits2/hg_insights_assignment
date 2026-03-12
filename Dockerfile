# Dockerfile
FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y git libpq-dev && apt-get clean

USER airflow

# Install dbt in a virtual environment to avoid conflicts with Airflow
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install dbt-postgres==1.8.2

COPY requirements.txt .
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install -r requirements.txt

COPY docker-compose.prod.yml /docker-compose.yml
