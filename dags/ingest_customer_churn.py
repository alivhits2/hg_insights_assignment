from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

DB_CONN = "postgresql+psycopg2://postgres:password@elt-postgres:5432/warehouse"
DATA_DIR = "/opt/airflow/data/customer_churn_data"
TARGET_TABLE = "customer_churn_data"
TARGET_SCHEMA = "staging"
TRACKING_TABLE = "ingested_files"


def init_tracking_table(engine):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    except IntegrityError:
        pass  # Another concurrent task already created the schema

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TRACKING_TABLE} (
                filename TEXT PRIMARY KEY,
                loaded_at TIMESTAMP DEFAULT NOW()
            )
        """))


def already_loaded(engine, filename):
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT 1 FROM {TARGET_SCHEMA}.{TRACKING_TABLE} WHERE filename = :f"),
            {"f": filename},
        )
        return result.fetchone() is not None


def mark_loaded(engine, filename):
    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO {TARGET_SCHEMA}.{TRACKING_TABLE} (filename) VALUES (:f)"),
            {"f": filename},
        )


def load_new_files():
    engine = create_engine(DB_CONN)
    init_tracking_table(engine)

    csv_files = list(Path(DATA_DIR).glob("*.csv"))
    if not csv_files:
        print("No CSV files found in data directory.")
        return

    loaded, skipped = 0, 0
    for csv_path in sorted(csv_files):
        filename = csv_path.name
        if already_loaded(engine, filename):
            print(f"Skipping {filename} (already loaded)")
            skipped += 1
            continue

        df = pd.read_csv(csv_path)
        df.columns = [c.lower() for c in df.columns]

        df.to_sql(TARGET_TABLE, engine, schema=TARGET_SCHEMA, if_exists="append", index=False)
        mark_loaded(engine, filename)
        print(f"Loaded {len(df)} rows from {filename} into {TARGET_SCHEMA}.{TARGET_TABLE}")
        loaded += 1

    print(f"Done: {loaded} file(s) loaded, {skipped} skipped.")


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

with DAG(
    dag_id="ingest_data_files",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    load_task = PythonOperator(
        task_id="load_new_csv_files",
        python_callable=load_new_files,
    )

    dbt_create_functions_task = BashOperator(
        task_id="create_db_functions",
        bash_command=(
            f"{DBT_BIN} run-operation create_scrub_pii_function"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_task = BashOperator(
        task_id="run_fct_customer_churn",
        bash_command=(
            f"{DBT_BIN} run"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROJECT_DIR}"
            f" --select fct_customer_churn"
        ),
    )

    dbt_test_task = BashOperator(
        task_id="test_fct_customer_churn",
        bash_command=(
            f"{DBT_BIN} test"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROJECT_DIR}"
            f" --select fct_customer_churn"
        ),
    )

    dbt_aggregations = [
        BashOperator(
            task_id=f"run_fct_customer_churn_by_{dim}",
            bash_command=(
                f"{DBT_BIN} run"
                f" --project-dir {DBT_PROJECT_DIR}"
                f" --profiles-dir {DBT_PROJECT_DIR}"
                f" --select fct_customer_churn_by_{dim}"
            ),
        )
        for dim in ["age", "gender", "internetservice", "techsupport", "churn"]
    ]

    load_task >> dbt_create_functions_task >> dbt_task >> dbt_test_task >> dbt_aggregations
