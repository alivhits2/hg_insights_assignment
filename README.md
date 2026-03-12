# HG Insights ELT Pipeline

An end-to-end ELT pipeline that ingests customer churn CSV files into a PostgreSQL data warehouse, applies PII scrubbing, and produces analytics-ready dbt models.

## What It Does

1. **Ingest** — An Airflow DAG watches a local directory for new CSV files and loads them into a PostgreSQL staging table. Already-loaded files are tracked and skipped on subsequent runs.
2. **Transform** — dbt models clean the data (null replacement, PII scrubbing) and produce an incremental fact table plus aggregation views broken down by age, gender, internet service, tech support, and churn status.
3. **Orchestrate** — The full pipeline runs hourly and is managed via the Airflow UI.

## Platforms

| Platform | Version | Purpose |
|---|---|---|
| Apache Airflow | 2.9.0 | Pipeline orchestration |
| PostgreSQL | 16 | Data warehouse |
| dbt-postgres | 1.8.2 | Data transformation |
| Docker / Docker Compose | — | Containerisation |

## Data Flow

```
data/customer_churn_data/*.csv
        ↓ (Airflow DAG)
staging.customer_churn_data
        ↓ (dbt - incremental table)
customer_analytics.fct_customer_churn
        ↓ (dbt - views)
customer_analytics.fct_customer_churn_by_age
customer_analytics.fct_customer_churn_by_gender
customer_analytics.fct_customer_churn_by_internetservice
customer_analytics.fct_customer_churn_by_techsupport
customer_analytics.fct_customer_churn_by_churn
```

---

## Installation & Setup

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Step 1 — Set up and start

Run the following commands to create a project directory, extract the bundled configuration from the image, and start all containers:

```bash
mkdir hg-elt-assignment && cd hg-elt-assignment
mkdir -p data/customer_churn_data
docker run --rm alivhits2/hg-elt-assignment:latest cat /docker-compose.yml > docker-compose.yml
docker compose up -d
```

The four commands above:
- Create the project directory and data folder
- Extract the bundled `docker-compose.yml` from the image (no manual file creation needed)
- Start all containers

This starts:
- `elt-postgres` — PostgreSQL 16 data warehouse
- `airflow-init` — initialises the Airflow database and creates the admin user, then exits
- `airflow-webserver` — Airflow UI (http://localhost:8080)
- `airflow-scheduler` — Airflow scheduler and task executor

Wait ~30 seconds for `airflow-init` to complete before using the UI.

### Step 2 — Add CSV files

Place customer churn CSV files in:

```
data/customer_churn_data/
```

Files must have these columns (case-insensitive):

| Column | Type |
|---|---|
| customerid | integer |
| age | integer |
| gender | text |
| tenure | integer |
| monthlycharges | float |
| contracttype | text |
| internetservice | text |
| totalcharges | float |
| techsupport | text |
| churn | text |

A sample dataset for this can be downloaded from [Kaggle](https://www.kaggle.com/datasets/abdullah0a/telecom-customer-churn-insights-for-analysis/data).

Each new file dropped in this directory will be automatically picked up by the next hourly DAG run.

### Step 3 — Trigger the pipeline

1. Open the Airflow UI at **http://localhost:8080**
2. Log in with **admin / admin**
3. Find the `ingest_data_files` DAG and toggle it on if not already enabled
4. Click **▶ Trigger DAG** to run it immediately

### Step 4 — Query the results

Connect to PostgreSQL with any SQL client (e.g. DataGrip):

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `warehouse` |
| Username | `postgres` |
| Password | `password` |

---

## Using the Airflow UI

The Airflow UI lets you monitor DAG runs, view task logs, and trigger pipelines manually. Key areas:

- **DAGs list** — shows all pipelines with their schedule and last run status
- **Grid view** — shows the history of runs and the status of each task within them
- **Graph view** — visualises the task dependency tree for a DAG
- **Logs** — click any task instance in Grid or Graph view to see its full execution log

For full documentation see the [Airflow 2.9.0 UI guide](https://airflow.apache.org/docs/apache-airflow/2.9.0/ui.html).

---

## DAG Tasks

The `ingest_data_files` DAG runs the following tasks in order:

```
load_new_csv_files
    → create_db_functions
        → run_fct_customer_churn
            → run_fct_customer_churn_by_age
            → run_fct_customer_churn_by_gender
            → run_fct_customer_churn_by_internetservice
            → run_fct_customer_churn_by_techsupport
            → run_fct_customer_churn_by_churn
```

## Stopping the Pipeline

```bash
docker compose down
```

To also remove all stored data:

```bash
docker compose down -v
```
