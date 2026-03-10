# 🏠 Airbnb ELT Pipeline
This pipeline ingests Airbnb data from **Amazon S3** into **Snowflake**, transforms it through a Medallion Architecture using **dbt**, and orchestrates everything with **Apache Airflow** and **Astronomer Cosmos**.

## Table of Contents
* [Tech Stack](#tech-stack)
* [Architecture](#architecture)
* [Pipeline DAG](#pipeline-dag)
* [Directories](#directories)
* [Setup Instructions](#setup-instructions)
* [Steps to Run](#steps-to-run)
* [Notes](#notes)

---

## Tech-Stack
* **Apache Airflow 3**: Orchestrates and schedules the entire ELT pipeline using the TaskFlow API with decorator syntax.
* **Astronomer Cosmos**: Integrates dbt with Airflow, rendering each dbt model as an individual Airflow task for full visibility and per-model retries.
* **dbt (Snowflake adapter)**: Handles all data transformations across the Medallion layers — Bronze, Silver, Gold, and Marts — including testing and SCD Type 2 snapshots.
* **Snowflake**: Cloud data warehouse used for storing and computing all pipeline layers from raw staging to final marts.
* **Amazon S3**: Stores raw Airbnb CSV files (listings, hosts, bookings) that are ingested into Snowflake via `COPY INTO`.
* **Docker & Docker Compose**: Containerizes the full Airflow stack (API server, scheduler, worker, triggerer, DAG processor) for local development.
* **PostgreSQL**: Used as the Airflow metadata database.
* **Redis**: Acts as the Celery message broker for distributed task execution.
* **SMTP / Gmail**: Sends email notifications on pipeline success or failure, including the exact failed task name.

---

## Architecture

**Data Ingestion**
1. Raw Airbnb CSV files (listings, hosts, bookings) land in Amazon S3.
2. S3KeySensor detects new files and triggers the ingestion tasks.
3. Snowflake `COPY INTO` loads the raw data into staging tables in parallel.

**Transformation**
1. dbt runs source tests to validate raw data before any transformation begins.
2. Bronze layer applies type casting, null handling, and timestamp normalization.
3. Silver layer applies business rules, deduplication, and incremental loads.
4. SCD Type 2 Snapshots capture historical changes for listings, hosts, and bookings.
5. Gold layer builds a star schema with dimension and fact tables.
6. Marts layer produces One Big Tables (OBTs) optimized for BI tools.

**Data Quality**
1. dbt tests run against all transformed models after the Gold/Marts layers complete.
2. A custom task inspects all Airflow task instances to identify the exact failed task.
3. Email alerts are sent on success or failure with full run details and log URLs.

---

## Pipeline DAG

```mermaid
flowchart TD
    START([Start]) --> ING

    subgraph ING ["📥 Data Ingestion — Parallel"]
        WL[S3 Sensor: Listings] --> CL[COPY → raw_listings]
        WH[S3 Sensor: Hosts]    --> CH[COPY → raw_hosts]
        WB[S3 Sensor: Bookings] --> CB[COPY → raw_bookings]
    end

    ING --> TS[Test Sources\ndbt test source:*]

    TS --> TRANS

    subgraph TRANS ["🔄 dbt Transformations — Cosmos"]
        B[Bronze\nTyping & cleaning]
        S[Silver\nBusiness rules]
        G[Gold\nDims + Facts]
        M[Marts\nOBTs]
        B --> S --> G --> M
    end

    TRANS --> SNAP

    subgraph SNAP ["📸 Snapshots — SCD Type 2"]
        SL[snapshot_listings]
        SH[snapshot_hosts]
        SB[snapshot_bookings]
    end

    SNAP --> DT[Test Models\ndbt test exclude source:*]
    DT --> GFT[Get Failed Task]

    GFT -->|all success| SUCCESS[Success Email]
    GFT -->|one failed|  FAILURE[Failure Email]

    SUCCESS --> END([End])
    FAILURE --> END
```

---

## Important-Directories
* `dags/`: Contains the main Airflow DAG — `airbnb_elt_pipeline.py`.
* `dbt/airbnb-analytics/models/bronze/`: Raw typing and cleaning models.
* `dbt/airbnb-analytics/models/silver/`: Business logic and deduplication models.
* `dbt/airbnb-analytics/models/gold/`: Star schema — dimension and fact tables.
* `dbt/airbnb-analytics/models/marts/`: One Big Tables for BI consumption.
* `dbt/airbnb-analytics/snapshots/`: SCD Type 2 snapshot definitions.
* `dbt/airbnb-analytics/tests/`: Custom data quality tests.
* `Dockerfile`: Custom Airflow image with dbt and Cosmos pre-installed.
* `docker-compose.yml`: Full local stack — Airflow, PostgreSQL, Redis.
* `.env.example`: Template for all required environment variables.

---

## Setup-Instructions
* Ensure Docker and Docker Compose are installed.
* Have a Snowflake account with a database, warehouse, and role ready.
* Have an AWS account with an S3 bucket containing the raw CSV files.
* For email notifications, create a Gmail App Password from [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).

---

## Steps-to-Run

**1. Clone the Repository:**
```bash
git clone https://github.com/your-username/airbnb-elt.git
cd airbnb-elt
```

**2. Set Up Environment Variables:**
```bash
cp .env.example .env
```
Fill in your credentials:
```env
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-s3-bucket-name
AIRFLOW__SMTP__SMTP_USER=your@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
```

**3. Build and Start the Stack:**
```bash
docker compose up --build
```

**4. Open the Airflow UI:**
```
URL:      http://localhost:8080
Username: airflow
Password: airflow
```

**5. Configure Airflow Connections:**

Go to **Admin → Connections** and add:

| Conn ID | Type | Details |
|---------|------|---------|
| `snowflake-conn` | Snowflake | Account, user, password, database, warehouse, role |
| `aws-conn` | Amazon Web Services | Access Key ID + Secret Access Key |
| `email-conn` | Email (SMTP) | Host: `smtp.gmail.com`, Port: `587` |

**6. Trigger the Pipeline:**

Find `airbnb_elt_pipeline` in the DAGs list → click ▶️ **Trigger DAG**

**7. Run dbt Locally (Optional):**
```bash
cd dbt/airbnb-analytics
dbt deps
dbt debug
dbt run
dbt test
dbt snapshot
```

---

## Notes
* The Cosmos `DbtTaskGroup` does not natively support snapshots — `DbtSnapshotLocalOperator` is used directly per snapshot file instead.
* Source tests (`dbt test source:*`) run before transformations to catch bad data early. Model tests (`dbt test exclude:source:*`) run after all layers are built.
* `{{ ti.task_id }}` in Airflow's `EmailOperator` returns the email task's own ID, not the failed task. A custom `@task` inspects `dag_run.get_task_instances()` to find the actual failed task and passes it via XCom.
* The S3KeySensor uses `mode='reschedule'` instead of `mode='poke'` to free up worker slots while waiting for files.
* Raw files are loaded using Snowflake's `COPY INTO` with explicit column casting directly in the `SELECT` clause for type safety.
