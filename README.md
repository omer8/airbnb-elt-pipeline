# 🏠 Airbnb ELT Pipeline

An end-to-end ELT pipeline that ingests Airbnb raw data from **Amazon S3** into **Snowflake**, transforms it through a medallion architecture using **dbt**, and orchestrates everything with **Apache Airflow** + **Astronomer Cosmos**.

---
## Table of Contents
* [Tech Stack](#tech-stack)
* [Architecture](#architecture)
* [Project Structure](#project-structure)
* [Pipeline Flow](#pipeline-flow)
* [Setup Instructions](#setup-instructions)
* [Steps to Run](#steps-to-run)
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

## Project Structure

```
airbnb-elt/
│
├── dags/
│   └── airbnb_elt_pipeline.py      # Main Airflow DAG
│
├── dbt/
│   └── airbnb-analytics/           # dbt project
│       ├── models/
│       │   ├── bronze/             # Raw → typed & cleaned
│       │   ├── silver/             # Business rules applied
│       │   ├── gold/               # Dims + Facts (star schema)
│       │   └── marts/              # OBTs for downstream tools
│       ├── snapshots/              # SCD Type 2 snapshots
│       ├── tests/                  # Custom data tests
│       └── dbt_project.yml
│
├── docs/
│   └── pipeline_dag.png            # Architecture diagram
│
├── .env.example                    # Environment variable template
├── .gitignore
├── Dockerfile                      # Custom Airflow image
├── docker-compose.yml              # Full stack setup
├── pyproject.toml                  # Python dependencies
└── README.md
```

---

## Pipeline Flow

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
