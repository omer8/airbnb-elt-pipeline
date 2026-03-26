# 🏠 Airbnb ELT Pipeline

> Production-grade ELT pipeline ingesting Airbnb data from **Amazon S3** into **Snowflake**, transformed across a full **Medallion architecture** with **dbt**, and orchestrated end-to-end by **Apache Airflow 3** + **Astronomer Cosmos**.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-3.x-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-Snowflake-FF694B?style=flat&logo=dbt&logoColor=white)](https://getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud_DW-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://snowflake.com)
[![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?style=flat&logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              ELT Pipeline Overview                           │
└──────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐      ┌──────────────────────────────────────────────────┐
  │   Source Data   │      │                  SNOWFLAKE                       │
  │                 │      │                                                  │
  │  listings.csv   │      │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
  │  hosts.csv      │─────▶│  │  BRONZE  │─▶│  SILVER  │─▶│     GOLD      │  │
  │  bookings.csv   │      │  │          │  │          │  │  (Star Schema)│  │
  │                 │      │  │Type cast │  │ Biz rules│  │  Dims + Facts │  │
  │  Amazon S3      │      │  │Null clean│  │ Dedup    │  │               │  │
  └─────────────────┘      │  │Timestamps│  │Incremental│  └───────┬───────┘  │
         │                 │  └──────────┘  └──────────┘          │          │
         │                 │       │                               │          │
         │  COPY INTO      │  ┌────▼─────────────────┐   ┌────────▼───────┐  │
         │  (parallel)     │  │  SCD TYPE 2 SNAPSHOTS│   │    MARTS       │  │
         │                 │  │                      │   │  (OBTs for BI) │  │
  ┌──────▼──────┐          │  │  listings_snapshot   │   │                │  │
  │ S3KeySensor │          │  │  hosts_snapshot      │   │  mart_listings │  │
  │  (trigger)  │          │  │  bookings_snapshot   │   │  mart_hosts    │  │
  └─────────────┘          │  └──────────────────────┘   │  mart_bookings │  │
                           │                             └────────────────┘  │
                           └──────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │                          ORCHESTRATION LAYER                             │
  │                                                                          │
  │   Apache Airflow 3 (TaskFlow API)  +  Astronomer Cosmos                  │
  │   ├── S3KeySensor → COPY INTO tasks (parallel)                           │
  │   ├── dbt source tests → Bronze → Silver → Snapshots → Gold → Marts      │
  │   ├── dbt tests (post-Gold)                                              │
  │   └── Email alerts on success / failure (task-level identification)      │
  │                                                                          │
  │   Workers: Celery + Redis  │  Metadata DB: PostgreSQL  │  UI: :8080      │
  └──────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Role |
|-------|------|------|
| Orchestration | Apache Airflow 3 | Schedules and runs the full pipeline via TaskFlow API |
| dbt Integration | Astronomer Cosmos | Renders each dbt model as an individual Airflow task — full DAG visibility + per-model retries |
| Transformation | dbt (Snowflake adapter) | Medallion layers, SCD Type 2 snapshots, data tests |
| Data Warehouse | Snowflake | Stores and computes all pipeline layers |
| Object Storage | Amazon S3 | Landing zone for raw Airbnb CSV files |
| Task Queue | Celery + Redis | Distributed task execution across Airflow workers |
| Containerization | Docker + Compose | Full local stack: API server, scheduler, worker, triggerer, DAG processor |
| Metadata DB | PostgreSQL | Airflow internal state |
| Alerting | Gmail SMTP | Email notifications on success/failure with failed task name + log URL |

---

## 🗂️ Project Structure

```
airbnb-elt/
├── dags/
│   └── airbnb_elt_pipeline.py       # Main Airflow DAG (TaskFlow API)
│
├── dbt/
│   └── airbnb-analytics/
│       ├── models/
│       │   ├── bronze/              # Raw → typed, cleaned, normalized
│       │   ├── silver/              # Business rules, dedup, incremental loads
│       │   ├── gold/                # Star schema: dims + facts
│       │   └── marts/               # One Big Tables (OBTs) for BI tools
│       ├── snapshots/               # SCD Type 2 (listings, hosts, bookings)
│       ├── tests/                   # Custom dbt data tests
│       └── dbt_project.yml
│
├── docs/
│   └── pipeline_dag.png
│
├── .env.example
├── .gitignore
├── Dockerfile                       # Custom Airflow image
├── docker-compose.yml               # Full stack: Airflow + Postgres + Redis
├── pyproject.toml
└── README.md
```

---

## 🔄 Pipeline Flow

```
S3KeySensor (listings / hosts / bookings)
      │
      ▼
COPY INTO Snowflake staging  ──────────────────────── (parallel, 3 tables)
      │
      ▼
dbt source tests  ◀── fail-fast: pipeline halts if raw data is invalid
      │
      ▼
Bronze models  ──  type casting · null handling · timestamp normalization
      │
      ▼
Silver models  ──  business rules · deduplication · incremental strategy
      │
      ├──▶  SCD Type 2 Snapshots  (listings / hosts / bookings history)
      │
      ▼
Gold models  ──  star schema: dim_listing · dim_host · dim_date · fct_bookings
      │
      ▼
Marts  ──  One Big Tables optimized for BI consumption
      │
      ▼
dbt tests (post-Gold)  ──  uniqueness · not-null · referential integrity
      │
      ▼
Email alert  ──  success or failure · exact failed task · Airflow log URL
```

---

## 📊 dbt Model Layers

### Bronze — Raw → Cleaned
Type casting, null handling, timestamp normalization. No business logic applied.

| Model | Source | Key Transforms |
|-------|--------|---------------|
| `bronze_listings` | `raw.listings` | Price to numeric, trim whitespace, parse dates |
| `bronze_hosts` | `raw.hosts` | Null coalescing, boolean casting |
| `bronze_bookings` | `raw.bookings` | Timestamp normalization, status typing |

### Silver — Business Rules Applied
Incremental loads, deduplication, enrichment.

| Model | Strategy | Description |
|-------|----------|-------------|
| `silver_listings` | Incremental | Deduped listings with business-rule validations |
| `silver_hosts` | Incremental | Host profiles with response rate / acceptance rate parsing |
| `silver_bookings` | Incremental | Cleaned bookings with referential checks |

### SCD Type 2 Snapshots
Historical change tracking for slowly changing dimensions.

| Snapshot | Tracks Changes On |
|----------|-----------------|
| `listings_snapshot` | Price, availability, minimum nights |
| `hosts_snapshot` | Superhost status, response rate |
| `bookings_snapshot` | Booking status changes |

### Gold — Star Schema

| Model | Type | Grain |
|-------|------|-------|
| `dim_listing` | Dimension | One row per listing |
| `dim_host` | Dimension | One row per host |
| `dim_date` | Dimension | One row per calendar date |
| `fct_bookings` | Fact | One row per booking event |

### Marts — Analytics-Ready OBTs

| Model | Description |
|-------|-------------|
| `mart_listings` | Listings enriched with host info, pricing stats, availability |
| `mart_hosts` | Host-level performance: occupancy, revenue, review scores |
| `mart_bookings` | Booking trends, lead time, cancellation rates |

---

## ⚙️ Setup & Run

### Prerequisites

- Docker & Docker Compose
- Snowflake account (database, warehouse, role configured)
- AWS account with S3 bucket containing raw CSVs
- Gmail App Password for SMTP alerts → [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)

### 1. Clone the repo

```bash
git clone https://github.com/omer8/airbnb-elt-pipeline.git
cd airbnb-elt-pipeline
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your-s3-bucket-name

# Alerting
AIRFLOW__SMTP__SMTP_USER=your@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
```

### 3. Build and start the full stack

```bash
docker compose up --build
```

This starts: Airflow API server · scheduler · worker · triggerer · DAG processor · PostgreSQL · Redis

### 4. Open the Airflow UI

```
http://localhost:8080
Username: airflow  |  Password: airflow
```

### 5. Add Airflow Connections

Go to **Admin → Connections**:

| Conn ID | Type | Details |
|---------|------|---------|
| `snowflake-conn` | Snowflake | Account, user, password, database, warehouse, role |
| `aws-conn` | Amazon Web Services | Access Key ID + Secret Access Key |
| `email-conn` | Email (SMTP) | Host: `smtp.gmail.com` · Port: `587` |

### 6. Trigger the pipeline

Find `airbnb_elt_pipeline` in the DAGs list → ▶️ **Trigger DAG**

---

## 🧩 Design Decisions

**Why Astronomer Cosmos instead of a single dbt Airflow operator?**
Cosmos renders each dbt model as its own Airflow task. This means per-model retries, granular failure visibility in the DAG graph, and no need to re-run the entire dbt project on a single-model failure — a significant operational advantage in production.

**Why SCD Type 2 snapshots?**
Airbnb listings change frequently — prices shift, superhosts gain and lose status, availability fluctuates. SCD Type 2 preserves the full history of these changes, enabling point-in-time analysis and trend reporting that a `MERGE`-only approach would destroy.

**Why OBTs in the Marts layer?**
The Gold star schema is normalized and join-heavy. For BI tools and ad-hoc analysts, pre-joining dims and facts into wide mart tables reduces query complexity, improves dashboard performance, and lowers the barrier for non-SQL-native consumers.

**Why Celery + Redis over the LocalExecutor?**
Parallel task execution across the three `COPY INTO` ingestion tasks and distributed dbt model runs require a proper task queue. Celery with Redis scales horizontally and mirrors a real production Airflow deployment.

---

## 🗺️ Potential Extensions

- [ ] Add **Great Expectations** or **Soda Core** for pre-ingestion data quality checks on raw S3 files
- [ ] Publish **dbt docs** to GitHub Pages (`dbt docs generate && dbt docs serve`)
- [ ] Add a **BI layer** container (Metabase or Apache Superset) to the Compose stack
- [ ] Replace S3 file polling with **S3 event notifications → SQS → Airflow trigger** for near-real-time ingestion

---

## 📄 License

[MIT](LICENSE) © [omer8](https://github.com/omer8)
