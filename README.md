# 🏠 Airbnb ELT Pipeline

An end-to-end ELT pipeline that ingests Airbnb raw data from **Amazon S3** into **Snowflake**, transforms it through a medallion architecture using **dbt**, and orchestrates everything with **Apache Airflow** + **Astronomer Cosmos**.

---

## 📐 Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────────────────┐     ┌──────────┐
│  Amazon S3  │────▶│                     Snowflake                                │────▶│ BI Tool  │
│             │     │  Raw → Bronze → Silver → Snapshots → Gold → Marts            │     │          │
│  listings/  │     └──────────────────────────────────────────────────────────────┘     └──────────┘
│  hosts/     │                              ▲
│  bookings/  │                              │
└─────────────┘                    ┌─────────────────┐
                                   │  Apache Airflow  │
                                   │  + dbt + Cosmos  │
                                   └─────────────────┘
```

---

## 🔄 Pipeline Flow

```mermaid
graph TD
    subgraph "Data Pipeline Architecture"
        
        %% Nodes
        A[🌐 Airbnb Data Source <br/> CSVs / API] 
        B[🐍 Python Scripts <br/> Data Extraction]
        C[(🪣 AWS S3 <br/> Data Lake / Storage)]
        D[⏱️ Apache Airflow <br/> Central Orchestration]
        E[(🐘 PostgreSQL / Snowflake <br/> Data Warehouse)]
        F[🛠️ dbt / SQL <br/> Data Transformation]
        G[📊 Superset / Metabase <br/> Visualization]

        %% Data Flow
        A -->|Extract| B
        B -->|Upload Raw Data| C
        C -->|Copy Into| E
        E -->|Read Raw Tables| F
        F -->|Write Clean/Fact Tables| E
        E -->|Query Data| G

        %% Orchestration Flow (Dotted lines like in your reference)
        D -.->|Triggers| B
        D -.->|Monitors| C
        D -.->|Triggers Load| E
        D -.->|Executes Models| F
        
    end
    
    %% Styling to match the visual vibe
    classDef storage fill:#336791,stroke:#fff,stroke-width:2px,color:#fff;
    classDef processing fill:#e25a1c,stroke:#fff,stroke-width:2px,color:#fff;
    classDef orchestration fill:#017CEE,stroke:#fff,stroke-width:2px,color:#fff;
    
    class C,E storage;
    class F processing;
    class D orchestration;
```

---

## 🗂️ Project Structure

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

## 🛠️ Tech Stack

| Tool | Role |
|------|------|
| **Apache Airflow 3** | Pipeline orchestration |
| **Astronomer Cosmos** | dbt ↔ Airflow integration (per-model tasks) |
| **dbt (Snowflake)** | Data transformation & testing |
| **Snowflake** | Cloud data warehouse |
| **Amazon S3** | Raw data storage |
| **Docker + Compose** | Local development environment |
| **PostgreSQL** | Airflow metadata database |
| **Redis** | Celery message broker |

---
