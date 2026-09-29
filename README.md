# Configuration-Driven Climate Intelligence Platform (Modern Data Stack)

A production-grade, highly automated, and location-agnostic data engineering platform designed to ingest, validate, transform, and visualize real-time meteorological metrics for **any global city**. By driving spatial parameters entirely through dynamic metadata configurations rather than hardcoded variables, this repository serves as a highly reusable ingestion framework. The architecture leverages **Airflow 3**, **Polars**, **Pydantic V2**, **dbt Core**, and **Google BigQuery** to build a type-safe, fully auditable system of record.

---

## ️ Architecture Blueprint & Data Flow

The platform implements **Blast Radius Isolation** by splitting the data lifecycle into two independent, decoupled orchestration tracks [3.x]:

```text
[ API Ingestion Loop ] ➔ [ Polars Struct Parsing ] ➔ [ Pydantic Firewall ] ➔ [ BigQuery Streaming ]
                                                                                   │
   ┌───────────────────────────────────────────────────────────────────────────────┘
   ▼
[ dbt Staging View ] ➔ [ dbt Final Mart Table ] ➔ [ Data Quality Tests ] ➔ [ Google Data Studio BI ]
```

1. **The Ingestion Pipeline (`extract.py` - Hourly):** Dynamically fetches target coordinates from the Airflow runtime metadata store, harvests environmental snapshots from the OpenWeather One Call 4.0 API, processes structures inside memory via Polars, runs strict validation boundaries via Pydantic, and streams live data straight into Google BigQuery alongside operational platform audit trails [1.12, 3.x].
2. **The Analytics Transformation Pipeline (`transform.py` - Weekly):** Aggregates raw storage logs into clean downstream dimensions and final fact models, executing automated data quality checks entirely inside containerized isolation (`/tmp/` file redirection) to prevent host permission locks [1.12, 3.x].

---

## ⚙️ Global Deployment & Configuration Setup

Because the core ingestion engine is entirely decoupled from spatial boundaries, deploying this pipeline for a new metropolitan zone requires zero code modifications. Simply register the following keys inside the **Airflow Variables Management UI** [3.x]:

| Airflow Variable Key | Expected Format | Functional Purpose | Default Example (Surrey, BC) |
| :--- | :--- | :--- | :--- |
| `openweather_api_key` | `String (Secret Token)` | Authenticates secure cloud requests to the OpenWeather gateway. | `[Your_Secure_API_Key]` |
| `target_latitude` | `Decimal String` | Sets the primary geographic latitude mapping vector. | `49.1900` |
| `target_longitude` | `Decimal String` | Sets the primary geographic longitude mapping vector. | `-122.8400` |

*Downstream dbt data models and Google Data Studio BI visualizations will dynamically scale, resolve, and display analytics according to the target coordinates captured inside the database warehouse layer [1.12].*

---

## ️ Tech Stack & Advanced Core Patterns

- **Orchestration:** Airflow 3 TaskFlow API utilizing the next-gen Task SDK for seamless, zero-copy, type-hinted in-memory data transmission across worker node tasks [3.x].
- **In-Memory Transformation Engine:** Polars. Avoids costly and destructive `.explode()` or `.unnest()` operations by utilizing explicit structural sub-indexing (`.list.get(0).struct.field()`) to achieve microsecond parsing performance [1.12].
- **Data Quality Firewall:** Pydantic V2. Intercepts incoming structures at the ingestion boundaries using a unified `SchemaCreator` mapping contract to catch schema drift (e.g., API dropping optional parameters like `visibility` on perfectly clear days) or corrupt telemetry outliers before they hit storage [1.12].
- **Cloud Data Warehouse:** Google BigQuery. Region-locked to `us-west1` (Oregon) and safeguarded with an automated monthly cost budget tracking threshold rule [1.12].
- **Analytics Engineering:** dbt-core. Implements modular staging layers and physical fact tables configured with automated sequential quality assurance tests (`unique`, `not_null`) [1.12].
- **CI/CD Automation:** GitHub Actions. Launches isolated runner environments on every code push to automatically validate syntax layout structures via a Rust-powered **Ruff** linter and execute regression unit tests [1.12].
- **Business Intelligence Reporting:** Google Data Studio (Looker Studio). Connects natively to BigQuery analytical tables using performance-optimized caching strategies [1.12].

---

## ️ Repository Structure

```text
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI/CD automation test script
├── dags/
│   ├── extract.py               # Hourly real-time API ingestion DAG
│   ├── transform.py             # Weekly isolated dbt compilation DAG
│   └── utils.py                 # Core Pydantic SchemaCreator & Polars engine
├── dbt_weather/                 # Full dbt project environment setup directory
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml      # BigQuery external table registry
│   │   │   ├── schema.yml       # Automated data testing firewall scripts
│   │   │   └── stg_hourly_history.sql
│   │   └── marts/
│   │       └── fct_surrey_weather.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── .gitignore
├── docker-compose.yaml
└── README.md
```

---

## ️ Enterprise Data Governance & Audit Trails

To enforce absolute traceability and clear data lineage, every single record streamed into the cloud warehouse is dynamically enriched with platform-level metadata tracking coordinates [1.12]:

- `ingested_at`: The exact UTC timestamp when the Airflow container processed and loaded the row.
- `airflow_dag_id`: The explicit identifier of the orchestrator file loop managing the data.
- `airflow_run_id`: The unique, deterministic runtime execution token mapping directly back to physical Airflow task worker logs for instant debugging.

Downstream data testing suites run `not_null` validation checkpoints on these parameters, establishing total confidence in the warehouse lineage footprint [1.12].

---

##  Business Intelligence Visualizations

The analytical data layer hooks directly into **Google Data Studio (Looker Studio)** to transform raw historical rows into a clean, actionable reporting environment [1.12]. Key visualizations include:
- **Hourly Temperature Trends:** Dual line charts tracking actual air temperatures against apparent 'Feels Like' heat index variables to chart localized microclimate comfort scales over time.
- **System Lineage Logs:** Exposes operational Airflow IDs right on the dashboard grid to prove system reliability, data transparency, and absolute root-cause traceability to final stakeholders [1.12].
