# DataMart S.A.S. - End-to-End ETL Pipeline & Data Warehouse

[![Apache Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow%203.1.5-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL%2016-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/Microservice-FastAPI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Container-Docker%20Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![uv](https://img.shields.io/badge/Build%20Tool-Astral%20uv-DE5FE9?logo=rust&logoColor=white)](https://astral.sh/uv)

Enterprise data platform built for **DataMart S.A.S.** to ingest, clean, audit, standardize, and load multi-source e-commerce retail data into a dimensional Data Warehouse (Star Schema) in PostgreSQL, fully orchestrated with Apache Airflow.

---

## 1. Architecture Overview

```text
+---------------------------------------------------------------------------------------+
|                                    DOCKER NETWORK                                     |
|                                                                                       |
|   +--------------------+     +------------------------+     +---------------------+   |
|   | data_sources/      |     | api/ (FastAPI Catalog) |     | airflow-apiserver   |   |
|   | - data.csv         |     | - GET /products        |     | - Web UI: 8080      |   |
|   | - online_retail.csv|     | - GET /products/{code} |     | - Dark Theme        |   |
|   +---------+----------+     +-----------+------------+     +----------+----------+   |
|             |                            |                             |              |
|             +------------+---------------+                             |              |
|                          |                                             |              |
|                          v                                             |              |
|             +----------------------------+                             |              |
|             | datamart_sales_etl_pipeline|                             |              |
|             | - Extract (Parquet Staging)|                             |              |
|             | - Transform & Audit        |                             |              |
|             | - Load (COPY + Upsert)     |                             |              |
|             +-------------+--------------+                             |              |
|                           |                                            |              |
|                           v                                            v              |
|             +----------------------------+              +-------------------------+   |
|             | datamart_analytical_dw     |              | datamart_airflow_       |   |
|             | - PostgreSQL 16 (Port 5433)|              |   metadata_db           |   |
|             | - Star Schema Data Mart    |              | - PostgreSQL 16 (Meta)  |   |
|             +----------------------------+              +-------------------------+   |
+---------------------------------------------------------------------------------------+
```

### Key Architectural Highlights:
1. **Ultra-Fast Container Builds with `uv`:** Extends official `apache/airflow:3.1.5` using Astral's `uv` package manager in [Dockerfile.airflow](Dockerfile.airflow). Dependencies (`pandas`, `pyarrow`, `psycopg2-binary`, `sqlalchemy`, `requests`) install in **114 milliseconds**.
2. **Dedicated Storage Separation:** Independent PostgreSQL instances for Airflow metadata (`postgres:5432`) and analytical Data Warehouse (`warehouse-postgres:5433`).
3. **Automated Zero-Touch Provisioning:** Airflow Connections (`postgres_dw`) and Variables are injected automatically via environment variables on container initialization.
4. **Idempotent Ingestion:** Deterministic MD5 hashing (`transaction_hash`, `reject_hash`) coupled with PostgreSQL `COPY FROM STDIN` streaming and `ON CONFLICT` clauses enables loading 1+ million records in under 50 seconds without duplicate risk.

---

## 2. Quickstart Guide (< 10 Minutes Setup)

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24.0+ recommended)
* Git

### Step 1: Clone the Repository
```bash
git clone https://github.com/AdrianoGC24/datamart_etl_orquestacion_airflow.git
cd datamart_etl_orquestacion_airflow
```

### Step 2: Verify Source Datasets
Ensure your raw CSV datasets are placed inside the `data_sources/` folder:
* `data_sources/data.csv` (Daily operational dump)
* `data_sources/online_retail.csv` (Extended transaction history)

### Step 3: Configure Environment Variables
Copy the provided environment template to `.env`:
```bash
cp .env.example .env
```
*(The default credentials and host mappings in `.env` are pre-configured to run out of the box).*

### Step 4: Launch the Entire Platform
Run the following single command to build the custom images, initialize databases, and start all services:
```bash
docker compose up -d --build
```

### Step 5: Verify Running Services
Check that all containers are healthy:
```bash
docker compose ps
```
Expected output:
* `datamart_airflow_apiserver` (Up - healthy) -> Port `8080:8080`
* `datamart_airflow_scheduler` (Up - healthy)
* `datamart_airflow_triggerer` (Up)
* `datamart_airflow_dag_processor` (Up)
* `datamart_analytical_dw` (Up - healthy) -> Port `5433:5432`
* `datamart_airflow_metadata_db` (Up - healthy) -> Internal `5432`

---

## 3. Validating Airflow Connections & Variables

### Accessing Airflow UI
1. Open your browser and navigate to: **[http://localhost:8080](http://localhost:8080)**
2. Log in using the administrator credentials configured in `.env`:
   * **Username:** `airflow` (or `admin`)
   * **Password:** `airflow` (or `admin`)

### Verifying Pre-Configured Connections
* Go to **Admin -> Connections** (or via CLI):
  ```bash
  docker exec datamart_airflow_scheduler airflow connections get postgres_dw
  ```
* Confirms connection to `warehouse-postgres:5432/datamart_dw` using login `dw_admin`.

### Verifying Pre-Configured Variables
* Go to **Admin -> Variables** (or via CLI):
  ```bash
  docker exec datamart_airflow_scheduler airflow variables list
  ```
* Confirms:
  * `data_sources_dir` = `/opt/airflow/data_sources`
  * `staging_dir` = `/opt/airflow/logs/staging`
  * `use_catalog_api` = `false`

---

## 4. Executing the ETL Pipeline

### Option A: Via Airflow Web UI
1. Locate the DAG: **`datamart_sales_etl_pipeline`**.
2. Unpause the DAG (toggle switch to blue/active).
3. Click **Trigger DAG** (Play button on the top right).

### Option B: Via Command Line (Instant Execution)
Trigger the DAG run directly inside the scheduler container:
```bash
docker exec datamart_airflow_scheduler airflow dags trigger datamart_sales_etl_pipeline
```

### DAG Task Breakdown:
* `extract_daily_sales_csv`: Reads `data.csv`, standardizes schema, stores Parquet in staging.
* `extract_historical_sales_csv`: Reads `online_retail.csv`, drops junk indexes, stores Parquet in staging.
* `transform_validate_and_build_star_schema`: Deduplicates across sources, audits rejects (unit price $\le 0$), standardizes product codes & dates (UTC), computes canonical descriptions, generates Star Schema entities.
* `load_star_schema_to_postgres_dw`: Connects to `postgres_dw` and streams data into PostgreSQL using high-speed atomic `COPY` upsert.

---

## 5. Verifying Data in PostgreSQL Data Warehouse

Connect to the analytical database using any SQL client (DBeaver, pgAdmin) or terminal:
* **Host:** `localhost`
* **Port:** `5433` (external port to avoid conflict with local instances)
* **Database:** `datamart_dw`
* **Username:** `dw_admin`
* **Password:** `dw_secure_password_2024`

### Command-line Quick Verification:
```bash
docker exec -it datamart_analytical_dw psql -U dw_admin -d datamart_dw -c "\
  SELECT 'dim_date' AS table, count(*) FROM dim_date UNION ALL \
  SELECT 'dim_products', count(*) FROM dim_products UNION ALL \
  SELECT 'dim_customers', count(*) FROM dim_customers UNION ALL \
  SELECT 'fact_transactions', count(*) FROM fact_transactions UNION ALL \
  SELECT 'fact_daily_product_metrics', count(*) FROM fact_daily_product_metrics UNION ALL \
  SELECT 'audit_rejected_records', count(*) FROM audit_rejected_records;"
```

Expected row counts:
| Table Name | Record Count | Description |
| :--- | :---: | :--- |
| `dim_date` | **305** | Standardized calendar days (UTC). |
| `dim_products` | **3,828** | Unique normalized products with canonical names & categories. |
| `dim_customers` | **4,372** | Customer dimension (including `UNIDENTIFIED` segment). |
| `fact_transactions` | **1,078,784** | Validated transaction lines (`SALE` and `RETURN`). |
| `fact_daily_product_metrics` | **278,011** | Daily product metrics: gross revenue, return adjustment, net revenue. |
| `audit_rejected_records` | **5,034** | Logged corrupt or price $\le 0$ records with rejection reason. |

---

## 6. Business Questions SQL Validation (Section 7)

All SQL queries answering the business questions are stored in [sql_validations/business_queries.sql](sql_validations/business_queries.sql).

Execute all queries at once:
```bash
docker exec -i datamart_analytical_dw psql -U dw_admin -d datamart_dw < ./sql_validations/business_queries.sql
```

### Key Business Insights Summary:
1. **Monthly Evolution:** Peak sales occurred in **September 2011** with **$2,039,375.24 USD** net revenue across 98,522 transactions.
2. **Category Performance:** **Hogar** generates the highest gross revenue (**$12,865,957.81 USD**), while **Papelería** has the highest return rate (**13.83%**).
3. **Critical Product Loss:** Reference `23843` (*PAPER CRAFT, LITTLE BIRDIE*) and `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*) suffered over **$491,000 USD** in refunds (up to 100% return rate).
4. **Geographic Concentration:** United Kingdom concentrates **91.51%** of transaction volume (avg ticket $18.60 USD). However, **Netherlands ($121.00 USD)** and **Australia ($117.19 USD)** exhibit 6x higher average order values.
5. **Customer Segmentation:** Preserving guest transactions saved **$3,510,553.28 USD** in gross revenue. Identified customers have higher order values ($22.40 vs $13.28) and a much lower return rate (6.86% vs 16.26%).
6. **Data Quality:** Identified **3,828 unique product codes**, with products having up to 4 textual description variations unified under canonical statistical modes.
7. **Actionable Recommendation:** Immediately pause and audit references `23843` and `23166` to prevent $491K in refund losses; offer a 5% first-order checkout incentive to convert guest users, reducing return rates by 58%.

---

## 7. Repository Structure

```text
datamart_etl_orquestacion_airflow/
├── .env.example              # Template for environment variables
├── .gitignore                # Excludes secrets, caches, and runtime logs
├── .dockerignore             # Optimizes Docker build context
├── docker-compose.yaml       # Multi-service container orchestration
├── Dockerfile.airflow        # Custom Airflow 3 image optimized with uv
├── requirements.txt          # Python dependencies
├── api/                      # Optional FastAPI product catalog microservice
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── dags/                     # Airflow DAG definition and modular ETL tasks
│   ├── datamart_etl_dag.py
│   └── scripts/
│       ├── __init__.py
│       ├── extract.py        # Task 1 & 2: Multi-source extraction
│       ├── transform.py      # Task 3: Cleaning, deduplication & Star Schema
│       └── load.py           # Task 4: High-speed COPY upsert to DW
├── data_sources/             # Raw input CSV datasets
├── warehouse_init/           # Automated DDL executed on Postgres startup
│   └── 01_init_schema.sql
├── sql_validations/          # Section 7 business queries
│   └── business_queries.sql
├── README.md                 # Complete setup and reproduction guide
└── TECHNICAL_DECISIONS.md    # Architecture and technical justifications
```

---

## 8. Tear Down & Environment Reset
To stop all containers and preserve database volumes:
```bash
docker compose down
```

To perform a complete factory reset (deleting volumes and starting from scratch):
```bash
docker compose down -v
```
