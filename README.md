# EOD Securities Pricing Analytics Pipeline

> **End-to-End Batch Data Engineering Project** — Polygon.io → AWS S3 → Snowflake → Power BI  
> Orchestrated with Apache Airflow on Docker | Alerts via Slack

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Snowflake Schema Design](#snowflake-schema-design)
- [Airflow DAGs](#airflow-dags)
- [SA Layer (Reporting Views)](#sa-layer-reporting-views)
- [Engineering Challenges & Solutions](#engineering-challenges--solutions)
- [Setup & Deployment](#setup--deployment)
- [Analytics Deliverables (Power BI)](#analytics-deliverables-power-bi)

---

## Overview

**RBF**, a global investment firm, required a fully automated daily batch analytics platform to replace manual CSV collection and ad-hoc reporting. Analysts previously stitched together pricing data by hand — delaying portfolio reviews, overnight risk adjustments, and sector monitoring.

This project delivers:

- **Automated EOD ingestion** of all U.S. equities and ETFs via the Polygon.io Grouped Daily Bars API
- **Multi-layer Snowflake data warehouse** (RAW → CORE → DM_DIM/DM_FACT → SA)
- **Data quality enforcement** with a reject table capturing invalid records (e.g. negative volumes)
- **Delta freshness checks** to ensure no stale data reaches analytics
- **Slack alerting** for pipeline failures and daily EOD load summaries
- **Power BI dashboards** serving 6 curated analytic views for trading, risk, and research teams

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Docker + Apache Airflow                              │
│                                                                             │
│  polygon.io ──► Ingestion DAG ──► AWS S3 ──► Snowflake External Stage      │
│                      │                            │                        │
│                      │              ┌─────────────▼─────────────┐         │
│                      │              │     RAW   (append-only)    │         │
│                      │              │     CORE  (cleansed/merged)│         │
│                      │              │     DM_DIM (dimensions)    │         │
│                      │              │     DM_FACT (fact table)   │         │
│                      │              │     SA    (report views)   │         │
│                      │              └─────────────┬─────────────┘         │
│                 Alert DAGs ◄─────── Slack ◄────────┘                       │
└──────────────────────────────────────────────────────────────────┬──────────┘
                                                                   │
                                                              Power BI
```

![Architecture Diagram](docs/architecture.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | [Polygon.io](https://polygon.io) Grouped Daily Bars API |
| Orchestration | Apache Airflow 3.x (CeleryExecutor) |
| Containerization | Docker + Docker Compose |
| Cloud Storage | AWS S3 (bronze landing zone) |
| Data Warehouse | Snowflake (multi-layer ELT) |
| Alerting | Slack Incoming Webhooks |
| Visualization | Microsoft Power BI |
| Language | Python 3.x, SQL |

---

## Project Structure

```
eod-securities-pipeline/
│
├── airflow/
│   └── dags/
│       ├── eod_ingestion_dag.py          # Main EOD ingestion DAG (8-task pipeline)
│       ├── etl_pipeline_template.py      # Generic ETL pattern template
│       └── helpers/
│           ├── polygon_eod_data_downloader.py  # Polygon API client + lookback logic
│           ├── slack_utils.py                  # Slack webhook helpers + failure callback
│           ├── test_airflow_aws_connection.py  # AWS connectivity smoke test
│           ├── test_slack_conn.py              # Slack connectivity smoke test
│           └── test_snowflake_aws_connection.py # Snowflake→S3 stage smoke test
│
├── snowflake/
│   ├── 01_setup/
│   │   └── init_snowflake_objects.sql    # Warehouse, DB, schemas, all tables
│   ├── 02_raw_to_core/
│   │   └── raw_to_core_merge.sql         # Dedup + MERGE into CORE with window functions
│   ├── 03_dimensions/
│   │   └── load_dim_tables.sql           # MERGE into DIM_SECURITY + DIM_DATE
│   ├── 04_facts/
│   │   └── load_fact_daily_price.sql     # MERGE into FACT_DAILY_PRICE
│   ├── 05_sa_layer/
│   │   └── sa_layer_views.sql            # 6 reporting views (analytics serving layer)
│   └── 06_reject/
│       └── reject_table_creation.sql     # Reject table + quarantine helpers
│
├── docker/
│   └── docker-compose.yaml              # Full Airflow stack (CeleryExecutor + Redis + PG)
│
├── docs/
│   ├── architecture.png
│   ├── SNOWFLAKE_SETUP.md
│   ├── AIRFLOW_CONNECTIONS.md
│   └── ENGINEERING_NOTES.md
│
├── .env.example                          # Template for required secrets
├── .gitignore
└── README.md
```

---

## Data Flow

### Step-by-Step Pipeline

```
[1] Download CSV          polygon_eod_data_downloader.py
    Polygon Grouped       Lookback up to N days to find last valid
    Daily Bars API   ───► trading day. Writes /tmp/eod_YYYY-MM-DD.csv
         │
[2] Verify Local File     verify_file_exists()
    Check file exists     Raises AirflowFailException if missing
    + log size       ───►
         │
[3] Upload to S3          LocalFilesystemToS3Operator
    Keyed as:             market/bronze/eod/eod_prices_YYYY-MM-DD.csv
    market/bronze/eod/────►
         │
[4] Snowflake Load (TaskGroup: t04_snowflake_load)
    │
    ├─ s01: COPY INTO RAW         External stage → RAW.RAW_EOD_PRICES
    ├─ s02: Check loaded          SnowflakeCheckOperator (delta freshness)
    ├─ s03: Pre-merge metrics     Count RAW rows, rejects, insert/update estimates
    ├─ s04: MERGE → CORE          Dedup via ROW_NUMBER(), reject negatives to REJECT table
    ├─ s05: MERGE → DIM_SECURITY  Upsert new symbols (parallel with s06)
    ├─ s06: MERGE → DIM_DATE      Upsert new calendar dates (parallel with s05)
    ├─ s07: MERGE → FACT          Join CORE × DIM_SECURITY × DIM_DATE
    └─ s08: Post-merge metrics    Count CORE and FACT rows for Slack summary
         │
[5] Slack Summary         notify_slack_summary()
    Posts trading date,   trigger_rule=all_done (fires even on partial failure)
    row counts, rejects ──►
```

### Dependency Graph

```
t01_download → t02_verify → t03_upload_s3
                                  │
                            t04_snowflake_load
                            │
                    s01_copy_raw → s02_check → s03_pre_metrics → s04_merge_core
                                                                       │
                                                          ┌────────────┴────────────┐
                                                    s05_dim_security         s06_dim_date
                                                          └────────────┬────────────┘
                                                                 s07_merge_fact
                                                                       │
                                                                s08_post_metrics
                                                                       │
                                                               t05_slack_summary
```

---

## Snowflake Schema Design

### Layer Architecture

```
SEC_PRICING database
│
├── RAW schema             ← Append-only landing. Never mutated.
│   └── RAW_EOD_PRICES         Source of truth; includes _SRC_FILE + _INGEST_TS audit cols
│
├── CORE schema            ← Cleansed, deduplicated, validated canonical records
│   ├── EOD_PRICES             MERGE target; ROW_NUMBER() dedup on (TRADE_DATE, SYMBOL)
│   └── EOD_PRICES_REJECT      Quarantine for invalid records (e.g. VOLUME < 0)
│
├── DM_DIM schema          ← Conformed dimensions
│   ├── DIM_SECURITY           Surrogate key via SEQUENCE; SYMBOL UNIQUE constraint
│   ├── DIM_DATE               DATE_SK = YYYYMMDD integer; full calendar attributes
│   └── DIM_SECURITY_ATTRIBUTES  Enrichment layer: name, type, sector, industry, URL
│
├── DM_FACT schema         ← Fact table at (SECURITY_ID, DATE_SK) grain
│   └── FACT_DAILY_PRICE        FK to both dims; composite PK prevents duplicates
│
└── SA schema              ← Serving/analytics layer (views only, no stored data)
    ├── VW_SECURITY_DAILY_PRICES         Base enriched view (joins all 4 tables)
    ├── VW_TOP20_EQUITY_BY_VOLUME_DAILY  Daily top 20 equities ranked by volume
    ├── VW_WATCHLIST_HISTORY             10 pre-defined watchlist stocks full history
    ├── VW_SECURITY_LAST_30D_DAILY_RETURN  Daily % returns with LAG() window function
    ├── VW_SECTOR_LIQUIDITY_LATEST       Sector traded value + % contribution (latest day)
    └── VW_ETF_LIQUIDITY_30D_SUMMARY     30-day rolling avg volume/value per ETF
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| RAW is append-only | Preserves full audit trail; re-processing safe |
| CORE uses MERGE (not INSERT) | Idempotent; handles late-arriving / reprocessed files |
| SEQUENCE for SECURITY_ID | Avoids identity column race conditions in concurrent loads |
| Reject table in CORE schema | Invalid rows never reach dimensions/facts but are traceable |
| DATE_SK as YYYYMMDD integer | Fast integer joins; human-readable without conversion |
| SA layer = views only | Zero storage cost; always reflects latest FACT state |

---

## Airflow DAGs

### `eod_ingestion_dag.py` — Main Production DAG

| Config | Value |
|---|---|
| Schedule | `5 21 * * 1-5` (Mon–Fri 21:05 UTC ≈ after 4PM NYSE close) |
| Catchup | `False` |
| Max Active Runs | `1` |
| Retries | `3` with 5-minute delay |
| Failure Callback | `on_task_failure()` → Slack alert |

**Airflow Variables required:**

| Variable | Description |
|---|---|
| `POLYGON_API_KEY` | Polygon.io API key |
| `LOOKBACK_DAYS` | Days to search back for last valid trading day (default: `10`) |
| `S3_BUCKET` | Target S3 bucket name |

**Airflow Connections required:**

| Connection ID | Type |
|---|---|
| `aws_default` | Amazon Web Services |
| `snowflake_default` | Snowflake |
| `slack_default` | HTTP (Slack Incoming Webhook) |

### Slack Notifications

Two types of notifications fire automatically:

1. **Task Failure Alert** — fires on any task failure via `on_task_failure` callback
2. **EOD Summary** — fires at pipeline end (even on partial failure) with:
   - Trading date processed
   - RAW row count + reject count
   - Estimated CORE inserts/updates
   - Final CORE and FACT row counts

---

## SA Layer (Reporting Views)

All 6 views are built on top of `VW_SECURITY_DAILY_PRICES` (the enriched base view joining FACT + both DIM tables + attributes).

| View | Description | Key Technique |
|---|---|---|
| `VW_SECURITY_DAILY_PRICES` | Base view — all securities with enrichment | 4-table JOIN |
| `VW_TOP20_EQUITY_BY_VOLUME_DAILY` | Daily top 20 equities by volume | `ROW_NUMBER() + QUALIFY` |
| `VW_WATCHLIST_HISTORY` | Full history for 10 curated watchlist stocks | CTE with inline VALUES |
| `VW_SECURITY_LAST_30D_DAILY_RETURN` | Daily % return (last 30 days, prior close) | `LAG()` window function |
| `VW_SECTOR_LIQUIDITY_LATEST` | Sector traded-value + % of market (latest day) | Multi-CTE aggregation |
| `VW_ETF_LIQUIDITY_30D_SUMMARY` | 30-day rolling avg volume/value per ETF | `AVG() OVER ... ROWS BETWEEN 29 PRECEDING` |

---

## Engineering Challenges & Solutions

### 1. Snowflake External Stage to S3

Setting up Snowflake's external stage required creating a dedicated IAM role with S3 read permissions, a Storage Integration object in Snowflake (to avoid storing AWS credentials), and granting the Snowflake AWS account principal trust in the IAM policy. The stage uses a named file format (CSV with header skip) and the `COPY INTO` command references `$STAGE/market/bronze/eod/` prefix-matched to the current trading date.

### 2. Window Functions in MERGE Source

The RAW → CORE merge uses `ROW_NUMBER() OVER (PARTITION BY TRADE_DATE, SYMBOL ORDER BY _INGEST_TS DESC)` inside the USING clause to deduplicate before merging. Snowflake does not allow window functions directly in a MERGE's USING subquery at certain nesting levels — this was resolved by wrapping the ranked CTE in an additional subquery so the `WHERE row_number_val = 1` filter operates on a materialized rowset.

### 3. Reject Table for Invalid Records

Rather than failing the entire load on bad data, records with `VOLUME < 0` (or other data quality violations) are routed to `CORE.EOD_PRICES_REJECT` with a `REJECT_REASON` column and `REJECT_TS` audit timestamp. This is implemented in the MERGE's `WHEN NOT MATCHED` branch via a conditional INSERT, allowing the pipeline to complete successfully while making rejected rows fully auditable and re-processable.

### 4. Pandas in Dockerized Airflow

The official `apache/airflow` Docker image does not include `pandas` by default. The solution was to extend the base image via a custom `Dockerfile` (referenced in `docker-compose.yaml` via `build: .`) with `_PIP_ADDITIONAL_REQUIREMENTS` for quick iteration, and a proper `Dockerfile` with `pip install pandas` for production. The `docker-compose.yaml` is configured to build the custom image rather than pull the stock one, ensuring pandas and other custom dependencies are available across all worker containers.

---

## Setup & Deployment

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Snowflake account (free trial works)
- AWS account with an S3 bucket
- Polygon.io API key (free Starter tier sufficient for grouped daily)
- Slack workspace with Incoming Webhooks app enabled

### 1. Clone and configure environment

```bash
git clone https://github.com/your-username/eod-securities-pipeline.git
cd eod-securities-pipeline
cp .env.example .env
# Fill in AIRFLOW_UID, JWT secrets, etc.
```

### 2. Start Airflow

```bash
cd docker
docker compose up airflow-init   # first-time DB init
docker compose up -d             # start all services
# UI available at http://localhost:8080 (admin / admin)
```

### 3. Configure Airflow Connections & Variables

Navigate to **Admin → Connections** and add:
- `aws_default` — AWS credentials with S3 read/write
- `snowflake_default` — Snowflake account, warehouse `WH_INGEST`, database `SEC_PRICING`
- `slack_default` — HTTP connection with Slack webhook path as password

Navigate to **Admin → Variables** and add:
- `POLYGON_API_KEY`
- `LOOKBACK_DAYS` = `10`
- `S3_BUCKET` = your bucket name

### 4. Initialize Snowflake

Run scripts in order inside Snowflake Worksheets:

```sql
-- Run in sequence:
01_setup/init_snowflake_objects.sql          -- warehouse, DB, schemas, tables
06_reject/reject_table_creation.sql          -- reject quarantine table
-- (DAG handles 02–04 at runtime via SQLExecuteQueryOperator)
05_sa_layer/sa_layer_views.sql               -- reporting views
```

### 5. Populate DIM_SECURITY_ATTRIBUTES

The `DM_DIM.DIM_SECURITY_ATTRIBUTES` table holds enrichment data (security name, type, sector, industry). Load this from a reference dataset (e.g. Polygon.io Ticker Details API or a static CSV) before running Power BI dashboards. Without it, the SA views will still function but `security_name` and `sector` columns will be NULL.

---

## Analytics Deliverables (Power BI)

Connect Power BI to Snowflake using the `SA` schema views. All 6 views are designed to be imported directly as tables.

| Dashboard | Source View | Key Visuals |
|---|---|---|
| Equity & ETF Liquidity Insights | `VW_ETF_LIQUIDITY_30D_SUMMARY` | Bar chart: ETF avg 30d volume rank |
| Watchlist Performance & Momentum | `VW_WATCHLIST_HISTORY` | Line chart: close price trend per stock |
| Volume & Traded-Value Intelligence | `VW_TOP20_EQUITY_BY_VOLUME_DAILY` | Treemap: traded value by symbol |
| Sector Liquidity Contribution | `VW_SECTOR_LIQUIDITY_LATEST` | Pie/donut: % contribution by sector |
| Daily Market Movers | `VW_SECURITY_LAST_30D_DAILY_RETURN` | Table: top gainers/losers by daily return |
| Daily Automated Refresh | All views | Scheduled dataset refresh (post-DAG completion) |

---

## License

This project is for portfolio and educational demonstration purposes.
