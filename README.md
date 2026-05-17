# Secure Financial Data Pipeline

An Apache Airflow-orchestrated ETL pipeline that ingests 284K+ credit card transactions through a Bronze/Silver/Gold medallion architecture on Delta Lake, with built-in fraud analytics, PySpark performance optimizations, and automated failure recovery.

## Architecture

```mermaid
flowchart LR
    CSV["creditcard.csv\n284,807 rows"]:::source

    subgraph Bronze["Bronze Layer — Raw Ingest"]
        B1["Schema enforcement\nMinimal validation\nPartitioned by ingest_date"]
    end

    subgraph Silver["Clean & Enrich"]
        S1["Deduplication\nType casting\nDerived columns\nZ-ORDER on tx_date"]
    end

    subgraph Gold["Aggregations"]
        G1["Daily fraud counts\nHourly avg amounts\nBroadcast joins"]
    end

    DL[("Delta Lake\n/data/delta/")]:::storage

    CSV --> Bronze --> Silver --> Gold --> DL

    Airflow["Apache Airflow\nDAG orchestration"]:::orchestrator
    Slack["Slack Webhook\nFailure alerts"]:::alert

    Airflow -.-> Bronze
    Airflow -.-> Silver
    Airflow -.-> Gold
    Airflow -- on_failure_callback --> Slack

    classDef source fill:#f5a623,color:#000
    classDef storage fill:#4a90d9,color:#fff
    classDef orchestrator fill:#017cee,color:#fff
    classDef alert fill:#e01e5a,color:#fff
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8+ |
| Processing | PySpark 3.5+ with delta-spark |
| Storage format | Delta Lake |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Linting | ruff |
| Testing | pytest |
| Language | Python 3.11 |

## Local Setup

### Prerequisites
- Docker Desktop ≥ 4.x
- 8 GB RAM available to Docker
- The Kaggle Credit Card Fraud Detection dataset (`creditcard.csv`)

### 1 — Download the dataset

Download `creditcard.csv` from [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) and place it at:

```
secure-financial-pipeline/data/creditcard.csv
```

### 2 — Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in SLACK_WEBHOOK_URL (optional for local dev)
```

### 3 — Start the stack

```bash
docker compose up --build -d
```

Services started:
- **airflow-webserver** → http://localhost:8080 (admin / admin)
- **airflow-scheduler**
- **postgres** (Airflow metadata DB)
- **redis** (message broker placeholder)

### 4 — Trigger the pipeline

Via the Airflow UI (http://localhost:8080), enable and trigger the DAGs in order:

1. `bronze_ingest`
2. `silver_clean`
3. `gold_aggregate`

Or via CLI inside the scheduler container:

```bash
docker exec airflow-scheduler airflow dags trigger bronze_ingest
docker exec airflow-scheduler airflow dags trigger silver_clean
docker exec airflow-scheduler airflow dags trigger gold_aggregate
```

### 5 — Run tests locally (without Docker)

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Performance Benchmarks

> Run on: _[fill in your machine specs]_

| Stage | Optimized | Unoptimized | Speedup |
|-------|-----------|-------------|---------|
| Silver clean | ~3 min | ~6 min | ~2× |
| Bronze ingest | _TBD_ | _TBD_ | — |
| Gold aggregate | _TBD_ | _TBD_ | — |

**Key optimizations applied:**
- Adaptive Query Execution (`spark.sql.adaptive.enabled=true`)
- Z-ORDER clustering on `transaction_date` (eliminates full-table scans on date-range queries)
- `DataFrame.cache()` for intermediate results reused across transformations
- Broadcast joins for small dimension tables in Gold layer

---

## Failure Recovery Design

### Retries with Exponential Backoff
All DAG tasks are configured with `retries=3`, `retry_delay=timedelta(minutes=2)`, and `retry_exponential_backoff=True`. This means failed tasks are retried at 2, 4, and 8 minute intervals before the DAG is marked as failed — absorbing transient network or resource errors without manual intervention.

### Idempotent Writes via Delta MERGE
Both Silver and Gold layers use Delta Lake `MERGE INTO` semantics instead of overwrite. Re-triggering a DAG for the same date range will update existing rows rather than create duplicates, making all writes safe to replay.

### SLA Monitoring
Each task carries `sla=timedelta(minutes=15)`. If a task has not completed within 15 minutes of its scheduled start, Airflow fires an SLA miss callback, which also posts to Slack.

### Slack Alerting
`src/utils/slack_alerts.py` contains a `send_failure_alert` function wired into every DAG's `on_failure_callback`. On any task failure it posts a structured message (DAG name, task ID, execution date, log URL) to the configured webhook.

---

## Security Considerations

### Secrets Management
All credentials (Slack webhook URL, database passwords) are loaded exclusively from environment variables (`.env` file) — never hardcoded. The `.env` file is listed in `.gitignore` and a `.env.example` template is committed instead.

### No Hardcoded Credentials
The codebase has no embedded passwords, API keys, or tokens. The CI pipeline validates this by running `ruff` checks and a secret-pattern grep.

### Structured Logging for Auditability
`src/utils/logging_config.py` configures JSON-structured logging across all pipeline stages. Each log record includes a timestamp, log level, module name, and pipeline stage — producing an audit trail compatible with SIEM ingestion (Splunk, Datadog, etc.).

### Container Isolation
Each service runs in its own Docker container with explicit port bindings. The Airflow webserver is the only service exposed externally; Postgres and Redis communicate only on the internal Docker network.

### Principle of Least Privilege
Airflow connections and variables reference secrets by name; the DAG code never reads raw credential values directly.
