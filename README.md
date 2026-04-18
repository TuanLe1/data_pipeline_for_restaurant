# Restaurant Analytics Pipeline

**Near Real-Time Data Platform for Multi-Branch F&B Operations**

> A POC lab project simulating a production-scale data pipeline using an open-source Modern Data Stack, running entirely on Docker locally.

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Data Flow](#data-flow)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Model](#data-model)
- [dbt Models](#dbt-models)
- [Observability & Data Quality](#observability--data-quality)
- [Production Mapping](#production-mapping)
- [Roadmap & Future Improvements](#roadmap--future-improvements)
- [Validated Results](#validated-results)

---

## Overview

This project builds a **near real-time analytics pipeline** for a Japanese restaurant chain (8 branches), pulling data from the CukCuk POS system and delivering revenue dashboards refreshed every **10 minutes**.

### Business Problem

- 8 branches, each generating 100–700 orders/day
- Need a consolidated dashboard for revenue, conversion rate, and AOV in near real-time
- Data sourced from a third-party API with rate limits and late-arriving records
- Must guarantee data integrity during backfills and consumer retries

### POC Objectives

- Validate a Modern Data Stack architecture locally before cloud deployment
- Understand how each pipeline component behaves under realistic data volumes
- Build a foundation that can scale from 8 → 100+ branches

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXTRACTION LAYER                         │
│                                                                 │
│  CukCuk API ──► ECS Worker (Producer) ──► Rate Limiter         │
│  (POS System)    (Async Python)            (Redis)              │
└──────────────────────────┬──────────────────────────────────────┘
                           │ JSONL files per branch
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LANDING ZONE  (Bronze)                      │
│                                                                 │
│         MinIO / S3  ── raw/orders/  ── raw/invoices/           │
│         S3 Event Notification → Redis Queue                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ S3 file path message
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                            │
│                                                                 │
│  Redis Queue ──► ECS Consumer ──► ClickHouse INSERT            │
│  (SQS equiv.)    (Long-poll)       Silver Layer                 │
│                   Retry → DLQ on 3rd failure                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ triggered after queue drains to 0
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   TRANSFORMATION LAYER                          │
│                                                                 │
│  dbt run  (every 10 min, incremental, lookback 2 days)         │
│  Silver ──► Gold   (master_sales_analytics, revenue_daily)     │
│  dbt test (revenue reconciliation + duplicate checks)          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SERVING LAYER                              │
│                                                                 │
│   ClickHouse Gold Layer ──► Dashboard / BI Tool                │
└─────────────────────────────────────────────────────────────────┘

  Airflow (Orchestrator): Daily master data sync + weekly self-healing backfill
```

---

## Data Flow

### 1. Real-Time Ingestion — every 10 minutes

```
Producer Worker
  for each of 8 branches:
    1. Get watermark: MAX(order_date) WHERE branch_id = ?
    2. Call CukCuk API with LastSyncDate = watermark
    3. Upload JSONL → MinIO  (raw/orders/{branch_id}_{uuid}.jsonl)
    4. Push S3 path → Redis Queue

Consumer Worker (always running)
  while True:
    1. brpop from Redis Queue
    2. Download file from MinIO
    3. Map columns per config schema
    4. INSERT → ClickHouse Silver  (ReplacingMergeTree)
    5. On failure: retry up to 3x → Dead Letter Queue

Producer (after all branches complete)
  1. Wait for Redis queue depth = 0
  2. Trigger: dbt run --select master_sales_analytics revenue_daily
  3. dbt test  (data quality gate)
  4. Sleep 10 minutes → repeat
```

### 2. Daily Batch — 1 AM every day

```
Airflow DAG: restaurant_elt_pipeline
  Task 1: master_extract_to_s3
          Full snapshot of Product + Customer data → MinIO
  Task 2: master_load_to_clickhouse
          MinIO → ClickHouse (ReplacingMergeTree dedup)
  Task 3: dbt_transform_and_test
          dbt run + dbt test
```

### 3. Weekly Self-Healing Backfill — 3 AM every Sunday

```
Airflow DAG: maintenance_weekly_backfill
  Loop over last 7 days:
    run_etl.py --phase trans --step extract --date {date}
    → re-fetches orders + invoices per branch for that date
  dbt run → rebuild mart with any backfilled data
```

### Late-Arriving Data Strategy

| Layer | Mechanism |
|-------|-----------|
| Bronze | Per-branch watermark → only fetch deltas |
| Silver | `ReplacingMergeTree` deduplicates on insert |
| Gold | Incremental lookback 2 days catches late records |
| Weekly | Full re-fetch of last 7 days catches anything missed |
| Query | `argMax(status, updated_at)` instead of `SELECT FINAL` for speed |

---

## Tech Stack

| Component | Local (POC) | Production Equivalent |
|-----------|-------------|----------------------|
| Message Queue | Redis (alpine) | AWS SQS + DLQ |
| Object Storage | MinIO | AWS S3 |
| Analytics DB | ClickHouse (single node) | ClickHouse Cloud / Cluster |
| Orchestrator | Airflow (LocalExecutor) | Amazon MWAA / ECS |
| Transformation | dbt-clickhouse 1.7 | dbt Cloud / self-hosted |
| Workers | Docker containers | AWS ECS Fargate |
| Secrets | config.json + AWS SSM | AWS SSM Parameter Store |
| Alerting | Slack Webhook | PagerDuty + Slack |

---

## Project Structure

```
data_pipeline_for_restaurant/
├── configs/
│   └── config.json                  # Schema, column mapping, credentials
│
├── dags/
│   ├── restaurant_etl_dag.py        # Daily production DAG
│   ├── weekly_backfill.py           # Self-healing DAG
│   ├── dbt_admin_deploy.py          # Manual dbt deploy DAG
│   │
│   └── repos/
│       ├── scripts/
│       │   ├── producer_worker.py   # Fetch API → MinIO → Redis
│       │   ├── consumer_worker.py   # Redis → ClickHouse ingestion
│       │   ├── run_etl.py           # CLI entry point for backfill
│       │   └── init_db.py           # DB + table initialization
│       │
│       ├── src/
│       │   ├── extractors/cukcuk/
│       │   │   ├── extractor.py     # Async API client, pagination, retry
│       │   │   └── auth.py          # HMAC-SHA256 signature auth
│       │   ├── transformers/
│       │   │   └── generic_transformer.py   # Schema mapping, type casting
│       │   ├── loaders/
│       │   │   ├── clickhouse_loader.py
│       │   │   └── s3_loader.py
│       │   ├── pipelines/
│       │   │   └── cukcuk_pipeline.py       # Orchestrates extract + load
│       │   └── utils/
│       │       ├── ssm_loader.py            # Config loading + AWS SSM merge
│       │       └── slack_alert.py           # Airflow failure notifications
│       │
│       └── dbt_project/
│           ├── models/
│           │   ├── staging/                 # Silver layer (views)
│           │   └── marts/                   # Gold layer (incremental tables)
│           ├── tests/                       # Custom data quality tests
│           └── macros/
│               └── hash_pii.sql             # MD5 PII anonymization
│
├── Dockerfile                       # Airflow image
├── Dockerfile_worker                # Producer / Consumer image
└── docker-compose.yaml
```

---

## Getting Started

### Prerequisites

- Docker Desktop ≥ 4.0
- AWS credentials (for SSM), or fill credentials directly into `config.json`
- CukCuk API credentials (AppID, Domain, SecretKey)

### 1. Clone & configure

```bash
git clone https://github.com/yourname/restaurant-analytics-pipeline
cd restaurant-analytics-pipeline

cp configs/config.json.example configs/config.json
# Fill in: CukCuk credentials, ClickHouse password, MinIO credentials
```

### 2. Create `.env`

```bash
cat > .env << EOF
AIRFLOW_UID=50000
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-southeast-1
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
EOF
```

### 3. Start the full stack

```bash
docker compose up -d
```

Startup order (managed by `depends_on` health checks):

1. `postgres` — Airflow metadata DB
2. `minio` — Object storage
3. `minio-init` — Creates `restaurant-datalake` bucket
4. `clickhouse` — Analytics DB
5. `db-init` — Creates raw tables + first dbt run
6. `redis-queue` — Message broker
7. `ingestion-producer` — Starts fetching data every 10 min
8. `ingestion-consumer` — Starts ingesting from queue
9. `airflow-webserver` + `airflow-scheduler`

### 4. Verify the pipeline

```bash
# Check all containers are healthy
docker compose ps

# Watch producer fetch + dbt trigger
docker compose logs -f ingestion-producer | grep -E "✅|❌|dbt|Sleeping"

# Watch consumer ingest
docker compose logs -f ingestion-consumer | grep -E "SUCCESS|FAIL|DLQ"

# Spot-check data in ClickHouse
docker compose exec clickhouse clickhouse-client \
  --user admin --password password \
  --query "
    SELECT report_date,
           countDistinct(order_id) AS orders,
           round(sum(net_revenue_inclusive), 2) AS revenue
    FROM restaurant_db.master_sales_analytics
    WHERE report_date >= today() - 3
    GROUP BY report_date
    ORDER BY report_date
  "
```

### 5. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / password |
| ClickHouse HTTP | http://localhost:8123 | admin / password |

### 6. Backfill historical data

```bash
# Backfill a specific date (fetches all 8 branches)
docker compose exec ingestion-producer \
  python3 scripts/run_etl.py --phase trans --step extract --date 2026-04-17

# After consumer processes the queue, rebuild the mart
docker compose exec ingestion-consumer \
  bash -c "cd /app/dbt_project && dbt run \
           --select master_sales_analytics revenue_daily \
           --profiles-dir /app/dbt_project --no-write-json"
```

---

## Data Model

### Bronze Layer — Raw ClickHouse tables

| Table | Engine | Order By | Description |
|-------|--------|----------|-------------|
| `order_header` | ReplacingMergeTree | `order_id` | POS orders |
| `order_detail` | ReplacingMergeTree | `order_id, order_detail_id` | Line items per order |
| `invoice_header` | ReplacingMergeTree | `ref_id` | Settled invoices |
| `invoice_detail` | ReplacingMergeTree | `ref_id, ref_detail_id` | Line items per invoice |
| `invoice_payment` | ReplacingMergeTree | `invoice_payment_id` | Payment methods |
| `product` | ReplacingMergeTree | `product_id` | Product catalog |
| `customer` | ReplacingMergeTree | `customer_id` | Customer list |

### Silver Layer — dbt views

| Model | Description |
|-------|-------------|
| `stg_invoice_header` | Filters nulls/invalid records, adds `report_date`, `year`, `month` |
| `stg_invoice_detail` | Pass-through from Bronze |
| `stg_order_header` | Pass-through from Bronze |
| `stg_order_detail` | Dedup by `order_detail_id` using `ROW_NUMBER()` |
| `stg_customer` | Hashes PII: `customer_name`, `customer_tel` → MD5 |
| `stg_product` | Selects relevant columns only |

### Gold Layer — dbt incremental tables

#### `master_sales_analytics`
Granular revenue analytics at the invoice line item level.

```
Key columns:
  branch_name, order_id, invoice_code, customer_id
  ref_detail_id, item_name, item_id
  ref_date, report_date, report_hour
  quantity
  net_revenue_pre_tax    = amount - allocation_amount
  tax_amount
  net_revenue_inclusive  = amount + tax_amount

Engine:      ReplacingMergeTree(ref_date)
Order by:    (report_date, order_id, item_id, ref_detail_id)
Strategy:    Incremental, lookback 2 days
```

#### `revenue_daily`
Daily revenue summary aggregated per branch.

```
Key columns:
  report_date, branch_name, year, month
  total_orders    = COUNT(DISTINCT ref_id)
  total_revenue   = SUM(total_amount)
  aov             = total_revenue / total_orders
```

---

## dbt Models

### Incremental Strategy

```sql
-- master_sales_analytics runs incrementally every 10 minutes.
-- First run: loads full history.
-- Subsequent runs: reprocesses only the last 2 days.

{% if is_incremental() %}
WHERE toDate(ref_date) >= toDate(now()) - 2
{% endif %}
```

**Why a 2-day lookback?**
The CukCuk API can return records late due to server-side delays. A 2-day window catches the vast majority of late arrivals. Combined with `ReplacingMergeTree` and `unique_key`, runs are fully idempotent.

**Why `argMax` instead of `SELECT FINAL`?**

```sql
-- FINAL forces a synchronous merge → 3–5x slower on large tables
SELECT order_id, status FROM silver_orders FINAL

-- argMax reads duplicates but returns only the latest value
-- Equivalent correctness at query time, significantly faster
SELECT order_id, argMax(status, updated_at) FROM silver_orders GROUP BY order_id
```

### PII Anonymization

```sql
-- stg_customer.sql
{{ hash_pii('customer_name') }} AS customer_name_hashed,
{{ hash_pii('customer_tel') }}  AS customer_tel_hashed

-- macros/hash_pii.sql
{% macro hash_pii(column_name) %}
    lower(hex(MD5(coalesce(cast({{ column_name }} as String), ''))))
{% endmacro %}
```

---

## Observability & Data Quality

### Automated dbt Tests — run after every 10-minute cycle

| Test | File | Failure condition |
|------|------|-------------------|
| Revenue reconciliation | `assert_no_revenue_discrepancy.sql` | Mart vs Silver diff > 1% |
| Duplicate line items | `assert_no_duplicate_line_items.sql` | `mart_rows != unique_keys` |
| Negative revenue | `assert_revenue_positive.sql` | `net_revenue_inclusive < 0` |
| Future-dated orders | `assert_order_date_not_in_future.sql` | `order_date > today()` |

### Manual Validation Queries

```sql
-- 1. End-to-end reconciliation: Gold vs Silver
SELECT
    m.report_date,
    countDistinct(m.order_id)                              AS mart_orders,
    countDistinct(h.ref_id)                                AS silver_orders,
    round(sum(m.net_revenue_inclusive), 2)                 AS mart_revenue,
    round(sum(d.amount + coalesce(d.tax_amount, 0)), 2)    AS silver_revenue,
    round(sum(m.net_revenue_inclusive)
          - sum(d.amount + coalesce(d.tax_amount, 0)), 2)  AS diff
FROM restaurant_db.master_sales_analytics m
JOIN restaurant_db.invoice_detail d
    ON  m.order_id      = d.ref_id
    AND m.item_id       = d.item_id
    AND m.ref_detail_id = d.ref_detail_id
JOIN restaurant_db.invoice_header h ON d.ref_id = h.ref_id
WHERE m.report_date >= today() - 7
GROUP BY m.report_date
ORDER BY m.report_date;

-- 2. Duplicate check in mart
SELECT order_id, item_id, ref_detail_id, count(*) AS cnt
FROM restaurant_db.master_sales_analytics
GROUP BY order_id, item_id, ref_detail_id
HAVING cnt > 1;

-- 3. Per-branch ingestion watermarks
SELECT 'order_header'   AS tbl, max(order_date) AS last_sync
FROM restaurant_db.order_header
UNION ALL
SELECT 'invoice_header', max(ref_date)
FROM restaurant_db.invoice_header;
```

### Alerting

| Trigger | Channel | Current status |
|---------|---------|----------------|
| Airflow task failure | Slack (`on_failure_callback`) | ✅ Active |
| dbt test failure | Container log | ⚠️ Needs Slack integration |
| DLQ has messages | Manual Redis check | ⚠️ Needs CloudWatch alarm |

---

## Production Mapping

This project is designed to map 1:1 with a production AWS stack.

```
Local POC                       →  Production AWS
────────────────────────────────────────────────────
Docker container (Producer)     →  ECS Fargate Task (auto-scaling)
Docker container (Consumer)     →  ECS Fargate Task (auto-scaling)
Redis alpine                    →  AWS SQS + Dead Letter Queue
MinIO                           →  AWS S3 + S3 Event Notifications
ClickHouse single node          →  ClickHouse Cloud / Cluster
Airflow LocalExecutor           →  Amazon MWAA / ECS Airflow
config.json                     →  AWS SSM Parameter Store
Slack Webhook                   →  PagerDuty + Slack
```

### Scaling Path

| Phase | Sellers | Changes needed |
|-------|---------|----------------|
| Phase 1 | 1,000 | Current architecture, increase ECS task count |
| Phase 2 | 10,000 | ClickHouse cluster sharded by `seller_id`, Redis → SQS, distributed job scheduler (DynamoDB lock) |

---

## Roadmap & Future Improvements

### High Priority — required before true production

**1. Parallel branch fetching**
Currently 8 branches are fetched sequentially, taking 4–8 minutes total.
Switching to `asyncio.gather` would reduce this to ~1–2 minutes:
```python
await asyncio.gather(*[fetch_branch(b_id) for b_id in branch_ids])
```

**2. Authenticated MinIO downloads in Consumer**
`consumer_worker.py` downloads files via anonymous HTTP.
Should use `boto3` with credentials for security and reliability.

**3. Slack alerts on dbt test failure**
Currently dbt test failures only appear in container logs.
The producer should call the Slack webhook when `result.returncode != 0`.

**4. Float32 → Decimal(10,4) for monetary columns**
`amount` and `tax_amount` use `Float32`, causing small rounding errors (~$5 on $42k).
Changing to `Decimal(10,4)` in `Target-Schema` and recreating tables eliminates this.

### Medium Priority — reliability improvements

**5. MV Reconciliation Job**
An hourly ECS task that compares Gold vs Silver row counts per `(seller_id, time_window)`.
Auto-heals windows older than 1 hour where diffs exceed 0.1%.
Prevents silent data loss from ClickHouse Materialized View failures.

**6. Spot Instance checkpoint**
When running on Spot/Preemptible instances, workers need to checkpoint
every 500 records to S3. On restart, resume from the checkpoint
instead of re-fetching the entire batch.

**7. End-to-end `ingestion_run_id` tracing**
Attach a UUID to S3 metadata, Redis message attributes, and a
`_ingestion_run_id` column in every ClickHouse table.
Allows tracing any record from API response all the way to the dashboard
during incident investigation.

**8. Adaptive Freshness Gap alerting**
Two-threshold alerting for data staleness:
- Peak hours (8 AM–10 PM): alert if gap > 15 minutes
- Off-peak (10 PM–8 AM): alert if gap > 45 minutes
Prevents alert fatigue from natural traffic drops overnight.

### Low Priority — nice to have

**9. ClickHouse Row-Level Security**
Add a fourth isolation layer at the DB level:
```sql
CREATE ROW POLICY branch_isolation ON invoice_header
USING branch_id = currentUser();
```
Defense-in-depth for multi-tenant deployments.

**10. Customer retention model**
`customer_retention.sql` is currently empty.
Opportunity to build cohort analysis: customers returning within 30/60/90 days,
CLV by branch, repeat visit rate by day-of-week.

**11. dbt Exposures**
Declare downstream dashboards in dbt `exposures` to track full lineage
from raw API data to BI tool, enabling impact analysis when upstream schemas change.

**12. Schema evolution handling**
When CukCuk adds new fields to the API response, the pipeline should
detect and log them to a `schema_changes` table rather than silently dropping them.
Alert when a previously unseen field appears.

**13. Redis → SQS migration guide**
A step-by-step guide to migrate from Redis Queue to AWS SQS when scaling.
The code change is minimal — replace `r.lpush` / `r.brpop`
with SQS `send_message` / `receive_message`. The architecture remains identical.

---

## Validated Results

The following was verified end-to-end on 2026-04-18 after full pipeline build and debugging:

| Check | Result |
|-------|--------|
| Data freshness | ~10–12 min end-to-end (API → Gold layer) |
| Revenue accuracy | `diff = 0.00` between Mart and Silver |
| Order count accuracy | 100% match across all layers |
| Duplicate-free | `mart_rows = mart_unique_keys` confirmed |
| Branch isolation | 8 branches with independent per-branch watermarks |
| Late data handling | Weekly backfill catches records missed by incremental |
| Idempotency | Backfill can be re-run multiple times with consistent results |
| Data quality gates | 4 automated dbt tests pass after every cycle |
| PII protection | `customer_name` and `customer_tel` hashed with MD5 |

**Revenue reconciliation — confirmed zero diff:**

| Date | Orders | Revenue | Mart vs Silver diff |
|------|--------|---------|---------------------|
| 2026-04-11 | 627 | $29,598 | $0.00 |
| 2026-04-12 | 591 | $30,885 | $0.00 |
| 2026-04-17 | 739 | $41,997 | $0.00 |
| 2026-04-18 | 245 | $10,582 | $0.00 |

---

## License

MIT — free to use for personal and commercial projects.

---

*Built to validate Modern Data Stack patterns before deploying to production cloud infrastructure.
Every architectural decision — Claim Check pattern, per-branch watermarks, incremental dbt with lookback,
`argMax` over `FINAL`, `ReplacingMergeTree` dedup — directly maps to how the same problems are solved at scale on AWS.*