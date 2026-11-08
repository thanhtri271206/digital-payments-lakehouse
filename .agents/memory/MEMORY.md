# Long-Term Project Memory & Context Anchor

> **Repository:** `digital-payments-lakehouse`  
> **Master Anchor / SSOT:** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Last Updated:** Active Execution (Milestone 1 Kickoff)

---

## 1. Project Profile & Operational Constraints

- **System Purpose:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform.
- **Learner Persona:** 3rd-year Information Systems student at UIT-VNUHCM aiming for Junior Data Engineer / Big Data roles at top Vietnamese enterprises (FPT, Viettel, VNPT, Techcombank, VPBank, Momo).
- **Background Knowledge:** Successfully completed Project 1 (`crypto-coin-pipeline`) with Airflow 3, Async HTTPX, MinIO/S3, DuckDB/MotherDuck, dbt Kimball Star Schema (145 tests), Pydantic v2, Docker CI.
- **Project 2 Technical Differentiator:** Streaming, Stateful Processing, Event-Time & Watermarking (2m), 3-Way Financial Reconciliation, Spark Structured Streaming, Delta Lake ACID Medallion, Failure Recovery & Idempotency.
- **Constraints (Confirmed):**
  - **Financial Hard Budget:** Under 100,000 VND (100% Local Docker Core + Databricks Free Edition / AWS Free Tier).
  - **Local Hardware:** 32 GB RAM, Intel Xeon W2133 3.6 GHz, ~100 GB free disk.
  - **Effort Budget:** 200 engineering hours (~3 hours/day over 2–3 months).
  - **Language Standards:** Technical documentation, READMEs, ADRs, and code comments in **English**. Mentorship dialogue in Vietnamese.

---

## 2. Global Architecture Anchors (From SSOT)

### 2.1. Dual-Track Deployment Topology
- **Track 1 (Local-First Core):** 100% reproducible local Docker Compose running Kafka KRaft, MinIO (S3 API), PySpark Structured Streaming, Airflow 3, dbt-spark, Metabase.
- **Track 2 (Cloud Showcase Branch):** Batch sync to AWS S3 + Databricks Free Edition running `dbt-databricks` for portfolio evidence and CV benchmarks.

### 2.2. The 5 Source Feeds (S1–S5) & Ground Truth (G0)
- **S1 (Payment Gateway Stream):** Kafka JSON stream. Lifecycle transitions from `INITIATED (1)` to `REFUNDED (6)`. At-least-once delivery.
- **S2 (Wallet / Internal Ledger):** Append-only ledger. Only `INSERT`, `ADJUSTMENT`, `REVERSAL`. No physical updates.
- **S3 (Master Data):** Merchant SCD Type 2 (NFC/NFD Vietnamese names) + User current-state.
- **S4 (External Settlements):** Partner Gateway Settlement CSV (multi-file + EOD) & Bank Statement CSV. Scenarios: `v1` vs `v2` resend, timezone boundaries.
- **S5 (FX Source):** Out of scope; fixed reference exchange rates only.
- **G0 (Ground Truth Weapon):** Deterministic output using fixed seed `20260930`. Acts as the absolute benchmark to automatically measure Precision and Recall of Data Quality and 3-Way Reconciliation algorithms.

### 2.3. Medallion Storage Standard
- **Bronze:** Append-only raw JSON/CSV payloads on MinIO (`s3a://bronze/`).
- **Silver Dual-Table:**
  - `silver_payment_events_history`: Append-only time series.
  - `silver_transactions_current`: 1 row per `transaction_id`, pre-aggregated Monotonic State MERGE, dynamic timer stuck detection (5m).
  - Exception branches: `silver_quarantine_events`, `silver_late_events_dlq`, `silver_lifecycle_exceptions`.
- **Gold:** Curated marts (`gold_rt_channel_metrics`, `gold_eod_reconciliation_runs`, `gold_eod_reconciliation_details`, `gold_fact_payment_analytics`).

---

## 3. Core Technical Invariants Quick-Sheet

1. `amount_minor` strictly `BIGINT` integer minor units (e.g. VND).
2. Internal Ledger `S2` strictly Append-only (corrections via `REVERSAL`).
3. Monotonic State Rank (1 -> 6) with Terminal Lock.
4. Internal storage in `UTC`; EOD cutoff at `23:59:59 Asia/Ho_Chi_Minh (UTC+07:00)`.
5. Streaming Watermark = 2 minutes (late events $> 2\text{m}$ to DLQ).
6. Micro-batch Circuit Breaker: $> 5\%$ error rate aborts batch ($\ge 50$ rows).
7. Dynamic Timer: 5 minutes in `PROCESSING` flags `operational_status = STUCK`.
8. 3-Way Reconciliation with 7 standard break classifications.
9. Deterministic Seed = `20260930`.

---

## 4. Working Agreement & Pedagogy

- Always follow [`.agents/workflows/teach-decide-and-build.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/teach-decide-and-build.md).
- Agent writes 100% production code, executes tests, and captures verifiable logs.
- Agent mentors through code: Deep-dive engine internals $\rightarrow$ ADR Trade-offs $\rightarrow$ Production Code $\rightarrow$ Interview Talking Points.
