# Architectural Decision Log & ADR Tracker

> **Repository:** `digital-payments-lakehouse`  
> **SSOT Reference:** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Status:** Living Document (Updated on every architectural choice)

---

## 1. Global Architectural Decisions (Locked in Phase 0–8)

| Decision ID | Area | Choice Selected | Alternatives Rejected | Primary Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **`ADR-01`** | **Deployment Topology** | Dual-track: Local Docker (100% Core) + Databricks Free Showcase | Cloud-only AWS/Databricks, Pure Local without Cloud | Zero cost ($< 100\text{k VND}$), 100% reproducible offline, real Databricks cloud evidence for CV. |
| **`ADR-02`** | **Message Broker** | Apache Kafka 3.8.0 KRaft mode | Kafka with ZooKeeper, RabbitMQ, Redpanda | KRaft eliminates ZooKeeper overhead; dual-listener supports local scripts and container networking. |
| **`ADR-03`** | **Compute Engine** | Apache Spark (Structured Streaming & Batch) | Apache Flink, Kafka Streams, DuckDB streaming | Spark provides a unified engine for both micro-batch streaming (SLO 30s) and large-scale dbt-spark batch joins. |
| **`ADR-04`** | **Storage & ACID Layer** | Delta Lake on MinIO (Local) / S3 (Cloud) | Apache Iceberg, Apache Hudi, Raw Parquet | Delta Lake provides native ACID transactions, Time Travel, schema enforcement, and tight Databricks compatibility. |
| **`ADR-05`** | **Silver Dual-Table Pattern** | Dual-table: `history` (append-only) + `current` (pre-aggregated merge) | Single SCD Type 2 streaming table | Prevents small files explosion and eliminate Delta merge multiple-source-row conflict. |
| **`ADR-06`** | **Monetary Representation** | `amount_minor` as `BIGINT` | `FLOAT`, `DOUBLE`, `DECIMAL(18,2)` | Completely eliminates IEEE 754 floating-point rounding errors in multi-million transaction reconciliations. |
| **`ADR-07`** | **Timezone Standard** | Storage in UTC, Cutoff at 23:59:59 UTC+07 | Storing local time, Storing without offset | Eliminates daylight saving / leap second drift; ensures clean EOD partition alignment. |
| **`ADR-08`** | **Reconciliation Framework** | `dbt-spark` / `dbt-databricks` with 3-Way Full Outer Join | Python Pandas script, Spark custom RDD matching | Declarative SQL models, automated lineage graph, dbt test contracts, versioned replay (`run_version`). |
| **`ADR-09`** | **Fault Tolerance & Safety** | 5% Circuit Breaker + 2m Watermark DLQ | Dropping late events, Silent error log | Protects downstream tables from poisoned batches; guarantees late records are recoverable via batch. |
| **`ADR-10`** | **Ground Truth Benchmark** | Deterministic Simulator $G0$ with seed `20260930` | Manual test assertions, random synthetic data | Provides reproducible truth to measure exact statistical Precision and Recall for DQ and Reconciliation. |

---

## 2. Active Milestone Decisions (Milestone 1 — M1)

*Decisions made during the execution of Milestone 1 tasks will be recorded here.*

| Decision ID | Task ID | Component | Decision Taken | Rationale & Trade-Off | Date |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ADR-M1-01` | `WBS-M1-T01` | Docker Network | Named Bridge `dp-network` with explicit subnet | Predictable container DNS resolution (`kafka:29092`, `minio:9000`). | 2026-11-05 |
| `ADR-M1-02` | `WBS-M1-T02` | Config Engine | Pydantic v2 Settings + YAML with Loguru structured JSON | Fail-fast validation of config invariants; structured JSON logs for auditability. | 2026-11-05 |
| `ADR-M1-03` | `WBS-M1-T03` | Data Contracts | Strict `amount_minor` BIGINT (float rejected) + ISO-8601 UTC | Zero IEEE 754 precision loss in financial aggregations; unambiguous time semantics. | 2026-11-05 |
| `ADR-M1-04` | `WBS-M1-T04` | State Machine | Monotonic integer rank (1 to 6) with immutable terminal locks | Prevents invalid regressions; allows only COMPLETED -> REFUNDED post-terminal. | 2026-11-05 |
| `ADR-M1-05` | `WBS-M1-T05` | Generator & Faults | Instance-bound `random.Random(seed)` with 7 standardized error classes | 100% byte-identical reproducibility on replay ($G0$ parity) and realistic edge testing. | 2026-11-05 |
| `ADR-M1-06` | `WBS-M1-T07` | Kafka Producer | Confluent-Kafka with snappy compression + resilient offline dry-run | Adapts seamlessly to local hardware limits without compromising production Kafka semantics. | 2026-11-05 |
