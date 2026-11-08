# Lakehouse Architectural Trade-Off Rubric

> **Authority:** Framework for evaluating competing engineering designs in `digital-payments-lakehouse`.

---

## Evaluation Axes & Scoring Dimensions

Every architectural decision must be evaluated across 6 primary axes:

### 1. Throughput vs. Latency
- **Sub-second Low Latency:** Continuous processing, streaming RPC (high cost, complex state recovery).
- **Near-Real-Time Micro-batch:** Spark Structured Streaming (p95 <= 30s freshness, high throughput, robust exactly-once semantics).
- **Daily Batch:** dbt EOD reconciliation (T+1 01:00 AM, high compute efficiency, deep join capability).

### 2. Compute vs. Storage Cost
- **Pre-aggregation at Ingestion:** Saves downstream analytical query time at the expense of higher streaming compute overhead.
- **Append-Only History + On-Demand Window Views:** Extremely cheap and fast streaming write (no updates), but analytical queries require window function scans.

### 3. File System & I/O Overhead (Small Files Problem)
- Frequent streaming commits write many tiny Parquet files (< 1MB), causing metadata explosion in Delta Lake.
- Mitigated via: Compaction/OPTIMIZE jobs, larger micro-batch trigger intervals (e.g., 5s–10s), and dual-table separation.

### 4. Operational & Code Complexity
- Monolithic vs. Decoupled services.
- Native SQL (dbt) vs. Complex PySpark UDFs.
- Always prefer simpler, declarative SQL patterns where possible, isolating procedural state logic to dedicated streaming micro-batches.

### 5. Invariants Compliance
- Any option that risks violating `amount_minor` as BIGINT, Monotonic State progression, or Ledger immutability is immediately disqualified.

### 6. Failure Recovery & Blast Radius
- How easily can a crashed job catch up?
- Does recovery require manual operator intervention, or is it 100% idempotent via checkpoint replay?
