# Workflow: Milestone Quality Gate Sign-Off

> **Workflow Name:** `milestone-gate-signoff`  
> **Invocation Trigger:** When all tasks of a Milestone (M1 to M5) are completed, prior to advancing to the next milestone.  
> **Mandatory Input:** Task completion evidence logs, test suite output, and Ground Truth ($G0$) evaluation report.

---

## 1. Quality Gate Matrix

| Gate | Target Deliverable | Critical Gate Check | Exit Sign-Off Criteria |
| :---: | :--- | :--- | :--- |
| **Gate 1** | Foundation Infra & Deterministic Simulator | Kafka KRaft dual-listener + MinIO buckets + Pydantic v2 Models + Seed `20260930` reproducibility | Zero docker crash, $G0$ exported, 100% unit tests pass |
| **Gate 2** | Walking Skeleton (Stream to Bronze) | Spark streaming reading Kafka to MinIO Delta Bronze + Checkpoint recovery | Zero dropped messages, ingestion partitioning verified, idempotent rerun |
| **Gate 3** | Stateful Streaming, Silver Dual-Table & DQ | ForeachBatch pre-aggregated merge, Watermark 2m, Stuck timer 5m, Circuit Breaker 5% | Zero multiple-match errors, DQ Precision/Recall $\ge 99\%$, Freshness $p95 \le 30\text{s}$ |
| **Gate 4** | Batch EOD 3-Way Reconciliation & Cloud | Airflow Completeness Gate, dbt 3-Way models (7 breaks), Databricks Cloud showcase | $G0$ reconciliation precision $\ge 99\%$, versioned replay verified |
| **Gate 5** | Serving Dashboards, Scale & Portfolio | Metabase Ops & Finance Dashboards, 5M scale benchmark, English ADRs & Portfolio README | Benchmark evidence logged, 0 blocker findings, portfolio ready |

---

## 2. Gate Review Execution Steps

1. **Audit All Task DoD:** Ensure every task in the milestone has an execution evidence log.
2. **Execute Full Test Suite:** Run `pytest tests/ -v` and capture test exit codes.
3. **Audit Invariants:** Check zero violation of Blocker invariants (`BIGINT`, Append-only ledger, Monotonic rank).
4. **Compile Milestone Gate Report:** Populate `.agents/templates/milestone-gate-report.md`.
5. **Human Sign-Off:** Require explicit user confirmation before unlocking the next milestone.
