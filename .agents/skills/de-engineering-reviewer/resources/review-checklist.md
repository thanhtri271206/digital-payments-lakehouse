# Engineering Adversarial Review Checklist

> **Reference:** Tailored for `digital-payments-lakehouse` adhering to `AGENTS.md` and `docs/DE_Project2_Master_Handoff.md`.

---

## 1. Data Correctness & Financial Invariants

- [ ] **Integer Minor Units:** Are currency amounts strictly represented as `amount_minor: int` (`BIGINT`)? Is any division or percentage calculation carefully guarded against floating point drift?
- [ ] **State Machine Monotonicity:** Are transitions validated against Monotonic State Rank (1: INITIATED -> 2: SELECTED -> 3: AUTHORIZED -> 4: PROCESSING -> 5: TERMINAL -> 6: REFUNDED)?
- [ ] **Terminal Lock & Conflict Resolution:** Is a terminal transaction (`COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`) prevented from being rewritten by another terminal event? Are conflicting terminal events redirected to `silver_lifecycle_exceptions`?
- [ ] **Ledger Immutability:** Does the internal wallet ledger prevent SQL `UPDATE` and `DELETE` operations? Are corrections modeled solely via `ADJUSTMENT` or `REVERSAL`?
- [ ] **Timezone & Cutoff Boundaries:** Are records stored with UTC timestamps? Does the EOD batch cutoff strictly evaluate against `23:59:59 Asia/Ho_Chi_Minh`?
- [ ] **SCD Type 2 Integrity:** For dimension updates (`dim_merchant_scd2`), are `effective_from`, `effective_to`, and `is_current` handled with point-in-time correctness?

---

## 2. Streaming & Micro-Batch Reliability

- [ ] **Watermark Handling:** Is the event-time watermark set to 2 minutes? Are events arriving later than 2 minutes preserved in `silver_late_events_dlq` rather than dropped?
- [ ] **Dynamic Timer & Stuck Transactions:** Does the dynamic timer calculate `deadline = last_progress_event_time + 5 minutes`? If expired, is `operational_status` set to `STUCK` while keeping `lifecycle_state = PROCESSING`?
- [ ] **Unsticking Logic:** Does a late valid terminal event properly unstick a transaction while preserving `was_ever_stuck = TRUE` in history?
- [ ] **Circuit Breaker Enforcement:** If error/poison records exceed 5% of a micro-batch (with batch size >= 50), does the pipeline abort to protect Silver tables?
- [ ] **Pre-Aggregated MERGE:** In `foreachBatch`, are multiple status events for the same `transaction_id` within the same micro-batch collapsed to the highest rank before the Delta `MERGE`?

---

## 3. Batch Reconciliation & dbt Modeling

- [ ] **Completeness Gate:** Does the Airflow EOD reconciliation DAG verify the presence of all 3 sources (Ledger, Partner CSV, Bank CSV) before triggering matching? If a source is missing, is the run marked `INCOMPLETE`?
- [ ] **Grace Period SLA:** Does the system wait until `06:00 AM T+1` before raising a `CRITICAL_SLA_BREACH` alert for missing files?
- [ ] **Multi-Version File Handling:** When a partner sends an amended file (`v2`), does the ingestion layer preserve both files append-only while setting `is_active = TRUE` only on the latest verified version?
- [ ] **3-Way Matching Hierarchy:** Does matching progress systematically: Level 1 (exact ID match) -> Level 2 (composite match on amount, date window, partner) -> Level 3 (break categorization)?
- [ ] **Break Classification:** Are un-reconciled records classified into the 7 standard categories (`MATCHED`, `MISSING_IN_PARTNER`, `MISSING_IN_BANK`, `ORPHAN_EXTERNAL`, `DUPLICATE`, `AMOUNT_MISMATCH`, `TIMING_CUTOFF_MISMATCH`)?
- [ ] **Versioned Backfill / Replay:** When re-running reconciliation for a past date, does the job generate a new incremented `run_version` (`v2`, `v3`) without overwriting historical run records?

---

## 4. Operational & Infrastructure Checks

- [ ] **Local-First Zero-Cost Parity:** Does the component run locally via `docker compose` with zero reliance on paid cloud services?
- [ ] **Deterministic Reproducibility:** Does the simulator or test data generation use an explicit seed (`20260930`)?
- [ ] **Credential Security:** Are secrets, tokens, and MinIO keys externalized through environment variables and excluded from git?
- [ ] **Delta Log & Compaction:** Are Delta table write paths partitioned appropriately without causing small-file proliferation?
- [ ] **Logging & Observability:** Do log statements include contextual attributes (`transaction_id`, `event_id`, `batch_id`, `business_date`) without leaking sensitive PII?
