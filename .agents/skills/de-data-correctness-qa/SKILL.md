---
name: DE Data Correctness QA
description: Validate data semantics, financial invariants, lakehouse contracts, 3-Way Reconciliation matching logic, and automated evaluation against Simulator Ground Truth G0 (Precision, Recall, F1).
---

# DE Data Correctness QA

The **DE Data Correctness QA** skill specializes in deep validation of data behavior, financial invariants, and analytical calculations across Bronze, Silver, and Gold Medallion layers.

## 1. When to Invoke

Invoke this skill whenever:
- A new data model, schema definition, or contract is created or updated.
- A transformation job (PySpark `foreachBatch`, dbt model) is implemented.
- Verifying 3-Way Financial Reconciliation algorithms against Break Types.
- Benchmarking Data Quality filtering or Reconciliation accuracy against the deterministic Ground Truth dataset (`G0`).

## 2. Core Data Dimensions to Audit

### 2.1. Financial Data Invariants
- **Integer Currency Representation:**
  - Verify all monetary columns (`amount_minor`, `fee_minor`, `net_amount_minor`, `balance_minor`) are stored and manipulated as `BIGINT` integers.
  - Assert that currency conversions or fee calculations use integer math or standard rounding methods (e.g., Banker's rounding) before persisting.
- **Ledger Invariant Verification:**
  - Confirm every ledger debit has a corresponding credit or follows the balanced double-entry equation.
  - Verify no SQL `UPDATE` or `DELETE` statements exist in ledger pipelines.

### 2.2. Lakehouse Medallion Schema & Grain Verification
- **Bronze (Raw Append-Only):**
  - Verify Kafka raw JSON payload is ingested without truncation or in-flight schema mutation.
  - Confirm ingestion timestamp and partition metadata are accurately attached.
- **Silver Dual-Table Contract:**
  - `silver_payment_events_history`: Grain is 1 row per valid status transition event. Verify event deduplication using `event_id` and 2-minute watermark.
  - `silver_transactions_current`: Grain is exactly 1 row per `transaction_id`. Verify pre-aggregation collapsed state matches the highest Monotonic State Rank.
  - Verify routing to exception tables:
    - `silver_quarantine_events`: Schema failures, negative amounts, invalid enums.
    - `silver_late_events_dlq`: Events older than 2 minutes event-time watermark.
    - `silver_lifecycle_exceptions`: Conflicting terminal events (e.g., `COMPLETED` followed by `FAILED`).
- **Gold Marts:**
  - `gold_rt_channel_metrics`: Verify 1-minute and 5-minute tumbling/sliding window aggregations.
  - `gold_eod_reconciliation_details`: Grain is 1 row per reconciled transaction reference.

### 2.3. 3-Way Reconciliation Matching Verification
- **Level 1 (Exact Match):** `transaction_id` matches across Ledger, Partner CSV, and Bank Statement; amounts match exactly.
- **Level 2 (Composite Match):** Fallback matching using composite key (`amount_minor` + `channel` + `settlement_date` +/- 1 day time window).
- **Level 3 (Break Classification):** Verify accurate categorization into the 7 standard Break Types:
  1. `MATCHED`: Complete agreement across all available sources.
  2. `MISSING_IN_PARTNER`: Present in Ledger and Bank, absent in Partner Clearing.
  3. `MISSING_IN_BANK`: Present in Ledger and Partner, absent in Bank Statement.
  4. `ORPHAN_EXTERNAL`: Present in Partner or Bank, but no matching record in internal Ledger.
  5. `DUPLICATE`: Multiple external records claiming the same internal transaction.
  6. `AMOUNT_MISMATCH`: Transaction IDs match, but `amount_minor` differs across sources.
  7. `TIMING_CUTOFF_MISMATCH`: Transaction completed near 23:59:59 cutoff and settled in different business dates.

### 2.4. Ground Truth G0 Automated Benchmark (Precision & Recall)
The Simulator generates a Ground Truth file (`data/ground_truth/G0_events_seed_{seed}.jsonl`).
Use this dataset to compute objective statistical metrics for the pipeline:

$$\text{Precision} = \frac{TP}{TP + FP}$$

$$\text{Recall} = \frac{TP}{TP + FN}$$

$$\text{F1-Score} = 2 \times \frac{\text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$$

- **DQ Quarantine Evaluation:**
  - $TP$: Poison records injected by simulator correctly routed to `silver_quarantine_events`.
  - $FP$: Valid events mistakenly rejected into quarantine.
  - $FN$: Poison records that slipped into `silver_payment_events_history`.
- **Reconciliation Break Evaluation:**
  - $TP$: Intentional discrepancies (amount mismatch, orphan, missing) correctly identified in `gold_eod_reconciliation_details`.
  - $FP$: Clean transactions erroneously flagged with a break code.
  - $FN$: Unmatched or corrupted transactions marked as `MATCHED`.

## 3. Standard Verification Commands

```bash
# Run unit tests on models & schemas
pytest tests/unit/test_models.py -v

# Run simulator deterministic output check
python -m src.simulator.main --seed 20260930 --count 1000 --out /tmp/g0_test.jsonl

# Run dbt data tests (when dbt is installed)
dbt test --select tag:data_quality

# Run G0 Precision/Recall evaluation script
python -m src.qa.evaluate_g0 --ground-truth data/ground_truth/G0_events_seed_20260930.jsonl
```

## 4. Output Contract

Deliver evaluations using the template in `.agents/templates/g0-evaluation-report.md`.
Report any precision or recall drops below 98% as **HIGH** severity findings.
