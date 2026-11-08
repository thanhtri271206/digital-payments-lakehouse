---
name: Financial Reconciliation dbt
description: Domain skill for batch EOD 3-Way Financial Reconciliation, dbt modeling (staging, intermediate, marts), 7 break type classifications, Airflow completeness gates, and Ground Truth G0 benchmarking.
---

# Financial Reconciliation dbt

This domain skill governs batch EOD financial reconciliation between Internal Wallet Ledger (`S2`), Partner Settlement CSVs (`S4`), and Bank Statements (`S4`) using `dbt-spark` and `dbt-databricks`.

---

## 1. Core Architecture Responsibilities

1. **3-Way Matching Hierarchy:**
   - **Level 1 (Exact Match):** Direct join on primary transaction keys (`transaction_id` = `settlement_id` = `bank_reference`).
   - **Level 2 (Composite Match):** Multi-attribute matching on `(amount_minor, business_date, payment_channel, user_id)`.
   - **Level 3 (Break Classification):** Unmatched records mapped to the 7 standard break categories.

2. **The 7 Standard Break Types:**
   - `MATCHED`: Balanced across all 3 sources.
   - `MISSING_IN_PARTNER`: Present in Ledger and Bank, missing in Partner CSV.
   - `MISSING_IN_BANK`: Present in Ledger and Partner, missing in Bank statement.
   - `ORPHAN_EXTERNAL`: Present in Partner or Bank, absent from Internal Ledger.
   - `DUPLICATE`: Multiple external claims for the same internal transaction.
   - `AMOUNT_MISMATCH`: Key matched but `amount_minor` differs across sources.
   - `TIMING_CUTOFF_MISMATCH`: Event occurred before 23:59:59 UTC+07 but settled next day.

3. **Versioned Replay & Grace Period SLA:**
   - Completeness Gate: If any file missing at 01:00 AM T+1, mark run `INCOMPLETE`.
   - Grace Period until 06:00 AM T+1 for delayed files.
   - Backfill creates new `run_version` (`v1`, `v2`, ...). Downstream consumption views read `MAX(run_version)`.

---

## 2. References

- [Reconciliation Break Patterns](./resources/reconciliation-break-patterns.md)
