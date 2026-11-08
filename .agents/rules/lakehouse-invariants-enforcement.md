# Lakehouse Technical Invariants Enforcement

> **Authority:** Non-negotiable technical contracts declared in `AGENTS.md` and `docs/DE_Project2_Master_Handoff.md`.  
> **Enforcement Scope:** All AI agents generating, reviewing, or debugging code.  
> **Severity:** Violations of Section 1 are **BLOCKER**; violations of Section 2 are **HIGH**.

---

## 1. Blocker Invariants (Direct Rejection on Violation)

### 1.1. Financial Data Type (`amount_minor`)
- **Invariant:** All monetary amounts MUST be represented as integer minor units (`amount_minor` of type `BIGINT` or Python `int`, e.g., VND, cents).
- **Strict Prohibition:** **NEVER use `FLOAT`, `DOUBLE`, or unconstrained decimal types** for monetary calculations or storage.
- **Rationale:** Floating-point representations introduce IEEE 754 precision inaccuracies, leading to rounding drift and reconciliation failures.

### 1.2. Internal Wallet Ledger (`S2`) Append-Only Contract
- **Invariant:** The Wallet / Internal Ledger table (`silver_wallet_ledger`) is **strictly Append-Only**.
- **Strict Prohibition:** **Physical `UPDATE` and `DELETE` operations are strictly forbidden.**
- **Correction Mechanism:** Any financial correction, dispute resolution, or error reversal MUST be posted as a new compensating record with `entry_type = 'REVERSAL'` or `'ADJUSTMENT'` referencing the original `transaction_id`.

### 1.3. Monotonic State Progression & Terminal Lock
- **Invariant:** Payment transaction lifecycle strictly follows the **Monotonic State Rank (1 -> 6)**:
  `INITIATED (1)` -> `PAYMENT_METHOD_SELECTED (2)` -> `AUTHORIZED (3)` -> `PROCESSING (4)` -> Terminal States (`COMPLETED (5)`, `FAILED (5)`, `CANCELLED (5)`, `EXPIRED (5)`). From `COMPLETED` may advance to `REFUNDED (6)`.
- **Terminal Lock:** Once a transaction reaches a Terminal State (Rank 5 or 6), lower-rank events arriving out of order are recorded in history but **CANNOT regress the current state**.
- **Terminal Conflict Handling:** If conflicting terminal events arrive (e.g., `COMPLETED` followed by `FAILED`), the current state remains locked, and the conflict MUST be routed to `silver_lifecycle_exceptions`.

### 1.4. Micro-batch Circuit Breaker
- **Invariant:** If the validation error rate within a single Spark streaming micro-batch exceeds **5%** (given batch size $\ge 50$), the **Circuit Breaker MUST trip** and abort the streaming query immediately.
- **Rationale:** Protects downstream Silver/Gold tables from mass data corruption caused by upstream schema changes or malicious poison injection.

---

## 2. High-Severity Invariants (Mandatory Compliance)

### 2.1. Time Semantics Separation
- **`event_time`:** Timestamp when the business event occurred on client/gateway. All business analytics, SLA tracking, and partner degradation metrics MUST derive from `event_time`.
- **`ingestion_time`:** Timestamp when the event arrived at Kafka/Lakehouse.
- **Strict Rule:** Never overwrite `event_time` with `ingestion_time` or system current timestamp.

### 2.2. Timezone Normalization
- **Storage Layer (Delta Lake):** Strictly normalized to **UTC** (ISO-8601 string or UTC timestamp).
- **Business Cutoff & Presentation:** EOD Cutoff (`23:59:59`) and BI dashboards operate in **`Asia/Ho_Chi_Minh` (UTC+07:00)**.
- **Cutoff Testing:** Edge-case tests must simulate transactions spanning the UTC vs UTC+07 boundary to guarantee zero transactions leak into the wrong business date.

### 2.3. Streaming Watermark & Late Event DLQ
- **Streaming Watermark:** Set to **2 minutes** on `event_time`.
- **Within Watermark ($\le 2\text{m}$):** Deduplicated and processed in real time.
- **Beyond Watermark ($> 2\text{m}$):** Preserved in Bronze, routed to `silver_late_events_dlq` for periodic batch reconciliation/correction. Never drop late events silently!

### 2.4. Operational Stuck Detection
- **Dynamic Timer:** If a transaction remains in `PROCESSING` for $> 5\text{ minutes}$ without a valid progress event:
  - `operational_status` is updated to `STUCK`.
  - `lifecycle_state` remains `PROCESSING`.
  - `was_ever_stuck` flag is set to `TRUE` and permanently preserved for operational auditing.

### 2.5. 3-Way Reconciliation Standards
- **Completeness Gate:** Airflow reconciliation DAG must verify the presence and completeness of all 3 sources (Ledger S2, Partner CSV S4, Bank Statement S4). Missing files set run status to `INCOMPLETE`.
- **7 Break Categories:** Any mismatch must be classified into one of the 7 standard categories:
  `MATCHED`, `MISSING_IN_PARTNER`, `MISSING_IN_BANK`, `ORPHAN_EXTERNAL`, `DUPLICATE`, `AMOUNT_MISMATCH`, `TIMING_CUTOFF_MISMATCH`.
- **Versioned Replay:** Re-runs and backfills must increment `run_version` (`v1`, `v2`, ...) without destroying previous run audit logs.

### 2.6. Deterministic Simulator Ground Truth ($G0$)
- **Fixed Seed:** Default seed is `20260930`. All fault injections must be deterministic and fully reproducible.
- **Benchmark:** Data Quality validation and 3-Way Reconciliation accuracy MUST be verified against `G0` (Precision $\ge 99\%$, Recall $\ge 99\%$).
