# Failure Drill & Chaos Recovery Report

> **Drill ID:** `[e.g., DRILL-M3-C07]`  
> **Target Scenario:** `[e.g., 15-Minute Streaming Outage & Backlog Catch-up]`  
> **Component Under Test:** `[Kafka / Spark Structured Streaming / Delta Lake]`  
> **Status:** `[PASS / FAIL / PARTIAL]`

---

## 1. Drill Hypothesis & Injection Setup

- **Failure Injected:** `[e.g., Kill Spark streaming driver container for 15 minutes while Kafka producer continues at 50 events/sec]`
- **Expected System Behavior:**
  - Zero data loss in Kafka topic (retained by partition log).
  - On restart, Spark picks up exactly from the last committed Delta Lake checkpoint.
  - Downstream `MERGE` handles backlog idempotently without duplicate rows or distorted GMV.
  - Temporary Freshness SLO breach during catch-up, returning to $p95 \le 30\text{s}$ once backlog is cleared.

---

## 2. Timeline & Execution Trace

| Timestamp (UTC+07) | Action / Event | Observed Metric | System State |
| :--- | :--- | :--- | :--- |
| `14:00:00` | Injected failure (Spark killed) | Stream lag = 0 | Outage initiated |
| `14:15:00` | Producer finished burst | Stream lag = 45,000 | Backlog accumulated |
| `14:15:05` | Spark restarted | Checkpoint read | Recovery started |
| `14:18:20` | Lag cleared | Freshness p95 = 18s | Normal operations restored |

---

## 3. Data Integrity & Invariants Audit

- **Bronze Row Count:** `[X]` vs **Total Produced:** `[X]` $\rightarrow$ `[MATCH]`
- **Silver Current Unique Keys:** `[Y]` vs **Total Unique Transactions:** `[Y]` $\rightarrow$ `[MATCH]`
- **Duplicate Monetary Impact:** `0 VND` (Zero deviation detected).
- **Final Verdict:** `[RECOVERY SUCCESSFUL]`
