# Workflow: Failure Drill & Chaos Recovery

> **Workflow Name:** `failure-drill-and-recovery`  
> **Invocation Trigger:** Validating system resilience during Milestone 3 (Stateful Streaming) and Milestone 4 (Batch Reconciliation).  
> **Target Cases:** Cases `C03`, `C04`, `C06`, `C07` from `docs/DE_Project2_Master_Handoff.md`.

---

## 1. Objective

To actively inject chaos, hardware crashes, network partitions, and data corruptions into the running local Docker stack, proving beyond doubt that the Lakehouse platform recovers gracefully, preserves data idempotency, and guarantees financial invariants.

---

## 2. Standard Failure Injection Drills

### Drill 1: The 15-Minute Streaming Outage Catch-up (`C07`)
1. **Setup:** Kafka simulator produces continuous traffic at 20 txns/sec.
2. **Action:** Execute `docker stop dp-spark-streaming` (or kill the streaming python process). Keep Kafka running for 15 minutes (or simulate 15m backlog using high burst).
3. **Recovery:** Restart Spark streaming job.
4. **Verification:**
   - Confirm checkpoint resumes from the exact committed micro-batch offset.
   - Confirm zero duplicate records in `silver_transactions_current`.
   - Record time taken to clear backlog and verify Freshness SLO recovery ($p95 \le 30\text{s}$).

### Drill 2: Circuit Breaker 5% Abort Drill (`C06`)
1. **Setup:** Inject a batch containing 6% poison records (negative `amount_minor` or invalid enum) with batch size $\ge 50$.
2. **Action:** Spark streaming processes micro-batch.
3. **Verification:**
   - Confirm Circuit Breaker trips with exit code / exception log.
   - Confirm micro-batch is aborted; zero corrupted rows reach downstream Silver/Gold tables.
   - Record alert event in log.

### Drill 3: Out-of-Order Terminal Conflict (`C14`, `C45`)
1. **Setup:** Inject a transaction where `COMPLETED` arrives first, followed by an out-of-order `FAILED` event.
2. **Action:** Micro-batch pre-aggregated MERGE executes.
3. **Verification:**
   - Confirm `silver_transactions_current` remains locked at `COMPLETED`.
   - Confirm the conflicting `FAILED` event is successfully routed to `silver_lifecycle_exceptions`.
   - Confirm accounting ledger remains balanced.

---

## 3. Reporting

Consolidate results into `.agents/templates/failure-drill-report.md` with verifiable terminal logs.
