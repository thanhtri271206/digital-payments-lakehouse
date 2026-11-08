---
name: DE Operational Readiness
description: Assess operational reliability, streaming SLOs, dynamic timer stuck handling, circuit breakers, failure recovery, Airflow SLAs, and local-versus-cloud deployment readiness.
---

# DE Operational Readiness

The **DE Operational Readiness** skill evaluates whether code and pipeline infrastructure can safely operate in production and local Docker environments under normal, burst, and failure conditions.

## 1. When to Invoke

Invoke this skill whenever:
- Modifying streaming jobs (PySpark Structured Streaming), Kafka configurations, or trigger intervals.
- Updating Airflow DAG schedules, sensors, or SLA alert rules.
- Reviewing failure recovery, idempotency, or backfill logic.
- Assessing Milestone gates for end-to-end operational viability.
- Validating the Dual-Track topology (Local Docker vs Databricks Cloud).

## 2. Operational Evaluation Dimensions

### 2.1. Near-Real-Time Streaming SLOs (NFR-RT-01, NFR-RT-02)
- **Freshness SLO:** End-to-end processing delay (`serving_time - event_time`) must satisfy:
  - $\text{p95} \le 30\text{ seconds}$
  - $\text{p99} \le 60\text{ seconds}$
- **Throughput Profile:** Baseline $\approx 5\text{ events/sec}$, burst capacity $\approx 50\text{ events/sec}$.
- **Lateness Tracking:** Distinguish pipeline processing lag (`serving_time - ingestion_time`) from external event arrival delay (`ingestion_time - event_time`).

### 2.2. Dynamic Timer & Stuck Transaction Detection (FR-LC-01..05)
- **Timeout Policy:** If a transaction remains in `PROCESSING` without a valid progress event for $> 300\text{ seconds}$ (5 minutes):
  - Mark `operational_status = STUCK`.
  - Maintain business `lifecycle_state = PROCESSING`.
- **Dynamic Deadline:** Deadline dynamically resets whenever an authorized higher-rank progress event arrives (`deadline = last_progress_event_time + 5 minutes`).
- **Unsticking Audit:** When a late terminal event arrives for a `STUCK` transaction, remove the `STUCK` operational status, advance to terminal state, and set `was_ever_stuck = TRUE` for historical SLA auditing.

### 2.3. Partner Degradation & Hysteresis Alerting (FR-OPS-01..04)
- **Degradation Detection:**
  - Short window: 1 minute ($\ge 20\text{ transactions}$).
  - Long window: 5 minutes ($\ge 100\text{ transactions}$).
  - Trigger conditions: Success Rate $< 90\%$ OR drop from baseline $\ge 8\%$.
- **Hysteresis Recovery (Anti-Flapping):**
  - Only resolve degradation alerts when Success Rate $\ge 95\%$ across **2 consecutive evaluation windows**.

### 2.4. Circuit Breaker & Fault Isolation (NFR-DQ-01)
- **Trip Condition:** In any micro-batch with $\ge 50\text{ records}$, if technical validation failures exceed $5\%$ ($0.05$):
  - Abort micro-batch immediately.
  - Fail the streaming query to prevent cascading corruption of Silver/Gold Delta tables.
  - Generate actionable operational alert with error distribution breakdown.

### 2.5. Crash, Outage & Replay Recovery (NFR-RT-04, NFR-RT-05)
- **At-Least-Once Delivery:** Kafka offsets must only commit after the micro-batch successfully writes to Delta Lake storage and checkpoint log.
- **Idempotent Upsert:** Replaying Kafka partitions must not produce duplicate records in Silver or Gold.
- **Outage Catch-up:** System must successfully catch up after a simulated 15-minute streaming job outage without memory exhaustion or data duplication.

### 2.6. Airflow EOD Orchestration & Grace Period (FR-REC-01..10)
- **Cutoff:** Daily cutoff at `23:59:59 Asia/Ho_Chi_Minh` (UTC+07:00).
- **Completeness Gate:** First run initiated at `01:00 AM T+1`. Must verify arrival of all 3 sources (Ledger CDC, Partner CSV, Bank Statement). If missing, mark `run_status = INCOMPLETE`.
- **Grace Period SLA:** Recheck periodically until `06:00 AM T+1`. If sources remain missing after 06:00 AM, elevate alert to `CRITICAL_SLA_BREACH`.
- **Versioned Backfill:** Reruns must write to a new incremented `run_version` (`v1 -> v2 -> v3`). Consumer views must query `max(run_version)`.

### 2.7. Storage Health & Small Files Mitigation
- Inspect Parquet file counts in Delta tables (`_delta_log`).
- Ensure micro-batch writes do not degrade query latency.
- Verify compaction job (`OPTIMIZE` / `bin-packing`) execution strategy.

## 3. Operational Verification Commands

```bash
# Verify Docker service health & ports
docker compose ps
docker compose logs kafka --tail=50
docker compose logs minio --tail=50

# Check Kafka topic partitions & lag
docker exec -it dp-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group spark-payment-stream

# Validate MinIO bucket accessibility & Delta checkpoints
docker exec -it dp-minio mc ls local/bronze
docker exec -it dp-minio mc ls local/checkpoints
```

## 4. Operational Sign-off Criteria

A deliverable is marked **Launch-Ready** only if:
1. Local Docker one-command execution (`docker compose up -d`) starts cleanly with zero manual hacks.
2. Failure recovery drills succeed (e.g., kill Spark container, resume, verify zero data loss).
3. Circuit breaker trips reliably when error injection exceeds 5%.
4. Streaming freshness metrics meet p95 <= 30s under baseline load (5 events/sec).
