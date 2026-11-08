---
name: Lakehouse Streaming Engine
description: Domain skill for Apache Kafka (KRaft), PySpark Structured Streaming, Delta Lake Medallion architecture, event-time watermarking, and streaming failure recovery.
---

# Lakehouse Streaming Engine

This domain skill governs real-time streaming operations, Kafka ingestion pipelines, and stateful Delta Lake processing in `digital-payments-lakehouse`.

---

## 1. Core Architecture Responsibilities

1. **Kafka KRaft Dual-Listener Ingestion:**
   - Broker configuration (`dp-kafka`), dual listeners (`INTERNAL://kafka:29092`, `EXTERNAL://localhost:9092`).
   - Topic partitioning (`payment.events.raw`, 3 partitions) and consumer group offset management.

2. **PySpark Structured Streaming & Watermarking:**
   - Unified micro-batch engine reading Kafka JSON streams.
   - Event-time watermarking ($2\text{ minutes}$ delay allowance).
   - In-stream deduplication on `(event_id, event_time)`.
   - Late events beyond watermark routing to `silver_late_events_dlq`.

3. **Silver Dual-Table Architecture:**
   - `silver_payment_events_history`: Append-only, time-series progression.
   - `silver_transactions_current`: 1 row per transaction, pre-aggregated state MERGE, dynamic timer stuck tracking (`operational_status = STUCK` after $5\text{ minutes}$).

4. **Data Quality & Circuit Breaker:**
   - Pydantic/PySpark validation rules.
   - Quarantine routing for poison records (`silver_quarantine_events`).
   - 5% micro-batch error abort trigger.

---

## 2. References

- [Streaming Failure Modes & Recovery](./resources/streaming-failure-modes.md)
