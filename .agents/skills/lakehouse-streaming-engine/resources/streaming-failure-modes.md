# Streaming Failure Modes & Recovery Catalog

> **Scope:** High-concurrency, stateful streaming hazards and resolutions.

---

## 1. Multiple Source Rows Matched in Delta MERGE
- **Symptom:** `AnalysisException: Cannot perform Merge as multiple source rows matched the same target row`.
- **Root Cause:** A single micro-batch contains multiple events for the same `transaction_id` (e.g. `AUTHORIZED` and `COMPLETED` occurring within the same 5-second trigger).
- **Resolution:** Group/collapse micro-batch by `transaction_id` and order by `monotonic_state_rank DESC` before invoking `target.merge()`.

---

## 2. Watermark State Store Memory Leak
- **Symptom:** Executor JVM heap fills up (`java.lang.OutOfMemoryError: Java heap space`).
- **Root Cause:** Watermark not applied to timestamp column, or unbounded state aggregation without cleanup deadline.
- **Resolution:** Explicitly declare `withWatermark("event_time", "2 minutes")` and use `dropDuplicates(["event_id", "event_time"])`.

---

## 3. Small Files Explosion in Streaming Sinks
- **Symptom:** Thousands of < 500KB Parquet files in MinIO/S3, query degradation on Silver tables.
- **Root Cause:** High-frequency micro-batch triggers (e.g., `ProcessingTime('1 second')`) writing tiny partitions.
- **Resolution:** Adjust trigger interval to `10 seconds`, enable Delta auto-compaction (`spark.databricks.delta.autoCompact.enabled = true`), and schedule periodic `OPTIMIZE` runs.

---

## 4. Kafka Rebalance Lag Spike
- **Symptom:** Consumer group undergoes frequent rebalancing; Spark streaming task timeouts.
- **Root Cause:** `max.poll.interval.ms` exceeded because micro-batch processing time was slower than poll timeout.
- **Resolution:** Increase `max.poll.interval.ms` (e.g., 300,000 ms) and limit `maxOffsetsPerTrigger` to cap micro-batch size.
