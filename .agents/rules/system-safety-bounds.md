# System Safety Bounds & Execution Guardrails

> **Target:** AI Coding Agents operating within `digital-payments-lakehouse`  
> **Source:** Adapted from `vudovn/ag-kit` PreToolUse safety hooks & production lakehouse safeguards.  
> **Authority:** Mandated technical bounds. Zero exceptions.

---

## 1. Objective

To prevent autonomous coding agents from inadvertently destroying persistent lakehouse state, corrupting Kafka topic offsets, wiping MinIO storage buckets, or committing sensitive configuration files during automated test runs.

---

## 2. Hard Destruction Bounds (Forbidden Commands)

Agents must **NEVER** generate, execute, or recommend the following destructive commands:

1. **Volume Wiping & Docker Teardown with Volumes:**
   - **FORBIDDEN:** `docker compose down -v` or `docker compose down --volumes`.
   - **ALLOWED:** `docker compose down` (preserves named volumes `kafka_data`, `minio_data`).
   - *Rationale:* Wiping volumes permanently destroys Delta Lake history, MinIO bronze checkpoints, and Kafka message retention, invalidating long-running test evidence.

2. **Recursive Host-Level Deletions:**
   - **FORBIDDEN:** `rm -rf /`, `rm -rf ~`, `rm -rf C:\`, `Remove-Item -Recurse -Force C:\*` or any wildcard deletion above workspace root `C:\DE-project\digital-payments-lakehouse`.
   - **ALLOWED:** Removing specific ephemeral build artifacts inside `.pytest_cache/`, `__pycache__/`, or `target/`.

3. **Unbounded Storage Bucket Purges:**
   - **FORBIDDEN:** Unconditional `mc rb --force` or `aws s3 rb --force` on root buckets (`bronze`, `silver`, `gold`, `checkpoints`).
   - **ALLOWED:** Recreating specific test partition prefixes inside sandbox directories during automated test teardown.

4. **Git Destructive Actions:**
   - **FORBIDDEN:** `git reset --hard` (unless explicitly instructed by the user), `git clean -fdx`, `git push --force` to main/trunk.

---

## 3. Storage & State Safety Rules

1. **Delta Lake ACID Directory Protection:**
   - Agents must never manually delete or edit JSON files inside `_delta_log/` directories.
   - Table schema evolutions and partition removals must be performed exclusively via Delta Lake APIs (`spark.read.format("delta")`, `dbt run`, or `VACUUM` with retention safety checks).

2. **Streaming Checkpoint Isolation:**
   - Every streaming query must use a unique, dedicated checkpoint path:
     `s3a://checkpoints/{query_name}/`.
   - Never share checkpoint directories across queries or modify checkpoint metadata manually.

3. **Kafka Topic Protection:**
   - Topics `payment.events.raw` and `wallet.ledger.cdc` must never be deleted during regular testing.
   - Resetting offsets must use explicit consumer group reset utilities (`kafka-consumer-groups.sh --reset-offsets`) with exact boundaries, never topic deletion.

---

## 4. Resource Consumption Safeguards

1. **Local Memory Isolation:**
   - Local PySpark jobs running in Docker or host must restrict driver and executor memory (`spark.driver.memory=2g`, `spark.executor.memory=2g`) to avoid starving the host machine (32 GB RAM total).
   - Never run unconstrained Spark benchmarks with more than 5 parallel concurrent jobs.

2. **Docker Network Confinement:**
   - All inter-service communication must strictly use internal Docker DNS (`kafka:29092`, `minio:9000`).
   - External listeners (`localhost:9092`, `localhost:9000`) are strictly reserved for host-level verification scripts and human developer access.
