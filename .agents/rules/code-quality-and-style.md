# Code Quality, Formatting & Implementation Standards

> **Scope:** All Python, PySpark, SQL, dbt, and Docker code generated or maintained in `digital-payments-lakehouse`.  
> **Target:** AI Implementers & Human Engineers.

---

## 1. Python & Pydantic v2 Standards

1. **Typing & Strict Validation:**
   - All models must use **Pydantic v2** (`BaseModel`, `Field`, `model_validator`, `field_validator`).
   - Strict type hints on all function signatures (`def process_batch(df: DataFrame, batch_id: int) -> BatchMetrics:`).
   - Use `typing.Optional`, `typing.List`, `typing.Dict`, `typing.Literal` explicitly.

2. **Error Handling & Logging:**
   - Use standard `logging` with structured JSON or clean timestamped outputs (`[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s`).
   - Never use bare `except:` clauses; always catch specific exceptions (`ValidationError`, `AnalysisException`, `KafkaException`).
   - Log critical operational metrics (events processed, dropped, quarantined, circuit breaker trip conditions).

3. **Code Style & Linting:**
   - Adhere to PEP 8.
   - Meaningful variable names over abbreviated single-letter variables (`transaction_id` over `tid`, `event_timestamp` over `ts`).

---

## 2. PySpark & Delta Lake Implementation Idioms

1. **Schema Definition:**
   - Always define explicit `StructType` schemas for reading JSON/Kafka payloads. Never rely on runtime schema inference (`inferSchema: false`).
   - Avoid creating unbounded Cartesian joins; enforce explicit broadcast hints only when lookup dimensions are verified $< 10\text{MB}$.

2. **Streaming ForeachBatch & MERGE Best Practices:**
   - In `foreachBatch(upsert_to_delta)`, **always collapse/pre-aggregate multi-row updates** for the same `transaction_id` by `monotonic_state_rank DESC` before executing the Delta Lake `MERGE`.
   - *Rationale:* Delta Lake throws `AnalysisException: Cannot perform Merge as multiple source rows matched the same target row` if duplicate keys exist in the source micro-batch.

3. **Checkpoint & Partitioning Conventions:**
   - Checkpoints must reside on S3/MinIO under dedicated folders: `s3a://checkpoints/{pipeline_name}/`.
   - Bronze tables must be partitioned by ingestion date (`date(ingestion_time)`).
   - Avoid high-cardinality partition keys (e.g., never partition by `transaction_id` or `user_id`).

---

## 3. SQL & dbt Standards

1. **Keyword Capitalization & Formatting:**
   - All SQL keywords must be **UPPERCASE** (`SELECT`, `FROM`, `WHERE`, `LEFT JOIN`, `GROUP BY`, `OVER`, `PARTITION BY`).
   - Column names and table aliases must be `snake_case`.

2. **dbt Modeling Layers:**
   - **Staging (`stg_`):** 1-to-1 view or ephemeral mapping over Delta tables. Rename columns, cast types, parse timestamps.
   - **Intermediate (`int_`):** Multi-source joins, state collapse, surrogate key hashing.
   - **Marts / Gold (`fct_`, `dim_`):** Star Schema, Business KPIs, 3-Way Reconciliation results.

3. **Idempotency & Versioning:**
   - All reconciliation models must parameterize `business_date` and `run_version`.
   - dbt tests must cover `unique`, `not_null`, `accepted_values`, and relational integrity.

---

## 4. Docker & Infrastructure Conventions

1. **Deterministic Naming:**
   - Container names prefixed with `dp-` (e.g., `dp-kafka`, `dp-minio`, `dp-kafka-ui`).
2. **Healthchecks:**
   - Every dependent service must declare a valid `healthcheck` block. Downstream services must use `condition: service_healthy`.
3. **Environment Isolation:**
   - Sensitive credentials and port configurations must reference `.env` with sane defaults.
