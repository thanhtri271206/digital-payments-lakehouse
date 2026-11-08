# Production Pydantic & PySpark Implementation Patterns

> **Scope:** Reusable coding idioms for `digital-payments-lakehouse`.

---

## 1. Pydantic v2 Invariant Validation Pattern

```python
from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, field_validator

class PaymentEvent(BaseModel):
    event_id: str
    transaction_id: str
    event_type: Literal[
        "INITIATED", "PAYMENT_METHOD_SELECTED", "AUTHORIZED",
        "PROCESSING", "COMPLETED", "FAILED", "CANCELLED", "EXPIRED", "REFUNDED"
    ]
    event_time: str
    ingestion_time: Optional[str] = None
    user_id: str
    merchant_id: str
    payment_channel: Literal["BANK_A", "GATEWAY_A", "GATEWAY_B"]
    payment_method: Literal["BANK_TRANSFER", "CARD", "WALLET_BALANCE", "QR"]
    amount_minor: int = Field(gt=0, description="Amount in integer minor unit (e.g. VND). Must be > 0.")
    currency: Literal["VND"] = "VND"
    status_reason: Optional[str] = None
    producer_version: str = "v1.0"

    @field_validator("amount_minor")
    @classmethod
    def validate_amount_minor(cls, v: int) -> int:
        if not isinstance(v, int):
            raise ValueError("amount_minor must strictly be an integer (BIGINT), never float")
        return v
```

---

## 2. PySpark Structured Streaming ForeachBatch Pre-Aggregated MERGE

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def upsert_silver_current(batch_df: DataFrame, batch_id: int, delta_target_path: str):
    """
    ForeachBatch micro-batch sink to silver_transactions_current.
    Pre-aggregates by transaction_id using Monotonic State Rank to prevent
    Delta Lake multiple-source-row match exceptions.
    """
    if batch_df.isEmpty():
        return

    # 1. Map state to Monotonic Rank (1 -> 6)
    rank_mapping = (
        F.when(F.col("event_type") == "INITIATED", 1)
        .when(F.col("event_type") == "PAYMENT_METHOD_SELECTED", 2)
        .when(F.col("event_type") == "AUTHORIZED", 3)
        .when(F.col("event_type") == "PROCESSING", 4)
        .when(F.col("event_type").isin("COMPLETED", "FAILED", "CANCELLED", "EXPIRED"), 5)
        .when(F.col("event_type") == "REFUNDED", 6)
        .otherwise(0)
    )

    ranked_df = batch_df.withColumn("state_rank", rank_mapping)

    # 2. Window collapse within micro-batch: pick highest rank, latest event_time
    window_spec = Window.partitionBy("transaction_id").orderBy(
        F.col("state_rank").desc(),
        F.col("event_time").desc()
    )

    deduped_batch = (
        ranked_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # 3. Delta Lake Idempotent MERGE with Monotonic Rank Guard
    target_table = DeltaTable.forPath(batch_df.sparkSession, delta_target_path)

    (
        target_table.alias("target")
        .merge(
            deduped_batch.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdate(
            condition="source.state_rank > target.current_state_rank",
            set={
                "current_lifecycle_state": "source.event_type",
                "current_state_rank": "source.state_rank",
                "last_progress_event_time": "source.event_time",
                "updated_at": F.current_timestamp()
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
```
