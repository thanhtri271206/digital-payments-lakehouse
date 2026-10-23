"""Unit and Integration Tests for Bronze Layer Ingestion."""

from datetime import datetime, timezone
import json
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from digital_payments_lakehouse.bronze.ingest_payment_events import (
    get_spark_session,
    transform_raw_to_bronze,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a SparkSession."""
    session = get_spark_session(app_name="pytest-bronze-ingestion")
    yield session
    session.stop()


def test_transform_raw_to_bronze_schema(spark: SparkSession) -> None:
    """Verify transform_raw_to_bronze extracts all 13 contract columns accurately."""
    sample_payload = {
        "event_id": "test-event-uuid-1",
        "transaction_id": "test-tx-uuid-1",
        "event_type": "INITIATED",
        "event_time": "2026-10-23T10:00:00Z",
        "ingestion_time": "2026-10-23T10:00:00.123456Z",
        "user_id": "usr_1001",
        "merchant_id": "mer_101",
        "payment_channel": "BANK_A",
        "payment_method": "BANK_TRANSFER",
        "amount_minor": 100000,
        "currency": "VND",
        "status_reason": None,
        "producer_version": "v1.0",
    }

    schema = StructType([
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    rows = [(
        b"test-tx-uuid-1",
        json.dumps(sample_payload).encode("utf-8"),
        "payment.events.raw",
        1,
        42,
        datetime(2026, 10, 23, 10, 0, 1, tzinfo=timezone.utc),
    )]

    raw_df = spark.createDataFrame(rows, schema=schema)
    bronze_df = transform_raw_to_bronze(raw_df)

    expected_cols = [
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_payload",
        "event_id",
        "transaction_id",
        "event_type",
        "event_time",
        "ingestion_time",
        "ingestion_date",
        "ingested_at_bronze",
    ]
    assert bronze_df.columns == expected_cols

    res = bronze_df.collect()[0]
    assert res["kafka_key"] == "test-tx-uuid-1"
    assert res["kafka_topic"] == "payment.events.raw"
    assert res["kafka_partition"] == 1
    assert res["kafka_offset"] == 42
    assert res["event_id"] == "test-event-uuid-1"
    assert res["transaction_id"] == "test-tx-uuid-1"
    assert res["event_type"] == "INITIATED"
    assert res["ingestion_date"] == "2026-10-23"
    assert res["ingested_at_bronze"] is not None


def test_bronze_delta_table_in_minio(spark: SparkSession) -> None:
    """Verify Delta table at s3a://bronze/payment_events exists and contains rows."""
    df = spark.read.format("delta").load("s3a://bronze/payment_events")
    assert df.count() >= 300
    assert "ingestion_date" in df.columns
    assert "raw_payload" in df.columns
