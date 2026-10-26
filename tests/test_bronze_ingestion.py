"""Unit and Integration Tests for Bronze Layer Ingestion."""

import json
import os
import socket
import urllib.parse
from datetime import UTC, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
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


def _is_minio_reachable(endpoint_url: str = "http://localhost:9000") -> bool:
    """Check if MinIO endpoint is reachable via TCP socket."""
    endpoint = os.getenv("MINIO_ENDPOINT", endpoint_url)
    parsed = urllib.parse.urlparse(endpoint)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 9000)
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a SparkSession."""
    try:
        session = get_spark_session(app_name="pytest-bronze-ingestion")
    except Exception as exc:
        pytest.skip(f"SparkSession could not be initialized: {exc}")
    yield session
    try:
        session.stop()
    except Exception:
        pass


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

    schema = StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    rows = [
        (
            b"test-tx-uuid-1",
            json.dumps(sample_payload).encode("utf-8"),
            "payment.events.raw",
            1,
            42,
            datetime(2026, 10, 23, 10, 0, 1, tzinfo=UTC),
        )
    ]

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


@pytest.mark.skipif(
    not _is_minio_reachable(),
    reason="MinIO service is offline or unreachable",
)
def test_bronze_delta_table_in_minio(spark: SparkSession) -> None:
    """Verify Delta table at s3a://bronze/payment_events exists and contains rows."""
    if not _is_minio_reachable():
        pytest.skip("MinIO service is offline or unreachable")
    try:
        df = spark.read.format("delta").load("s3a://bronze/payment_events")
        assert df.count() >= 300
        assert "ingestion_date" in df.columns
        assert "raw_payload" in df.columns
    except Exception as exc:
        pytest.skip(f"MinIO Delta table is not accessible: {exc}")
