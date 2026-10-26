"""Bronze Layer Ingestion for Payment Events.

Consumes raw events from Kafka topic 'payment.events.raw', extracts metadata,
partitions by ingestion_date, and writes append-only to Delta Lake MinIO S3A.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    date_format,
    get_json_object,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bronze.ingest_payment_events")


def load_config(config_path: str | Path = "config/config.yaml") -> dict[str, Any]:
    """Load system configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        logger.warning("Config %s not found. Using defaults.", config_path)
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_spark_session(
    app_name: str = "bronze-payment-events-ingestion",
    minio_endpoint: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
    config: dict[str, Any] | None = None,
) -> SparkSession:
    """Initialize and configure SparkSession with Delta Lake and MinIO S3A support."""
    cfg = config or {}
    minio_cfg = cfg.get("minio", {})

    endpoint = (
        minio_endpoint
        or os.getenv("MINIO_ENDPOINT")
        or minio_cfg.get("endpoint_url", "http://minio:9000")
    )
    access_key = (
        minio_access_key
        or os.getenv("MINIO_ACCESS_KEY")
        or minio_cfg.get("access_key", "admin")
    )
    secret_key = (
        minio_secret_key
        or os.getenv("MINIO_SECRET_KEY")
        or minio_cfg.get("secret_key", "password123")
    )

    logger.info(
        "Initializing SparkSession: endpoint=%s, access_key=%s", endpoint, access_key
    )

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Read streaming events from Kafka topic."""
    logger.info(
        "Connecting Kafka stream: brokers=%s, topic=%s", bootstrap_servers, topic
    )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_raw_to_bronze(raw_df: DataFrame) -> DataFrame:
    """Transform raw Kafka message DataFrame into the Bronze contract schema.

    Schema:
      - kafka_key (StringType): Key partition ('transaction_id')
      - kafka_topic (StringType): Topic name
      - kafka_partition (IntegerType): Partition index
      - kafka_offset (LongType): Physical offset
      - kafka_timestamp (TimestampType): Kafka ingestion timestamp
      - raw_payload (StringType): Original full JSON message
      - event_id (StringType): Fast extract UUID of event
      - transaction_id (StringType): Correlation ID
      - event_type (StringType): Lifecycle status rank
      - event_time (TimestampType): Business event timestamp (UTC)
      - ingestion_time (TimestampType): Simulator emission timestamp (UTC)
      - ingestion_date (StringType): YYYY-MM-DD for partitioning
      - ingested_at_bronze (TimestampType): Spark bronze write timestamp
    """
    payload_col = col("value").cast(StringType())

    transformed = (
        raw_df.withColumn("kafka_key", col("key").cast(StringType()))
        .withColumn("kafka_topic", col("topic").cast(StringType()))
        .withColumn("kafka_partition", col("partition").cast(IntegerType()))
        .withColumn("kafka_offset", col("offset").cast(LongType()))
        .withColumn("kafka_timestamp", col("timestamp").cast(TimestampType()))
        .withColumn("raw_payload", payload_col)
        .withColumn("event_id", get_json_object(payload_col, "$.event_id"))
        .withColumn("transaction_id", get_json_object(payload_col, "$.transaction_id"))
        .withColumn("event_type", get_json_object(payload_col, "$.event_type"))
        .withColumn(
            "event_time", to_timestamp(get_json_object(payload_col, "$.event_time"))
        )
        .withColumn(
            "ingestion_time",
            to_timestamp(get_json_object(payload_col, "$.ingestion_time")),
        )
        .withColumn(
            "ingestion_date",
            date_format(
                coalesce(
                    to_timestamp(get_json_object(payload_col, "$.ingestion_time")),
                    col("timestamp"),
                ),
                "yyyy-MM-dd",
            ),
        )
        .withColumn("ingested_at_bronze", current_timestamp())
        .select(
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
        )
    )
    return transformed


def write_bronze_stream(
    df: DataFrame,
    checkpoint_location: str,
    target_location: str,
    available_now: bool = True,
    processing_time: str | None = None,
) -> Any:
    """Write stream to Delta Lake partitioned by ingestion_date."""
    logger.info(
        "Writing stream to Delta Lake: target=%s, checkpoint=%s",
        target_location,
        checkpoint_location,
    )

    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("checkpointLocation", checkpoint_location)
    )

    if available_now:
        writer = writer.trigger(availableNow=True)
    elif processing_time:
        writer = writer.trigger(processingTime=processing_time)

    query = writer.start(target_location)
    return query


def main() -> None:
    """CLI Entrypoint for Bronze Payment Events Ingestion."""
    parser = argparse.ArgumentParser(
        description="Bronze Payment Events Spark Structured Streaming"
    )
    parser.add_argument(
        "--config", default="config/config.yaml", help="Path to config.yaml"
    )
    parser.add_argument(
        "--kafka-bootstrap", default=None, help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--kafka-topic", default="payment.events.raw", help="Kafka topic"
    )
    parser.add_argument("--minio-endpoint", default=None, help="MinIO S3 endpoint URL")
    parser.add_argument(
        "--target-table",
        default="s3a://bronze/payment_events",
        help="Delta Lake target location",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="s3a://checkpoints/bronze_payment_events",
        help="Streaming checkpoint directory",
    )
    parser.add_argument(
        "--trigger-mode",
        choices=["availableNow", "continuous"],
        default="availableNow",
        help="Streaming trigger mode",
    )
    parser.add_argument(
        "--processing-time",
        default="5 seconds",
        help="Processing time interval for continuous mode",
    )

    args = parser.parse_args()

    config = load_config(args.config)
    kafka_cfg = config.get("kafka", {})

    bootstrap_servers = (
        args.kafka_bootstrap
        or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or kafka_cfg.get("bootstrap_servers", "kafka:29092")
    )
    topic = args.kafka_topic or kafka_cfg.get(
        "topic_payment_events", "payment.events.raw"
    )

    spark = get_spark_session(
        app_name="bronze-payment-events-ingest",
        minio_endpoint=args.minio_endpoint,
        config=config,
    )

    try:
        raw_stream = read_kafka_stream(
            spark, bootstrap_servers=bootstrap_servers, topic=topic
        )
        bronze_df = transform_raw_to_bronze(raw_stream)

        is_available_now = args.trigger_mode == "availableNow"
        query = write_bronze_stream(
            df=bronze_df,
            checkpoint_location=args.checkpoint_location,
            target_location=args.target_table,
            available_now=is_available_now,
            processing_time=args.processing_time if not is_available_now else None,
        )

        logger.info(
            "Stream started. Awaiting termination (ID=%s, RunID=%s)...",
            query.id,
            query.runId,
        )
        query.awaitTermination()
        logger.info("Stream finished processing batch successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
