"""Verification script for Bronze Delta Lake Layer.

Inspects the Delta table 's3a://bronze/payment_events', printing schema,
row count, partition layout, and sample records.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bronze.verify_bronze")


def load_config(config_path: str | Path = "config/config.yaml") -> dict[str, Any]:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_spark_session(
    minio_endpoint: str | None = None,
    config: dict[str, Any] | None = None,
) -> SparkSession:
    """Initialize SparkSession for Delta table verification."""
    cfg = config or {}
    minio_cfg = cfg.get("minio", {})

    endpoint = (
        minio_endpoint
        or os.getenv("MINIO_ENDPOINT")
        or minio_cfg.get("endpoint_url", "http://minio:9000")
    )
    access_key = os.getenv("MINIO_ACCESS_KEY") or minio_cfg.get("access_key", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY") or minio_cfg.get("secret_key", "password123")

    packages = [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    spark = (
        SparkSession.builder.appName("verify-bronze-payment-events")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def verify_bronze_table(spark: SparkSession, table_path: str = "s3a://bronze/payment_events") -> None:
    """Verify Delta table exists, display schema, total row count, and sample records."""
    print("=" * 80)
    print(f"VERIFYING BRONZE DELTA LAKE TABLE: {table_path}")
    print("=" * 80)

    try:
        df = spark.read.format("delta").load(table_path)
    except Exception as e:
        logger.error("Failed to load Delta table at %s: %s", table_path, e)
        print(f"ERROR: Cannot load Delta table at {table_path}. Details: {e}")
        return

    print("\n--- 1. SCHEMA SPECIFICATION ---")
    df.printSchema()

    total_count = df.count()
    print(f"\n--- 2. RECORD COUNT ---")
    print(f"Total Rows in Bronze Table: {total_count}")

    print("\n--- 3. PARTITION DISTRIBUTION (ingestion_date) ---")
    partition_df = df.groupBy("ingestion_date").agg(count("*").alias("count")).orderBy("ingestion_date")
    partition_df.show(truncate=False)

    print("\n--- 4. EVENT TYPE BREAKDOWN ---")
    df.groupBy("event_type").agg(count("*").alias("count")).orderBy("count", ascending=False).show(truncate=False)

    print("\n--- 5. SAMPLE 5 RECORDS ---")
    sample_cols = [
        "event_id",
        "transaction_id",
        "event_type",
        "event_time",
        "ingestion_time",
        "ingestion_date",
        "kafka_partition",
        "kafka_offset",
    ]
    df.select(sample_cols).show(5, truncate=False)
    print("=" * 80)


def main() -> None:
    """CLI Entrypoint for Verification."""
    parser = argparse.ArgumentParser(description="Verify Bronze Delta Table")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    parser.add_argument("--minio-endpoint", default=None, help="MinIO endpoint")
    parser.add_argument("--table-path", default="s3a://bronze/payment_events", help="Delta table path")
    args = parser.parse_args()

    config = load_config(args.config)
    spark = get_spark_session(minio_endpoint=args.minio_endpoint, config=config)
    try:
        verify_bronze_table(spark, table_path=args.table_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
