# BÁO CÁO RÀ SOÁT TASK 2.1 & ĐẶC TẢ TRIỂN KHAI TASK 2.2
## Milestone 2: Kafka to Delta Lake Bronze Ingestion

> **Người thực hiện:** Thanhtri271206  
> **Người rà soát (PM / Lead Mentor):** Antigravity AI  
> **Trạng thái:** TASK 2.1 APPROVED (100% PASSED) — TIẾN HÀNH TASK 2.2  

---

## 1. Kết quả Rà soát Kỹ thuật Task 2.1 (Hạ tầng Spark Container)

### 1.1. Đánh giá Cấu hình Dockerfile & Docker Compose
- **Dockerfile (`docker/spark/Dockerfile`):**
  - Sử dụng nền tảng `python:3.12-slim` kết hợp `default-jre-headless` (Java 17).
  - Đã cài đặt đầy đủ các thư viện cốt lõi: `pyspark==3.5.3`, `delta-spark==3.2.0`, `confluent-kafka`, `pyarrow`, `pydantic`.
  - **Điểm sáng kỹ thuật (Senior Observation):** Bạn đã áp dụng kỹ thuật **Pre-fetching Maven JARs** ngay trong quá trình build Docker image và sao chép vào thư mục `jars/` của PySpark. Kỹ thuật này giúp:
    1. Container khởi chạy các job Spark **ngay lập tức**, không tốn thời gian tải lại các file JAR từ Maven Central qua mạng.
    2. Toàn bộ stack có thể chạy **offline 100%**, đáp ứng đúng cam kết trong SRS.
- **Docker Compose (`docker/docker-compose.yml`):**
  - Khai báo đúng `depends_on: condition: service_healthy` cho cả `dp-kafka` và `dp-minio`.
  - Thiết lập chuẩn xác các biến môi trường kết nối nội bộ: `kafka:29092` và `http://minio:9000`.
  - Mount đúng 3 thư mục mã nguồn (`src`, `config`, `tests`) vào `/app`.

### 1.2. Kết quả Thực nghiệm Thực tế (Live Verification Evidence)
Chúng tôi đã thực hiện lệnh kiểm tra trực tiếp bên trong container `dp-spark`:
1. **Kiểm tra PySpark & Delta Package:**
   ```text
   PySpark version: 3.5.3 | Delta imported successfully
   ```
2. **Kiểm tra Ghi/Đọc Transaction Log Delta Lake trên MinIO S3A:**
   Đã chạy thực nghiệm ghi một DataFrame vào `s3a://bronze/test_table` và đọc ngược lại:
   ```text
   Delta Lake write & read on MinIO S3A SUCCESSFUL! Count: 2
   ```
   Kiểm tra MinIO bucket `bronze`: File Parquet và commit JSON `_delta_log/00000000000000000000.json` đã được khởi tạo chuẩn chỉnh, không có lỗi S3A authentication. Đã dọn dẹp bảng test thành công.

---

## 2. Đặc tả Triển khai Task 2.2: Lập trình Spark Streaming Ingestion Job

### 2.1. Mục tiêu Nghiệp vụ
Xây dựng job Spark Structured Streaming đọc thông điệp JSON từ Kafka topic `payment.events.raw` và ghi Append-only vào bảng Delta `bronze_payment_events` trên MinIO.

### 2.2. Vị trí & Cấu trúc File
- **File Ingestion Job:** `src/digital_payments_lakehouse/bronze/ingest_payment_events.py`
- **File Verification Script:** `src/digital_payments_lakehouse/bronze/verify_bronze.py`

### 2.3. Quy chuẩn Data Contract tầng Bronze
1. **Bảo toàn tính Bất biến (Immutability):**
   - Lưu chuỗi JSON nguyên bản trong cột `raw_payload = col("value").cast("string")`.
2. **Metadata Trích xuất Nhanh (Lightweight Parsing):**
   - `kafka_key = col("key").cast("string")`
   - `kafka_topic = col("topic")`
   - `kafka_partition = col("partition")`
   - `kafka_offset = col("offset")`
   - `kafka_timestamp = col("timestamp")`
   - Parse JSON để lấy: `event_id`, `transaction_id`, `event_type`, `event_time`, `ingestion_time`.
3. **Phân vùng & Timestamp kiểm toán:**
   - `ingestion_date = date_format(col("ingestion_time"), "yyyy-MM-dd")` (Cột Partition).
   - `ingested_at_bronze = current_timestamp()` (Thời điểm ghi vào Lakehouse).
4. **Vị trí lưu trữ:**
   - Target Delta Path: `s3a://bronze/payment_events`
   - Checkpoint Path: `s3a://checkpoints/bronze_payment_events`
   - Partition By: `ingestion_date`
   - Output Mode: `append`

### 2.4. Tính năng Linh hoạt của Job (CLI Flags)
Job cần hỗ trợ 2 chế độ kích hoạt (Triggers):
- **Continuous Micro-batch:** `--processing-time "5 seconds"` (Chế độ chạy liên tục chuẩn Streaming).
- **Trigger AvailableNow:** `--available-now` (Xử lý toàn bộ backlog hiện có rồi dừng lại — cực kỳ tiện lợi cho unit test, CI/CD và chạy bắt kịp backlog sau sự cố mà không tốn tài nguyên treo máy).

---

## 3. Khung sườn Code Mẫu (Boilerplate) cho Task 2.2

### [src/digital_payments_lakehouse/bronze/ingest_payment_events.py](file:///c:/Users/ADMIN/DE-project/digital-payments-lakehouse/src/digital_payments_lakehouse/bronze/ingest_payment_events.py)

```python
"""Bronze Ingestion Job: Kafka payment.events.raw -> MinIO Delta Lake bronze_payment_events."""

import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, from_json
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session(app_name: str = "BronzePaymentEventsIngestion") -> SparkSession:
    """Initialize SparkSession with Delta Lake and MinIO S3A configs."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    return builder.getOrCreate()


PAYMENT_EVENT_JSON_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("amount_minor", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("status_reason", StringType(), True),
        StructField("producer_version", StringType(), True),
    ]
)


def run_ingestion(
    bootstrap_servers: str = "kafka:29092",
    topic: str = "payment.events.raw",
    output_path: str = "s3a://bronze/payment_events",
    checkpoint_path: str = "s3a://checkpoints/bronze_payment_events",
    available_now: bool = False,
    processing_time: str = "5 seconds",
) -> None:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc stream từ Kafka
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Transform & trích xuất metadata
    raw_df = kafka_stream.select(
        col("key").cast("string").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_payload"),
    )

    parsed_df = raw_df.withColumn(
        "payload_struct", from_json(col("raw_payload"), PAYMENT_EVENT_JSON_SCHEMA)
    ).select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("raw_payload"),
        col("payload_struct.event_id").alias("event_id"),
        col("payload_struct.transaction_id").alias("transaction_id"),
        col("payload_struct.event_type").alias("event_type"),
        col("payload_struct.event_time").alias("event_time"),
        col("payload_struct.ingestion_time").alias("ingestion_time"),
        date_format(col("payload_struct.ingestion_time"), "yyyy-MM-dd").alias(
            "ingestion_date"
        ),
        current_timestamp().alias("ingested_at_bronze"),
    )

    # 3. Ghi stream vào Delta Lake Bronze
    writer = (
        parsed_df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .option("checkpointLocation", checkpoint_path)
    )

    if available_now:
        query = writer.trigger(availableNow=True).start(output_path)
    else:
        query = writer.trigger(processingTime=processing_time).start(output_path)

    query.awaitTermination()
```

---

## 4. Kế hoạch Nghiệm thu Gate 2 (Verification Plan)

Sau khi hoàn thành code, chúng ta sẽ thực thi:
1. **Simulator Run:** Chạy simulator bắn 50 giao dịch (~250 events) vào Kafka.
2. **Spark Stream Execution:** Thực thi job ingestion bên trong `dp-spark`.
3. **Delta Audit:** Chạy script `verify_bronze.py` để:
   - Đọc bảng Delta `s3a://bronze/payment_events`.
   - Khẳng định số lượng dòng ghi nhận khớp hoàn toàn với Kafka offset.
   - Kiểm tra cây thư mục MinIO `ingestion_date=...` và `_delta_log`.
