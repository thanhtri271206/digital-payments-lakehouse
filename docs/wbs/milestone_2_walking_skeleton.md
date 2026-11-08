# WBS MILESTONE 2 — THE "WALKING SKELETON" (VERTICAL SLICE 1: STREAM TO BRONZE)

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/00_wbs_dictionary_standard.md)  
> **Kế hoạch tổng thể & Quality Gates:** [`docs/wbs/README.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/README.md)  
> **Vai trò điều hành:** Senior Technical Project Manager (TPM) & Lead Data Platform Architect  
> **Ngân sách Kế hoạch:** **25 giờ kỹ thuật**  
> **Trọng tâm Chuyển giao:** Luồng PySpark Structured Streaming sơ khởi (Vertical Slice 1), ghi Append-only xuống Delta Lake Bronze trên MinIO, Checkpoint recovery và Diễn tập phục hồi khi hệ thống gặp sự cố (Crash Recovery Drill).

---

## 1. TỔNG QUAN PHẠM VI & MỤC TIÊU MILESTONE 2

Milestone 2 đóng vai trò là lát cắt dọc đầu tiên (**Walking Skeleton / Tracer Bullet**) của toàn bộ kiến trúc Lakehouse. Mục tiêu trọng tâm là thiết lập một luồng dữ liệu end-to-end hoàn chỉnh từ tầng Message Broker (Kafka) đi qua Streaming Compute Engine (Apache Spark) và đáp xuống Storage Engine phân tán (Delta Lake trên MinIO) trong trạng thái hoạt động ổn định:
1. **PySpark Structured Streaming & S3A Connector:** Cấu hình môi trường Spark với đầy đủ JAR extensions: `spark-sql-kafka-0-10`, `delta-spark`, và Hadoop-AWS (`hadoop-aws`, `aws-java-sdk-bundle`) để bắt tay tin cậy với MinIO S3 API.
2. **Raw JSON Payload & Technical Metadata Preservation:** Lưu trữ nguyên bản 100% payload JSON từ Kafka vào cột `raw_payload` (STRING) và làm giàu các cột metadata kỹ thuật hệ thống (`_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingestion_time`).
3. **Strict Bronze Append-Only Invariant:** Cài đặt Delta Lake sink với chế độ ghi thuần chèn (`Append`), phân vùng vật lý theo `_ingestion_date = YYYY-MM-DD` (ngày tiếp nhận của nền tảng), tuyệt đối cấm mọi thao tác UPDATE/DELETE/MERGE tại tầng Bronze.
4. **Resilient Checkpointing:** Lưu trữ toàn bộ streaming state và transaction log checkpoints trên MinIO bucket `s3a://checkpoints/bronze_payment_events/`, đảm bảo khả năng khôi phục chính xác vị trí offset sau khi khởi động lại.
5. **Crash & Outage Recovery Drill (Drill C07/Q6/Q7):** Thực hiện kịch bản kiểm thử ngắt kết nối đột ngột (giết container Spark khi dữ liệu vẫn đang được bơm vào Kafka), tích lũy backlog và kích hoạt lại để chứng minh cam kết **At-least-once Delivery + Zero Data Loss** và đo lường tốc độ bắt kịp (catch-up latency).
6. **Gate 2 Acceptance:** Vượt qua toàn bộ tiêu chí nghiệm thu khắt khe của **Gate 2** để mở khóa Milestone 3 (Stateful Streaming & Silver DQ).

---

## 2. WORK BREAKDOWN STRUCTURE (WBS) MATRIX & EFFORT ALLOCATION

| Work Package | Mã Task | Tên Task Kỹ thuật | Phân loại | Ngân sách | Task Tiên quyết |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **WP 2.1: Spark & Storage Setup** | `WBS-M2-T01` | PySpark & Delta Lake Container Environment with S3A Extensions | `[D1]` | 3 h | `WBS-M1-T08` |
| | `WBS-M2-T02` | Resilient Spark Session Factory & MinIO S3A Configuration | `[D2]` | 3 h | `WBS-M2-T01` |
| **WP 2.2: Stream Ingestion to Bronze** | `WBS-M2-T03` | Kafka Structured Streaming Consumer & Rate Management | `[D1]` | 3 h | `WBS-M2-T02` |
| | `WBS-M2-T04` | Raw Payload Preservation & Ingestion Metadata Schema | `[D1]` | 3 h | `WBS-M2-T03` |
| | `WBS-M2-T05` | Delta Lake Bronze Sink Implementation & Checkpoint Config | `[D1]` | 5 h | `WBS-M2-T04` |
| **WP 2.3: Resilience & Gate 2 Audit** | `WBS-M2-T06` | Delta Table Inspection, Schema Enforcement & Log Profiling | `[D2]` | 3 h | `WBS-M2-T05` |
| | `WBS-M2-T07` | Crash & Outage Recovery Drill (Zero Loss Backlog Catch-up) | `[D1]` | 3 h | `WBS-M2-T05` |
| | `WBS-M2-T08` | Automated Ingestion Test Suite & Quality Gate 2 Sign-Off | `[D1]` | 2 h | `WBS-M2-T06`, `T07` |
| **TỔNG CỘNG** | **8 Tasks** | **Toàn bộ Gói việc Milestone 2** | | **25 h** | |

---

## 3. ĐẶC TẢ CHI TIẾT TỪNG TASK KỸ THUẬT (TASK SPECIFICATIONS)

### [WBS-M2-T01] PySpark & Delta Lake Container Environment with S3A Extensions

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M1-T08` (Hoàn thành Gate 1).
- **Input Artifacts:**
  - `docker/docker-compose.yml`.
  - Phiên bản Spark tương thích: Apache Spark 3.5.x, Delta Lake 3.2.x, Hadoop 3.3.x.
- **Detailed Implementation Steps:**
  1. Mở rộng `docker/docker-compose.yml` để bổ sung dịch vụ Spark Streaming Master / Worker hoặc Spark Runner Container:
     - Image: `bitnami/spark:3.5.1` hoặc custom Dockerfile `docker/spark/Dockerfile`.
     - Mount thư mục mã nguồn `src/` và thư mục cấu hình `config/` vào container.
     - Cấu hình mạng Docker `internal-net` để Spark giao tiếp được với Kafka (`kafka:29092`) và MinIO (`minio:9000`).
  2. Khai báo bộ JAR packages bắt buộc trong cấu hình `spark-defaults.conf` hoặc tham số `--packages`:
     - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
     - `io.delta:delta-spark_2.12:3.2.0`
     - `org.apache.hadoop:hadoop-aws:3.3.4`
     - `com.amazonaws:aws-java-sdk-bundle:1.12.262`
  3. Cài đặt các biến môi trường cấu hình bộ nhớ tối ưu cho Workstation 32GB RAM:
     - `SPARK_DRIVER_MEMORY: "4g"`, `SPARK_EXECUTOR_MEMORY: "4g"`.
- **Deliverables / Output Artifacts:**
  - Cập nhật file: `docker/docker-compose.yml`.
  - Script kiểm tra môi trường: `scripts/test_spark_env.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t01/`
  - **Artifacts:**
    - `logs/spark_container_startup.log` (Log khởi động container Spark master/worker).
    - `logs/test_spark_env_output.log` (Log nạp thành công các class Delta Lake và S3A).
    - `screenshots/spark_master_web_ui.png` (Ảnh chụp giao diện Spark Master Web UI `:8081` hoặc Spark UI `:4040`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Container Spark khởi động thành công, Spark Web UI truy cập được tại `http://localhost:4040` (khi job chạy) hoặc port master UI `http://localhost:8081`.
  - [ ] **AC-2:** Script `test_spark_env.py` nạp thành công các class của Delta Lake (`io.delta.sql.DeltaSparkSessionExtension`) và Hadoop-AWS (`org.apache.hadoop.fs.s3a.S3AFileSystem`) không bị lỗi `ClassNotFoundException`.
- **Verification Method:**
  ```powershell
  # Chạy script kiểm tra nạp thư viện Spark
  docker compose exec -T spark python /opt/spark-apps/scripts/test_spark_env.py
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Xung đột phiên bản giữa `hadoop-aws` và `aws-java-sdk-bundle` dẫn đến lỗi `NoSuchMethodError`.
  - *Mitigation:* Ghim cứng cặp phiên bản đã được kiểm chứng tương thích: Hadoop 3.3.4 đi kèm AWS Java SDK 1.12.262.

---

### [WBS-M2-T02] Resilient Spark Session Factory & MinIO S3A Configuration

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M2-T01`.
- **Input Artifacts:**
  - `config/config.yaml`.
  - Thông tin MinIO credentials từ `.env`.
- **Detailed Implementation Steps:**
  1. Xây dựng module tạo Spark Session tập trung trong `src/digital_payments_lakehouse/engine/spark_session.py`:
     - Định nghĩa lớp `SparkSessionFactory` cấu hình toàn bộ các thông số kỹ thuật bắt buộc:
       ```python
       builder = (
           SparkSession.builder.appName("DigitalPaymentsLakehouse-BronzeIngestion")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config(
               "spark.sql.catalog.spark_catalog",
               "org.apache.spark.sql.delta.catalog.DeltaCatalog",
           )
           # MinIO S3A Endpoints & Credentials
           .config(
               "spark.hadoop.fs.s3a.endpoint",
               f"http://{minio_host}:{minio_port}",
           )
           .config(
               "spark.hadoop.fs.s3a.access.key", minio_access_key
           )
           .config(
               "spark.hadoop.fs.s3a.secret.key", minio_secret_key
           )
           .config("spark.hadoop.fs.s3a.path.style.access", "true")
           .config(
               "spark.hadoop.fs.s3a.impl",
               "org.apache.hadoop.fs.s3a.S3AFileSystem",
           )
           .config(
               "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
           )
           # Tối ưu hóa Delta Lake & Structured Streaming
           .config(
               "spark.sql.streaming.forceDeleteTempCheckpointLocation",
               "true",
           )
           .config("spark.sql.shuffle.partitions", "3")
       )
```
  2. Viết logic kiểm tra kết nối (Connectivity Handshake) tới MinIO:
     - Thử tạo một DataFrame nhỏ và ghi xuống `s3a://bronze/_handshake_test` rồi đọc lại để đảm bảo quyền đọc/ghi trên bucket hoạt động thông suốt.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/engine/spark_session.py`.
  - Test suite: `tests/unit/test_spark_session.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t02/`
  - **Artifacts:**
    - `logs/pytest_test_spark_session.log` (Log chạy test khởi tạo singleton session và handshake).
    - `logs/minio_s3a_handshake.log` (Log kết nối đọc ghi thành công vào `s3a://bronze/_handshake_test`).
    - `data_samples/minio_s3a_handshake_metadata.json` (Trích xuất metadata test write DataFrame lên MinIO).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Hàm `get_spark_session()` khởi tạo thành công Singleton session với đầy đủ cấu hình Delta và S3A.
  - [ ] **AC-2:** Đọc và ghi thành công dữ liệu thử nghiệm vào MinIO qua giao thức `s3a://`.
  - [ ] **AC-3:** Số partitions shuffle mặc định được giới hạn về 3 (thay vì 200 mặc định của Spark) để tránh bùng nổ task trên máy đơn.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_spark_session.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lỗi `AmazonS3Exception: 400 Bad Request` do không cấu hình `fs.s3a.path.style.access = true` đối với MinIO.
  - *Mitigation:* Bắt buộc set path-style access và tắt SSL cho môi trường local trong `SparkSessionFactory`.

---

### [WBS-M2-T03] Kafka Structured Streaming Consumer & Rate Management

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M2-T02`.
- **Input Artifacts:**
  - Topic Kafka: `payment.events.raw`.
  - Cấu hình luồng: `config/config.yaml` (`streaming_slo.baseline_events_per_sec`).
- **Detailed Implementation Steps:**
  1. Cài đặt luồng đọc dữ liệu từ Kafka trong `src/digital_payments_lakehouse/streaming/bronze_reader.py`:
     - Sử dụng format `"kafka"` của PySpark Structured Streaming:
       ```python
       kafka_stream = (
           spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
           .option("subscribe", "payment.events.raw")
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option(
               "maxOffsetsPerTrigger", 1000
           )  # Điều tiết khối lượng mỗi micro-batch
           .load()
       )
```
  2. Quản lý tốc độ đọc (Rate Management & Backpressure):
     - Đặt `maxOffsetsPerTrigger = 1000` để đảm bảo micro-batch không bị quá tải khi xử lý tồn đọng sau downtime.
  3. Xử lý schema thô của Kafka:
     - Giữ nguyên các cột metadata mặc định do Kafka cung cấp: `key` (BINARY), `value` (BINARY), `topic` (STRING), `partition` (INT), `offset` (LONG), `timestamp` (TIMESTAMP), `timestampType` (INT).
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/bronze_reader.py`.
  - Test suite: `tests/unit/test_bronze_reader.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t03/`
  - **Artifacts:**
    - `logs/pytest_test_bronze_reader.log` (Log chạy test streaming reader và maxOffsetsPerTrigger).
    - `data_samples/kafka_raw_stream_schema.txt` (Output của `df.printSchema()` chứng minh binary key/value và metadata).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Hàm `read_kafka_payment_stream()` trả về một Streaming DataFrame hợp lệ với schema khớp với Kafka source.
  - [ ] **AC-2:** Cấu hình `maxOffsetsPerTrigger` được kiểm chứng bằng test mô phỏng micro-batch không vượt quá ngưỡng quy định.
  - [ ] **AC-3:** Tham số `failOnDataLoss: false` được cấu hình để tránh crash job khi topic retention xóa các message cũ trong môi trường dev.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_bronze_reader.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Spark không kết nối được tới Kafka do sai lệch hostname giữa máy host (`localhost:9092`) và container nội bộ (`kafka:29092`).
  - *Mitigation:* Cấu hình tham số `bootstrap_servers` lấy động từ biến môi trường hoặc file config tùy theo ngữ cảnh thực thi.

---

### [WBS-M2-T04] Raw Payload Preservation & Ingestion Metadata Schema

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M2-T03`.
- **Input Artifacts:**
  - Quy chuẩn Medallion Bronze trong `docs/wbs/00_wbs_dictionary_standard.md`.
- **Detailed Implementation Steps:**
  1. Cài đặt module chuyển đổi schema Bronze trong `src/digital_payments_lakehouse/streaming/bronze_transformer.py`:
     - Giải mã cột `value` từ BINARY sang STRING UTF-8 và đặt tên là `raw_payload`.
     - Tuyệt đối **không parse** `raw_payload` thành các cột nghiệp vụ ở tầng Bronze để bảo đảm nguyên tắc bảo toàn nguyên bản dữ liệu nguồn (Payload Preservation).
  2. Bổ sung các cột metadata kỹ thuật kiểm toán (Auditing Metadata):
     - `_kafka_topic`: `col("topic")`
     - `_kafka_partition`: `col("partition")`
     - `_kafka_offset`: `col("offset")`
     - `_kafka_timestamp`: `col("timestamp")` (Thời điểm message được ghi vào broker)
     - `_ingestion_time`: `current_timestamp()` theo múi giờ `UTC` (Thời điểm Spark xử lý micro-batch)
     - `_ingestion_date`: `to_date(col("_ingestion_time"))` (Định dạng `YYYY-MM-DD`, dùng làm partition key vật lý)
  3. Định nghĩa Schema tường minh cho bảng Delta Bronze (`BronzePaymentEventsSchema`):
     - Khóa duy nhất tự nhiên để audit: `(_kafka_topic, _kafka_partition, _kafka_offset)`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/bronze_transformer.py`.
  - Test suite: `tests/unit/test_bronze_transformer.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t04/`
  - **Artifacts:**
    - `logs/pytest_test_bronze_transformer.log` (Log chạy test schema Bronze, bảo toàn payload JSON và 6 cột metadata).
    - `data_samples/bronze_row_transformed_sample.json` (Trích xuất 1 dòng mẫu DataFrame chứng minh raw_payload và metadata kỹ thuật).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Cột `raw_payload` chứa chuỗi JSON nguyên bản, không bị mất ký tự unicode tiếng Việt (NFC/NFD) hay làm tròn số nguyên lớn.
  - [ ] **AC-2:** Đầy đủ 6 cột metadata kỹ thuật: `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingestion_time`, `_ingestion_date`.
  - [ ] **AC-3:** Cột `_ingestion_date` được tính chính xác dựa trên múi giờ chuẩn hóa UTC.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_bronze_transformer.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lệch múi giờ khi gọi `current_timestamp()` do hệ điều hành host dùng UTC+7 trong khi Spark dùng UTC.
  - *Mitigation:* Ép cấu hình `spark.sql.session.timeZone = "UTC"` trong Spark Session Factory.

---

### [WBS-M2-T05] Delta Lake Bronze Sink Implementation & Checkpoint Config

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M2-T04`.
- **Input Artifacts:**
  - MinIO Bucket: `s3a://bronze/` và `s3a://checkpoints/`.
- **Detailed Implementation Steps:**
  1. Xây dựng module ghi Delta Lake Streaming trong `src/digital_payments_lakehouse/streaming/bronze_writer.py`:
     - Cấu hình `DataStreamWriter`:
       ```python
       query = (
           transformed_stream.writeStream.format("delta")
           .outputMode("append")  # BẮT BUỘC APPEND-ONLY
           .partitionBy("_ingestion_date")  # Phân vùng vật lý theo ngày tiếp nhận
           .option(
               "checkpointLocation",
               "s3a://checkpoints/bronze_payment_events/",
           )
           .trigger(processingTime="10 seconds")  # Chu kỳ micro-batch 10s
           .start("s3a://bronze/payment_events/")
       )
```
  2. Quản lý Checkpointing & Fault Tolerance:
     - Đảm bảo checkpoint location được ghi trực tiếp lên MinIO bucket `checkpoints`.
     - Cấu hình commit protocol an toàn cho S3A filesystem.
  3. Quản lý vòng đời tiến trình streaming:
     - Viết hàm `start_bronze_ingestion_job()` có khả năng lắng nghe tín hiệu kết thúc nhẹ nhàng (`awaitTerminationOrTimeout`).
     - Tích hợp Structured Logging ghi nhận tiến độ từng micro-batch thông qua `StreamingQueryListener`: số lượng bản ghi đã xử lý, thời gian thực thi micro-batch, offset đã commit.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/bronze_writer.py`.
  - Entrypoint thực thi: `src/digital_payments_lakehouse/streaming/main_bronze.py`.
  - Integration test: `tests/integration/test_bronze_streaming_e2e.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t05/`
  - **Artifacts:**
    - `logs/pytest_test_bronze_streaming_e2e.log` (Log chạy test streaming end-to-end ghi MinIO).
    - `logs/spark_streaming_query_listener.log` (Log tiến độ các micro-batch, records processed, duration).
    - `screenshots/spark_ui_streaming_tab_active.png` (Ảnh giao diện Spark UI tab Structured Streaming thể hiện Input Rate, Process Rate).
    - `data_samples/minio_bronze_checkpoints_ls.txt` (Danh sách files commits và offsets trong `s3a://checkpoints/bronze_payment_events/`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Job ghi dữ liệu liên tục vào `s3a://bronze/payment_events/` ở định dạng Delta Lake chuẩn (có thư mục `_delta_log/`).
  - [ ] **AC-2:** Chế độ ghi là **Append-only 100%**, không có bất kỳ lệnh merge/update nào.
  - [ ] **AC-3:** Thư mục checkpoint `s3a://checkpoints/bronze_payment_events/` lưu giữ các file commits, metadata, và offsets sau mỗi micro-batch.
  - [ ] **AC-4:** Bảng được phân vùng thành các thư mục con dạng `_ingestion_date=YYYY-MM-DD/`.
- **Verification Method:**
  ```powershell
  # Chạy test integration ghi luồng vào MinIO local
  uv run pytest tests/integration/test_bronze_streaming_e2e.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lỗi `TransactionCommitFailedException` hoặc multi-driver concurrency write conflict trên Delta Lake.
  - *Mitigation:* Đảm bảo mỗi streaming query có checkpoint location độc lập và duy nhất 1 driver instance ghi vào bảng Bronze.

---

### [WBS-M2-T06] Delta Table Inspection, Schema Enforcement & Log Profiling

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M2-T05`.
- **Input Artifacts:**
  - Bảng Delta: `s3a://bronze/payment_events/`.
- **Detailed Implementation Steps:**
  1. Xây dựng công cụ kiểm tra và audit bảng Delta trong `src/digital_payments_lakehouse/utils/delta_inspector.py`:
     - Viết hàm kiểm tra metadata bảng: `DESCRIBE DETAIL delta.`s3a://bronze/payment_events/``.
     - Viết hàm kiểm tra lịch sử commit: `DESCRIBE HISTORY delta.`s3a://bronze/payment_events/``.
     - Đọc và phân tích dung lượng trung bình của các file Parquet sinh ra trong từng partition.
  2. Kiểm tra tính toàn vẹn và Schema Enforcement của Delta Lake:
     - Thử nghiệm cố tình ghi một batch có schema không tương thích (ví dụ thêm cột không khai báo mà không bật mergeSchema) $\rightarrow$ Xác nhận Delta Lake chặn đứng thao tác ghi này để bảo vệ bảng.
  3. Cấu hình chính sách nén log và dọn dẹp file nhỏ (Small Files Profile):
     - Thiết lập thuộc tính bảng: `delta.autoOptimize.optimizeWrite = true` và `delta.autoOptimize.autoCompact = true`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/utils/delta_inspector.py`.
  - CLI script: `scripts/inspect_bronze.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t06/`
  - **Artifacts:**
    - `logs/inspect_bronze_output.log` (Lệnh `inspect_bronze.py` xuất số lượng files, tổng size, partition details).
    - `data_samples/delta_bronze_history_table.txt` (Output `DESCRIBE HISTORY` thể hiện các thao tác WRITE append-only).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Lệnh `python scripts/inspect_bronze.py` xuất ra định dạng bảng đẹp mắt trên terminal thể hiện: Số lượng files, Tổng dung lượng, Version Delta hiện tại, Partition columns.
  - [ ] **AC-2:** Bảng Delta từ chối các bản ghi vi phạm cấu trúc schema định sẵn.
  - [ ] **AC-3:** Delta Transaction Log ghi nhận đầy đủ lịch sử các thao tác `WRITE` kèm metrics `numOutputRows`.
- **Verification Method:**
  ```powershell
  # Thực thi script kiểm tra bảng Bronze
  uv run python scripts/inspect_bronze.py
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Hiện tượng bùng nổ file nhỏ (Small Files Problem) do chu kỳ micro-batch 10s sinh ra file Parquet kích thước vài KB.
  - *Mitigation:* Lập kế hoạch job chạy batch `OPTIMIZE` định kỳ hoặc bật cấu hình optimizeWrite của Delta Lake.

---

### [WBS-M2-T07] Crash & Outage Recovery Drill (Zero Loss Backlog Catch-up)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M2-T05`.
- **Input Artifacts:**
  - Simulator phát dữ liệu từ Milestone 1.
  - Streaming job từ `WBS-M2-T05`.
- **Detailed Implementation Steps:**
  1. Thiết kế kịch bản diễn tập sự cố (Failure Recovery Drill - C07 / Q6 / Q7):
     - **Pha 1 (Bình thường):** Khởi động luồng streaming, dùng simulator bơm 500 events vào Kafka. Chờ Spark xử lý xong và commit offset.
     - **Pha 2 (Đột tử hệ thống):** Giết đột ngột tiến trình Spark streaming (`kill -9` hoặc `docker compose stop spark`).
     - **Pha 3 (Tích tụ Backlog):** Trong khi Spark đang chết hoàn toàn, tiếp tục dùng Simulator bơm thêm 1,000 events vào topic `payment.events.raw` trên Kafka để mô phỏng backlog tích tụ khi hệ thống gặp sự cố trong 5–15 phút.
     - **Pha 4 (Phục hồi & Catch-up):** Khởi động lại Spark streaming job với cùng checkpoint location.
     - **Pha 5 (Kiểm chứng):** Đo lường thời gian bắt kịp backlog, kiểm tra số lượng bản ghi ghi nhận tại bảng Bronze so với tổng số event phát ra.
  2. Tự động hóa kịch bản diễn tập thành bài test tích hợp `tests/integration/test_crash_recovery_drill.py`.
- **Deliverables / Output Artifacts:**
  - Test suite diễn tập: `tests/integration/test_crash_recovery_drill.py`.
  - Báo cáo kết quả diễn tập: `docs/reports/drill_c07_crash_recovery.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t07/`
  - **Artifacts:**
    - `logs/crash_recovery_drill_execution.log` (Log chi tiết các pha: Normal 500 txns, Kill container, Backlog 1,000 txns, Resume & Catch-up).
    - `logs/pytest_test_crash_recovery.log` (Log kết quả chạy test tự động kiểm chứng).
    - `data_samples/backlog_catchup_metrics.json` (Số liệu đo lường tốc độ bắt kịp backlog < 60s và xác thực chính xác 1,500 rows).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1 (Zero Data Loss):** Tổng số bản ghi ghi vào bảng `bronze_payment_events` khớp chính xác 100% với tổng số 1,500 events phát ra từ Kafka (không thiếu 1 bản ghi nào).
  - [ ] **AC-2 (Exact Offset Resume):** Khi khởi động lại, Spark đọc chính xác từ offset tiếp theo đã ghi nhận trong checkpoint, không đọc lại từ đầu (`earliest`).
  - [ ] **AC-3 (At-least-once Guarantee):** Các bản ghi có thể bị lặp offset do crash giữa micro-batch được đánh dấu nhận diện đầy đủ thông qua khóa `(_kafka_topic, _kafka_partition, _kafka_offset)`.
  - [ ] **AC-4:** Job bắt kịp toàn bộ backlog 1,000 sự kiện trong thời gian $< 60$ giây sau khi phục hồi.
- **Verification Method:**
  ```powershell
  # Chạy bài test diễn tập Crash & Recovery
  uv run pytest tests/integration/test_crash_recovery_drill.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* File checkpoint bị hỏng (corrupted) khi container bị kill ngay đúng tích tắc đang ghi log checkpoint.
  - *Mitigation:* Delta Lake và Spark Structured Streaming sử dụng cơ chế ghi file tạm atomic rename (`.compact` / metadata json), cho phép tự động rollback về commit hợp lệ liền trước.

---

### [WBS-M2-T08] Automated Ingestion Test Suite & Quality Gate 2 Sign-Off

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 2 hours
- **Prerequisites / Dependencies:** `WBS-M2-T06`, `WBS-M2-T07`.
- **Input Artifacts:**
  - Tiêu chí nghiệm thu Gate 2 trong `docs/wbs/README.md`.
  - Toàn bộ deliverables của Milestone 2.
- **Detailed Implementation Steps:**
  1. Viết kịch bản kiểm thử toàn diện cho Gate 2 (`tests/integration/test_milestone_2_gate.py`):
     - Xác minh toàn bộ chuỗi: Simulator $\rightarrow$ Kafka $\rightarrow$ Spark Structured Streaming $\rightarrow$ MinIO Delta Lake Bronze.
     - Kiểm tra tính bất biến: Không có bất kỳ lệnh sửa đổi dữ liệu nào trên tầng Bronze.
     - Kiểm tra schema bảng Bronze và sự hiện diện của đầy đủ các cột metadata.
  2. Thu thập và đóng gói bằng chứng thực thi (Evidence Artifacts):
     - Chụp ảnh hoặc trích xuất log của Spark Structured Streaming UI (Active streaming query, Input Rate vs Process Rate).
     - Trích xuất bảng kết quả truy vấn `DESCRIBE HISTORY bronze_payment_events`.
     - Lập văn bản ký duyệt nghiệm thu: `docs/reports/gate_2_signoff.md`.
- **Deliverables / Output Artifacts:**
  - Test suite nghiệm thu: `tests/integration/test_milestone_2_gate.py`.
  - Báo cáo bằng chứng Gate 2: `docs/reports/gate_2_signoff.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m2/wbs-m2-t08/`
  - **Artifacts:**
    - `logs/gate_2_verification_suite.log` (Log chạy toàn bộ kịch bản nghiệm thu Gate 2).
    - `logs/pytest_milestone_2_full.log` (Log kết quả chạy toàn bộ test suite M2 pass 100%).
    - `screenshots/spark_ui_15min_stability.png` (Ảnh Spark UI chứng minh luồng chạy ổn định 15 phút).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** 100% tests trong `tests/` pass trơn tru không lỗi.
  - [ ] **AC-2:** Vượt qua 100% tiêu chí kỹ thuật của **Gate 2 (Milestone 2 $\rightarrow$ Milestone 3)** đã cam kết trong `docs/wbs/README.md`.
  - [ ] **AC-3:** Có bằng chứng log thực tế chứng minh luồng streaming chạy ổn định liên tục ít nhất 15 phút với tải baseline 5–50 events/giây.
- **Verification Method:**
  ```powershell
  # Chạy test nghiệm thu Gate 2
  uv run pytest tests/integration/test_milestone_2_gate.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Thiếu tài nguyên bộ nhớ trên máy trạm khi chạy cả Docker container lẫn Spark driver local.
  - *Mitigation:* Tinh chỉnh cấu hình JVM heap size của Spark Driver xuống mức `2g` trong quá trình chạy test suite tự động.

---

## 4. MILESTONE 2 QUALITY GATE 2 CHECKLIST (TIÊU CHÍ CHỐT SỔ M2)

> **Thư mục Lưu trữ Bằng chứng Nghiệm thu:** `docs/evidence/m2/gate_2_signoff/`  
> Toàn bộ artifacts nghiệm thu phải được tập hợp đầy đủ trước phiên review.

Trước khi ký nghiệm thu (Sign-off) Milestone 2 để chuyển sang Milestone 3 (Stateful Streaming & Silver DQ), TPM và Học viên bắt buộc phải đánh dấu kiểm tra đủ toàn bộ các mục sau:

- [ ] **Gate 2.1 (Walking Skeleton E2E):** Luồng dữ liệu chạy thông suốt từ Simulator $\rightarrow$ Kafka topic `payment.events.raw` $\rightarrow$ Spark Streaming $\rightarrow$ MinIO Delta Bronze table. *(Artifacts: `docs/evidence/m2/gate_2_signoff/e2e_streaming_bronze.log`, `spark_ui_active.png`)*
- [ ] **Gate 2.2 (Bronze Append-Only):** Dữ liệu tầng Bronze được ghi thuần Append-only, không có update/delete, partition vật lý chuẩn `_ingestion_date = YYYY-MM-DD`. *(Artifacts: `docs/evidence/m2/gate_2_signoff/delta_history_audit.txt`)*
- [ ] **Gate 2.3 (Payload Preservation):** Toàn vẹn 100% chuỗi JSON thô tại cột `raw_payload`, đầy đủ 6 trường metadata kỹ thuật đường ống. *(Artifacts: `docs/evidence/m2/gate_2_signoff/bronze_payload_sample.json`)*
- [ ] **Gate 2.4 (Checkpoint & Recovery):** Thư mục checkpoint lưu trên MinIO hoạt động hoàn hảo; vượt qua bài diễn tập Crash & Outage Recovery Drill không mất bất kỳ bản ghi nào (Zero Data Loss). *(Artifacts: `docs/evidence/m2/gate_2_signoff/crash_recovery_drill_report.md`, `offset_resume_log.txt`)*
- [ ] **Gate 2.5 (Code Quality & Verification):** 100% mã nguồn tuân thủ Type Hints, Pydantic, Structured Logging, bộ test tự động pass 100%. *(Artifacts: `docs/evidence/m2/gate_2_signoff/pytest_final_m2.log`)*

---

## 5. SCHEDULE RISK, CONTINGENCY BUFFER & REMINDER

> [!WARNING] Cảnh báo Nguy cơ Vượt Ngân sách Thời gian (Schedule Overrun Risk)
> - **Ngân sách Kế hoạch:** **25 giờ**.
> - **Khu vực có rủi ro cao gây trễ tiến độ:**
>   1. **`WBS-M2-T01` & `WBS-M2-T02` (Hadoop S3A / MinIO Connector):** Đây là khu vực thường xuyên xảy ra lỗi tương thích thư viện (JAR hell) giữa Spark, Delta Lake, Hadoop-AWS và AWS SDK trên môi trường container local. Quá trình gỡ lỗi classpath có thể làm hao hụt thêm 2–4 giờ.
>   2. **`WBS-M2-T07` (Crash Recovery Drill):** Việc mô phỏng chính xác hành vi kill container giữa chừng và kiểm tra tính toàn vẹn của checkpoint có thể cần thử nghiệm lặp lại nhiều lần để có kịch bản test ổn định.
> - **Nguyên tắc Đánh đổi của TPM (Quality Leverage Policy):**
>   * **Ưu tiên hàng đầu là Tính ổn định của Walking Skeleton và Cơ chế Checkpointing.** Đây là xương sống kết nối nguồn với hồ dữ liệu; nếu cơ chế checkpoint hoặc kết nối S3A chập chờn, luồng Stateful Streaming phức tạp ở Milestone 3 sẽ không thể triển khai được.
>   * Cho phép tổng thời gian thực tế của Milestone 2 có thể dao động đến **28–30 giờ** (+15–20% Contingency Buffer) nếu thời gian đó phục vụ trực tiếp cho việc hoàn thiện bài diễn tập phục hồi sự cố (Crash Recovery Drill) — một trong những điểm nhấn kỹ thuật đắt giá nhất trong hồ sơ năng lực của học viên.

