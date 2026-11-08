# WBS MILESTONE 3 — STATEFUL STREAMING, DUAL-TABLE SILVER & REAL-TIME DATA QUALITY

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/00_wbs_dictionary_standard.md)  
> **Kế hoạch tổng thể & Quality Gates:** [`docs/wbs/README.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/README.md)  
> **Vai trò điều hành:** Senior Technical Project Manager (TPM) & Lead Data Platform Architect  
> **Ngân sách Kế hoạch Ban đầu (Baseline):** **55 giờ kỹ thuật**  
> **Ngân sách Điều chỉnh Thực tế (Adjusted):** **58 giờ kỹ thuật** *(Chi tiết tại Mục 6)*  
> **Trọng tâm Chuyển giao:** Lõi Stateful Streaming phức tạp nhất dự án: `foreachBatch` pipeline, 5% Circuit Breaker, Watermark (2m) deduplication & Late Event DLQ, Silver Dual-Table (`history` + `current` pre-aggregated MERGE), Dynamic Timer phát hiện kẹt `STUCK` (5m), và Gold RT Metrics với cơ chế chống rung Hysteresis.

---

## 1. TỔNG QUAN PHẠM VI & MỤC TIÊU MILESTONE 3

Milestone 3 là "trái tim" của hệ thống xử lý thời gian thực, nơi giải quyết các thách thức kỹ thuật phức tạp nhất của một Data Engineer chuyên về Streaming và Lakehouse. Không đi theo hướng xử lý phi trạng thái (stateless) đơn giản, Milestone 3 triển khai cơ chế **Stateful Event-Time Processing** và kiến trúc **Dual-Table Silver** để đồng thời đáp ứng yêu cầu lưu vết kiểm toán và cập nhật trạng thái mới nhất:
1. **Multi-Destination Micro-Batch Dispatcher:** Xây dựng framework `foreachBatch` đọc từ `bronze_payment_events`, phân luồng xử lý đồng thời tới nhiều đích Delta Lake mà vẫn đảm bảo tính nguyên tử (atomic transaction) và idempotency.
2. **5% Circuit Breaker & Quarantine Routing:** Kiểm định hợp đồng dữ liệu qua Pydantic v2 / PySpark Rules. Nếu tỷ lệ poison records trong micro-batch $\ge 50$ dòng vượt quá **5%**, ngắt ngay lập tức streaming job để bảo vệ downstream. Tách các bản ghi lỗi riêng lẻ vào `silver_quarantine_events`.
3. **Event-Time Watermarking (2 phút) & DLQ:** Khử trùng `event_id` trong cửa sổ trượt 2 phút. Các sự kiện đến trễ $> 2$ phút không bị drop mà rẽ nhánh sang `silver_late_events_dlq` để phục vụ luồng Batch EOD hợp nhất sau.
4. **Silver Dual-Table Architecture:**
   - `silver_payment_events_history` (Append-only): Lưu trữ toàn bộ lịch sử chuyển trạng thái phục vụ phân tích hành trình.
   - `silver_transactions_current` (1 dòng / `transaction_id`): Áp dụng kỹ thuật **Pre-aggregated Collapse** theo Monotonic State Rank (1 $\rightarrow$ 6) ngay trong bộ nhớ micro-batch trước khi gọi lệnh Delta `MERGE INTO`, triệt tiêu hoàn toàn lỗi multiple-source-row conflict.
5. **Dynamic Timer & Stuck Transaction Detection:** Quản lý hạn chót giao dịch (`deadline = last_progress_event_time + 5m`). Gắn cờ `operational_status = STUCK` nếu quá hạn mà vẫn ở `PROCESSING`. Gỡ cờ khi có terminal event đến muộn nhưng bảo lưu vết `was_ever_stuck = TRUE`. Bắt xung đột Terminal (ví dụ nhận `FAILED` sau `COMPLETED`) vào `silver_lifecycle_exceptions`.
6. **Gold RT Metrics & Hysteresis Partner Degradation:** Tổng hợp metrics theo cửa sổ 1m và 5m, phát hiện suy giảm đối tác và áp dụng thuật toán **Hysteresis** (chỉ gỡ cờ suy giảm khi Success Rate phục hồi $\ge 95\%$ liên tục trong 2 chu kỳ).
7. **Gate 3 Acceptance:** Vượt qua toàn bộ tiêu chí khắt khe của **Gate 3** để tiến vào Milestone 4.

---

## 2. WORK BREAKDOWN STRUCTURE (WBS) MATRIX & EFFORT ALLOCATION

| Work Package | Mã Task | Tên Task Kỹ thuật | Phân loại | Ngân sách | Task Tiên quyết |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **WP 3.1: Micro-Batch & Data Quality** | `WBS-M3-T01` | PySpark `foreachBatch` Streaming Dispatcher Framework | `[D1]` | 4 h | `WBS-M2-T08` |
| | `WBS-M3-T02` | Schema Validation, Quarantine Routing & 5% Circuit Breaker | `[D1]` | 5 h | `WBS-M3-T01` |
| | `WBS-M3-T03` | Watermark (2-min) Deduplication & Late Event DLQ Sink | `[D1]` | 5 h | `WBS-M3-T02` |
| **WP 3.2: Silver Dual-Table & State Progression** | `WBS-M3-T04` | Append-Only History Table Sink (`silver_payment_events_history`) | `[D2]` | 4 h | `WBS-M3-T03` |
| | `WBS-M3-T05` | Pre-aggregated State Collapse & Delta MERGE (`silver_transactions_current`) | `[D1]` | 7 h | `WBS-M3-T04` |
| | `WBS-M3-T06` | Dynamic Timer & 5-minute Stuck Transaction Detector | `[D1]` | 4 h | `WBS-M3-T05` |
| | `WBS-M3-T07` | Terminal Conflict Resolution & `silver_lifecycle_exceptions` Branching | `[D2]` | 3 h | `WBS-M3-T05` |
| **WP 3.3: RT Metrics & Degradation Engine** | `WBS-M3-T08` | Multi-Window Aggregation Engine (1m & 5m Windows) | `[D1]` | 5 h | `WBS-M3-T05` |
| | `WBS-M3-T09` | Partner Degradation Alerting with 2-Window Hysteresis Recovery | `[D1]` | 5 h | `WBS-M3-T08` |
| | `WBS-M3-T10` | Delta Gold Sink & Serving View for `gold_rt_channel_metrics` | `[D2]` | 4 h | `WBS-M3-T09` |
| **WP 3.4: Drills & Gate 3 Audit** | `WBS-M3-T11` | Automated Real-Time Failure Drills (Poison, Stuck, Late Storms) | `[D1]` | 6 h | `WBS-M3-T06`, `T07`, `T10` |
| | `WBS-M3-T12` | Milestone 3 End-to-End Test Suite & Quality Gate 3 Sign-Off | `[D1]` | 6 h | `WBS-M3-T11` |
| **TỔNG CỘNG** | **12 Tasks** | **Toàn bộ Gói việc Milestone 3** | | **58 h** | *(Baseline 55h + 3h)* |

---

## 3. ĐẶC TẢ CHI TIẾT TỪNG TASK KỸ THUẬT (TASK SPECIFICATIONS)

### [WBS-M3-T01] PySpark `foreachBatch` Streaming Dispatcher Framework

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M2-T08` (Hoàn thành Gate 2).
- **Input Artifacts:**
  - Bảng Delta Bronze: `s3a://bronze/payment_events/`.
  - Spark Session Factory: `src/digital_payments_lakehouse/engine/spark_session.py`.
- **Detailed Implementation Steps:**
  1. Xây dựng module đọc streaming từ Delta Bronze trong `src/digital_payments_lakehouse/streaming/silver_dispatcher.py`:
     - Sử dụng `spark.readStream.format("delta").load("s3a://bronze/payment_events/")`.
     - Cấu hình `ignoreChanges = "false"` để đảm bảo chỉ đọc các append mới.
  2. Thiết kế mẫu Dispatcher `process_microbatch(batch_df: DataFrame, batch_id: int)`:
     - Tạo cấu trúc điều phối micro-batch theo dạng pipeline:
       `Extract Bronze Payload` $\rightarrow$ `Validate & Split (Valid / Poison / Late)` $\rightarrow$ `Write Quarantine / DLQ` $\rightarrow$ `Write History` $\rightarrow$ `Merge Current` $\rightarrow$ `Compute RT Metrics`.
     - Bọc toàn bộ micro-batch trong khối đo lường hiệu năng: Ghi nhận `batch_id`, số lượng input rows, thời gian xử lý micro-batch (ms) vào structured logger.
  3. Cấu hình streaming query trigger:
     - Thiết lập `.trigger(processingTime="15 seconds")`.
     - Quản lý checkpoint location chuyên biệt: `s3a://checkpoints/silver_payment_dispatcher/`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/silver_dispatcher.py`.
  - Test suite: `tests/unit/test_silver_dispatcher.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t01/`
  - **Artifacts:**
    - `logs/pytest_test_silver_dispatcher.log` (Log kết quả unit test dispatcher và pipeline flow).
    - `logs/microbatch_dispatcher_execution.log` (Log ghi nhận `batch_id`, input rows, execution time ms).
    - `screenshots/spark_ui_silver_foreachbatch.png` (Ảnh Spark UI hiển thị active foreachBatch streaming query).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Hàm `process_microbatch` nhận và phân rã thành công Streaming DataFrame thành batch DataFrame thông thường trên từng chu kỳ trigger 15s.
  - [ ] **AC-2:** Ghi log định dạng JSON có đầy đủ `batch_id`, `input_rows`, `execution_time_ms`.
  - [ ] **AC-3:** Khởi động và dừng luồng streaming không bị rò rỉ checkpoint hoặc uncommitted tasks.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_silver_dispatcher.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Xử lý quá nhiều tác vụ ghi bên trong một `foreachBatch` gây kéo dài thời gian micro-batch vượt quá 15s.
  - *Mitigation:* Cache DataFrame hợp lệ (`valid_df.persist()`) trước khi rẽ nhánh ghi vào History và Current, sau đó giải phóng bộ nhớ (`valid_df.unpersist()`) ở cuối batch.

---

### [WBS-M3-T02] Schema Validation, Quarantine Routing & 5% Circuit Breaker

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M3-T01`.
- **Input Artifacts:**
  - Pydantic schema `PaymentEvent` từ Milestone 1.
  - Cấu hình ngưỡng: `config/config.yaml` (`data_quality.circuit_breaker_error_rate = 0.05`, `circuit_breaker_min_batch_size = 50`).
- **Detailed Implementation Steps:**
  1. Xây dựng module kiểm định chất lượng dữ liệu trong `src/digital_payments_lakehouse/streaming/validator.py`:
     - Parse cột `raw_payload` (JSON string) bằng schema PySpark chuẩn.
     - Kiểm tra các quy tắc kỹ thuật nghiêm ngặt:
       * `amount_minor` bắt buộc kiểu `BIGINT` và $> 0$.
       * `event_id`, `transaction_id`, `user_id`, `merchant_id` không được phép `NULL` hoặc rỗng.
       * `event_type` phải nằm trong danh mục enum hợp lệ.
       * `event_time` phải là chuỗi timestamp hợp lệ.
  2. Triển khai cơ chế rẽ nhánh (Branching Logic):
     - Tách micro-batch thành hai tập: `valid_records_df` và `quarantine_records_df`.
     - Thêm các cột metadata lỗi vào quarantine: `rejection_reason`, `rejection_timestamp`, `failed_rule`.
  3. **Thực thi Invariant Circuit Breaker (5%):**
     - Đếm `total_count = batch_df.count()` và `error_count = quarantine_records_df.count()`.
     - Nếu `total_count >= 50` VÀ `(error_count / total_count) > 0.05`:
       * Ghi log mức độ `CRITICAL` với thông điệp: `"CIRCUIT_BREAKER_ACTIVATED: Error rate exceeds 5% threshold"`.
       * Ném ra ngoại lệ `CircuitBreakerTrippedException` để abort ngay lập tức micro-batch, không commit offset, ngăn chặn lan truyền dữ liệu bẩn.
  4. Ghi tập lỗi vào Delta table `s3a://silver/quarantine_events/` (Append-only) nếu tỷ lệ lỗi $\le 5\%$.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/validator.py`, `src/digital_payments_lakehouse/streaming/exceptions.py`.
  - Test suite: `tests/unit/test_circuit_breaker.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t02/`
  - **Artifacts:**
    - `logs/circuit_breaker_trip_test.log` (Log ghi nhận CircuitBreakerTrippedException và abort micro-batch khi lỗi > 5%).
    - `logs/pytest_test_circuit_breaker.log` (Log chạy unit test circuit breaker và validation rules).
    - `data_samples/quarantine_records_sample.jsonl` (Trích xuất các dòng poison records trong bảng `silver_quarantine_events` kèm `rejection_reason`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Tách chính xác 100% poison records (ví dụ `amount_minor < 0`) vào `quarantine_records_df`.
  - [ ] **AC-2:** Kích hoạt `CircuitBreakerTrippedException` và dừng job thành công khi tiêm micro-batch có 6 lỗi trên 100 bản ghi (6% > 5%).
  - [ ] **AC-3:** Không kích hoạt Circuit Breaker nếu tổng số bản ghi trong batch $< 50$ (dù tỷ lệ lỗi cao) để tránh false positives khi tải thấp.
  - [ ] **AC-4:** Bảng `s3a://silver/quarantine_events/` ghi nhận chính xác lý do loại bỏ của từng dòng lỗi.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_circuit_breaker.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lệnh `.count()` trên PySpark DataFrame gây ra 2 lần tính toán (action scan) làm chậm micro-batch.
  - *Mitigation:* Sử dụng biểu thức tổng hợp `selectExpr("count(1) as total", "sum(case when is_invalid then 1 else 0 end) as errors")` để lấy cả 2 chỉ số chỉ trong 1 lượt scan duy nhất.

---

### [WBS-M3-T03] Watermark (2-min) Deduplication & Late Event DLQ Sink

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M3-T02`.
- **Input Artifacts:**
  - `config/config.yaml` (`streaming_slo.watermark_lateness_seconds = 120`).
- **Detailed Implementation Steps:**
  1. Xây dựng module quản lý thời gian và lọc trùng trong `src/digital_payments_lakehouse/streaming/watermark_filter.py`:
     - Chuyển đổi chuỗi `event_time` sang kiểu `TimestampType` UTC.
     - Khai báo Watermark: `.withWatermark("event_time", "2 minutes")`.
     - Lọc trùng theo khóa: `.dropDuplicates(["event_id"])`.
  2. Tách luồng sự kiện đến trễ (Late Events DLQ Routing):
     - Do Watermark tích hợp của Spark mặc định drop bỏ dữ liệu trễ ngoài cửa sổ mà không thông báo, ta cài đặt một custom filter để so sánh `event_time` với watermark threshold hiện tại của micro-batch:
       * `is_late_event`: `event_time < (current_watermark_timestamp)`.
       * Nếu `is_late_event == True`: Định tuyến các bản ghi này vào tập `late_events_df`.
  3. Ghi dữ liệu trễ vào Delta Lake:
     - Lưu trữ tại `s3a://silver/late_events_dlq/` với chế độ Append-only.
     - Bổ sung metadata: `watermark_at_arrival`, `arrival_lateness_seconds`.
     - Dữ liệu tại DLQ này sẽ được Airflow DAG ở Milestone 4 đọc và hợp nhất định kỳ vào bảng tổng hợp.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/watermark_filter.py`.
  - Test suite: `tests/unit/test_watermark_dlq.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t03/`
  - **Artifacts:**
    - `logs/pytest_test_watermark_dlq.log` (Log chạy test lọc trùng trong 2m và tách dòng trễ > 2m).
    - `data_samples/late_events_dlq_sample.jsonl` (Trích xuất các dòng trễ chuyển vào `silver_late_events_dlq` kèm `arrival_lateness_seconds`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Loại bỏ 100% các sự kiện duplicate `event_id` phát sinh trong vòng 2 phút.
  - [ ] **AC-2:** Các sự kiện có `event_time` trễ $> 120$ giây so với watermark bị chặn khỏi luồng streaming chính và chuyển hướng 100% sang `silver_late_events_dlq`.
  - [ ] **AC-3:** Bảng `silver_late_events_dlq` ghi nhận đầy đủ bản ghi kèm chỉ số độ trễ `arrival_lateness_seconds`.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_watermark_dlq.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Watermark state trong bộ nhớ Spark Executor phình to nếu số lượng event_id quá lớn.
  - *Mitigation:* Cửa sổ watermark 2 phút là tương đối ngắn, Spark sẽ tự động dọn dẹp state store sau mỗi commit. Đảm bảo cấu hình RocksDB state store nếu quy mô scale lên 5M giao dịch.

---

### [WBS-M3-T04] Append-Only History Table Sink (`silver_payment_events_history`)

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M3-T03`.
- **Input Artifacts:**
  - Tập dữ liệu hợp lệ: `valid_events_df` từ `WBS-M3-T03`.
- **Detailed Implementation Steps:**
  1. Cài đặt Sink ghi lịch sử sự kiện trong `src/digital_payments_lakehouse/streaming/silver_history_writer.py`:
     - Nhận tập `valid_events_df` (đã qua lọc lỗi và lọc trùng).
     - Ghi xuống Delta table `s3a://silver/payment_events_history/` với outputMode `"append"`.
  2. Tối ưu hóa cấu trúc bảng History:
     - Phân vùng vật lý theo `event_date = to_date(event_time)` (theo ngày nghiệp vụ UTC).
     - Bổ sung cột thứ tự: `state_rank` (tính toán từ `event_type` dựa trên Monotonic Rank Table 1 $\rightarrow$ 6).
  3. Cấu hình tính bất biến (Immutability Invariant):
     - Bật tính năng kiểm tra chỉ chèn: Tuyệt đối không bao giờ thực hiện MERGE hoặc UPDATE trên bảng này.
     - Bảng phục vụ truy vấn Window functions (`LEAD`, `LAG`) để xác định khoảng thời gian tồn tại `valid_from` / `valid_to` của từng trạng thái mà không gây phân mảnh file nhỏ.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/silver_history_writer.py`.
  - Bảng Delta: `s3a://silver/payment_events_history/`.
  - Test suite: `tests/unit/test_silver_history.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t04/`
  - **Artifacts:**
    - `logs/pytest_test_silver_history.log` (Log chạy test append-only sink và tính toán `state_rank`).
    - `data_samples/silver_history_delta_sample.json` (Trích xuất mẫu bảng `silver_payment_events_history` chứng minh append-only và cột `state_rank` 1->6).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Ghi thành công 100% sự kiện hợp lệ vào `silver_payment_events_history` ở chế độ thuần Append.
  - [ ] **AC-2:** Mỗi bản ghi có đầy đủ cột `state_rank` nguyên (từ 1 đến 6).
  - [ ] **AC-3:** Phân vùng đúng theo `event_date`.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_silver_history.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Ghi đồng thời vào bảng History và bảng Current làm xung đột lock trên hệ thống lưu trữ S3A.
  - *Mitigation:* Hai bảng nằm ở hai đường dẫn độc lập trên MinIO (`s3a://silver/payment_events_history/` và `s3a://silver/transactions_current/`), Delta Lake transaction log hoàn toàn tách biệt.

---

### [WBS-M3-T05] Pre-aggregated State Collapse & Delta MERGE (`silver_transactions_current`)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 7 hours
- **Prerequisites / Dependencies:** `WBS-M3-T04`.
- **Input Artifacts:**
  - Quy chuẩn Monotonic State Rank từ `docs/DE_Project2_Master_Handoff.md`.
  - Target Delta table: `s3a://silver/transactions_current/`.
- **Detailed Implementation Steps:**
  1. Cài đặt thuật toán Pre-aggregation (Collapse) trong `src/digital_payments_lakehouse/streaming/current_state_merger.py`:
     - **Vấn đề cốt lõi:** Trong một micro-batch, một giao dịch có thể phát sinh nhiều sự kiện (ví dụ cả `INITIATED`, `AUTHORIZED`, `PROCESSING` cùng cập bến). Nếu thực hiện lệnh Delta `MERGE INTO` trực tiếp sẽ gây lỗi nghiêm trọng: `DeltaIllegalStateException: Cannot perform MERGE as multiple source rows matched the same target row`.
     - **Giải pháp kỹ thuật (In-Memory Collapse):**
       * Sử dụng hàm cửa sổ PySpark `row_number().over(Window.partitionBy("transaction_id").orderBy(col("state_rank").desc(), col("event_time").desc()))`.
       * Lọc lấy dòng có `rank = 1` đại diện cho trạng thái tiến triển cao nhất của giao dịch trong micro-batch đó.
  2. Thực thi câu lệnh Delta `MERGE INTO`:
     ```python
     delta_table = DeltaTable.forPath(
         spark, "s3a://silver/transactions_current/"
     )
     (
         delta_table.alias("target")
         .merge(
             source_collapsed_df.alias("source"),
             "target.transaction_id = source.transaction_id",
         )
         .whenMatchedUpdate(
             condition="source.state_rank > target.current_state_rank",  # MONOTONIC PROGRESSION LOCK
             set={
                 "current_lifecycle_state": "source.event_type",
                 "current_state_rank": "source.state_rank",
                 "last_progress_event_time": "source.event_time",
                 "updated_at": "current_timestamp()",
             },
         )
         .whenNotMatchedInsert(
             values={
                 "transaction_id": "source.transaction_id",
                 "user_id": "source.user_id",
                 "merchant_id": "source.merchant_id",
                 "payment_channel": "source.payment_channel",
                 "payment_method": "source.payment_method",
                 "amount_minor": "source.amount_minor",
                 "currency": "source.currency",
                 "current_lifecycle_state": "source.event_type",
                 "current_state_rank": "source.state_rank",
                 "operational_status": "lit('NORMAL')",
                 "last_progress_event_time": "source.event_time",
                 "was_ever_stuck": "lit(false)",
                 "created_at": "source.event_time",
                 "updated_at": "current_timestamp()",
             }
         )
         .execute()
     )
```
  3. Khóa bảo vệ Monotonic (Monotonic Progression Lock):
     - Nếu một sự kiện bậc thấp đến muộn hơn sự kiện bậc cao (ví dụ `AUTHORIZED (3)` đến sau `PROCESSING (4)`), điều kiện `source.state_rank > target.current_state_rank` sẽ không thỏa mãn $\rightarrow$ Target giữ nguyên trạng thái bậc 4, không bị lùi về bậc 3.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/current_state_merger.py`.
  - Test suite: `tests/unit/test_current_state_merger.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t05/`
  - **Artifacts:**
    - `logs/pytest_test_current_state_merger.log` (Log chạy test collapse in-memory và MERGE không xung đột).
    - `logs/delta_merge_plan_explain.txt` (Kế hoạch thực thi EXPLAIN của lệnh MERGE INTO trên Delta Lake).
    - `data_samples/silver_transactions_current_sample.json` (Trích xuất bảng current chứng minh duy nhất 1 dòng / transaction_id và bảo toàn monotonic rank).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Bảng `silver_transactions_current` duy trì chính xác **1 dòng duy nhất trên mỗi `transaction_id`** (Grain tuyệt đối).
  - [ ] **AC-2:** Không bao giờ xảy ra lỗi "multiple source rows matched" dù micro-batch chứa 5 sự kiện của cùng 1 transaction.
  - [ ] **AC-3:** Trạng thái giao dịch không bao giờ bị thụt lùi khi nhận sự kiện bậc thấp đến muộn.
  - [ ] **AC-4:** Số tiền `amount_minor` được bảo toàn nguyên vẹn kiểu số nguyên lớn.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_current_state_merger.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Hiệu năng câu lệnh `MERGE INTO` giảm sút khi bảng `silver_transactions_current` đạt quy mô hàng triệu dòng.
  - *Mitigation:* Đảm bảo bảng Current được partition hợp lý hoặc cấu hình Z-ORDER theo `transaction_id` định kỳ.

---

### [WBS-M3-T06] Dynamic Timer & 5-minute Stuck Transaction Detector

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M3-T05`.
- **Input Artifacts:**
  - `config/config.yaml` (`streaming_slo.stuck_timeout_seconds = 300`).
  - Target table: `silver_transactions_current`.
- **Detailed Implementation Steps:**
  1. Xây dựng module phát hiện giao dịch kẹt trong `src/digital_payments_lakehouse/streaming/stuck_detector.py`:
     - Định nghĩa logic Dynamic Timer:
       * Giao dịch ở trạng thái `current_lifecycle_state == 'PROCESSING'` (Rank 4).
       * Thời gian kể từ tiến triển gần nhất: `deadline = last_progress_event_time + 5 minutes`.
       * Nếu `current_watermark_time > deadline` VÀ `operational_status == 'NORMAL'`:
         $\rightarrow$ Cập nhật `operational_status = 'STUCK'`.
         $\rightarrow$ Đánh dấu cờ lịch sử `was_ever_stuck = TRUE`.
         $\rightarrow$ **GIỮ NGUYÊN** trạng thái nghiệp vụ `current_lifecycle_state = 'PROCESSING'`.
  2. Xử lý gỡ kẹt khi nhận Terminal Event đến muộn (Late Terminal Unsticking):
     - Khi nhận được sự kiện kết thúc hợp lệ (`COMPLETED`, `FAILED`, `CANCELLED`):
       * Cập nhật `current_lifecycle_state` sang trạng thái kết thúc tương ứng (Rank 5).
       * Gỡ cờ vận hành: `operational_status = 'NORMAL'`.
       * **BẮT BUỘC DUY TRÌ** cờ `was_ever_stuck = TRUE` để phục vụ báo cáo phân tích hiệu năng và SLA vận hành.
  3. Tích hợp quét định kỳ:
     - Chạy lệnh quét và cập nhật trạng thái `STUCK` trên bảng Current sau mỗi $N$ micro-batches (hoặc theo micro-batch trigger).
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/stuck_detector.py`.
  - Test suite: `tests/unit/test_stuck_detector.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t06/`
  - **Artifacts:**
    - `logs/pytest_test_stuck_detector.log` (Log chạy test phát hiện kẹt sau 300s và gỡ cờ khi có terminal muộn).
    - `logs/dynamic_timer_stuck_audit.log` (Log quét bảng current và gắn nhãn operational_status = STUCK).
    - `data_samples/stuck_transactions_sample.json` (Trích xuất các dòng có `operational_status = STUCK`, `lifecycle_state = PROCESSING` và cờ `was_ever_stuck = true`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Giao dịch nằm ở `PROCESSING` quá 300 giây được tự động gán cờ `operational_status = 'STUCK'`.
  - [ ] **AC-2:** Trạng thái nghiệp vụ gốc `current_lifecycle_state` vẫn giữ nguyên là `PROCESSING` (không bị ghi đè thành STUCK).
  - [ ] **AC-3:** Khi nhận terminal event muộn, cờ `operational_status` trở về `NORMAL` nhưng `was_ever_stuck` vẫn là `TRUE`.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_stuck_detector.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lệch múi giờ giữa đồng hồ máy chủ tính stuck và `event_time` của sự kiện.
  - *Mitigation:* Toàn bộ phép so sánh thời gian kẹt phải tính dựa trên Watermark Event-Time của luồng dữ liệu, không dùng đồng hồ `datetime.now()` của máy host.

---

### [WBS-M3-T07] Terminal Conflict Resolution & `silver_lifecycle_exceptions` Branching

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M3-T05`.
- **Input Artifacts:**
  - Yêu cầu Q14/Q45 trong `docs/DE_Project2_Master_Handoff.md`.
- **Detailed Implementation Steps:**
  1. Cài đặt module phát hiện xung đột trạng thái kết thúc trong `src/digital_payments_lakehouse/streaming/conflict_resolver.py`:
     - Trường hợp xung đột: Một giao dịch đã ở Terminal State (Rank 5: `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`) lại nhận tiếp một Terminal State khác đối kháng (ví dụ: đã `COMPLETED` thành công mà 10 phút sau lại nhận thêm event `FAILED` từ một kênh đối tác lỗi).
  2. Logic xử lý bất biến (Terminal Lock Invariant):
     - Tuyệt đối không ghi đè trạng thái Terminal hiện tại trên `silver_transactions_current`.
     - Chặn sự kiện đối kháng, gán nhãn `conflict_type = 'TERMINAL_STATE_CONFLICT'`.
     - Ghi nhận chi tiết vào bảng Delta ngoại lệ: `s3a://silver/lifecycle_exceptions/`.
  3. Cấu trúc bảng `silver_lifecycle_exceptions`:
     - `transaction_id`, `existing_state`, `conflicting_event_id`, `conflicting_event_type`, `conflicting_event_time`, `detected_at`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/conflict_resolver.py`.
  - Bảng Delta: `s3a://silver/lifecycle_exceptions/`.
  - Test suite: `tests/unit/test_conflict_resolver.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t07/`
  - **Artifacts:**
    - `logs/pytest_test_conflict_resolver.log` (Log chạy test chặn terminal state đối kháng).
    - `data_samples/silver_lifecycle_exceptions_sample.json` (Trích xuất các bản ghi xung đột terminal COMPLETED theo sau bởi FAILED trong bảng exceptions).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Giao dịch đã `COMPLETED` không bị thay đổi sang `FAILED` khi nhận event đối kháng.
  - [ ] **AC-2:** Bản ghi đối kháng được ghi nhận 100% vào `silver_lifecycle_exceptions` kèm đầy đủ thông tin đối chiếu.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_conflict_resolver.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Trường hợp hoàn tiền `REFUNDED` (Rank 6) bị nhầm lẫn là xung đột terminal với `COMPLETED` (Rank 5).
  - *Mitigation:* Trong State Machine, chuyển từ `COMPLETED (5)` sang `REFUNDED (6)` là chuyển đổi tiến bước hợp lệ, chỉ những chuyển đổi cùng cấp Rank 5 đối kháng mới bị tính là xung đột.

---

### [WBS-M3-T08] Multi-Window Aggregation Engine (1m & 5m Windows)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M3-T05`.
- **Input Artifacts:**
  - Cấu hình cửa sổ trong `config/config.yaml` (`short_window_seconds = 60`, `long_window_seconds = 300`).
- **Detailed Implementation Steps:**
  1. Cài đặt module tính toán số liệu thời gian thực trong `src/digital_payments_lakehouse/streaming/rt_metrics_aggregator.py`:
     - Nhận luồng sự kiện hợp lệ từ tầng Silver.
     - Sử dụng hàm cửa sổ trượt PySpark Structured Streaming:
       * Cửa sổ ngắn 1 phút: `.groupBy(window("event_time", "1 minute", "30 seconds"), col("payment_channel"))`.
       * Cửa sổ dài 5 phút: `.groupBy(window("event_time", "5 minutes", "1 minute"), col("payment_channel"))`.
  2. Tính toán các chỉ số vận hành cốt lõi:
     - `total_volume`: Tổng số giao dịch trong cửa sổ.
     - `success_volume`: Số giao dịch đạt `COMPLETED`.
     - `failed_volume`: Số giao dịch bị `FAILED`.
     - `success_rate`: `success_volume / total_volume` (tính dạng float tỷ lệ, ví dụ `0.95`).
     - `total_amount_minor`: Tổng giá trị tiền tệ trong cửa sổ (số nguyên `BIGINT`).
     - `p95_freshness_seconds`: Độ trễ end-to-end (`serving_time - event_time`) ở phân vị 95%.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/rt_metrics_aggregator.py`.
  - Test suite: `tests/unit/test_rt_metrics_aggregator.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t08/`
  - **Artifacts:**
    - `logs/pytest_test_rt_metrics_aggregator.log` (Log chạy test cửa sổ 1m/5m, success rate, p95 freshness).
    - `data_samples/rt_metrics_multi_window_sample.json` (Trích xuất kết quả metrics cửa sổ 1m và 5m chứng minh amount_minor BIGINT).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Tính toán chính xác `success_rate` cho từng `payment_channel` trên cả 2 cửa sổ 1m và 5m.
  - [ ] **AC-2:** Chỉ số `total_amount_minor` bảo toàn kiểu số nguyên 64-bit `BIGINT`, không bị tràn số.
  - [ ] **AC-3:** Đo lường chính xác `p95_freshness_seconds` cam kết đạt $\le 30s$.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_rt_metrics_aggregator.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Tính năng tính quantile (p95 freshness) chính xác tốn nhiều bộ nhớ trong streaming aggregation.
  - *Mitigation:* Sử dụng hàm xấp xỉ phân vị `percentile_approx(freshness, 0.95, 100)` tối ưu hóa tài nguyên.

---

### [WBS-M3-T09] Partner Degradation Alerting with 2-Window Hysteresis Recovery

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M3-T08`.
- **Input Artifacts:**
  - Cấu hình ngưỡng trong `config/config.yaml`:
    * `alert_success_rate_threshold = 0.90` (SR < 90%)
    * `alert_baseline_drop_threshold = 0.08` (Sụt giảm >= 8%)
    * `min_volume_short = 20`, `min_volume_long = 100`
    * `recovery_success_rate_threshold = 0.95`
    * `recovery_consecutive_windows = 2`
- **Detailed Implementation Steps:**
  1. Cài đặt thuật toán phát hiện suy giảm trong `src/digital_payments_lakehouse/streaming/partner_degradation.py`:
     - **Điều kiện kích hoạt cảnh báo suy giảm (`is_degraded = TRUE`):**
       * (Cửa sổ 1m: `volume >= 20` VÀ `success_rate < 0.90`) HOẶC
       * (Cửa sổ 5m: `volume >= 100` VÀ `success_rate < 0.90`) HOẶC
       * Sụt giảm đột ngột: `current_success_rate <= (channel_baseline_rate - 0.08)`.
  2. **Cơ chế chống rung cảnh báo (Hysteresis Recovery Engine):**
     - Để ngăn ngừa hiện tượng "nhấp nháy cảnh báo" (Alert Flapping: vừa bật cảnh báo ở phút thứ $N$, phút $N+1$ hơi tăng nhẹ lại tắt, phút $N+2$ lại bật), ta áp dụng cơ chế Hysteresis:
       * Khi một kênh thanh toán đang trong trạng thái `is_degraded = TRUE`:
       * Chỉ hủy bỏ cảnh báo (`is_degraded = FALSE`) khi và chỉ khi kênh đó đạt **`success_rate >= 0.95` duy trì liên tục trong ít nhất 2 cửa sổ đánh giá kế tiếp nhau**.
     - Duy trì bộ nhớ trạng thái phục hồi (`consecutive_healthy_windows: int`) cho từng đối tác.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/partner_degradation.py`.
  - Test suite: `tests/unit/test_partner_degradation.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t09/`
  - **Artifacts:**
    - `logs/pytest_test_partner_degradation.log` (Log chạy test cảnh báo sụt giảm và thuật toán chống rung Hysteresis).
    - `logs/hysteresis_recovery_audit.log` (Log chứng minh hệ thống từ chối gỡ cảnh báo khi mới chỉ đạt 95% ở 1 cửa sổ và chỉ gỡ sau 2 chu kỳ liên tiếp).
    - `data_samples/partner_degradation_events.json` (Trích xuất các bản ghi cảnh báo suy thoái).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Gắn cờ `is_degraded = TRUE` khi Success Rate giảm dưới 90% với khối lượng giao dịch đạt tối thiểu.
  - [ ] **AC-2:** Không phát cảnh báo nếu số lượng giao dịch $< 20$ txns/phút (tránh nhiễu dữ liệu thưa).
  - [ ] **AC-3 (Hysteresis Invariant):** Không gỡ cảnh báo nếu kênh chỉ đạt 95% trong 1 cửa sổ; bắt buộc duy trì $\ge 95\%$ đủ 2 cửa sổ liên tiếp mới chính thức xóa cờ suy giảm.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_partner_degradation.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Quản lý trạng thái 2 cửa sổ liên tiếp trong môi trường streaming phân tán phức tạp.
  - *Mitigation:* Lưu trữ trạng thái bộ đếm `consecutive_healthy_windows` trực tiếp trong bảng tổng hợp hoặc sử dụng state store nhỏ gọn theo từng `payment_channel`.

---

### [WBS-M3-T10] Delta Gold Sink & Serving View for `gold_rt_channel_metrics`

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M3-T09`.
- **Input Artifacts:**
  - DataFrame tổng hợp có gắn nhãn suy giảm từ `WBS-M3-T09`.
- **Detailed Implementation Steps:**
  1. Cài đặt Sink ghi bảng Gold trong `src/digital_payments_lakehouse/streaming/gold_metrics_writer.py`:
     - Ghi dữ liệu tổng hợp xuống Delta table `s3a://gold/rt_channel_metrics/`.
     - Sử dụng chế độ `complete` hoặc `update` (hoặc `append` kèm metadata `window_start`, `window_end`).
  2. Tạo View phục vụ Dashboard trực tiếp:
     - Tạo Spark SQL View `gold_view_rt_channel_latest` luôn đọc kết quả của cửa sổ gần nhất phục vụ Metabase truy vấn thời gian thực với độ trễ thấp ($< 1$ giây).
  3. Cấu hình tối ưu hóa lưu trữ tầng Gold:
     - Tự động dọn dẹp các partition cũ hơn 7 ngày để tối ưu không gian đĩa trên MinIO.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/streaming/gold_metrics_writer.py`.
  - Bảng Delta: `s3a://gold/rt_channel_metrics/`.
  - SQL View: `src/digital_payments_lakehouse/sql/views/gold_view_rt_channel_latest.sql`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t10/`
  - **Artifacts:**
    - `logs/pytest_test_gold_metrics_writer.log` (Log chạy test ghi bảng Gold Delta và tốc độ truy vấn View < 500ms).
    - `data_samples/gold_rt_channel_metrics_sample.json` (Trích xuất các dòng dữ liệu tầng Gold phục vụ Metabase).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Bảng `s3a://gold/rt_channel_metrics/` được cập nhật liên tục sau mỗi chu kỳ micro-batch.
  - [ ] **AC-2:** View `gold_view_rt_channel_latest` trả về đúng trạng thái mới nhất của tất cả các kênh thanh toán.
  - [ ] **AC-3:** Tốc độ truy vấn từ Metabase tới bảng Gold đạt $< 500ms$.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_gold_metrics_writer.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Bảng Gold bị ghi đè không nhất quán nếu có hai tiến trình streaming chạy song song.
  - *Mitigation:* Sử dụng giao thức commit ACID của Delta Lake để bảo đảm tính nhất quán tuần tự.

---

### [WBS-M3-T11] Automated Real-Time Failure Drills (Poison, Stuck, Late Storms)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M3-T06`, `WBS-M3-T07`, `WBS-M3-T10`.
- **Input Artifacts:**
  - Toàn bộ pipeline streaming Silver & Gold.
  - Simulator phát lỗi từ Milestone 1.
- **Detailed Implementation Steps:**
  1. Xây dựng bộ kịch bản diễn tập sự cố tự động trong `tests/integration/test_realtime_failure_drills.py`:
     - **Drill 1 (Poison Storm & Circuit Breaker):** Bơm 200 bản ghi trong đó có 20 bản ghi âm tiền (10% lỗi > 5%) $\rightarrow$ Xác nhận Streaming Job chủ động dừng, log `CRITICAL`, bảng Bronze bảo lưu dữ liệu nhưng Silver không bị nhiễm bẩn.
     - **Drill 2 (Out-of-Order Storm):** Bơm 500 giao dịch bị đảo lộn thứ tự event $\rightarrow$ Xác nhận bảng `silver_transactions_current` cập nhật đúng Monotonic State Rank, không có giao dịch nào bị lùi trạng thái.
     - **Drill 3 (Stuck Transaction Sweep):** Bơm 50 giao dịch bị ngắt terminal $\rightarrow$ Xác nhận sau 5 phút hệ thống gắn cờ `operational_status = 'STUCK'`; bơm tiếp terminal event muộn $\rightarrow$ Xác nhận cờ gỡ về `NORMAL` và `was_ever_stuck == TRUE`.
     - **Drill 4 (Terminal Conflict Catch):** Bơm 10 giao dịch nhận cả `COMPLETED` lẫn `FAILED` $\rightarrow$ Xác nhận bảng `silver_lifecycle_exceptions` ghi nhận đủ 10 trường hợp.
  2. Thu thập log và bằng chứng kiểm thử (Evidence Capture):
     - Ghi nhận metrics của từng drill vào báo cáo nghiệm thu kỹ thuật.
- **Deliverables / Output Artifacts:**
  - Kịch bản diễn tập: `tests/integration/test_realtime_failure_drills.py`.
  - Báo cáo kết quả diễn tập: `docs/reports/drill_milestone_3_failures.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t11/`
  - **Artifacts:**
    - `logs/realtime_failure_drills_execution.log` (Log chi tiết cả 4 Drills: Poison Storm, Out-of-Order Storm, Stuck Sweep, Terminal Conflict).
    - `logs/pytest_test_realtime_failure_drills.log` (Log kiểm thử tích hợp 100% pass).
    - `screenshots/spark_streaming_under_failure_drill.png` (Ảnh Spark UI trong lúc kích hoạt kịch bản tiêm lỗi bão).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** 100% cả 4 bài diễn tập sự cố (Drills 1 đến 4) đều đạt kết quả mong đợi (PASS).
  - [ ] **AC-2:** Không có hiện tượng rò rỉ dữ liệu lỗi vào bảng `silver_transactions_current`.
  - [ ] **AC-3:** Báo cáo diễn tập chứa đầy đủ log thực thi và bảng đối chiếu dữ liệu trước/sau sự cố.
- **Verification Method:**
  ```powershell
  uv run pytest tests/integration/test_realtime_failure_drills.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Kịch bản Stuck Sweep cần chờ 5 phút thật sự khiến thời gian chạy test suite quá dài.
  - *Mitigation:* Trong môi trường test, cho phép override tham số `stuck_timeout_seconds = 10` qua biến môi trường để bài test hoàn thành trong 15 giây mà vẫn kiểm chứng đúng 100% logic nghiệp vụ.

---

### [WBS-M3-T12] Milestone 3 End-to-End Test Suite & Quality Gate 3 Sign-Off

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M3-T11`.
- **Input Artifacts:**
  - Tiêu chí nghiệm thu Gate 3 trong `docs/wbs/README.md`.
  - Toàn bộ deliverables của Milestone 3.
- **Detailed Implementation Steps:**
  1. Viết kịch bản kiểm thử toàn diện cho Gate 3 (`tests/integration/test_milestone_3_gate.py`):
     - Chạy simulator liên tục 30 phút ở tốc độ 20 events/s.
     - Kiểm tra toàn bộ 5 bảng Delta Lake tầng Silver (`history`, `current`, `quarantine`, `late_dlq`, `exceptions`) và 1 bảng Gold (`rt_channel_metrics`).
     - Đo lường Freshness SLO thực tế của 10.000 giao dịch $\rightarrow$ Xác nhận p95 $\le 30s$ và p99 $\le 60s$.
  2. Lập hồ sơ ký duyệt chuyển pha Gate 3:
     - Tạo văn bản `docs/reports/gate_3_signoff.md`.
     - Đính kèm biểu đồ throughput và latency trích xuất từ Spark Streaming Query Listener.
- **Deliverables / Output Artifacts:**
  - Test suite nghiệm thu: `tests/integration/test_milestone_3_gate.py`.
  - Văn bản ký duyệt: `docs/reports/gate_3_signoff.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m3/wbs-m3-t12/`
  - **Artifacts:**
    - `logs/gate_3_e2e_verification.log` (Log chạy toàn bộ kịch bản kiểm thử Gate 3).
    - `logs/pytest_milestone_3_full.log` (Log kết quả chạy 100% test suite Milestone 3).
    - `logs/freshness_slo_benchmark.log` (Thống kê đo lường Freshness p95 <= 30s và p99 <= 60s trên 10,000 txns).
    - `screenshots/gold_rt_metrics_query.png` (Ảnh truy vấn View gold_view_rt_channel_latest).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** 100% tiêu chí của **Gate 3 (Milestone 3 $\rightarrow$ Milestone 4)** đạt chuẩn PASS.
  - [ ] **AC-2:** Freshness SLO đạt p95 $\le 30s$ trên tập dữ liệu kiểm thử 10.000 events.
  - [ ] **AC-3:** Không phát sinh bất kỳ lỗi `DeltaIllegalStateException` nào trong suốt quá trình chạy kiểm thử.
- **Verification Method:**
  ```powershell
  uv run pytest tests/integration/test_milestone_3_gate.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Tải CPU cao trên workstation khi vừa chạy Spark Streaming vừa chạy test assertions.
  - *Mitigation:* Giới hạn số lượng executor cores của Spark ở mức 4 cores, dành 2 cores cho tiến trình kiểm thử.

---

## 4. MILESTONE 3 QUALITY GATE 3 CHECKLIST (TIÊU CHÍ CHỐT SỔ M3)

> **Thư mục Lưu trữ Bằng chứng Nghiệm thu:** `docs/evidence/m3/gate_3_signoff/`  
> Toàn bộ artifacts nghiệm thu phải được tập hợp đầy đủ trước phiên review.

Trước khi ký nghiệm thu Milestone 3 để mở khóa Milestone 4 (Batch EOD & Reconciliation), TPM và Học viên bắt buộc phải kiểm tra đủ các mục sau:

- [ ] **Gate 3.1 (Circuit Breaker):** Kích hoạt ngắt job thành công khi tỷ lệ poison records $> 5\%$; rẽ nhánh chính xác poison lẻ vào `silver_quarantine_events`. *(Artifacts: `docs/evidence/m3/gate_3_signoff/circuit_breaker_abort_event.log`, `quarantine_sample.jsonl`)*
- [ ] **Gate 3.2 (Watermark & DLQ):** Khử trùng đúng `event_id` trong 2 phút; chuyển toàn bộ sự kiện trễ $> 2$ phút vào `silver_late_events_dlq`. *(Artifacts: `docs/evidence/m3/gate_3_signoff/late_dlq_sample.jsonl`, `watermark_dedup.log`)*
- [ ] **Gate 3.3 (Dual-Table Integrity):**
  - `silver_payment_events_history` ghi nhận đầy đủ lịch sử (Append-only).
  - `silver_transactions_current` đạt chuẩn 1 dòng / `transaction_id`, áp dụng pre-aggregation collapse loại bỏ hoàn toàn lỗi merge conflict. *(Artifacts: `docs/evidence/m3/gate_3_signoff/silver_preaggregated_merge_history.log`, `silver_current_sample.jsonl`)*
- [ ] **Gate 3.4 (Stuck & Conflict Detection):**
  - Giao dịch kẹt ở `PROCESSING > 5m` được gắn cờ `STUCK`; gỡ cờ khi có terminal muộn nhưng bảo lưu `was_ever_stuck = TRUE`. *(Artifacts: `docs/evidence/m3/gate_3_signoff/stuck_transaction_timer_audit.log`)*
  - Sự kiện Terminal đối kháng được ghi nhận vào `silver_lifecycle_exceptions`. *(Artifacts: `docs/evidence/m3/gate_3_signoff/lifecycle_exceptions_sample.json`)*
- [ ] **Gate 3.5 (Gold RT & Hysteresis):** Bảng `gold_rt_channel_metrics` cập nhật đều đặn; thuật toán Hysteresis yêu cầu đủ 2 chu kỳ $\ge 95\%$ mới gỡ cảnh báo suy giảm đối tác. *(Artifacts: `docs/evidence/m3/gate_3_signoff/hysteresis_recovery_audit.log`, `gold_rt_metrics_query.png`)*
- [ ] **Gate 3.6 (SLO Compliance):** Đạt cam kết Freshness p95 $\le 30s$ và p99 $\le 60s$. *(Artifacts: `docs/evidence/m3/gate_3_signoff/freshness_slo_p95_p99.txt`, `pytest_final_m3.log`)*

---

## 5. SCHEDULE RISK, CONTINGENCY BUFFER & REMINDER

> [!WARNING] Cảnh báo Nguy cơ Vượt Ngân sách Thời gian (Schedule Overrun Risk)
> - **Ngân sách Kế hoạch:** **55 giờ**.
> - **Khu vực có rủi ro cao gây trễ tiến độ:**
>   1. **`WBS-M3-T05` (Pre-aggregated MERGE):** Logic gộp dòng in-memory theo Monotonic Rank và kết hợp câu lệnh MERGE của Delta Lake có thể phát sinh các ca biên phức tạp liên quan đến thứ tự xuất hiện của các event trong micro-batch.
>   2. **`WBS-M3-T09` (Hysteresis State Tracking):** Việc lưu giữ trạng thái 2 cửa sổ liên tiếp để chống rung cảnh báo trong môi trường Structured Streaming đòi hỏi kiểm thử cẩn trọng để tránh leak bộ nhớ.
> - **Nguyên tắc Đánh đổi của TPM (Quality Leverage Policy):**
>   * Ưu tiên hàng đầu là **Tính đúng đắn của trạng thái giao dịch và cơ chế ngắt mạch Circuit Breaker**. Không được phép cắt giảm các bài test diễn tập lỗi (Failure Drills) vì đây là minh chứng cốt lõi cho năng lực Senior/Junior Data Engineer.

---

## 6. PHÂN TÍCH BIẾN ĐỘNG NGÂN SÁCH & GIẢI TRÌNH (EFFORT VARIANCE ANALYSIS & RATIONALE)

- **Số giờ biến động (Variance Hours):** **+3 giờ** (Tổng ngân sách điều chỉnh: **58 giờ**, so với baseline ban đầu là **55 giờ**).
- **Bảng chi tiết các Task phát sinh nỗ lực & Lý do Kỹ thuật:**

| Mã Task | Tên Task | Giờ Baseline | Giờ Điều chỉnh | Chênh lệch | Lý do Kỹ thuật Chi tiết |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `WBS-M3-T05` | Pre-aggregated MERGE into Current | 5 h | **7 h** | **+2 h** | Xử lý triệt để bài toán biên: Trong cùng 1 micro-batch xuất hiện đồng thời cả sự kiện hủy (`CANCELLED`) và sự kiện thành công (`COMPLETED`) đến từ 2 thread khác nhau; bắt buộc bổ sung thuật toán deterministic tie-breaker để đảm bảo kết quả merge luôn nhất quán. |
| `WBS-M3-T09` | Partner Degradation & Hysteresis | 4 h | **5 h** | **+1 h** | Cài đặt bộ nhớ trạng thái chống rung 2 chu kỳ liên tiếp trên PySpark Streaming yêu cầu viết thêm logic custom stateful accumulator hoặc Delta lookup để tránh lỗi out-of-sync giữa các micro-batches. |
| **TỔNG CỘNG** | | 55 h | **58 h** | **+3 h** | |

- **Tác động Tích cực đối với Nền tảng & Giá trị Hồ sơ Kỹ sư:**
  * Việc đầu tư thêm 3 giờ kỹ thuật giúp loại bỏ 100% nguy cơ xảy ra lỗi `DeltaIllegalStateException` — lỗi kinh điển mà hầu hết các kỹ sư streaming ít kinh nghiệm đều gặp phải khi thực hiện MERGE trên Delta Lake.
  * Cơ chế Hysteresis chống rung cảnh báo và Circuit Breaker tự ngắt là hai "điểm cộng vàng" trong mắt các Trưởng nhóm Kỹ thuật (Engineering Leads) khi phỏng vấn, khẳng định tư duy thiết kế hệ thống theo chuẩn Enterprise Production.

