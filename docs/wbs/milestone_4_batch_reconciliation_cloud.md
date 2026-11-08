# WBS MILESTONE 4 — BATCH EOD INGESTION, 3-WAY RECONCILIATION (DBT) & CLOUD SHOWCASE

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/00_wbs_dictionary_standard.md)  
> **Kế hoạch tổng thể & Quality Gates:** [`docs/wbs/README.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/README.md)  
> **Vai trò điều hành:** Senior Technical Project Manager (TPM) & Lead Data Platform Architect  
> **Ngân sách Kế hoạch Ban đầu (Baseline):** **55 giờ kỹ thuật**  
> **Ngân sách Điều chỉnh Thực tế (Adjusted):** **59 giờ kỹ thuật** *(Chi tiết tại Mục 6)*  
> **Trọng tâm Chuyển giao:** Hệ thống đối soát tài chính cuối ngày (Batch EOD T+1): Ingest đa nguồn (Sổ cái S2, Merchant SCD2 S3, Partner & Bank CSV S4), Airflow Completeness Gate & Grace Period SLA, dbt 3-Way Reconciliation (Level 1 $\rightarrow$ Level 3, 7 Break Types), lũy đẳng với `run_version`, Databricks Cloud Showcase và đối chiếu Ground Truth `G0` đạt $\ge 99.5\%$ Precision/Recall.

---

## 1. TỔNG QUAN PHẠM VI & MỤC TIÊU MILESTONE 4

Nếu Milestone 3 giải quyết bài toán vận hành Near-Real-Time, thì Milestone 4 là thước đo tối thượng về **tính chính xác tài chính (Financial Correctness)** của nền tảng. Xóa bỏ hoàn toàn tư duy sai lầm *"Batch là bản sao lưu của Streaming"*, luồng Batch EOD phục vụ mục đích nghiệp vụ kế toán độc lập: đối soát độc lập giữa 3 nguồn dữ liệu không tin tưởng lẫn nhau:
1. **Multi-Source Batch Ingestion:**
   - **S2 (Internal Wallet Ledger):** Sổ cái ví điện tử bất biến (Append-only CDC Fact), chỉ chấp nhận các bút toán `INSERT`, `ADJUSTMENT`, `REVERSAL`.
   - **S3 (Merchant Master Data):** Bảng chiều SCD Type 2 với `effective_from`, `effective_to`, `is_current` hỗ trợ Point-in-time join.
   - **S4 (External Partner Settlements & Bank Statements):** Các tệp CSV đối tác với kịch bản đa phiên bản (`v1` gửi thiếu, `v2` gửi bù/sửa lỗi) và mô phỏng lệch múi giờ (UTC vs UTC+07).
2. **Airflow Orchestration & Completeness Gate:**
   - Chốt sổ EOD tại mốc `23:59:59 UTC+07` (17:00 UTC). Airflow kích hoạt DAG đối soát lần đầu lúc `01:00 AM (T+1)`.
   - **Completeness Gate:** Kiểm tra sự hiện diện của đủ 3 nguồn. Nếu thiếu nguồn, tự động chờ đến `06:00 AM (T+1)` (Grace Period SLA). Quá hạn vẫn thiếu thì phát cảnh báo `CRITICAL_SLA_BREACH` và đánh dấu trạng thái run là `INCOMPLETE`.
   - Quản lý phiên bản tệp (`file_version`, `is_active`) thông qua metadata manifest để chỉ nạp phiên bản hợp lệ mới nhất.
3. **dbt 3-Way Reconciliation Engine:**
   - Phân cấp đối soát 3 tầng:
     * **Level 1 (Exact Match):** Khớp chính xác 1:1 theo các khóa tham chiếu giao dịch (`transaction_id` / `settlement_id` / `bank_reference`).
     * **Level 2 (Composite Match):** Khớp tổ hợp mờ (Số tiền + Ngày giao dịch + Cửa sổ thời gian $\pm 10$ phút + Kênh đối tác).
     * **Level 3 (Break Classification):** Tự động phân loại 100% các giao dịch không khớp vào **7 nhóm Break Types** chuẩn mực.
   - **Versioned Backfill & Replay:** Không xóa đè dữ liệu cũ khi rerun hoặc backfill; sinh `run_version` mới (`v1`, `v2`...). Tầng Presentation luôn đọc `run_version` cao nhất.
4. **Cloud Showcase Branch (Databricks + AWS S3):**
   - Đẩy dữ liệu batch lên AWS S3 và chạy `dbt-databricks` trên cụm Databricks Free Edition để lấy bằng chứng năng lực Cloud thực chiến cho CV.
5. **Ground Truth (`G0`) Benchmark:**
   - Tự động so sánh kết quả phân loại đối soát với file Ground Truth `G0` sinh ra từ Milestone 1, cam kết đạt **Precision $\ge 99.5\%$** và **Recall $\ge 99.5\%$**.
6. **Gate 4 Acceptance:** Vượt qua toàn bộ tiêu chí khắt khe của **Gate 4** để tiến vào Milestone 5.

---

## 2. WORK BREAKDOWN STRUCTURE (WBS) MATRIX & EFFORT ALLOCATION

| Work Package | Mã Task | Tên Task Kỹ thuật | Phân loại | Ngân sách | Task Tiên quyết |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **WP 4.1: Batch Sources Generation** | `WBS-M4-T01` | S2 Internal Wallet Ledger CDC Generator & Bronze Ingestion | `[D1]` | 4 h | `WBS-M1-T08` |
| | `WBS-M4-T02` | S3 Merchant Master SCD Type 2 Generator & Silver Dim Model | `[D1]` | 5 h | `WBS-M4-T01` |
| | `WBS-M4-T03` | S4 Partner CSV & Bank Statement Generator (Multi-Version Mock) | `[D1]` | 5 h | `WBS-M4-T01` |
| **WP 4.2: Airflow Orchestration & Gate** | `WBS-M4-T04` | Airflow Stack Configuration & MinIO/Spark/dbt Operators | `[D2]` | 4 h | `WBS-M2-T08` |
| | `WBS-M4-T05` | EOD Cutoff Sensor & Completeness Gate (06:00 AM SLA) | `[D1]` | 6 h | `WBS-M4-T03`, `T04` |
| | `WBS-M4-T06` | Version-Aware CSV Ingestion & Manifest Management DAG | `[D1]` | 5 h | `WBS-M4-T05` |
| **WP 4.3: dbt 3-Way Reconciliation Core** | `WBS-M4-T07` | dbt Project Layout, Sources Contract & Staging Models | `[D2]` | 4 h | `WBS-M4-T06` |
| | `WBS-M4-T08` | 3-Way Reconciliation Matching Engine (Level 1 & Level 2) | `[D1]` | 6 h | `WBS-M4-T07` |
| | `WBS-M4-T09` | Exception Categorization (7 Break Types) & `run_version` Model | `[D1]` | 6 h | `WBS-M4-T08` |
| **WP 4.4: Cloud Showcase & Gate 4 Audit** | `WBS-M4-T10` | Databricks Cloud Showcase Branch with AWS S3 & `dbt-databricks` | `[D1]` | 5 h | `WBS-M4-T09` |
| | `WBS-M4-T11` | Ground Truth (`G0`) Automated Precision & Recall Evaluator | `[D1]` | 5 h | `WBS-M4-T09`, `WBS-M1-T06` |
| | `WBS-M4-T12` | Milestone 4 Automated Test Suite & Quality Gate 4 Sign-Off | `[D1]` | 4 h | `WBS-M4-T10`, `T11` |
| **TỔNG CỘNG** | **12 Tasks** | **Toàn bộ Gói việc Milestone 4** | | **59 h** | *(Baseline 55h + 4h)* |

---

## 3. ĐẶC TẢ CHI TIẾT TỪNG TASK KỸ THUẬT (TASK SPECIFICATIONS)

### [WBS-M4-T01] S2 Internal Wallet Ledger CDC Generator & Bronze Ingestion

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M1-T08` (Hoàn thành Gate 1).
- **Input Artifacts:**
  - Đặc tả S2 trong `docs/DE_Project2_Master_Handoff.md`.
  - Schema Pydantic: `src/simulator/models.py`.
- **Detailed Implementation Steps:**
  1. Xây dựng module sinh dữ liệu sổ cái nội bộ trong `src/simulator/ledger_generator.py`:
     - Grain: 1 dòng cho mỗi bút toán ghi Nợ (`DEBIT`) hoặc Có (`CREDIT`).
     - Schema: `ledger_entry_id`, `transaction_id`, `account_id`, `entry_type` (`INSERT`, `ADJUSTMENT`, `REVERSAL`), `direction` (`DEBIT` / `CREDIT`), `amount_minor` (số nguyên `BIGINT`), `currency`, `entry_time`, `business_date`, `reference_id`, `created_at`.
  2. Thực thi Invariant Kế toán (Ledger Immutability Invariant):
     - Tuyệt đối cấm phát sinh sự kiện `UPDATE` hoặc `DELETE` vật lý trên sổ cái.
     - Nếu giao dịch có sự cố hoặc hủy, sinh cặp bản ghi đảo ngược `REVERSAL` cân bằng số dư.
  3. Ingestion xuống Bronze Lakehouse:
     - Ghi nhận vào topic Kafka `wallet.ledger.cdc` hoặc ghi trực tiếp dạng JSON Append-only xuống `s3a://bronze/wallet_ledger_cdc/`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/ledger_generator.py`.
  - Bảng Delta: `s3a://bronze/wallet_ledger_cdc/`.
  - Test suite: `tests/unit/test_ledger_generator.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t01/`
  - **Artifacts:**
    - `logs/pytest_test_ledger_generator.log` (Log chạy test quy tắc Double-entry DEBIT = CREDIT và amount_minor BIGINT).
    - `data_samples/wallet_ledger_sample.jsonl` (Mẫu dữ liệu sổ cái CDC chứng minh tính append-only và bút toán REVERSAL/ADJUSTMENT).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Dữ liệu sổ cái tuân thủ nguyên tắc Double-entry accounting: Tổng `DEBIT` bằng tổng `CREDIT` cho mỗi giao dịch thành công.
  - [ ] **AC-2:** Trường `amount_minor` là số nguyên `BIGINT`, không chứa giá trị âm hoặc số thực.
  - [ ] **AC-3:** 100% bản ghi được ghi Append-only vào `bronze_wallet_ledger_cdc`.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_ledger_generator.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Sai lệch múi giờ khi sinh trường `business_date` (phải tính theo `Asia/Ho_Chi_Minh`).
  - *Mitigation:* Ép hàm tính `business_date` chuyển đổi từ `entry_time (UTC)` sang timezone `Asia/Ho_Chi_Minh` trước khi lấy giá trị ngày `YYYY-MM-DD`.

---

### [WBS-M4-T02] S3 Merchant Master SCD Type 2 Generator & Silver Dim Model

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M4-T01`.
- **Input Artifacts:**
  - Đặc tả S3 trong `docs/DE_Project2_Master_Handoff.md`.
- **Detailed Implementation Steps:**
  1. Xây dựng module sinh dữ liệu Merchant Master trong `src/simulator/merchant_generator.py`:
     - Hỗ trợ mô hình **SCD Type 2**:
       * `merchant_id`, `merchant_name` (hỗ trợ tiếng Việt Unicode chuẩn NFC), `category`, `city`, `status`, `effective_from`, `effective_to`, `is_current`.
     - Giả lập kịch bản biến động thông tin: Một số merchant thay đổi `category` hoặc `city` tại các mốc thời gian giữa tháng, hệ thống đóng bản ghi cũ (`is_current = false`, cập nhật `effective_to`) và tạo bản ghi mới (`is_current = true`, `effective_to = NULL`).
  2. Xây dựng mô hình Silver Dimension bằng PySpark / SQL:
     - Lưu trữ tại `s3a://silver/dim_merchant_scd2/`.
     - Hỗ trợ hàm truy vấn Point-in-time join: Khớp đúng thông tin của merchant tại thời điểm giao dịch xảy ra (`txn.event_time BETWEEN dim.effective_from AND coalesce(dim.effective_to, '9999-12-31')`).
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/merchant_generator.py`.
  - Bảng Delta: `s3a://silver/dim_merchant_scd2/`.
  - Test suite: `tests/unit/test_merchant_scd2.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t02/`
  - **Artifacts:**
    - `logs/pytest_test_merchant_scd2.log` (Log chạy test không chồng lấn thời gian hiệu lực và test Point-in-time join).
    - `data_samples/dim_merchant_scd2_sample.json` (Trích xuất các dòng SCD2 có `effective_from`, `effective_to`, `is_current` và ký tự tiếng Việt UTF-8).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Bảng `dim_merchant_scd2` không có khoảng thời gian hiệu lực bị chồng lấn (No overlapping validity periods) cho cùng một `merchant_id`.
  - [ ] **AC-2:** Tên tiếng Việt có dấu được bảo toàn nguyên vẹn định dạng UTF-8.
  - [ ] **AC-3:** Point-in-time join trả về chính xác thuộc tính lịch sử tương ứng với `event_time`.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_merchant_scd2.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lỗi Late-arriving Dimension khi giao dịch phát sinh trước cả thời điểm merchant được tạo trong hệ thống.
  - *Mitigation:* Trong mô hình SCD2, tạo một bản ghi mặc định `effective_from = '1970-01-01'` với nhãn `category = 'UNKNOWN'` cho các trường hợp chưa kịp đồng bộ thông tin.

---

### [WBS-M4-T03] S4 Partner CSV & Bank Statement Generator (Multi-Version Mock)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M4-T01`.
- **Input Artifacts:**
  - Đặc tả S4 trong `docs/DE_Project2_Master_Handoff.md`.
  - Simulator Seed từ Milestone 1.
- **Detailed Implementation Steps:**
  1. Xây dựng module sinh tệp đối soát đối tác trong `src/simulator/partner_generator.py`:
     - **Partner A (Gateway Settlement CSV):** `transaction_id`, `settlement_id`, `settlement_time`, `amount`, `fee`, `net_amount`, `status`.
     - **Partner B (Bank Statement CSV):** `bank_reference`, `transaction_reference`, `value_date`, `amount`, `direction`, `fee`.
  2. Cài đặt kịch bản Đa phiên bản (Multi-Version Mock - Q19 / Q20 / Q39):
     - Sinh tệp phiên bản `v1` lúc 01:00 AM: Cố tình bỏ sót 5% giao dịch hoặc gửi sai số tiền ở một số dòng.
     - Sinh tệp phiên bản `v2` (Resend/Corrected) lúc 04:30 AM: Bổ sung các giao dịch thiếu và điều chỉnh số liệu đúng, kèm manifest metadata `file_version = 2`, `replaces_file = 'partner_a_settlement_20261015_v1.csv'`.
  3. Mô phỏng lệch múi giờ (Timezone Discrepancy Mock):
     - File Bank Statement chốt theo giờ UTC (`value_date`), trong khi Gateway chốt theo `UTC+07`, tạo ra các ca lệch biên ngày chốt sổ (Timing Mismatch).
  4. Xuất file ra thư mục nạp dữ liệu: `data/batch_sources/partner_settlements/` và `data/batch_sources/bank_statements/`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/partner_generator.py`.
  - Tệp mẫu CSV: `data/batch_sources/.../gateway_settlement_20261015_v1.csv`, `..._v2.csv`, `bank_statement_20261015.csv`.
  - Test suite: `tests/unit/test_partner_generator.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t03/`
  - **Artifacts:**
    - `logs/pytest_test_partner_generator.log` (Log chạy test sinh file đa phiên bản v1/v2 và lệch múi giờ).
    - `data_samples/partner_settlement_v1_v2_diff.txt` (So sánh chênh lệch giữa v1 gửi thiếu và v2 gửi bù sửa lỗi).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Sinh thành công các cặp file `v1` và `v2` có sự khác biệt rõ ràng về số lượng dòng và nội dung điều chỉnh.
  - [ ] **AC-2:** File CSV chứa đầy đủ các trường khớp với format đặc tả ngân hàng.
  - [ ] **AC-3:** Có các ca kiểm thử cố tình tạo ra Break tài chính: Lệch tiền (`AMOUNT_MISMATCH`), thiếu dòng (`MISSING_IN_PARTNER`, `MISSING_IN_BANK`).
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_partner_generator.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Định dạng ngày tháng trong file CSV bị lẫn lộn giữa `DD/MM/YYYY` và `YYYY-MM-DD`.
  - *Mitigation:* Chuẩn hóa định dạng xuất file CSV theo chuẩn ISO `YYYY-MM-DD HH:mm:ss`.

---

### [WBS-M4-T04] Airflow Stack Configuration & MinIO/Spark/dbt Connection Operators

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M2-T08`.
- **Input Artifacts:**
  - `docker/docker-compose.yml`.
  - Airflow 3 / 2.9 Docker image.
- **Detailed Implementation Steps:**
  1. Mở rộng `docker/docker-compose.yml` để tích hợp Apache Airflow:
     - Dịch vụ: `airflow-webserver`, `airflow-scheduler`, `airflow-init`.
     - Database backend: PostgreSQL container (`dp-postgres` port `5432`).
     - Volume mounts: Mount `dags/` vào `/opt/airflow/dags`, `plugins/` vào `/opt/airflow/plugins`, và mount project workspace để Airflow kích hoạt được `dbt` và script Spark.
  2. Cấu hình Connections & Hooks trong Airflow:
     - MinIO S3 Connection (`aws_default` trỏ tới `http://minio:9000`).
     - Spark Connection (`spark_default` trỏ tới cụm Spark local).
  3. Cài đặt các thư viện provider:
     - `apache-airflow-providers-apache-spark`, `apache-airflow-providers-amazon`, `dbt-core`, `dbt-spark`.
- **Deliverables / Output Artifacts:**
  - Cập nhật file: `docker/docker-compose.yml`.
  - Script khởi tạo: `scripts/airflow_init.sh` hoặc `.ps1`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t04/`
  - **Artifacts:**
    - `logs/airflow_init_and_startup.log` (Log khởi động cụm Airflow webserver, scheduler, postgres).
    - `screenshots/airflow_webserver_ui.png` (Ảnh Airflow UI `:8082` hiển thị các connections MinIO S3, Spark, dbt).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Airflow Webserver khởi động thành công, truy cập được tại `http://localhost:8088` (hoặc port 8085).
  - [ ] **AC-2:** Airflow Scheduler nhận diện đúng các DAGs trong thư mục `dags/` mà không có lỗi Import Error.
  - [ ] **AC-3:** Kết nối thành công từ Airflow Task tới MinIO S3 API qua `S3Hook`.
- **Verification Method:**
  ```powershell
  # Kiểm tra Airflow CLI trong container
  docker compose exec airflow-webserver airflow dags list
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Airflow chiếm dụng quá nhiều RAM khi chạy trên workstation cùng với Kafka và Spark.
  - *Mitigation:* Cấu hình `AIRFLOW__CORE__EXECUTOR: LocalExecutor` và giới hạn `parallelism = 4`, `max_active_tasks_per_dag = 2`.

---

### [WBS-M4-T05] EOD Cutoff Sensor & Completeness Gate (06:00 AM SLA)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M4-T03`, `WBS-M4-T04`.
- **Input Artifacts:**
  - Cấu hình Cutoff trong `config/config.yaml`:
    * `cutoff_time_local = "23:59:59"`
    * `initial_run_local = "01:00:00"`
    * `grace_period_deadline_local = "06:00:00"`
- **Detailed Implementation Steps:**
  1. Xây dựng Custom Airflow Sensor trong `dags/sensors/completeness_sensor.py`:
     - Kiểm tra sự hiện diện của đủ **3 nguồn dữ liệu bắt buộc** cho ngày đối soát `business_date = D`:
       1. Nguồn Sổ cái: Bảng `silver_wallet_ledger` đã hoàn tất chốt sổ ngày $D$.
       2. Nguồn Gateway: Đã có tệp đối soát của Gateway trong MinIO bucket `s3://partner-settlements/`.
       3. Nguồn Ngân hàng: Đã có tệp sao kê ngân hàng trong MinIO bucket `s3://bank-statements/`.
  2. **Thực thi Completeness Gate & Grace Period SLA (FR-REC-01..04, FR-REC-10):**
     - DAG được lên lịch chạy lúc **01:00 AM T+1**.
     - Nếu phát hiện **thiếu bất kỳ nguồn nào**:
       * Không ném lỗi ngay lập tức mà chuyển sang cơ chế **Grace Period (Chờ gia hạn)**.
       * Cấu hình Sensor `mode='reschedule'`, `poke_interval=1800` (thử lại mỗi 30 phút).
     - **Mốc chốt chặn 06:00 AM (Hard Deadline):**
       * Nếu đến 06:00 AM mà vẫn thiếu nguồn:
       * Sensor timeout $\rightarrow$ Chuyển tiếp tới task `mark_run_incomplete`.
       * Ghi nhận trạng thái phiên đối soát là `INCOMPLETE` vào bảng metadata `gold_eod_reconciliation_runs`.
       * Gửi cảnh báo mức độ cao `CRITICAL_SLA_BREACH` tới log/webhook.
       * Tuyệt đối không cho phép tiến hành đối soát và cấm báo cáo thành công giả mạo.
- **Deliverables / Output Artifacts:**
  - Mã nguồn Sensor: `dags/sensors/completeness_sensor.py`.
  - DAG: `dags/dag_eod_completeness_gate.py`.
  - Test suite: `tests/unit/test_completeness_sensor.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t05/`
  - **Artifacts:**
    - `logs/pytest_test_completeness_sensor.log` (Log test sensor 3 nguồn và chế độ reschedule).
    - `logs/airflow_sensor_grace_period_timeout.log` (Log sensor kích hoạt timeout lúc 06:00 AM và đánh dấu INCOMPLETE).
    - `screenshots/airflow_dag_completeness_gate_view.png` (Ảnh giao diện DAG Tree View hiển thị nhánh Completeness Gate).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Sensor phát hiện đủ 3 nguồn sẽ cho phép DAG chạy ngay lập tức lúc 01:00 AM.
  - [ ] **AC-2:** Khi cố tình giấu file Bank CSV, Sensor tự động chờ và thử lại mỗi 30 phút mà không chiếm dụng worker slot (`mode='reschedule'`).
  - [ ] **AC-3:** Quá 06:00 AM, task thất bại đúng quy chuẩn, đánh dấu phiên chạy là `INCOMPLETE` và kích hoạt cảnh báo SLA Breach.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_completeness_sensor.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Sensor ở chế độ `poke` làm cạn kiệt pool kết nối của Airflow.
  - *Mitigation:* Bắt buộc dùng `mode='reschedule'` để giải phóng worker task trong khoảng thời gian chờ.

---

### [WBS-M4-T06] Version-Aware CSV Ingestion & Manifest Management DAG

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M4-T05`.
- **Input Artifacts:**
  - Tệp CSV đa phiên bản từ `WBS-M4-T03`.
- **Detailed Implementation Steps:**
  1. Xây dựng module quản lý Manifest trong `src/digital_payments_lakehouse/batch/manifest_manager.py`:
     - Lưu trữ metadata của từng file tiếp nhận vào Delta table `s3a://bronze/file_ingestion_manifest/`:
       * `file_id`, `source_type` (`GATEWAY` / `BANK`), `file_name`, `business_date`, `file_version: int`, `file_size_bytes`, `record_count`, `checksum_sha256`, `is_active: bool`, `received_at`.
  2. Thuật toán Phân giải Phiên bản (Version Resolution Algorithm):
     - Khi nhận file mới:
       * Kiểm tra `checksum_sha256`: Nếu trùng hash với file đã nhận $\rightarrow$ Đánh dấu `DUPLICATE_FILE`, bỏ qua không nạp lại.
       * Nếu khác hash nhưng cùng `business_date` và cùng đối tác:
         $\rightarrow$ Xác định đây là phiên bản gửi đè sửa lỗi (`v2`).
         $\rightarrow$ Nạp file `v2` vào Bronze với `file_version = 2`.
         $\rightarrow$ Cập nhật bản ghi manifest của file `v1` cũ thành `is_active = FALSE`, và đặt file `v2` mới thành `is_active = TRUE`.
  3. Tích hợp PySpark Ingestion Task:
     - Đọc nội dung CSV của phiên bản Active và ghi Append-only xuống `s3a://bronze/partner_settlements/` và `s3a://bronze/bank_statements/`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/batch/manifest_manager.py`.
  - DAG Task: `dags/tasks/task_ingest_partner_files.py`.
  - Test suite: `tests/unit/test_manifest_manager.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t06/`
  - **Artifacts:**
    - `logs/pytest_test_manifest_manager.log` (Log chạy test phân giải phiên bản file v1/v2 và hash checksum).
    - `data_samples/file_manifest_registry_sample.json` (Trích xuất bảng manifest ghi nhận file version, hash SHA-256 và cờ `is_active`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Nhận diện và bỏ qua tệp trùng lặp hoàn toàn dựa trên checksum SHA-256.
  - [ ] **AC-2:** Khi nhận file `v2`, hệ thống cập nhật đúng cờ `is_active = TRUE` cho `v2` và hủy kích hoạt `v1`.
  - [ ] **AC-3:** Toàn bộ lịch sử các file đều được bảo lưu Append-only (không xóa file `v1` vật lý trên S3/MinIO).
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_manifest_manager.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Tên file không tuân thủ quy tắc đặt tên khiến parser không trích xuất được `file_version`.
  - *Mitigation:* Nếu không trích xuất được version từ tên file, đọc metadata header bên trong file hoặc tự động tăng version dựa trên mốc thời gian nhận file.

---

### [WBS-M4-T07] dbt Project Layout, Sources Contract & Staging Models

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M4-T06`.
- **Input Artifacts:**
  - Các bảng Delta tại Bronze và Silver.
  - Cấu hình dbt: `dbt_project.yml`, `profiles.yml`.
- **Detailed Implementation Steps:**
  1. Khởi tạo cấu trúc thư mục dbt trong `dbt/`:
     ```
     dbt/
       ├── dbt_project.yml
       ├── profiles.yml
       ├── models/
       │   ├── staging/           # stg_*.sql
       │   ├── intermediate/      # int_*.sql
       │   └── marts/             # fct_*, dim_*
       ├── tests/                 # Singular test queries
       └── macros/                # Custom matching macros
     ```
  2. Khai báo `sources.yml` kết nối trực tiếp tới các bảng Delta Lake trên MinIO:
     - Sources: `bronze_wallet_ledger`, `bronze_partner_settlements`, `bronze_bank_statements`, `silver_transactions_current`.
     - Định nghĩa Data Contracts và kiểm tra kiểu dữ liệu (`data_type: bigint` cho `amount_minor`).
  3. Viết các Staging Models (`stg_*.sql`):
     - `stg_wallet_ledger.sql`: Lọc các bản ghi hợp lệ, chuẩn hóa kiểu dữ liệu.
     - `stg_partner_settlements.sql`: Chỉ lọc lấy các bản ghi thuộc tệp có `is_active = TRUE`.
     - `stg_bank_statements.sql`: Chuẩn hóa số tiền và tham chiếu giao dịch.
- **Deliverables / Output Artifacts:**
  - Cấu hình dbt: `dbt/dbt_project.yml`, `dbt/profiles.yml`.
  - Models: `dbt/models/staging/stg_wallet_ledger.sql`, `stg_partner_settlements.sql`, `stg_bank_statements.sql`.
  - Tests: `dbt/models/staging/schema.yml`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t07/`
  - **Artifacts:**
    - `logs/dbt_debug_connection.log` (Log lệnh `dbt debug` kết nối thành công catalog Spark/Delta).
    - `logs/dbt_run_staging.log` (Log lệnh `dbt run --select staging`).
    - `logs/dbt_test_staging.log` (Log lệnh `dbt test --select staging` pass 100% tests).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Lệnh `dbt debug` kết nối thành công tới Spark/Delta Lake Catalog.
  - [ ] **AC-2:** Lệnh `dbt run --select staging` biên dịch và thực thi 100% models không lỗi.
  - [ ] **AC-3:** Lệnh `dbt test --select staging` pass toàn bộ generic tests (`not_null`, `unique`, `relationships`).
- **Verification Method:**
  ```powershell
  cd dbt
  dbt debug
  dbt run --select staging
  dbt test --select staging
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* `dbt-spark` kết nối Thrift Server bị timeout hoặc thiếu driver Hive Metastore.
  - *Mitigation:* Cấu hình `method: session` hoặc sử dụng ODBC/Thrift kết nối trực tiếp tới Spark container port 10000.

---

### [WBS-M4-T08] 3-Way Reconciliation Matching Engine (Level 1 & Level 2)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M4-T07`.
- **Input Artifacts:**
  - Staging models từ `WBS-M4-T07`.
  - Quy chuẩn Matching Hierarchy trong `docs/DE_Project2_Master_Handoff.md`.
- **Detailed Implementation Steps:**
  1. Xây dựng Intermediate Model Level 1 trong `dbt/models/intermediate/int_reconciliation_level1_exact.sql`:
     - **Quy tắc Level 1 (Khớp chính xác 1:1):**
       * Khớp khóa chính: `ledger.transaction_id = partner.transaction_id = bank.transaction_reference`.
       * Khớp tuyệt đối số tiền: `ledger.amount_minor = partner.amount_minor = bank.amount_minor`.
       * Đánh dấu: `match_status = 'MATCHED_LEVEL_1'`.
  2. Xây dựng Intermediate Model Level 2 trong `dbt/models/intermediate/int_reconciliation_level2_composite.sql`:
     - Nhận tập dữ liệu chưa khớp (Unmatched) từ Level 1.
     - **Quy tắc Level 2 (Khớp tổ hợp mờ - Composite Matching):**
       * Khi khóa tham chiếu bị khuyết hoặc lệch nhẹ format:
       * Khớp theo tổ hợp 4 thuộc tính:
         1. `amount_minor`: Khớp chính xác 100% số tiền.
         2. `payment_channel`: Khớp kênh đối tác.
         3. `business_date`: Khớp cùng ngày nghiệp vụ.
         4. `event_time window`: Thời điểm phát sinh cách nhau không quá $\pm 10$ phút (`abs(unix_timestamp(ledger.time) - unix_timestamp(partner.time)) <= 600`).
       * Đánh dấu: `match_status = 'MATCHED_LEVEL_2_COMPOSITE'`.
- **Deliverables / Output Artifacts:**
  - SQL Models: `dbt/models/intermediate/int_reconciliation_level1_exact.sql`, `int_reconciliation_level2_composite.sql`.
  - Test suite: `dbt/tests/assert_no_false_positive_level2_matches.sql`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t08/`
  - **Artifacts:**
    - `logs/dbt_run_intermediate.log` (Log lệnh `dbt run --select intermediate` thực thi Level 1 và Level 2).
    - `logs/dbt_test_intermediate.log` (Log chạy singular test kiểm tra không khớp chéo false positive).
    - `data_samples/reconciliation_matched_sample.csv` (Trích xuất các dòng khớp chính xác Level 1 và khớp tổ hợp Level 2).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Level 1 khớp chính xác $\ge 90\%$ các giao dịch chuẩn thông thường.
  - [ ] **AC-2:** Level 2 khớp thành công các giao dịch bị mất transaction_id nhưng đủ điều kiện thời gian và số tiền.
  - [ ] **AC-3:** Không sinh ra bất kỳ quan hệ khớp Cartesian (1 nhiều) trong câu lệnh matching.
- **Verification Method:**
  ```powershell
  cd dbt
  dbt run --select intermediate
  dbt test --select intermediate
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Phép so sánh cửa sổ thời gian $\pm 10$ phút gây ra khớp chéo giữa 2 giao dịch có cùng số tiền phát sinh gần nhau.
  - *Mitigation:* Bổ sung ràng buộc phụ: Chỉ cho phép khớp Level 2 nếu trong cửa sổ đó tồn tại duy nhất một cặp bản ghi ứng viên (Unique candidate pair lock).

---

### [WBS-M4-T09] Exception Categorization (7 Break Types) & `run_version` Model

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M4-T08`.
- **Input Artifacts:**
  - Kết quả khớp từ `WBS-M4-T08`.
  - Danh mục 7 Break Types trong SSOT.
- **Detailed Implementation Steps:**
  1. Xây dựng Mart Model phân loại lỗi trong `dbt/models/marts/fct_reconciliation_breaks.sql`:
     - Tự động gắn nhãn 100% các dòng không khớp vào đúng **7 nhóm Break Types chuẩn mực**:
       1. `MATCHED`: Khớp hoàn toàn cả 3 nguồn.
       2. `MISSING_IN_PARTNER`: Có trong Ledger và Bank nhưng không có trong Partner Settlement CSV.
       3. `MISSING_IN_BANK`: Có trong Ledger và Partner nhưng tiền chưa về tài khoản ngân hàng.
       4. `ORPHAN_EXTERNAL`: Có trong Partner hoặc Bank nhưng không tìm thấy trong Sổ cái nội bộ.
       5. `DUPLICATE`: Giao dịch bị ghi nhận 2 lần trên một nguồn độc lập.
       6. `AMOUNT_MISMATCH`: Khớp mã giao dịch nhưng số tiền `amount_minor` giữa các nguồn bị chênh lệch.
       7. `TIMING_CUTOFF_MISMATCH`: Giao dịch phát sinh cận biên 23:59:59 bị đối tác đẩy sang ngày hôm sau.
  2. **Thực thi Cơ chế Versioned Backfill / Replay (`run_version`):**
     - Lưu trữ kết quả đối soát tại bảng `s3a://gold/eod_reconciliation_details/`.
     - Mỗi lần chạy đối soát (chạy lần đầu, chạy lại sau khi nhận file v2, hoặc backfill ngày T-5), dbt sinh một `run_id` duy nhất và gán số thứ tự phiên bản tăng dần: `run_version: int` (`v1`, `v2`, `v3`...).
     - Tuyệt đối không xóa đè dữ liệu đối soát của các phiên trước đó (Append-only Run History).
  3. Tạo Reporting View `gold_view_reconciliation_active`:
     - Sử dụng SQL Window function `row_number().over(partitionBy("business_date").orderBy(col("run_version").desc()))` để View luôn luôn trỏ về kết quả của phiên đối soát mới nhất.
- **Deliverables / Output Artifacts:**
  - Models: `dbt/models/marts/fct_reconciliation_breaks.sql`, `fct_reconciliation_runs.sql`.
  - SQL View: `dbt/models/marts/gold_view_reconciliation_active.sql`.
  - Test suite: `dbt/tests/assert_break_types_complete_coverage.sql`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t09/`
  - **Artifacts:**
    - `logs/dbt_run_marts.log` (Log lệnh `dbt run --select marts` phân loại 7 Break Types và gán `run_version`).
    - `logs/dbt_test_marts.log` (Log dbt test bao phủ 100% Break Types).
    - `data_samples/break_types_distribution_summary.csv` (Bảng thống kê tỷ lệ phân bổ của 7 nhóm Break Types).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** 100% giao dịch sau đối soát được phân loại chính xác vào một trong 7 Break Types.
  - [ ] **AC-2:** Chạy lại đối soát cùng một ngày với file `v2` sinh ra `run_version = 2` mà không làm mất dữ liệu của `run_version = 1`.
  - [ ] **AC-3:** View `gold_view_reconciliation_active` phản ánh đúng số liệu của `run_version = 2`.
- **Verification Method:**
  ```powershell
  cd dbt
  dbt run --select marts
  dbt test --select marts
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Sai lệch thứ tự phân loại dẫn đến một giao dịch vừa bị tính là AMOUNT_MISMATCH vừa bị tính là TIMING_CUTOFF_MISMATCH.
  - *Mitigation:* Thiết lập cấu trúc `CASE WHEN` tuần tự với độ ưu tiên rõ ràng theo quy chuẩn nghiệp vụ tài chính.

---

### [WBS-M4-T10] Databricks Cloud Showcase Branch with AWS S3 & `dbt-databricks`

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M4-T09`.
- **Input Artifacts:**
  - Tài khoản AWS Free Tier và Databricks Community / Free Edition.
  - Thư viện `dbt-databricks`.
- **Detailed Implementation Steps:**
  1. Cấu hình nhánh Cloud Showcase (Cloud Track):
     - Tạo bucket AWS S3: `s3://dp-lakehouse-showcase/`.
     - Viết script đồng bộ một tập dữ liệu batch mẫu (100.000 dòng) từ MinIO local lên AWS S3: `scripts/sync_to_s3.py`.
  2. Thiết lập kết nối Databricks:
     - Tạo Serverless SQL Warehouse hoặc Cluster nhỏ trên Databricks Free Edition.
     - Cấu hình file `dbt/profiles.yml` thêm target `databricks`:
       ```yaml
       databricks:
         target: dev
         outputs:
           dev:
             type: databricks
             host: "{{ env_var('DATABRICKS_HOST') }}"
             http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
             token: "{{ env_var('DATABRICKS_TOKEN') }}"
             schema: reconciliation_gold
```
  3. Thực thi và thu thập bằng chứng Cloud:
     - Chạy `dbt run --target databricks` và `dbt test --target databricks`.
     - Chụp ảnh màn hình Databricks SQL Lineage Graph, Execution DAG, và query editor hiển thị bảng `fct_reconciliation_breaks`.
- **Deliverables / Output Artifacts:**
  - Script đồng bộ: `scripts/sync_to_s3.py`.
  - Cấu hình dbt: `dbt/profiles.yml` (target `databricks`).
  - Bằng chứng Cloud: `docs/evidence/databricks_lineage.png`, `docs/evidence/databricks_run_log.txt`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t10/`
  - **Artifacts:**
    - `logs/s3_sync_execution.log` (Log script sync 100k dòng mẫu lên bucket AWS S3).
    - `logs/dbt_databricks_run.log` (Log lệnh `dbt run --target databricks` trên cụm Databricks).
    - `screenshots/databricks_job_run_lineage.png` (Ảnh giao diện Databricks hiển thị Lineage Graph).
    - `screenshots/databricks_sql_query_breaks.png` (Ảnh truy vấn bảng `fct_reconciliation_breaks` trên Databricks SQL Warehouse).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Script `sync_to_s3.py` đẩy thành công dữ liệu mẫu lên AWS S3 không tốn chi phí vượt Free Tier.
  - [ ] **AC-2:** Lệnh `dbt run --target databricks` chạy thành công 100% models trên Databricks Cloud.
  - [ ] **AC-3:** Thu thập đủ 2 ảnh chụp bằng chứng (Lineage & Query Result) để đưa vào CV Portfolio.
- **Verification Method:**
  ```powershell
  uv run python scripts/sync_to_s3.py
  cd dbt
  dbt run --target databricks --select marts
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Bị trừ tiền ngoài ý muốn trên AWS hoặc hết hạn Databricks Trial.
  - *Mitigation:* Chỉ sử dụng S3 Standard dung lượng nhỏ (< 1GB), bật AWS Billing Alarm 0 USD, và hoàn thành chụp bằng chứng trong vòng 1 phiên làm việc.

---

### [WBS-M4-T11] Ground Truth (`G0`) Automated Precision & Recall Evaluator

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M4-T09`, `WBS-M1-T06`.
- **Input Artifacts:**
  - File Ground Truth: `data/ground_truth/G0_events_seed_20260930.jsonl`.
  - Kết quả đối soát: Bảng `fct_reconciliation_breaks` tại Gold.
- **Detailed Implementation Steps:**
  1. Xây dựng module đánh giá độc lập trong `src/digital_payments_lakehouse/evaluation/g0_evaluator.py`:
     - Đọc tệp Ground Truth `G0` chứa thông tin thật về các lỗi đã tiêm (Duplicate, Missing, Amount Mismatch...).
     - Đọc kết quả phân loại từ bảng `fct_reconciliation_breaks`.
  2. Tính toán Ma trận nhầm lẫn (Confusion Matrix) cho từng loại Break:
     - **True Positive (TP):** Simulator tiêm lỗi $X$, dbt đối soát phát hiện đúng lỗi $X$.
     - **False Positive (FP):** Simulator không tiêm lỗi, nhưng dbt lại báo lỗi $X$ (báo động giả).
     - **False Negative (FN):** Simulator tiêm lỗi $X$, nhưng dbt lại báo MATCHED (bỏ lọt lỗi).
  3. Tính toán các chỉ số độ chính xác:
     - $\text{Precision} = \frac{TP}{TP + FP}$
     - $\text{Recall} = \frac{TP}{TP + FN}$
     - $F_1\text{-Score} = 2 \times \frac{\text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$
  4. Xuất Báo cáo Đánh giá Tự động:
     - Lưu tại `docs/reports/g0_reconciliation_evaluation.md`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/digital_payments_lakehouse/evaluation/g0_evaluator.py`.
  - Báo cáo kết quả: `docs/reports/g0_reconciliation_evaluation.md`.
  - Test suite: `tests/integration/test_g0_accuracy.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t11/`
  - **Artifacts:**
    - `logs/g0_precision_recall_eval_report.txt` (Log đo lường tự động Precision >= 99.5% và Recall >= 99.5%).
    - `logs/pytest_test_g0_accuracy.log` (Log kiểm thử tự động benchmark với file Ground Truth).
    - `data_samples/g0_confusion_matrix.json` (Ma trận nhầm lẫn chi tiết đối chiếu từng Break Type với Ground Truth G0).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Điểm **Precision toàn cục đạt $\ge 99.5\%$**.
  - [ ] **AC-2:** Điểm **Recall toàn cục đạt $\ge 99.5\%$**.
  - [ ] **AC-3:** Báo cáo xuất ra bảng chi tiết Precision/Recall cho từng nhóm trong số 7 Break Types.
- **Verification Method:**
  ```powershell
  uv run pytest tests/integration/test_g0_accuracy.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Sai lệch nhỏ trong định nghĩa Break Type giữa Simulator và dbt model khiến Recall bị tụt dưới 99%.
  - *Mitigation:* Chuẩn hóa chung một bộ từ điển `BreakType` Enum giữa `src/simulator/` và `dbt/`.

---

### [WBS-M4-T12] Milestone 4 Automated Test Suite & Quality Gate 4 Sign-Off

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M4-T10`, `WBS-M4-T11`.
- **Input Artifacts:**
  - Tiêu chí nghiệm thu Gate 4 trong `docs/wbs/README.md`.
  - Toàn bộ deliverables của Milestone 4.
- **Detailed Implementation Steps:**
  1. Xây dựng bài kiểm tra tích hợp toàn diện Gate 4 (`tests/integration/test_milestone_4_gate.py`):
     - Kích hoạt Airflow DAG chạy end-to-end qua CLI: Chốt EOD $\rightarrow$ Completeness Gate $\rightarrow$ Ingest CSVs $\rightarrow$ Kích hoạt `dbt run` $\rightarrow$ Kiểm tra kết quả trong Delta Gold.
     - Xác minh kịch bản nạp bù file `v2`: Số liệu đối soát cập nhật sang phiên bản mới mà không xóa đè lịch sử.
  2. Soạn thảo văn bản nghiệm thu Gate 4:
     - Tạo file `docs/reports/gate_4_signoff.md`.
     - Tổng hợp kết quả test, độ bao phủ dbt test và điểm đánh giá Ground Truth `G0`.
- **Deliverables / Output Artifacts:**
  - Test suite nghiệm thu: `tests/integration/test_milestone_4_gate.py`.
  - Văn bản ký duyệt: `docs/reports/gate_4_signoff.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m4/wbs-m4-t12/`
  - **Artifacts:**
    - `logs/gate_4_e2e_verification.log` (Log chạy toàn bộ kịch bản nghiệm thu E2E Milestone 4).
    - `logs/pytest_milestone_4_full.log` (Log kết quả chạy 100% test suite Milestone 4).
    - `data_samples/final_reconciliation_summary.csv` (Trích xuất kết quả đối soát cuối ngày).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** 100% tiêu chí của **Gate 4 (Milestone 4 $\rightarrow$ Milestone 5)** đạt chuẩn PASS.
  - [ ] **AC-2:** Luồng EOD chạy tự động hoàn toàn dưới sự điều phối của Airflow DAG.
  - [ ] **AC-3:** Đạt chứng chỉ độ chính xác Ground Truth với Precision/Recall $\ge 99.5\%$.
- **Verification Method:**
  ```powershell
  uv run pytest tests/integration/test_milestone_4_gate.py -v -s
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Airflow CLI trigger task trên Windows Docker đôi khi bị lag phản hồi.
  - *Mitigation:* Sử dụng Airflow REST API để trigger DAG và polling trạng thái qua HTTP endpoint.

---

## 4. MILESTONE 4 QUALITY GATE 4 CHECKLIST (TIÊU CHÍ CHỐT SỔ M4)

> **Thư mục Lưu trữ Bằng chứng Nghiệm thu:** `docs/evidence/m4/gate_4_signoff/`  
> Toàn bộ artifacts nghiệm thu phải được tập hợp đầy đủ trước phiên review.

Trước khi ký nghiệm thu Milestone 4 để bước vào Milestone 5 (Serving & Packaging), TPM và Học viên bắt buộc phải đánh dấu kiểm tra đủ toàn bộ các mục sau:

- [ ] **Gate 4.1 (Completeness Gate & SLA):** Airflow DAG phát hiện đúng sự hiện diện của 3 nguồn lúc 01:00 AM; tự động chờ đến 06:00 AM Grace Period SLA và báo động `CRITICAL_SLA_BREACH` khi thiếu file. *(Artifacts: `docs/evidence/m4/gate_4_signoff/airflow_completeness_gate.png`, `sla_timeout_log.txt`)*
- [ ] **Gate 4.2 (Version-Aware Ingestion):** Ingest thành công file phiên bản `v2`, hủy kích hoạt file `v1` cũ trong manifest nhưng bảo lưu 100% lịch sử file thô. *(Artifacts: `docs/evidence/m4/gate_4_signoff/file_manifest_history.json`)*
- [ ] **Gate 4.3 (3-Way Matching Hierarchy):** Thuật toán dbt khớp chính xác Level 1 và Level 2, phân loại không sót bản ghi nào vào 7 Break Types. *(Artifacts: `docs/evidence/m4/gate_4_signoff/reconciliation_breaks_summary.csv`)*
- [ ] **Gate 4.4 (Idempotency & run_version):** Chạy lại đối soát sinh `run_version` mới, không xóa đè dữ liệu cũ, View active trỏ về đúng phiên bản cao nhất. *(Artifacts: `docs/evidence/m4/gate_4_signoff/run_version_idempotency_audit.txt`)*
- [ ] **Gate 4.5 (Ground Truth Validation):** Độ chính xác đối soát đối chiếu với `G0` đạt **Precision $\ge 99.5\%$** và **Recall $\ge 99.5\%$**. *(Artifacts: `docs/evidence/m4/gate_4_signoff/g0_eval_report.md`, `confusion_matrix.json`)*
- [ ] **Gate 4.6 (Cloud Showcase):** Chạy thành công dbt model trên Databricks Free Edition kết nối AWS S3, thu thập đầy đủ ảnh chụp Lineage Graph. *(Artifacts: `docs/evidence/m4/gate_4_signoff/databricks_lineage.png`, `dbt_databricks_run.log`)*

---

## 5. SCHEDULE RISK, CONTINGENCY BUFFER & REMINDER

> [!WARNING] Cảnh báo Nguy cơ Vượt Ngân sách Thời gian (Schedule Overrun Risk)
> - **Ngân sách Kế hoạch:** **55 giờ**.
> - **Khu vực có rủi ro cao gây trễ tiến độ:**
>   1. **`WBS-M4-T08` & `WBS-M4-T09` (dbt Reconciliation Logic):** Việc xây dựng logic matching 3 bên ở Level 2 kết hợp xử lý 7 nhóm Break Types bằng SQL thuần có thể phát sinh nhiều ca kiểm thử sai lệch số tiền phức tạp.
>   2. **`WBS-M4-T10` (Cloud Showcase Setup):** Việc cấu hình mạng giữa AWS S3, IAM policies và token xác thực Databricks có thể làm mất thêm thời gian đối với học viên chưa có nhiều trải nghiệm Cloud.
> - **Nguyên tắc Đánh đổi của TPM (Quality Leverage Policy):**
>   * Ưu tiên hàng đầu là **Độ chính xác tài chính tuyệt đối ($\ge 99.5\%$ với `G0`) và bằng chứng Cloud Showcase trên Databricks**. Đây là hai "vũ khí sát thương cao nhất" trên CV giúp học viên vượt qua các vòng phỏng vấn kỹ thuật hóc búa tại các ngân hàng lớn.

---

## 6. PHÂN TÍCH BIẾN ĐỘNG NGÂN SÁCH & GIẢI TRÌNH (EFFORT VARIANCE ANALYSIS & RATIONALE)

- **Số giờ biến động (Variance Hours):** **+4 giờ** (Tổng ngân sách điều chỉnh: **59 giờ**, so với baseline ban đầu là **55 giờ**).
- **Bảng chi tiết các Task phát sinh nỗ lực & Lý do Kỹ thuật:**

| Mã Task | Tên Task | Giờ Baseline | Giờ Điều chỉnh | Chênh lệch | Lý do Kỹ thuật Chi tiết |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `WBS-M4-T05` | Completeness Gate & SLA | 4 h | **6 h** | **+2 h** | Xử lý cơ chế cảm biến Airflow sensor với chế độ `reschedule` đi kèm logic Grace Period 06:00 AM đòi hỏi viết test suite mô phỏng tiến trình thời gian phức tạp (time mocking) để kiểm tra không bị deadlock worker. |
| `WBS-M4-T09` | Exception Categorization & run_version | 5 h | **6 h** | **+1 h** | Đảm bảo tính toán toàn vẹn cho 7 Break Types bao quát các ca giao dịch mồ côi (Orphan) hai chiều và cơ chế sinh view đọc `run_version` cao nhất mà không bị giảm hiệu năng truy vấn. |
| `WBS-M4-T11` | G0 Precision / Recall Evaluator | 4 h | **5 h** | **+1 h** | Viết module sinh Confusion Matrix chi tiết cho từng loại lỗi riêng lẻ và tự động xuất biểu đồ trực quan Markdown giúp báo cáo nghiệm thu đạt chuẩn tài liệu Enterprise. |
| **TỔNG CỘNG** | | 55 h | **59 h** | **+4 h** | |

- **Tác động Tích cực đối với Nền tảng & Giá trị Hồ sơ Kỹ sư:**
  * Việc đầu tư thêm 4 giờ kỹ thuật mang lại một hệ thống đối soát tài chính đạt cấp độ sẵn sàng sản xuất (Production-grade), giải quyết thấu đáo vấn đề nan giải nhất của ngành thanh toán: tệp đối soát gửi lại (`v2`), kiểm soát SLA nghiêm ngặt và kiểm chứng độ chính xác định lượng bằng toán học trên Ground Truth.

