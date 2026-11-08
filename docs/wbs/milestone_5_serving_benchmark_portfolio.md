# WBS MILESTONE 5 — SERVING DASHBOARDS, SCALE BENCHMARK & PORTFOLIO PACKAGING

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/00_wbs_dictionary_standard.md)  
> **Kế hoạch tổng thể & Quality Gates:** [`docs/wbs/README.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/README.md)  
> **Vai trò điều hành:** Senior Technical Project Manager (TPM) & Lead Data Platform Architect  
> **Ngân sách Kế hoạch Ban đầu (Baseline):** **35 giờ kỹ thuật**  
> **Ngân sách Điều chỉnh Thực tế (Adjusted):** **37 giờ kỹ thuật** *(Chi tiết tại Mục 6)*  
> **Trọng tâm Chuyển giao:** Tầng phục vụ trực quan hóa (Metabase Serving), Kiểm thử chịu tải quy mô lớn (Scale Benchmark 1M–5M giao dịch), Tối ưu hóa lưu trữ Delta Lake (`OPTIMIZE`, `Z-ORDER`, `VACUUM`), Hoàn thiện tài liệu kiến trúc quốc tế (ADRs C02, C15, C18), README tiếng Anh chuẩn Enterprise và Đóng gói toàn diện hồ sơ năng lực (Portfolio Package) cho học viên.

---

## 1. TỔNG QUAN PHẠM VI & MỤC TIÊU MILESTONE 5

Milestone 5 là chặng về đích của toàn bộ dự án, biến các dòng code và bảng dữ liệu kỹ thuật thành một **sản phẩm hoàn chỉnh có thể tương tác trực quan và chứng minh năng lực thực chiến** trước các nhà tuyển dụng hàng đầu:
1. **Enterprise Serving Dashboards (Metabase):** Dựng 2 Dashboards phục vụ 2 nhóm người dùng chuyên biệt:
   - *Dashboard 1 (Payment Operations):* Giám sát luồng Near-Real-Time, tỷ lệ thành công theo kênh, độ trễ Freshness SLO, danh sách giao dịch bị kẹt `STUCK`, và cảnh báo suy giảm đối tác theo cơ chế Hysteresis.
   - *Dashboard 2 (Finance & Reconciliation):* Báo cáo kết quả đối soát EOD, trực quan hóa 7 Break Types, tổng số tiền chênh lệch, và khả năng drill-down chi tiết về từng giao dịch gốc.
2. **Scale Benchmark (1M – 5M Transactions):** Chạy bài toán kiểm thử sức chịu tải ở quy mô hàng triệu giao dịch, đo lường throughput xử lý của Spark và độ trễ ghi dữ liệu Delta Lake.
3. **Delta Lake Storage Engine Tuning:** Khắc phục bài toán bùng nổ file nhỏ (Small Files Problem) sinh ra từ micro-batch streaming bằng các câu lệnh `OPTIMIZE`, `Z-ORDER BY`, và thu hồi dung lượng đĩa bằng `VACUUM`.
4. **Architecture Decision Records (ADRs):** Soạn thảo 3 văn bản quyết định kiến trúc chuyên sâu bằng tiếng Anh chuẩn quốc tế cho các bài toán phân loại `[D3-ADR]`:
   - `ADR-C02`: Near-duplicate fuzzy matching trade-offs.
   - `ADR-C15`: Late financial adjustments as-of reporting architecture.
   - `ADR-C18`: PII masking & Column-level security design for payments lakehouse.
5. **Portfolio Packaging & One-Command Reproducibility:** Hoàn thiện file `README.md` chính của repository bằng tiếng Anh, tích hợp kiến trúc trực quan, hướng dẫn khởi động 1 câu lệnh (`docker compose up -d`), và bộ kịch bản tự tin bảo vệ dự án trong các vòng phỏng vấn kỹ thuật.
6. **Final Gate 5 Sign-Off:** Hoàn tất nghiệm thu toàn diện dự án.

---

## 2. WORK BREAKDOWN STRUCTURE (WBS) MATRIX & EFFORT ALLOCATION

| Work Package | Mã Task | Tên Task Kỹ thuật | Phân loại | Ngân sách | Task Tiên quyết |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **WP 5.1: Metabase Serving & Dashboards** | `WBS-M5-T01` | Metabase Deployment & Serving Engine Connection Handshake | `[D2]` | 3.0 h | `WBS-M3-T12`, `WBS-M4-T12` |
| | `WBS-M5-T02` | Dashboard 1: Payment Operations Near-Real-Time Monitoring | `[D1]` | 3.5 h | `WBS-M5-T01` |
| | `WBS-M5-T03` | Dashboard 2: Finance EOD 3-Way Reconciliation & Breaks | `[D1]` | 3.5 h | `WBS-M5-T01` |
| **WP 5.2: Benchmark & Storage Tuning** | `WBS-M5-T04` | 1M–5M Transactions Stress Test Benchmark Harness | `[D1]` | 5.0 h | `WBS-M5-T01` |
| | `WBS-M5-T05` | Delta Lake Storage Tuning (`OPTIMIZE`, `Z-ORDER`, `VACUUM`) | `[D1]` | 5.0 h | `WBS-M5-T04` |
| **WP 5.3: ADRs, README & Portfolio Packaging** | `WBS-M5-T06` | Formal Architecture Decision Records (`ADR C02`, `C15`, `C18`) | `[D3]` | 4.0 h | `WBS-M4-T12` |
| | `WBS-M5-T07` | Comprehensive Test Suite Hardening & Quarantine Audit | `[D1]` | 4.0 h | `WBS-M5-T05` |
| | `WBS-M5-T08` | Enterprise English README & One-Command Reproducibility | `[D1]` | 5.0 h | `WBS-M5-T06`, `T07` |
| | `WBS-M5-T09` | Final Project Sign-Off & Portfolio Interview Defense Kit | `[D1]` | 4.0 h | `WBS-M5-T08` |
| **TỔNG CỘNG** | **9 Tasks** | **Toàn bộ Gói việc Milestone 5** | | **37 h** | *(Baseline 35h + 2h)* |

---

## 3. ĐẶC TẢ CHI TIẾT TỪNG TASK KỸ THUẬT (TASK SPECIFICATIONS)

### [WBS-M5-T01] Metabase Deployment & Serving Engine Connection Handshake

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M3-T12`, `WBS-M4-T12` (Hoàn thành Gate 3 & Gate 4).
- **Input Artifacts:**
  - `docker/docker-compose.yml`.
  - Tầng Gold Delta Lake trên MinIO hoặc PostgreSQL serving proxy.
- **Detailed Implementation Steps:**
  1. Tích hợp Metabase vào `docker/docker-compose.yml`:
     - Image: `metabase/metabase:latest`.
     - Port mapping: `3000:3000`.
     - Cấu hình database metadata backend sử dụng PostgreSQL container (`dp-postgres`).
  2. Thiết lập cơ chế Serving Layer Handshake:
     - Phương án A (Trực tiếp qua Spark Thrift JDBC): Kết nối Metabase tới Apache Spark Thrift Server port 10000 để truy vấn trực tiếp các bảng Delta Lake `gold_rt_channel_metrics`, `fct_reconciliation_breaks`.
     - Phương án B (Serving Cache qua DuckDB / Postgres): Thiết lập job đồng bộ nhẹ từ Delta Gold sang PostgreSQL serving views để đảm bảo tốc độ phản hồi Dashboard đạt $< 200ms$.
  3. Tự động hóa cấu hình Metabase thông qua API (Metabase Setup Script):
     - Tạo sẵn user quản trị, database connection và sync schema tự động qua script Python `scripts/setup_metabase.py`.
- **Deliverables / Output Artifacts:**
  - Cập nhật file: `docker/docker-compose.yml`.
  - Script tự động hóa: `scripts/setup_metabase.py`.
  - Hướng dẫn kết nối: `docs/serving_setup.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t01/`
  - **Artifacts:**
    - `logs/metabase_setup_api.log` (Log chạy script API tạo user và cấu hình kết nối catalog Delta/Spark).
    - `screenshots/metabase_login_and_connection.png` (Ảnh giao diện Metabase kết nối thành công tới database tầng Gold).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Metabase Web UI truy cập được tại `http://localhost:3000`.
  - [ ] **AC-2:** Kết nối thành công tới cơ sở dữ liệu tầng Gold, đồng bộ đầy đủ danh sách các bảng và view.
  - [ ] **AC-3:** Thời gian thực thi một câu lệnh truy vấn mẫu trên bảng Gold trả về kết quả trong vòng $< 1$ giây.
- **Verification Method:**
  ```powershell
  # Chạy script setup Metabase tự động
  uv run python scripts/setup_metabase.py

  # Kiểm tra HTTP response của Metabase API
  powershell -Command "Invoke-RestMethod -Uri http://localhost:3000/api/health"
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Metabase đồng bộ schema định kỳ làm chậm tài nguyên của cụm Spark container.
  - *Mitigation:* Tắt tính năng tự động quét metadata chuyên sâu (deep schema scan) trong cài đặt của Metabase, chỉ quét thủ công khi có thay đổi cấu trúc bảng.

---

### [WBS-M5-T02] Dashboard 1: Payment Operations Near-Real-Time Monitoring

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3.5 hours
- **Prerequisites / Dependencies:** `WBS-M5-T01`.
- **Input Artifacts:**
  - Bảng Delta: `gold_rt_channel_metrics` và `silver_transactions_current`.
- **Detailed Implementation Steps:**
  1. Xây dựng Dashboard 1 trên Metabase (`Payment Operations Command Center`):
     - **Thẻ KPI Vận hành (Top Cards):**
       * Total Transaction Volume (1m / 5m).
       * Overall Success Rate (%) kèm mã màu (Xanh nếu $\ge 95\%$, Đỏ nếu $< 90\%$).
       * End-to-End Freshness p95 (Giây, cam kết $\le 30s$).
       * Số lượng giao dịch đang bị kẹt (`Current STUCK Count`).
     - **Biểu đồ Chuỗi thời gian (Time-series Charts):**
       * Throughput (TPS) theo từng kênh thanh toán (`BANK_A`, `GATEWAY_A`, `GATEWAY_B`).
       * Tỷ lệ thành công theo thời gian thực (Success Rate Trend).
     - **Khu vực Cảnh báo Đối tác (Partner Degradation Alert Banner):**
       * Danh sách các đối tác đang bị gắn cờ `is_degraded = TRUE`.
       * Hiển thị số chu kỳ phục hồi liên tiếp (`consecutive_healthy_windows`) theo cơ chế Hysteresis.
     - **Bảng Drill-down Giao dịch Kẹt (Live Stuck Transactions):**
       * Liệt kê chi tiết các giao dịch có `operational_status = 'STUCK'`: `transaction_id`, `amount_minor`, `payment_channel`, `last_progress_event_time`, `duration_in_processing`.
  2. Xuất cấu hình Dashboard dạng JSON: `dashboards/metabase_payment_ops.json`.
- **Deliverables / Output Artifacts:**
  - Metabase Dashboard ID: 1 (`Payment Operations Monitoring`).
  - File cấu hình xuất: `dashboards/metabase_payment_ops.json`.
  - Ảnh chụp màn hình: `docs/evidence/dashboard_payment_ops.png`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t02/`
  - **Artifacts:**
    - `screenshots/metabase_payment_ops_dashboard.png` (Ảnh chụp toàn cảnh Dashboard 1: KPI cards, TPS charts, Stuck transactions table, Hysteresis alert banner).
    - `data_samples/metabase_payment_ops_export.json` (File JSON cấu hình dashboard).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Dashboard tự động làm mới (Auto-refresh) mỗi 15–30 giây mà không bị đơ trình duyệt.
  - [ ] **AC-2:** Hiển thị trực quan cảnh báo khi kích hoạt kịch bản Partner Degradation.
  - [ ] **AC-3:** Bảng giao dịch kẹt phản ánh đúng danh sách các giao dịch bị kẹt quá 5 phút.
- **Verification Method:**
  ```powershell
  # Kiểm tra file export Dashboard tồn tại và hợp lệ
  uv run python -m json.tool dashboards/metabase_payment_ops.json > $null
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lượng dữ liệu streaming liên tục làm trình duyệt bị tràn bộ nhớ khi bật auto-refresh quá nhanh.
  - *Mitigation:* Truy vấn Dashboard thông qua View giới hạn trong khoảng thời gian 2 giờ gần nhất (`WHERE window_start >= current_timestamp() - INTERVAL 2 HOURS`).

---

### [WBS-M5-T03] Dashboard 2: Finance EOD 3-Way Reconciliation & Breaks

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3.5 hours
- **Prerequisites / Dependencies:** `WBS-M5-T01`.
- **Input Artifacts:**
  - Bảng dbt Marts: `fct_reconciliation_breaks`, `fct_reconciliation_runs`.
  - View: `gold_view_reconciliation_active`.
- **Detailed Implementation Steps:**
  1. Xây dựng Dashboard 2 trên Metabase (`Finance & EOD Reconciliation Hub`):
     - **Thẻ Trạng thái Phiên Đối soát (Run Status Cards):**
       * Ngày kinh doanh (`business_date`).
       * Phiên đối soát hiện hành (`run_version: v1, v2...`).
       * Trạng thái phiên: `RECONCILED` (Xanh) / `COMPLETED_WITH_BREAKS` (Vàng) / `INCOMPLETE` (Đỏ).
       * Tổng giá trị chênh lệch tài chính (`Total Break Amount Minor` quy đổi sang VND).
     - **Biểu đồ Phân bổ Sai lệch (Break Distribution Breakdown):**
       * Biểu đồ tròn/thanh phân bố tỷ lệ của **7 Break Types** (`MATCHED`, `MISSING_IN_PARTNER`, `MISSING_IN_BANK`, `ORPHAN_EXTERNAL`, `DUPLICATE`, `AMOUNT_MISMATCH`, `TIMING_CUTOFF_MISMATCH`).
     - **Bảng Tra cứu Chi tiết Sai lệch (Break Investigation Table):**
       * Cho phép lọc theo `break_type` và `payment_channel`.
       * Hiển thị chi tiết từng giao dịch: `transaction_id`, số tiền trên Sổ cái, số tiền trên Gateway, số tiền trên Sao kê ngân hàng, và số tiền chênh lệch (`variance_amount`).
  2. Xuất cấu hình Dashboard dạng JSON: `dashboards/metabase_finance_reconciliation.json`.
- **Deliverables / Output Artifacts:**
  - Metabase Dashboard ID: 2 (`Finance EOD Reconciliation`).
  - File cấu hình xuất: `dashboards/metabase_finance_reconciliation.json`.
  - Ảnh chụp màn hình: `docs/evidence/dashboard_finance_reconciliation.png`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t03/`
  - **Artifacts:**
    - `screenshots/metabase_finance_reconciliation_dashboard.png` (Ảnh chụp toàn cảnh Dashboard 2: Run status cards, Break distribution chart, Drill-down variance table).
    - `data_samples/metabase_finance_reconciliation_export.json` (File JSON cấu hình dashboard).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Dashboard hiển thị chính xác tổng số tiền chênh lệch của từng loại Break.
  - [ ] **AC-2:** Bộ lọc (Filter) theo `business_date` và `run_version` hoạt động mượt mà.
  - [ ] **AC-3:** Cung cấp đầy đủ thông tin để kiểm toán viên tài chính có thể drill-down tới từng bút toán gốc.
- **Verification Method:**
  ```powershell
  uv run python -m json.tool dashboards/metabase_finance_reconciliation.json > $null
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Số tiền `amount_minor` dạng integer lớn bị hiển thị nhầm lẫn nếu không có định dạng tiền tệ.
  - *Mitigation:* Sử dụng tính năng formatting của Metabase để hiển thị đơn vị VND có dấu chấm phân cách hàng nghìn.

---

### [WBS-M5-T04] 1M–5M Transactions Stress Test Benchmark Harness

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M5-T01`.
- **Input Artifacts:**
  - Simulator từ Milestone 1.
  - Toàn bộ pipeline Streaming & Batch.
- **Detailed Implementation Steps:**
  1. Xây dựng Benchmark Runner trong `src/digital_payments_lakehouse/benchmark/stress_runner.py`:
     - Thiết kế 3 kịch bản tải trọng định lượng:
       * **Kịch bản 1 (Baseline):** 100.000 giao dịch (~500.000 events) chạy trong 10 phút.
       * **Kịch bản 2 (Stress Test):** 1.000.000 giao dịch (~5.000.000 events) bơm ở tốc độ tối đa của máy trạm.
       * **Kịch bản 3 (Peak Burst):** Bơm dồn dập 500 events/giây trong 2 phút để đo lường độ trễ hàng đợi Kafka và khả năng co giãn của Spark micro-batch.
  2. Thu thập và đo lường các chỉ số hiệu năng (Performance Profiling):
     - Throughput trung bình (Events/giây và Transactions/giây).
     - Micro-batch processing time và scheduling delay qua Spark Query Listener.
     - Mức tiêu thụ tài nguyên phần cứng (CPU utilization, RAM consumption, Disk I/O).
  3. Lập Báo cáo Hiệu năng Benchmark:
     - Tạo file `docs/reports/benchmark_scale_1m_5m.md` đính kèm bảng số liệu và biểu đồ so sánh.
- **Deliverables / Output Artifacts:**
  - Mã nguồn runner: `src/digital_payments_lakehouse/benchmark/stress_runner.py`.
  - Báo cáo benchmark: `docs/reports/benchmark_scale_1m_5m.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t04/`
  - **Artifacts:**
    - `logs/benchmark_1m_throughput_run.log` (Log chi tiết bài test chạy 1,000,000 giao dịch).
    - `logs/pytest_test_scale_benchmark.log` (Log chạy kiểm thử tích hợp benchmark).
    - `data_samples/benchmark_scale_metrics.json` (Số liệu Throughput TPS, scheduling delay và RAM/CPU usage).
    - `screenshots/spark_ui_1m_benchmark_streaming.png` (Ảnh Spark UI trong lúc chạy tải 1M txns).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Hệ thống hoàn thành bài kiểm thử chịu tải 1.000.000 giao dịch mà không bị crash, Out Of Memory (OOM) hay rớt container.
  - [ ] **AC-2:** Ghi nhận đầy đủ thông số Throughput, p95 Latency và RAM utilization vào báo cáo.
  - [ ] **AC-3:** Xác nhận tính toàn vẹn dữ liệu: Số lượng dòng ghi nhận tại Delta Bronze và Silver khớp chính xác 100% với số lượng sinh ra.
- **Verification Method:**
  ```powershell
  # Chạy bài benchmark 100k giao dịch (Test nhanh)
  uv run python -m src.digital_payments_lakehouse.benchmark.stress_runner --count 100000 --workers 4
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Đĩa SSD máy trạm bị đầy khi sinh 5M giao dịch kèm logs.
  - *Mitigation:* Tự động xóa dữ liệu thô Kafka sau khi benchmark hoàn tất và cấu hình retention 24h trong thời gian test.

---

### [WBS-M5-T05] Delta Lake Storage Tuning (`OPTIMIZE`, `Z-ORDER`, `VACUUM`)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M5-T04`.
- **Input Artifacts:**
  - Các bảng Delta sau khi nạp dữ liệu benchmark: Bronze, Silver, Gold.
- **Detailed Implementation Steps:**
  1. Phân tích hiện trạng lưu trữ (Small Files Problem Analysis):
     - Sử dụng script `scripts/inspect_bronze.py` để đo lường:
       * Tổng số lượng file Parquet sinh ra trong từng partition.
       * Kích thước trung bình của mỗi file (thường chỉ vài KB đến vài MB do micro-batch 10s).
  2. Thực thi tối ưu hóa lưu trữ (Compaction & Clustering):
     - Viết kịch bản bảo trì định kỳ trong `src/digital_payments_lakehouse/utils/delta_maintenance.py`:
       * **Compaction (`OPTIMIZE`):** Gộp các file nhỏ thành các file lớn kích thước tối ưu (~128MB đến 256MB).
       * **Multidimensional Clustering (`Z-ORDER BY`):**
         - Bảng `silver_transactions_current`: `Z-ORDER BY (transaction_id, merchant_id)`.
         - Bảng `gold_rt_channel_metrics`: `Z-ORDER BY (payment_channel, window_start)`.
         - Bảng `fct_reconciliation_breaks`: `Z-ORDER BY (business_date, break_type)`.
       * **Thu hồi không gian đĩa (`VACUUM`):** Dọn dẹp các file rác cũ hơn thời gian lưu giữ (retention threshold), cấu hình `spark.databricks.delta.vacuum.parallelDelete.enabled = true`.
  3. Đo lường hiệu quả trước và sau khi tối ưu:
     - So sánh tốc độ thực thi câu lệnh truy vấn lọc (Point lookup và Range query) trước và sau khi Z-ORDER $\rightarrow$ Chứng minh thời gian truy vấn giảm từ $3\times$ đến $10\times$ nhờ kỹ thuật Data Skipping.
- **Deliverables / Output Artifacts:**
  - Mã nguồn bảo trì: `src/digital_payments_lakehouse/utils/delta_maintenance.py`.
  - Báo cáo tối ưu hóa: `docs/reports/delta_storage_optimization.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t05/`
  - **Artifacts:**
    - `logs/delta_optimize_vacuum_execution.log` (Log lệnh OPTIMIZE, Z-ORDER và VACUUM).
    - `data_samples/storage_tuning_metrics_before_after.txt` (Bảng so sánh số lượng files, kích thước trung bình và thời gian query trước/sau tối ưu).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Giảm ít nhất $80\%$ tổng số lượng file nhỏ sau khi chạy lệnh `OPTIMIZE`.
  - [ ] **AC-2:** Tốc độ truy vấn tìm kiếm theo `transaction_id` tăng tốc ít nhất $3\times$ nhờ Z-ORDER.
  - [ ] **AC-3:** Lệnh `VACUUM` dọn dẹp an toàn các file snapshot cũ mà không làm hỏng tính toàn vẹn của Time Travel hiện hành.
- **Verification Method:**
  ```powershell
  # Chạy script bảo trì và nén file Delta
  uv run python -m src.digital_payments_lakehouse.utils.delta_maintenance --table silver_transactions_current --vacuum-hours 168
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lệnh `VACUUM` với retention quá ngắn làm hỏng các streaming job đang đọc snapshot cũ.
  - *Mitigation:* Luôn duy trì retention mặc định an toàn (168 giờ = 7 ngày) trong môi trường production, chỉ dùng retention 0 giờ trong kịch bản test có flag override rõ ràng.

---

### [WBS-M5-T06] Formal Architecture Decision Records (`ADR C02`, `C15`, `C18`)

- **Classification:** `[D3-ADR]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M4-T12`.
- **Input Artifacts:**
  - Danh mục Scope D3 trong `docs/DE_Project2_Master_Handoff.md`.
  - Chuẩn định dạng ADR quốc tế (Michael Nygard format).
- **Detailed Implementation Steps:**
  1. Soạn thảo **`docs/adr/ADR-001-C02-near-duplicate-fuzzy-matching.md`**:
     - *Bối cảnh:* Các giao dịch mạng chập chờn phát sinh nhiều event gần giống nhau nhưng lệch vài mili-giây.
     - *Quyết định:* Phân tích so sánh giữa Exact Deduplication (Watermark + Primary Key) vs Fuzzy Levenshtein Matching; lý do loại trừ fuzzy matching khỏi luồng core streaming để tránh false positives tài chính.
  2. Soạn thảo **`docs/adr/ADR-002-C15-late-financial-adjustments-as-of-reporting.md`**:
     - *Bối cảnh:* Các khoản điều chỉnh tài chính phát sinh sau ngày T+30 ảnh hưởng thế nào tới báo cáo đã đóng sổ.
     - *Quyết định:* Lựa chọn kiến trúc Bitemporal Modeling (Transaction Time vs Valid Time) kết hợp cơ chế `run_version` để hỗ trợ báo cáo dạng "As-Of" mà không sửa đè lịch sử kế toán.
  3. Soạn thảo **`docs/adr/ADR-003-C18-pii-masking-and-column-level-security.md`**:
     - *Bối cảnh:* Ràng buộc tuân thủ bảo vệ dữ liệu cá nhân (PCI-DSS và Nghị định 13/2023/NĐ-CP).
     - *Quyết định:* Thiết kế giải pháp phân tầng mã hóa: Không lưu PII trong Kafka payload thô, mã hóa một chiều (SHA-256 + Salt) đối với định danh tài khoản, và cấp quyền truy cập theo cột (Column-level access control) tại tầng Presentation.
  4. Toàn bộ 3 tài liệu ADR được viết bằng **Tiếng Anh (Formal Technical English)**.
- **Deliverables / Output Artifacts:**
  - 3 File ADR hoàn chỉnh:
    * `docs/adr/ADR-001-C02-near-duplicate-fuzzy-matching.md`
    * `docs/adr/ADR-002-C15-late-financial-adjustments-as-of-reporting.md`
    * `docs/adr/ADR-003-C18-pii-masking-and-column-level-security.md`
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t06/`
  - **Artifacts:**
    - `docs_review/adr_markdown_syntax_check.log` (Log kiểm tra cú pháp và liên kết của 3 tài liệu ADR).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Đầy đủ các phần chuẩn của 1 ADR: Context, Decision Drivers, Considered Options, Pros & Cons Matrix, Decision Outcome.
  - [ ] **AC-2:** Ngôn ngữ tiếng Anh chuẩn kỹ thuật, văn phong rõ ràng, lập luận sắc bén.
  - [ ] **AC-3:** Các quyết định giải trình mạch lạc lý do lựa chọn giải pháp kỹ thuật phù hợp với ràng buộc của nền tảng.
- **Verification Method:**
  ```powershell
  # Kiểm tra cú pháp Markdown của các file ADR
  powershell -Command "Get-ChildItem docs/adr/*.md | Select-Object Name, Length"
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Tài liệu ADR quá sơ sài hoặc mang tính lý thuyết suông.
  - *Mitigation:* Gắn kèm các đoạn mã code giả lập (pseudocode) và bảng phân tích trade-off định lượng cụ thể vào từng ADR.

---

### [WBS-M5-T07] Comprehensive Test Suite Hardening & Quarantine Audit

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M5-T05`.
- **Input Artifacts:**
  - Toàn bộ test suite trong `tests/` và `dbt/tests/`.
- **Detailed Implementation Steps:**
  1. Rà soát và củng cố toàn bộ bộ test Pytest (Unit & Integration):
     - Đảm bảo 100% các bài test thực thi độc lập (Hermetic Tests), không phụ thuộc vào thứ tự chạy.
     - Phát hiện và cách ly các bài test chập chờn (Flaky Tests) do timing hoặc race condition mạng.
     - Tối ưu hóa thời gian chạy của test suite: Sử dụng `pytest-xdist` để chạy song song các unit test.
  2. Rà soát bộ test dbt:
     - Kiểm tra toàn bộ 145+ dbt tests (bao gồm generic schema tests và custom singular financial tests).
     - Xác nhận câu lệnh `dbt test` hoàn thành với **0 failures / 0 errors**.
  3. Đo lường độ bao phủ mã nguồn (Code Coverage):
     - Chạy `pytest --cov=src --cov-report=html --cov-report=term`.
     - Đảm bảo tổng độ bao phủ branch coverage đạt $\ge 85\%$.
- **Deliverables / Output Artifacts:**
  - Báo cáo Coverage: `htmlcov/index.html`.
  - Test suite hoàn thiện: `tests/`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t07/`
  - **Artifacts:**
    - `logs/pytest_full_project_suite.log` (Log chạy toàn bộ Pytest suite pass 100% kèm coverage report >= 85%).
    - `logs/dbt_test_full_suite.log` (Log chạy 145+ dbt tests pass 100% không có failure).
    - `data_samples/coverage_summary.txt` (Báo cáo tóm tắt tỷ lệ coverage theo từng package).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Lệnh `pytest tests/` pass 100% không có bất kỳ test failure nào.
  - [ ] **AC-2:** Lệnh `dbt test` pass 100% trên toàn bộ các models Silver và Gold.
  - [ ] **AC-3:** Độ bao phủ code coverage của các package lõi (`simulator/`, `streaming/`, `batch/`) đạt $\ge 85\%$.
- **Verification Method:**
  ```powershell
  uv run pytest tests/ --cov=src --cov-report=term-missing
  cd dbt
  dbt test
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Test tích hợp chạy quá lâu khiến vòng phản hồi (feedback loop) bị chậm.
  - *Mitigation:* Tách riêng marker `@pytest.mark.integration` và `@pytest.mark.unit` để có thể chạy kiểm tra nhanh unit tests trong vòng $< 30$ giây.

---

### [WBS-M5-T08] Enterprise English README & One-Command Reproducibility

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M5-T06`, `WBS-M5-T07`.
- **Input Artifacts:**
  - Toàn bộ kết quả thực thi và bằng chứng của dự án.
- **Detailed Implementation Steps:**
  1. Viết mới toàn diện file `README.md` chính của repository bằng **Tiếng Anh chuyên nghiệp (English)**:
     - **Hero Section:** Tên dự án, Badges (CI passing, Python 3.12, Spark 3.5, Delta Lake 3.2, dbt, Coverage 88%).
     - **Executive Problem Statement:** Tại sao ngành thanh toán số cần kiến trúc Lakehouse kép (Real-time Monitoring vs EOD 3-Way Reconciliation)?
     - **Architecture Diagram:** Sơ đồ kiến trúc trực quan (vẽ bằng Mermaid hoặc nhúng ảnh SVG chất lượng cao) thể hiện rõ luồng dữ liệu Dual-Track (Local-First Docker + Databricks Showcase).
     - **Key Engineering Highlights:**
       * Strict Invariants (`amount_minor` BIGINT, Event-time vs Ingestion-time).
       * Stateful Streaming Dual-Table Silver & Monotonic State Progression.
       * 5% Circuit Breaker & Automatic Quarantine.
       * EOD 3-Way Reconciliation with 7 Break Types & Idempotent `run_version`.
       * Ground Truth `G0` validation achieving $\ge 99.5\%$ Precision/Recall.
     - **One-Command Reproducibility Guide:**
       ```bash
       git clone https://github.com/thanhtri271206/digital-payments-lakehouse.git
       cd digital-payments-lakehouse
       make dev-up        # Khởi động sạch toàn bộ Docker stack
       make run-pipeline  # Chạy Simulator, Spark Streaming, và dbt Reconciliation
       make test          # Chạy toàn bộ test suite
```
     - **Dashboards Gallery:** Ảnh chụp 2 Dashboards Metabase kèm chú thích nghiệp vụ.
     - **Performance Benchmarks & Profiling Table:** Số liệu đo lường thực tế 1M–5M giao dịch.
- **Deliverables / Output Artifacts:**
  - File tài liệu chính: `README.md`.
  - Thư mục hình ảnh / diagrams: `docs/assets/`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t08/`
  - **Artifacts:**
    - `logs/readme_links_verification.log` (Log kiểm tra 100% hyperlinks trong README hợp lệ).
    - `screenshots/one_command_reproducibility_demo.png` (Ảnh/GIF minh họa khởi động 1 câu lệnh `docker compose up -d`).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** File `README.md` được viết bằng tiếng Anh chuẩn kỹ sư quốc tế, không có lỗi ngữ pháp hoặc chính tả.
  - [ ] **AC-2:** Một người dùng mới có thể clone repository về và chạy thành công toàn bộ hệ thống bằng đúng 1 câu lệnh hướng dẫn trong README.
  - [ ] **AC-3:** Chứa đầy đủ các liên kết tới các tài liệu chi tiết trong `docs/wbs/` và `docs/adr/`.
- **Verification Method:**
  ```powershell
  # Kiểm tra tính toàn vẹn của các file links trong README.md
  powershell -Command "Get-Content README.md | Select-String -Pattern '\[.*\]\(.*\)'"
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Tài liệu README quá dài làm người đọc bị ngợp.
  - *Mitigation:* Sử dụng thẻ `<details><summary>` để thu gọn các đoạn log cấu hình chi tiết, giữ cho trang chính thanh thoát, tập trung vào điểm nhấn kiến trúc.

---

### [WBS-M5-T09] Final Project Sign-Off & Portfolio Interview Defense Kit

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M5-T08`.
- **Input Artifacts:**
  - Toàn bộ sản phẩm bàn giao từ Milestone 1 đến Milestone 5.
  - Tiêu chí nghiệm thu Gate 5 trong `docs/wbs/README.md`.
- **Detailed Implementation Steps:**
  1. Soạn thảo Bộ câu hỏi Phỏng vấn & Hướng dẫn Giải trình Kỹ thuật (Interview Defense Kit):
     - Lưu tại `docs/portfolio/interview_defense_guide.md`.
     - Tổng hợp 15 câu hỏi phỏng vấn hóc búa nhất mà các nhà tuyển dụng thường hỏi đối với dự án này, kèm câu trả lời mẫu theo mô hình **STAR (Situation - Task - Action - Result)**:
       * *Ví dụ 1:* "Tại sao bạn lại chọn kiến trúc Dual-Table Silver thay vì chỉ dùng một bảng duy nhất?"
       * *Ví dụ 2:* "Làm thế nào bạn xử lý được lỗi multiple source rows match trong câu lệnh MERGE của Delta Lake?"
       * *Ví dụ 3:* "Tại sao không dùng FLOAT cho tiền tệ và bạn đã chứng minh điều đó bằng test case nào?"
       * *Ví dụ 4:* "Bạn giải thích cơ chế Hysteresis trong bài toán Partner Degradation như thế nào?"
  2. Thực hiện phiên thẩm định và Ký duyệt Chốt sổ Dự án (Final Sign-Off):
     - Soạn văn bản `docs/reports/final_project_signoff.md`.
     - Đánh giá toàn bộ 5 Quality Gates (Gate 1 $\rightarrow$ Gate 5).
     - Chính thức công nhận hoàn thành xuất sắc Phase 9 (Build & Review Loop).
- **Deliverables / Output Artifacts:**
  - Hướng dẫn phỏng vấn: `docs/portfolio/interview_defense_guide.md`.
  - Văn bản nghiệm thu tổng kết: `docs/reports/final_project_signoff.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m5/wbs-m5-t09/`
  - **Artifacts:**
    - `logs/final_verification_audit.log` (Log kiểm tra toàn bộ 5 Gates và pipeline).
    - `docs_review/portfolio_defense_kit_review.txt` (Biên bản tự đánh giá 15 câu hỏi phỏng vấn STAR).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Bộ câu hỏi phỏng vấn bao quát đầy đủ 100% các khía cạnh kỹ thuật then chốt của dự án.
  - [ ] **AC-2:** Văn bản Final Sign-Off xác nhận 100% các tiêu chí của **Gate 5 (Project Sign-off)** đạt chuẩn PASS tuyệt đối.
  - [ ] **AC-3:** Hồ sơ dự án sẵn sàng để đính kèm vào CV và trình bày trực tiếp trong các buổi phỏng vấn tuyển dụng Junior Data Engineer.
- **Verification Method:**
  ```powershell
  # Kiểm tra văn bản nghiệm thu tổng kết tồn tại
  Test-Path docs/reports/final_project_signoff.md
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Học viên nhớ chi tiết code nhưng lúng túng khi giải thích bức tranh kiến trúc tổng thể.
  - *Mitigation:* Cung cấp sẵn các sơ đồ tóm tắt dạng "Elevator Pitch" trong bộ Defense Kit để học viên dễ dàng hệ thống hóa kiến thức trong 2 phút giới thiệu bản thân.

---

## 4. MILESTONE 5 QUALITY GATE 5 CHECKLIST (TIÊU CHÍ CHỐT SỔ TOÀN DỰ ÁN)

> **Thư mục Lưu trữ Bằng chứng Nghiệm thu:** `docs/evidence/m5/gate_5_signoff/`  
> Toàn bộ artifacts nghiệm thu phải được tập hợp đầy đủ trước phiên review.

Để chính thức ký duyệt nghiệm thu dự án (Project Sign-Off), toàn bộ các tiêu chí dưới đây bắt buộc phải đạt trạng thái **PASS**:

- [ ] **Gate 5.1 (Serving Dashboards):** Metabase hiển thị trực quan 2 Dashboards (Payment Ops Near-RT và Finance Reconciliation) kết nối trực tiếp với tầng Gold. *(Artifacts: `docs/evidence/m5/gate_5_signoff/metabase_payment_ops_live.png`, `metabase_finance_reconciliation_live.png`)*
- [ ] **Gate 5.2 (Scale Benchmark):** Vượt qua bài kiểm tra chịu tải 1M–5M giao dịch ổn định, có số liệu throughput và latency cụ thể. *(Artifacts: `docs/evidence/m5/gate_5_signoff/benchmark_1m_throughput.log`, `benchmark_scale_metrics.json`)*
- [ ] **Gate 5.3 (Storage Optimization):** Bảng Delta Lake được tối ưu hóa bằng `OPTIMIZE` và `Z-ORDER BY`, giải quyết triệt để vấn đề Small Files. *(Artifacts: `docs/evidence/m5/gate_5_signoff/storage_optimization_metrics.txt`)*
- [ ] **Gate 5.4 (ADR Documentation):** Hoàn thành 3 văn bản Architecture Decision Records (`ADR C02`, `C15`, `C18`) bằng tiếng Anh chuẩn quốc tế. *(Artifacts: `docs/evidence/m5/gate_5_signoff/adr_compliance_audit.log`)*
- [ ] **Gate 5.5 (Test Coverage & Integrity):** Toàn bộ Pytest suite và dbt tests pass 100%, độ bao phủ code coverage $\ge 85\%$. *(Artifacts: `docs/evidence/m5/gate_5_signoff/pytest_coverage_report.log`, `dbt_test_report.log`)*
- [ ] **Gate 5.6 (Master Portfolio Packaging):** File `README.md` chính viết bằng tiếng Anh hoàn chỉnh, hỗ trợ khởi động và tái lập 100% chỉ với 1 câu lệnh (`docker compose up -d`). *(Artifacts: `docs/evidence/m5/gate_5_signoff/one_command_reproducibility_demo.png`)*

---

## 5. SCHEDULE RISK, CONTINGENCY BUFFER & REMINDER

> [!WARNING] Cảnh báo Nguy cơ Vượt Ngân sách Thời gian (Schedule Overrun Risk)
> - **Ngân sách Kế hoạch:** **35 giờ**.
> - **Khu vực có rủi ro cao gây trễ tiến độ:**
>   1. **`WBS-M5-T04` & `WBS-M5-T05` (Benchmark & Storage Tuning):** Quá trình sinh và nạp 1M–5M bản ghi có thể mất nhiều giờ chạy máy và phát sinh lỗi thiếu đĩa trống trên workstation.
>   2. **`WBS-M5-T08` (Enterprise README):** Việc trau chuốt câu từ tiếng Anh, thiết kế diagrams chuyên nghiệp và quay video/GIF demo thường tiêu tốn nhiều thời gian hơn dự kiến.
> - **Nguyên tắc Đánh đổi của TPM (Quality Leverage Policy):**
>   * Ưu tiên hàng đầu là **Hình thức Trình bày Đỉnh cao và Khả năng Tái lập 1-Command**. Nhà tuyển dụng thường chỉ dành 2–3 phút để lướt qua GitHub repository; một file README tiếng Anh xuất sắc với diagrams trực quan và demo sinh động sẽ mang lại 80% ấn tượng đầu tiên.

---

## 6. PHÂN TÍCH BIẾN ĐỘNG NGÂN SÁCH & GIẢI TRÌNH (EFFORT VARIANCE ANALYSIS & RATIONALE)

- **Số giờ biến động (Variance Hours):** **+2 giờ** (Tổng ngân sách điều chỉnh: **37 giờ**, so với baseline ban đầu là **35 giờ**).
- **Bảng chi tiết các Task phát sinh nỗ lực & Lý do Kỹ thuật:**

| Mã Task | Tên Task | Giờ Baseline | Giờ Điều chỉnh | Chênh lệch | Lý do Kỹ thuật Chi tiết |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `WBS-M5-T05` | Delta Storage Tuning | 4 h | **5 h** | **+1 h** | Bổ sung phân tích so sánh chi tiết hiệu năng Data Skipping (Query execution plans) trước và sau khi chạy `Z-ORDER BY` để lấy số liệu thực chứng đưa vào báo cáo kỹ thuật. |
| `WBS-M5-T08` | Enterprise English README | 4 h | **5 h** | **+1 h** | Tích hợp sơ đồ Mermaid đa tầng chi tiết và biên soạn kịch bản One-command setup đa nền tảng (cả PowerShell Windows và Makefile Linux/macOS) bảo đảm tính tương thích tuyệt đối cho mọi người đánh giá. |
| **TỔNG CỘNG** | | 35 h | **37 h** | **+2 h** | |

- **Tác động Tích cực đối với Nền tảng & Giá trị Hồ sơ Kỹ sư:**
  * Việc đầu tư thêm 2 giờ giúp hoàn thiện một bộ mặt hồ sơ dự án (Portfolio Front-page) đạt đẳng cấp chuyên nghiệp quốc tế, biến một bài tập cá nhân thành một sản phẩm mã nguồn mở (Open-source Platform) có độ chỉn chu tương đương với các giải pháp thanh toán thực tế tại các công ty công nghệ lớn.

