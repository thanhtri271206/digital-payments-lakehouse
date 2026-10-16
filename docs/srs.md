# MASTER HANDOFF & BLUEPRINT (v3.0) — DE PORTFOLIO PROJECT 2

## Digital Payments Monitoring & Reconciliation Platform

> **Mục đích tài liệu:** Đây là Single Source of Truth (SSOT) tổng hợp toàn bộ bối cảnh, ràng buộc, mô hình dữ liệu, yêu cầu chức năng/phi chức năng (Q1–Q46), kiến trúc vật lý và lộ trình triển khai đã được chốt từ Phase 0 đến hết Phase 8.
> **Trạng thái hiện tại:** Đã đóng toàn bộ Phase 0 -> Phase 8. Chuẩn bị bắt đầu **Phase 9 (Build & Review Loop) — Milestone 1**.

---

## 1. BỐI CẢNH, MỤC TIÊU & NGUYÊN TẮC LÀM VIỆC

### 1.1. Hồ sơ & Mục tiêu (CONFIRMED)

- **Bối cảnh:** Sinh viên năm 3 ngành Hệ thống Thông tin (UIT-VNUHCM).
- **Định hướng:** Data Engineer / Big Data (mục tiêu Intern/Fresher nâng lên Junior, ứng tuyển đầu năm 2027 tại các doanh nghiệp lớn/ngân hàng ở Việt Nam như FPT, Viettel, VNPT, KMS, Bosch...).
- **Kinh nghiệm nền tảng:** Đã hoàn thành Project 1 (`crypto-coin-pipeline`) với Airflow 3, Async HTTPX, MinIO/S3, DuckDB/MotherDuck, dbt (Kimball Star Schema, 145 tests, data contracts), Pydantic v2, Docker, GitHub Actions CI, Streamlit.
- **Mục tiêu Project 2:** Phải khác biệt hoàn toàn với Project 1, tập trung giải quyết các khoảng trống: **Streaming, Stateful Processing, Event-time & Watermarking, Multi-source Ingestion, 3-Way Financial Reconciliation, Distributed Compute (Spark/Databricks), Delta Lake ACID, Failure Recovery & Idempotency**.
- **Ngôn ngữ tài liệu kỹ thuật:** README, ADR và tài liệu kiến trúc viết bằng **Tiếng Anh (English)**.

### 1.2. Ràng buộc Dự án (Constraints - CONFIRMED)

- **Thời gian:** 2–3 tháng (~3 giờ/ngày, tổng quỹ thời gian ~200 giờ).
- **Ngân sách (Hard Budget):** Dưới 100.000 VND (ưu tiên tuyệt đối Free Tier và Local Docker).
- **Phần cứng Local:** RAM 32 GB, CPU Intel Xeon W2133 3.6 GHz, ~100 GB đĩa trống, chạy Docker tốt.
- **Cloud:** Có AWS Account và Databricks Free Edition (AWS là optional, không ép dùng dịch vụ trả phí nếu không phục vụ trực tiếp cho requirement).

### 1.3. Working Agreement (Cách làm việc giữa Mentor & Học viên)

1. **Không nhảy cóc:** Đi từ Requirement -> Data Behavior -> Failure Mode -> Engineering Solution -> Technology.
2. **Cách thức triển khai (Phase 9):**
   - Mentor đưa requirement/spec, cấu trúc file, pseudocode/boilerplate, input/output mẫu và Acceptance Criteria.
   - Học viên tự viết phần logic chính, chạy thực nghiệm và gửi lại Code + Log + Screenshot/Evidence.
   - Mentor review nghiêm khắc (chỉ rõ Blocker/High/Medium), học viên sửa đến khi đạt Definition of Done (DoD) mới qua bước tiếp theo.
3. **Phương pháp cắt lát:** Áp dụng **Vertical Slicing (Agile Tracer Bullet)** — xây dựng luồng E2E đơn giản chạy thông suốt trước, sau đó đắp dần các lớp phức tạp (Watermark, SCD2, DLQ, Circuit Breaker).

---

## 2. DOMAIN, PERSONAS & BUSINESS CAPABILITIES

### 2.1. Domain đã chốt

**Digital Payments / E-Wallet — Transaction Monitoring & 3-Way Reconciliation Platform**

- Hệ thống ví điện tử giả lập xử lý giao dịch qua nhiều kênh/đối tác thanh toán (Payment Channels/Gateways), ghi nhận sổ cái nội bộ (Internal Wallet Ledger) và đối soát cuối ngày (EOD T+1) với tệp thanh toán từ đối tác (Partner Clearing CSV) cùng sao kê ngân hàng (Bank Statement).
- **Nguyên tắc kiến trúc cốt lõi:** Loại bỏ hoàn toàn tư duy _"Batch là backup cho Streaming"_. Luồng Near-Real-Time (Streaming) và luồng EOD (Batch) tồn tại độc lập vì phục vụ hai mục đích nghiệp vụ khác nhau.

### 2.2. Năm Nhóm Người dùng (Personas) & Nhu cầu

1. **Payment Operations:** Giám sát giao dịch gần thời gian thực (freshness ~30s), theo dõi tỷ lệ thành công (success rate), độ trễ, lý do lỗi, phát hiện giao dịch bị kẹt (`STUCK`) và sự suy giảm hiệu năng của đối tác (partner degradation).
2. **Finance / Reconciliation Operations:** Đối soát EOD/T+1 giữa 3 nguồn (Ledger - Partner - Bank), phân loại sai lệch (missing, orphan, amount mismatch, duplicate, timing mismatch) và drill-down về từng giao dịch gốc.
3. **Data / BI Analyst:** Phân tích lịch sử trên Data Warehouse/Marts (GMV, funnel chuyển đổi, hiệu quả theo merchant/ngành hàng).
4. **Risk / Compliance:** Chỉ phát hiện các tín hiệu rủi ro dựa trên tập luật tĩnh (rule-based velocity/threshold), **không** làm ML Fraud hay tự động khóa tài khoản.
5. **Data Platform Engineering:** Giám sát độ trễ pipeline, lag, tỷ lệ lỗi/quarantine, đảm bảo chạy lại lũy đẳng (idempotent rerun) và backfill có kiểm soát.

---

## 3. SOURCE INVENTORY, DATA SCHEMAS & REALISM SPEC

### 3.1. Quy chuẩn Toàn cục (Global Data Standards)

- **Biểu diễn tiền tệ (`amount_minor`):** Bắt buộc dùng số nguyên (`BIGINT` / integer minor unit, ví dụ `100000` VND). Tuyệt đối không dùng `FLOAT`/`DOUBLE` để tránh sai số dấu phẩy động.
- **Mô hình thời gian (Timestamp Model):** Tách biệt rõ `event_time` (thời điểm sự kiện nghiệp vụ xảy ra) và `ingestion_time` (thời điểm nền tảng nhận sự kiện). Mọi chỉ số nghiệp vụ phải tính theo `event_time`.
- **Quy chuẩn Múi giờ (Timezone):** Lưu trữ nội bộ chuẩn hóa theo `UTC`. Tầng hiển thị báo cáo/dashboard và mốc chốt sổ cuối ngày (cutoff) quy chiếu theo `Asia/Ho_Chi_Minh (UTC+07:00)`. Chủ động giả lập nguồn lệch múi giờ (`UTC` vs `UTC+07`) để kiểm thử lỗi biên ngày (cutoff boundary).
- **Quy mô dữ liệu (Scale Parameters):**
  - Normal workload: `100K–500K transactions/day` (~100K users, ~5K merchants).
  - Stress / Benchmark: `1M`, `5M`, `10M transactions/day`.
  - Lưu trữ: Raw retention `30–90 ngày`, Historical analytical data `6–12 tháng`.

### 3.2. Chi tiết các Nguồn Dữ liệu (S1–S5 & Ground Truth G0)

#### S1 — Payment Gateway Event Stream (Kafka JSON, At-least-once)

- **Grain:** 1 event cho mỗi lần chuyển trạng thái giao dịch.
- **Schema:** `event_id`, `transaction_id`, `event_type`, `event_time`, `ingestion_time`, `user_id`, `merchant_id`, `payment_channel` (`BANK_A`, `GATEWAY_A`...), `payment_method` (`BANK_TRANSFER`, `CARD`, `WALLET_BALANCE`, `QR`), `amount_minor`, `currency`, `status_reason`, `producer_version`. (Không nhúng thuộc tính khả biến của Merchant/User vào event).
- **Vòng đời (Lifecycle):**
  `INITIATED (Rank 1)` -> `PAYMENT_METHOD_SELECTED (Rank 2)` -> `AUTHORIZED (Rank 3)` -> `PROCESSING (Rank 4)` -> Terminal States (`Rank 5`: `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`). Từ `COMPLETED` có thể chuyển sang `REFUNDED (Rank 6)`.

#### S2 — Wallet / Internal Ledger OLTP Simulator (Append-only + CDC)

- **Grain:** 1 dòng cho mỗi bút toán kế toán (ledger entry).
- **Schema:** `ledger_entry_id`, `transaction_id`, `account_id`, `entry_type` (`INSERT`, `ADJUSTMENT`, `REVERSAL`), `direction` (`DEBIT`/`CREDIT`), `amount_minor`, `currency`, `entry_time`, `business_date`, `reference_id`, `created_at`.
- **Invariants:** Không bao giờ `UPDATE` hoặc `DELETE` vật lý trên sổ cái. Điều chỉnh số liệu phải thông qua bản ghi `REVERSAL` hoặc `ADJUSTMENT` tham chiếu về giao dịch gốc.

#### S3 — Master Data (`Merchant` SCD Type 2 & `User` Current-State)

- **Merchant Schema (SCD Type 2):** `merchant_id`, `merchant_name` (hỗ trợ tiếng Việt NFC/NFD), `category`, `city`, `status`, `effective_from`, `effective_to`, `is_current`. Hỗ trợ point-in-time join và late-arriving dimension.
- **User Schema (SCD Type 1):** `user_id`, `country`, `kyc_level`, `created_at`, `status`.

#### S4 — External Partner Settlement Files & Bank Statements (Batch CSV)

- **Partner A (Gateway Settlement CSV - nhiều file trong ngày + file chốt EOD):** `transaction_id`, `settlement_id`, `settlement_time`, `amount`, `fee`, `net_amount`, `status`.
- **Partner B (Bank Statement CSV - hàng ngày):** `bank_reference`, `transaction_reference`, `value_date`, `amount`, `direction`, `fee`.

#### S5 — FX Source

- **Quyết định:** Loại bỏ Real FX API khỏi core scope. Chỉ dùng tỷ giá giả lập cố định/tham chiếu nếu cần.

#### G0 — Simulator Ground Truth (Vũ khí kiểm chứng độ chính xác)

- Simulator sử dụng `deterministic seed` (ví dụ `seed = 20260930`). Khi sinh dữ liệu và chủ động tiêm lỗi (duplicate, late, stuck, poison, mismatch), Simulator ghi đồng thời bản chất thật của giao dịch vào `G0`.
- Dùng để đo lường tự động **Precision / Recall** của tầng Data Quality và thuật toán Đối soát (Reconciliation).

---

## 4. TỔNG HỢP YÊU CẦU CHỨC NĂNG & PHI CHỨC NĂNG (LOCKED Q1–Q46)

### Nhóm 1: Near-Real-Time Monitoring & Streaming Reliability (Q1–Q9)

- **NFR-RT-01 (Freshness SLO - Q1, Q9):** End-to-end freshness (`serving_time - event_time`) đạt **p95 <= 30s, p99 <= 60s**. Theo dõi tách biệt với Event lateness (`ingestion_time - event_time`).
- **NFR-RT-02 (Throughput - Q2):** Baseline thiết kế `~5 events/s`, burst `~50 events/s` (cấu hình mở rộng lên kịch bản stress test).
- **NFR-RT-03 (Late/OOO Window & Correction Path - Q3, Q4, Q5, Q37):** Xử lý trực tiếp trên luồng streaming các sự kiện đến trễ trong cửa sổ **Watermark = 2 phút**. Các sự kiện đến trễ `> 2 phút` không bị mất (nhờ lưu ở Bronze) và được đẩy sang bảng `silver_late_events_dlq` để job Batch hợp nhất (Merge/Correction) định kỳ.
- **NFR-RT-04 & 05 (Crash & Outage Recovery - Q6, Q7):** Đảm bảo **At-least-once delivery + Idempotent downstream handling**. Khi streaming job sập (ngắn hạn 2 phút hoặc dài hạn 15 phút), khi bật lại phải xử lý toàn bộ backlog không mất dữ liệu, không nhân đôi kết quả tài chính; cho phép tạm thời vi phạm Freshness SLO trong lúc bắt kịp (catch-up) backlog.
- **NFR-RT-06 (Freshness vs. Correctness Priority - Q8):** Các chỉ số vận hành (Operational metrics) ưu tiên **Freshness**; các chỉ số tài chính/đối soát (Financial metrics) bắt buộc ưu tiên **Correctness**.

### Nhóm 2: Payment Lifecycle & Stuck Transaction Detection (Q10–Q14, Q45)

- **FR-LC-01 (Derived Stuck Status - Q10):** Nếu giao dịch nằm ở `PROCESSING` quá **5 phút** mà không có tiến triển hợp lệ -> gắn cờ trạng thái vận hành `operational_status = STUCK`, **giữ nguyên** trạng thái nghiệp vụ gốc `lifecycle_state = PROCESSING`.
- **FR-LC-02, 03, 04 (Dynamic Timer & Unsticking - Q11, Q12):** Sử dụng cơ chế **Dynamic Timer** tính từ thời điểm có sự kiện nghiệp vụ tiến triển hợp lệ gần nhất (`deadline = last_progress_event_time + 5 phút`). Không dùng heartbeat cấp transaction ở phiên bản đầu. Sự kiện hợp lệ mới đến sẽ gỡ cờ `STUCK`.
- **FR-LC-05 (Late Terminal Audit - Q13):** Khi nhận sự kiện kết thúc (`COMPLETED`...) đến muộn cho một giao dịch đang `STUCK`, sau khi validate hợp lệ sẽ cập nhật `lifecycle_state = COMPLETED`, gỡ trạng thái `STUCK` hiện tại nhưng bắt buộc giữ cờ lịch sử `was_ever_stuck = TRUE` để phục vụ phân tích vận hành.
- **FR-LC-06, 07 (Transition Matrix & Terminal Conflicts - Q14, Q45):** Áp dụng **Monotonic State Rank (1 -> 6) + Terminal Lock**.
  - Sự kiện bậc thấp đến muộn hơn sự kiện bậc cao vẫn được lưu vào bảng History nhưng không làm lùi trạng thái hiện tại và không reset timer.
  - Nếu xuất hiện 2 sự kiện Terminal mâu thuẫn nhau (ví dụ đã `COMPLETED` lại nhận thêm `FAILED`), tuyệt đối không tự động ghi đè; giữ nguyên trạng thái hiện tại và đẩy bản ghi xung đột vào `silver_lifecycle_exceptions`.

### Nhóm 3: Payment Channel & Partner Degradation (Q15–Q17, Q46)

- **FR-OPS-01, 02 (Detection Logic - Q15):** Phát hiện suy giảm hiệu năng đối tác dựa trên kết hợp 3 yếu tố: **Ngưỡng cố định** (`Success Rate < 90%`) HOẶC **Mức sụt giảm so với baseline** (`>= 8%`), kèm điều kiện **Số lượng giao dịch tối thiểu** (`>= 20 txns / 1 phút`, `>= 100 txns / 5 phút`). Chỉ cảnh báo trên Dashboard, không tự động đổi luồng định tuyến (routing).
- **FR-OPS-03 (Multi-window - Q16):** Sử dụng song song cửa sổ ngắn (`1 phút` - phát hiện sớm) và cửa sổ dài (`5 phút` - xác nhận suy giảm kéo dài).
- **FR-OPS-04 & SCOPE-OPS-01 (Hysteresis Recovery - Q17, Q46):** Chống hiện tượng nhấp nháy cảnh báo (_alert flapping_) bằng ngưỡng phục hồi riêng: chỉ đóng cảnh báo khi `Success Rate >= 95%` duy trì liên tục trong **2 cửa sổ liên tiếp**. Giữ cơ chế cảnh báo đơn giản qua bảng/biểu đồ, không xây dựng alerting engine phức tạp.

### Nhóm 4: Batch/EOD & 3-Way Reconciliation (Q18–Q22, Q38, Q39, Q46)

- **FR-REC-01..04 (Cutoff & Completeness Gate - Q18, Q46):** Chốt sổ theo `business_date` lúc `23:59:59 UTC+07:00`. Khởi chạy đối soát lần đầu lúc `01:00 AM (T+1)`. Kiểm tra sự hiện diện của đủ 3 nguồn (Ledger, Partner CSV, Bank CSV); nếu thiếu nguồn, bắt buộc gắn trạng thái run là `INCOMPLETE` (không được báo cáo là đối soát thành công).
- **FR-REC-05..09 (Version-aware File Ingestion - Q19, Q20, Q39):** Phân biệt file trùng lặp (duplicate) và file gửi đè sửa lỗi (corrected/resend) thông qua version/manifest metadata. Lưu trữ Append-only toàn bộ các phiên bản file kèm cờ `file_version` / `is_active` để chỉ đưa phiên bản hợp lệ mới nhất vào phiên đối soát. Nếu metadata mập mờ không phân xử được -> đẩy vào exception handling.
- **FR-REC-10 (Grace Period SLA - Q21, Q46):** Cho phép khoảng thời gian chờ (Grace Period) đến `06:00 AM (T+1)` để tự động chạy lại đối soát khi file trễ cập bến. Quá `06:00 AM` vẫn `INCOMPLETE` mới phát cảnh báo `CRITICAL_SLA_BREACH`.
- **FR-PLAT-01 (Versioned Backfill/Replay - Q22):** Khi chạy lại đối soát hoặc backfill (ví dụ sửa logic cho ngày `T-5`), không xóa kết quả cũ mà sinh `run_version` mới (`v1`, `v2`...); các View ở tầng tiêu thụ luôn đọc `run_version` cao nhất của từng `business_date`.
- **Matching Hierarchy & Break Types:**
  - Level 1: Khớp chính xác theo khóa tham chiếu (`transaction_id` / `settlement_id` / `bank_reference`).
  - Level 2: Khớp tổ hợp (Composite match: số tiền + ngày + cửa sổ thời gian + đối tác).
  - Level 3: Đánh dấu ngoại lệ (Unmatched / Exception) với 7 nhóm trạng thái: `MATCHED`, `MISSING_IN_PARTNER`, `MISSING_IN_BANK`, `ORPHAN_EXTERNAL`, `DUPLICATE`, `AMOUNT_MISMATCH`, `TIMING_CUTOFF_MISMATCH`.

### Nhóm 5: Data Quality, Contracts & Schema Evolution (Q23–Q24, Q34)

- **NFR-DQ-01 (Circuit Breaker - Q23, Q46):** Các bản ghi vi phạm kỹ thuật (`amount_minor < 0`, null khóa chính, sai enum) bị tách sang bảng Quarantine. Nhưng nếu tỷ lệ lỗi trong một batch/micro-batch **> 5%** (với tổng số bản ghi `>= 50`), **Circuit Breaker** sẽ ngắt (abort/fail) job ngay lập tức để bảo vệ dữ liệu downstream.
- **NFR-DQ-02 (Safe Schema Evolution - Q24):** Tự động chấp nhận cột mới được thêm vào (backward-compatible), nhưng fail job ngay lập tức nếu phát hiện thay đổi phá vỡ hợp đồng (breaking changes: đổi kiểu dữ liệu, xóa cột bắt buộc).
- **Multi-layer Contract Enforcement (Q34):** Tầng 1 kiểm tra và rẽ nhánh (valid vs quarantine) bằng code PySpark/Pydantic trước khi ghi; Tầng 2 khóa chặt bằng `CHECK` constraints và Schema Enforcement vật lý trên bảng Delta Lake.

---

## 5. KIẾN TRÚC HỆ THỐNG & DATA MODELING (LOCKED Q25–Q44)

### 5.1. Quyết định Công nghệ & Mô hình Triển khai

| Thành phần                                | Quyết định đã chốt                                                                        | Lý do & Giải pháp Kỹ thuật                                                                                                                                                                                                                                                             |
| ----------------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Mô hình Triển khai (Q30, Q43)**         | **Dual-Track Topology (Local-First Core + Databricks Cloud Showcase)**                    | Toàn bộ luồng E2E chạy 100% trong **Local Docker Compose** (tốn 0 VND, đáp ứng 1-command reproducibility, không bị lỗi mạng/timeout). Đồng thời có nhánh đẩy dữ liệu batch lên **AWS S3 + Databricks Free Edition** để chạy `dbt-databricks` và benchmark lấy bằng chứng Cloud cho CV. |
| **Message Broker (Q25)**                  | **Apache Kafka (KRaft mode)**                                                             | Chạy không cần ZooKeeper, cấu hình Dual-Listener (`localhost:9092` cho máy host, `kafka:29092` cho container nội bộ), đáp ứng hoàn hảo yêu cầu giữ backlog và replay.                                                                                                                  |
| **Compute Engine (Q26)**                  | **Apache Spark (Structured Streaming & Batch)**                                           | Unified engine xử lý cả micro-batch streaming (SLO 30s) và batch EOD.                                                                                                                                                                                                                  |
| **Storage Layer (Q27, Q29, Q32)**         | **Delta Lake trên MinIO (Local) / AWS S3 (Cloud)** theo **Strict Medallion Architecture** | Đảm bảo ACID transactions, Time Travel, Schema Enforcement và `MERGE` upserts. Lớp Bronze lưu nguyên bản JSON (Append-only) để đảm bảo khả năng replay 100%.                                                                                                                           |
| **Reconciliation & Transformation (Q38)** | **dbt (`dbt-spark` / `dbt-databricks`)**                                                  | Viết logic đối soát 3 bên và xây dựng bảng Gold bằng SQL tường minh, đi kèm hệ thống dbt tests & lineage, trong khi vẫn đẩy toàn bộ tính toán xuống Spark Engine.                                                                                                                      |
| **Orchestration (Q28)**                   | **Apache Airflow**                                                                        | Điều phối luồng ingest file CSV, kiểm tra Completeness Gate, kích hoạt `dbt run`, xử lý Grace Period SLA và job hợp nhất DLQ.                                                                                                                                                          |
| **Serving & BI (Q31)**                    | **Metabase (hoặc Apache Superset)**                                                       | Chạy trên Docker, kết nối tới tầng Gold để vẽ 2 Dashboard chuẩn Enterprise (Payment Ops Near-RT & Finance EOD Reconciliation).                                                                                                                                                         |

### 5.2. Thiết kế Bảng tại 3 Lớp Medallion (Q33–Q36, Q44)

1. **Lớp Bronze (Raw Immutable Append-Only):**
   - `bronze_payment_events`: Lưu nguyên vẹn payload Kafka JSON, partition theo ngày tiếp nhận.
   - `bronze_wallet_ledger_cdc`: Lưu nguyên vẹn sự kiện CDC sổ cái.
   - `bronze_partner_settlements` & `bronze_bank_statements`: Lưu nguyên vẹn nội dung file CSV kèm `file_id`, `file_version`, `ingestion_time`.
2. **Lớp Silver (Validated, Deduplicated, Enriched & Exception Branches):**
   - **Giải pháp Dual-Table cho Giao dịch (Q44):**
     - `silver_payment_events_history` (Append-only): Lưu mọi bước chuyển trạng thái hợp lệ (đã lọc trùng `event_id` bằng Spark Watermark 2 phút). Suy ra `valid_from` / `valid_to` bằng hàm Window khi truy vấn phân tích mà không gây bùng nổ file nhỏ (small files).
     - `silver_transactions_current` (1 dòng / `transaction_id`): Trong mỗi `foreachBatch`, gộp (collapse) các sự kiện của cùng một giao dịch theo **Monotonic State Rank** trước khi `MERGE` để tránh lỗi multiple-source-row match. Theo dõi trực tiếp `current_lifecycle_state`, `operational_status` (`NORMAL`/`STUCK`), `last_progress_event_time`, `was_ever_stuck`.
   - **Các bảng Nhánh Lỗi & Sổ cái tại Silver:**
     - `silver_quarantine_events`: Chứa các poison records bị loại ở bước validate.
     - `silver_late_events_dlq`: Chứa các sự kiện có `event_time` trễ quá watermark 2 phút để job Batch hợp nhất sau.
     - `silver_lifecycle_exceptions`: Chứa các sự kiện Terminal mâu thuẫn nhau (Q14).
     - `silver_wallet_ledger`: Bảng Fact chỉ chèn (`INSERT`, `ADJUSTMENT`, `REVERSAL`).
     - `silver_dim_merchant_scd2` & `silver_dim_user`.
3. **Lớp Gold (Curated Business & Analytics Marts):**
   - `gold_rt_channel_metrics`: Bảng tổng hợp theo cửa sổ 1 phút và 5 phút phục vụ theo dõi Freshness SLO, Success Rate và phát hiện Partner Degradation (kèm logic Hysteresis).
   - `gold_eod_reconciliation_runs`: Bảng lưu metadata của từng lần chạy đối soát (`run_id`, `business_date`, `run_version`, `run_status`: `INCOMPLETE` / `COMPLETED_WITH_BREAKS` / `RECONCILED`).
   - `gold_eod_reconciliation_details`: Bảng chi tiết kết quả đối soát 3 bên ở cấp độ giao dịch kèm phân loại 7 loại Break.
   - `gold_fact_payment_analytics`: Bảng Star Schema phục vụ BI phân tích GMV, Funnel và Merchant.

---

## 6. PHÂN BỔ ĐỘ SÂU CÁC ENGINEERING CASES (D1 / D2 / D3 SCOPE)

- **D1 (Implement + Automated Test + Portfolio Evidence/Demo):**
  - `C01` Duplicate event & Watermark deduplication.
  - `C03` Out-of-order & Late event (> 2m DLQ + Batch correction).
  - `C04` Missing terminal event (`PROCESSING > 5m` -> `STUCK` + Dynamic Timer).
  - `C06` Poison record + Quarantine + 5% Circuit Breaker.
  - `C10` Merchant SCD Type 2 & Point-in-time enrichment.
  - `C11` & `C12` Immutable Ledger invariants & Integer minor-unit money handling (`amount_minor`).
  - `C13` & `C14` 3-Way Reconciliation & Version-aware Settlement CSV ingestion (`v1` vs `v2`).
  - `C17` Ground Truth (`G0`) automated evaluation (Precision/Recall của DQ & Reconciliation).
  - `C20` Versioned Backfill / Replay (`run_version`).
  - `C23` One-command reproducible local Docker stack + Deterministic seed.
- **D2 (Implement Core Mechanism):**
  - `C05` Safe Schema Evolution.
  - `C07` Silent source / 15-minute outage backlog catch-up.
  - `C08` & `C24` Scale benchmark (1M–5M rows) & Partitioning/Compaction.
  - `C09` Ledger/Reference data CDC ingestion.
  - `C16`, `C19`, `C21`, `C22` Multi-layer DQ, dbt Lineage/Contracts, Observability dashboard, Pytest suite.
- **D3 (Design & Architecture Decision Record - ADR Only):**
  - `C02` Near-duplicate fuzzy matching, `C15` Late financial adjustment as-of reporting, `C18` PII Masking & Column-level security design.

---

## 7. LỘ TRÌNH TRIỂN KHAI 5 MILESTONES (PHASE 9 ROADMAP)

1. **Milestone 1 — Nền móng Hạ tầng & Deterministic Simulator (`G0`):**
   - Thiết lập cấu trúc repo, `config/config.yaml`, `docker-compose.yml` (Kafka KRaft dual-listener + Kafka UI + MinIO auto-bucket init).
   - Xây dựng Python Payment Simulator với `random.Random(seed)` cố định, sinh giao dịch theo đúng Lifecycle và chủ động tiêm các nhóm lỗi chuẩn hóa, đồng thời xuất file đối chiếu Ground Truth `G0`.
2. **Milestone 2 — The "Walking Skeleton" (Vertical Slice 1: Kafka -> Spark -> Delta Bronze):**
   - Viết job Spark Structured Streaming đọc từ Kafka topic `payment.events.raw` và ghi Append-only xuống bảng Delta `bronze_payment_events` trên MinIO kèm checkpoint.
3. **Milestone 3 — Stateful Streaming, Dual-Table Silver & Data Quality:**
   - Xây dựng luồng đọc từ Bronze sang Silver sử dụng `foreachBatch`: kiểm tra Pydantic/PySpark DQ + Circuit Breaker (5%), tách dòng lỗi vào `silver_quarantine_events`, tách dòng trễ `> 2 phút` vào `silver_late_events_dlq`.
   - Cập nhật bảng `silver_payment_events_history` và thực hiện pre-aggregated `MERGE` theo Monotonic State Rank vào `silver_transactions_current` (xử lý cờ `STUCK` 5 phút và bắt xung đột Terminal vào `silver_lifecycle_exceptions`).
   - Tính toán bảng `gold_rt_channel_metrics` (cửa sổ 1m/5m + Hysteresis partner degradation).
4. **Milestone 4 — Luồng Batch EOD, 3-Way Reconciliation (`dbt`) & Cloud Showcase:**
   - Xây dựng bộ sinh dữ liệu Sổ cái (`S2`), Merchant SCD2 (`S3`) và file CSV đối soát/sao kê (`S4` có kịch bản gửi đè file `v2` và lệch múi giờ).
   - Viết Airflow DAG kiểm tra Completeness Gate (`INCOMPLETE` vs Grace Period), quản lý `file_version`, và kích hoạt `dbt` models thực hiện đối soát 3 bên (Level 1 -> Level 3) kèm `run_version`.
   - Thực hiện nhánh showcase trên **Databricks Free Edition + AWS S3** và đối chiếu độ chính xác (Precision/Recall) với `G0`.
5. **Milestone 5 — Serving Dashboard (Metabase), Benchmark & Đóng gói Portfolio:**
   - Kết nối Metabase dựng 2 Dashboard (Payment Ops Near-RT & Finance Reconciliation).
   - Chạy benchmark hiệu năng, hoàn thiện bộ Unit/Integration Tests, viết ADRs và README tiếng Anh chuẩn kỹ sư.

---
