# WBS MILESTONE 1 — FOUNDATION INFRASTRUCTURE, CONFIGURATION & DETERMINISTIC SIMULATOR (`G0`)

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md)  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/00_wbs_dictionary_standard.md)  
> **Kế hoạch tổng thể & Quality Gates:** [`docs/wbs/README.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/wbs/README.md)  
> **Vai trò điều hành:** Senior Technical Project Manager (TPM) & Lead Data Platform Architect  
> **Ngân sách Kế hoạch:** **30 giờ kỹ thuật**  
> **Trọng tâm Chuyển giao:** Hạ tầng Docker cốt lõi, Domain Models, State Machine, Simulator phát sinh dữ liệu tất định và Tệp Ground Truth `G0`.

---

## 1. TỔNG QUAN PHẠM VI & MỤC TIÊU MILESTONE 1

Milestone 1 là nền móng kỹ thuật quyết định sự thành bại của toàn bộ nền tảng Lakehouse. Mục tiêu của Milestone này không chỉ đơn thuần là dựng hạ tầng container, mà là xây dựng một **bộ sinh dữ liệu giả lập có kiểm soát (Deterministic Simulator)** với độ chân thực tương đương hệ thống ngân hàng/fintech thực tế:
1. **Local Infrastructure Stack:** Khởi động sạch cụm Kafka KRaft (Dual-listener) và MinIO (S3 API) với cơ chế tự động khởi tạo topics và storage buckets.
2. **Domain Modeling & Contracts:** Đóng gói 100% cấu trúc thực thể thanh toán (S1) và đối chiếu Ground Truth bằng Pydantic v2, áp dụng nghiêm ngặt các Global Invariants (`amount_minor` dạng `BIGINT`, chuẩn hóa múi giờ `UTC`).
3. **Payment Lifecycle State Machine:** Xây dựng máy trạng thái hữu hạn kiểm soát tiến trình giao dịch từ Rank 1 đến Rank 6 (`INITIATED` $\rightarrow$ `REFUNDED`), ngăn chặn chuyển đổi trạng thái phi lý.
4. **Controlled Fault Injection Engine:** Chủ động tiêm 7 nhóm dị biệt dữ liệu chuẩn hóa (Duplicate, Late within watermark, Late DLQ, Out-of-order, Stuck, Poison record, Conflicting terminal states) theo tỷ lệ xác suất cấu hình trước.
5. **Ground Truth (`G0`) Weapon:** Tự động xuất file Ground Truth song song với luồng sự kiện để làm "thước đo chân lý", phục vụ việc tính toán tự động **Precision / Recall** của tầng Data Quality và thuật toán Đối soát ở các milestone sau.
6. **Gate 1 Acceptance:** Vượt qua toàn bộ tiêu chí nghiệm thu khắt khe của **Gate 1** để mở khóa Milestone 2.

---

## 2. WORK BREAKDOWN STRUCTURE (WBS) MATRIX & EFFORT ALLOCATION

| Work Package | Mã Task | Tên Task Kỹ thuật | Phân loại | Ngân sách | Task Tiên quyết |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **WP 1.1: Infra & Base Config** | `WBS-M1-T01` | Docker Compose Stack Hardening & Service Automation | `[D1]` | 4 h | *Không* |
| | `WBS-M1-T02` | Project Layout, Virtualenv Management & Config Engine | `[D2]` | 4 h | `WBS-M1-T01` |
| **WP 1.2: Core Domain & State Machine** | `WBS-M1-T03` | Pydantic v2 Data Contracts & Strict Invariants | `[D1]` | 4 h | `WBS-M1-T02` |
| | `WBS-M1-T04` | Payment Lifecycle State Machine (Rank 1 $\rightarrow$ 6) | `[D1]` | 6 h | `WBS-M1-T03` |
| **WP 1.3: Fault Engine & Ground Truth** | `WBS-M1-T05` | Deterministic Seed Generator & Controlled Fault Injection | `[D1]` | 5 h | `WBS-M1-T04` |
| | `WBS-M1-T06` | Ground Truth (`G0`) Exporter & Seed Parity Check | `[D1]` | 3 h | `WBS-M1-T05` |
| **WP 1.4: Kafka Producer & Gate 1 Audit** | `WBS-M1-T07` | Resilient Kafka Producer with Rate Throttling | `[D2]` | 2 h | `WBS-M1-T05` |
| | `WBS-M1-T08` | Automated E2E Verification Suite & Quality Gate 1 Audit | `[D1]` | 2 h | `WBS-M1-T06`, `T07` |
| **TỔNG CỘNG** | **8 Tasks** | **Toàn bộ Gói việc Milestone 1** | | **30 h** | |

---

## 3. ĐẶC TẢ CHI TIẾT TỪNG TASK KỸ THUẬT (TASK SPECIFICATIONS)

### [WBS-M1-T01] Docker Compose Stack Hardening & Service Automation

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** Không có (Bắt đầu dự án).
- **Input Artifacts:**
  - File hiện có: `docker/docker-compose.yml`.
  - Đặc tả dịch vụ trong SSOT: Kafka 3.8.0 KRaft mode, MinIO RELEASE, Kafka UI.
- **Detailed Implementation Steps:**
  1. Kiểm tra và chuẩn hóa cấu hình `docker/docker-compose.yml`:
     - Kafka KRaft: Cấu hình Dual-Listener (`INTERNAL://kafka:29092` cho kết nối container nội bộ và `EXTERNAL://localhost:9092` cho script chạy từ máy host). Đảm bảo `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`.
     - MinIO Server: Mở port `9000` (API) và `9001` (Console Web UI). Cấu hình root user/password an toàn qua `.env`.
  2. Xây dựng dịch vụ khởi tạo `kafka-init`:
     - Tự động tạo 2 topic chuẩn: `payment.events.raw` (3 partitions, replication factor 1) và `wallet.ledger.cdc` (3 partitions, replication factor 1).
     - Thiết lập retention policy: 7 ngày (`retention.ms=604800000`).
  3. Xây dựng dịch vụ khởi tạo `minio-init`:
     - Tự động thiết lập alias client `mc` kết nối tới `minio:9000`.
     - Tự động tạo đủ 5 buckets bắt buộc: `bronze`, `silver`, `gold`, `ground-truth`, `checkpoints`.
  4. Cấu hình Healthcheck tin cậy cho từng container và khai báo cơ chế phụ thuộc `depends_on: {condition: service_healthy}` để đảm bảo toàn bộ stack khởi động theo đúng thứ tự logic.
  5. Viết script `Makefile` hoặc script PowerShell `scripts/infra_up.ps1` để đóng gói lệnh bật/tắt hạ tầng.
- **Deliverables / Output Artifacts:**
  - File hoàn thiện: `docker/docker-compose.yml`.
  - File môi trường mẫu: `.env.example` và `.env`.
  - Automation scripts: `scripts/infra_up.ps1` và `scripts/infra_down.ps1`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t01/`
  - **Artifacts:**
    - `logs/docker_compose_up.log` (Log chi tiết lệnh khởi động hạ tầng).
    - `logs/docker_compose_ps.log` (Danh sách container và trạng thái health check `healthy` / `exited (0)`).
    - `logs/kafka_topics_list.log` (Output lệnh kiểm tra 2 topics trên Kafka broker).
    - `logs/minio_mc_ls.log` (Output lệnh `mc ls local/` hiển thị 5 buckets).
    - `screenshots/kafka_ui_clusters_topics.png` (Ảnh giao diện Kafka UI hiển thị cluster active và 2 topics).
    - `screenshots/minio_console_buckets.png` (Ảnh giao diện MinIO Console hiển thị 5 buckets).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Lệnh `docker compose up -d` khởi động sạch 100% container (`kafka`, `kafka-init`, `kafka-ui`, `minio`, `minio-init`) trong trạng thái `healthy` hoặc `exited (0)`.
  - [ ] **AC-2:** Kafka UI tại `http://localhost:8080` hiển thị kết nối Active tới cluster, nhìn thấy 2 topic `payment.events.raw` và `wallet.ledger.cdc` với đúng 3 partitions.
  - [ ] **AC-3:** MinIO Console tại `http://localhost:9001` hiển thị đủ 5 buckets: `bronze`, `silver`, `gold`, `ground-truth`, `checkpoints`.
  - [ ] **AC-4:** Máy host gửi ping và kết nối thành công tới Kafka qua `localhost:9092` và MinIO qua `localhost:9000`.
- **Verification Method:**
  ```powershell
  # Khởi động hạ tầng
  powershell -ExecutionPolicy Bypass -File scripts/infra_up.ps1

  # Kiểm tra trạng thái container
  docker compose ps

  # Kiểm tra danh sách topic Kafka qua container CLI
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

  # Kiểm tra danh sách bucket MinIO
  docker compose run --rm minio-init mc ls local/
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lỗi xung đột cổng `9092`, `9000` hoặc `8080` do có dịch vụ khác trên máy host đang chiếm dụng.
  - *Mitigation:* Cung cấp script kiểm tra cổng trước khi start (`Test-NetConnection -Port 9092`) và cho phép override port trong file `.env`.

---

### [WBS-M1-T02] Project Layout, Virtualenv Management & Config Engine

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M1-T01`.
- **Input Artifacts:**
  - File cấu hình: `config/config.yaml`.
  - File quản lý gói: `pyproject.toml`, `.python-version`.
- **Detailed Implementation Steps:**
  1. Tổ chức lại cấu trúc mã nguồn theo chuẩn Enterprise:
     ```
     src/
       ├── digital_payments_lakehouse/
       │   ├── config/           # Pydantic Settings & YAML loader
       │   └── utils/            # Structured Logger, Time utilities
       └── simulator/            # Core Simulator Package
           ├── models.py         # Data contracts
           ├── lifecycle.py      # State Machine
           ├── generator.py      # Seed generator
           ├── fault_injector.py # Fault injection rules
           └── main.py           # CLI entrypoint
     tests/
       ├── unit/
       └── integration/
     data/
       └── ground_truth/
     ```
  2. Quản lý thư viện qua `uv` / `pyproject.toml`:
     - Khai báo dependencies cốt lõi: `pydantic>=2.7.0`, `pyyaml>=6.0`, `confluent-kafka>=2.4.0`, `loguru>=0.7.2`, `typer>=0.12.0`, `pytest>=8.0.0`.
  3. Cài đặt Config Loader Module (`src/digital_payments_lakehouse/config/settings.py`):
     - Định nghĩa schema cấu hình bằng Pydantic `BaseModel`: `ProjectSettings`, `StreamingSLOSettings`, `PartnerDegradationSettings`, `DataQualitySettings`, `SimulatorSettings`.
     - Tự động parse và validate file `config/config.yaml`. Nếu file config thiếu trường hoặc sai kiểu dữ liệu, ứng dụng dừng ngay với thông báo lỗi rõ ràng.
  4. Cài đặt Structured Logging Module (`src/digital_payments_lakehouse/utils/logger.py`):
     - Cấu hình Loguru xuất log định dạng JSON có cấu trúc ra `sys.stdout` và luân chuyển file tại `logs/simulator.log`.
- **Deliverables / Output Artifacts:**
  - Cấu hình dependency: `pyproject.toml` hoàn thiện.
  - Mã nguồn: `src/digital_payments_lakehouse/config/settings.py`, `src/digital_payments_lakehouse/utils/logger.py`.
  - Unit test: `tests/unit/test_config.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t02/`
  - **Artifacts:**
    - `logs/uv_sync.log` (Log lệnh cài đặt thư viện sạch không conflict).
    - `logs/pytest_test_config.log` (Log chạy test nạp config hợp lệ và kích hoạt ValidationError).
    - `data_samples/sample_structured_log.json` (Trích xuất 5 dòng JSON log từ loguru kiểm chứng cấu trúc chuẩn).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Lệnh `uv sync` hoặc `pip install -e .` cài đặt thành công toàn bộ dependencies mà không gặp conflict.
  - [ ] **AC-2:** Module `settings.py` nạp thành công `config/config.yaml` và gán đúng các giá trị mặc định.
  - [ ] **AC-3:** Unit test `test_config.py` pass 100%, kiểm tra cả trường hợp file cấu hình hợp lệ và trường hợp cấu hình sai để kích hoạt `ValidationError`.
  - [ ] **AC-4:** Logger xuất chuỗi log chuẩn JSON có `timestamp`, `level`, `module`, `event`.
- **Verification Method:**
  ```powershell
  # Kiểm tra cài đặt môi trường
  uv run python -c "import pydantic, confluent_kafka, loguru; print('Environment Healthy')"

  # Chạy test nạp config
  uv run pytest tests/unit/test_config.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* `confluent-kafka` yêu cầu C-library binary (`librdkafka`) có thể gặp lỗi compile trên một số môi trường Windows.
  - *Mitigation:* Sử dụng pre-compiled wheel chính thức của `confluent-kafka` trên Windows qua `uv pip install`.

---

### [WBS-M1-T03] Pydantic v2 Data Contracts & Strict Invariants

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 4 hours
- **Prerequisites / Dependencies:** `WBS-M1-T02`.
- **Input Artifacts:**
  - File khởi thảo: `src/simulator/models.py`.
  - Quy chuẩn tiền tệ & thời gian: `docs/wbs/00_wbs_dictionary_standard.md`.
  - Schema S1 & G0 trong `docs/DE_Project2_Master_Handoff.md`.
- **Detailed Implementation Steps:**
  1. Hoàn thiện Pydantic model `PaymentEvent` trong `src/simulator/models.py`:
     - Khai báo các enum chuẩn: `EventType` (`INITIATED`, `PAYMENT_METHOD_SELECTED`, `AUTHORIZED`, `PROCESSING`, `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`, `REFUNDED`), `PaymentChannel` (`BANK_A`, `GATEWAY_A`, `GATEWAY_B`), `PaymentMethod` (`BANK_TRANSFER`, `CARD`, `WALLET_BALANCE`, `QR`).
     - **Thực thi Invariant Tiền tệ:** `amount_minor: int = Field(gt=0)` (kiểu `BIGINT`). Tuyệt đối không dùng kiểu float. Cung cấp flag riêng cho kịch bản cố tình inject poison record (`amount_minor <= 0`).
     - **Thực thi Invariant Thời gian:** `event_time: str` theo chuẩn ISO-8601 UTC (`YYYY-MM-DDTHH:mm:ss.SSSZ`). Trường `ingestion_time: Optional[str] = None` chỉ được gán khi event được đẩy vào broker hoặc ghi nhận tại platform.
  2. Xây dựng model `GroundTruthRecord`:
     - Các trường: `transaction_id`, `user_id`, `merchant_id`, `payment_channel`, `payment_method`, `amount_minor`, `currency`, `lifecycle_sequence` (List các event_types), `expected_final_state`, `expected_operational_status` (`NORMAL`, `STUCK`, `QUARANTINED`, `EXCEPTION`), `was_ever_stuck_expected: bool`, `injected_faults: List[str]`, `created_at`.
  3. Viết các custom validators và serialization helper:
     - `@field_validator('event_time')`: Đảm bảo parse được theo chuẩn UTC ISO format.
     - Hàm `.to_json_bytes()` tối ưu hóa cho Kafka producer.
- **Deliverables / Output Artifacts:**
  - File hoàn thiện: `src/simulator/models.py`.
  - Test suite: `tests/unit/test_models.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t03/`
  - **Artifacts:**
    - `logs/pytest_test_models.log` (Log kết quả chạy test contracts, float validation và ISO-8601 parsing).
    - `data_samples/payment_event_valid_sample.json` (Serialize mẫu 1 event hợp lệ với amount_minor là BIGINT).
    - `data_samples/payment_event_poison_sample.json` (Serialize mẫu 1 poison record bị chặn hoặc rẽ nhánh).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Model chặn đứng các giá trị `float` truyền vào `amount_minor` (tự động reject hoặc ép kiểu nguyên chặt chẽ không làm tròn số).
  - [ ] **AC-2:** Model bắt buộc chuỗi `event_time` hợp lệ theo ISO-8601 UTC.
  - [ ] **AC-3:** Serialize đối tượng `PaymentEvent` sang JSON string hoặc bytes bảo toàn nguyên vẹn độ chính xác của số nguyên lớn `BIGINT`.
  - [ ] **AC-4:** 100% unit tests trong `tests/unit/test_models.py` pass với đầy đủ các test cases biên.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_models.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* JSON serializer mặc định của Python có thể chuyển đổi sai định dạng datetime hoặc làm tròn số nguyên quá lớn.
  - *Mitigation:* Sử dụng phương thức `.model_dump_json()` tích hợp của Pydantic v2 (viết bằng Rust core), đảm bảo tốc độ cao và độ chính xác tuyệt đối.

---

### [WBS-M1-T04] Payment Lifecycle State Machine (Rank 1 $\rightarrow$ 6)

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 6 hours
- **Prerequisites / Dependencies:** `WBS-M1-T03`.
- **Input Artifacts:**
  - Q14 & Monotonic State Rank spec trong `docs/DE_Project2_Master_Handoff.md`.
- **Detailed Implementation Steps:**
  1. Xây dựng module `src/simulator/lifecycle.py`:
     - Định nghĩa **State Rank Table**:
       * Rank 1: `INITIATED`
       * Rank 2: `PAYMENT_METHOD_SELECTED`
       * Rank 3: `AUTHORIZED`
       * Rank 4: `PROCESSING`
       * Rank 5 (Terminal States): `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`
       * Rank 6 (Post-Terminal State): `REFUNDED` (chỉ hợp lệ sau `COMPLETED`)
  2. Cài đặt lớp `PaymentStateMachine`:
     - Phương thức `get_valid_next_states(current_state: EventType) -> List[EventType]`.
     - Phương thức `is_transition_allowed(from_state: EventType, to_state: EventType) -> bool`.
     - Phương thức `generate_lifecycle_path(terminal_type: str) -> List[EventType]`: Tự động sinh lộ trình các bước hợp lệ cho một giao dịch. Ví dụ:
       * Success Path: `INITIATED (1)` $\rightarrow$ `PAYMENT_METHOD_SELECTED (2)` $\rightarrow$ `AUTHORIZED (3)` $\rightarrow$ `PROCESSING (4)` $\rightarrow$ `COMPLETED (5)`.
       * Refund Path: Đi tiếp từ `COMPLETED (5)` $\rightarrow$ `REFUNDED (6)`.
       * Gateway Failure Path: `INITIATED (1)` $\rightarrow$ `...` $\rightarrow$ `PROCESSING (4)` $\rightarrow$ `FAILED (5)`.
       * Abandoned Path: `INITIATED (1)` $\rightarrow$ `PAYMENT_METHOD_SELECTED (2)` $\rightarrow$ `EXPIRED (5)` hoặc `CANCELLED (5)`.
  3. Xây dựng logic gán nhãn thời gian giữa các bước chuyển:
     - Giãn cách thời gian thực tế: Mỗi bước chuyển cách nhau một khoảng delta ngẫu nhiên (ví dụ: từ 500ms đến 5000ms).
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/lifecycle.py`.
  - Test suite: `tests/unit/test_lifecycle.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t04/`
  - **Artifacts:**
    - `logs/pytest_test_lifecycle.log` (Log kết quả chạy test ma trận chuyển đổi trạng thái Rank 1 -> 6).
    - `logs/lifecycle_coverage_report.txt` (Báo cáo đo lường test coverage đạt >= 90%).
    - `data_samples/lifecycle_path_samples.json` (Mẫu các lộ trình sinh: Success, Refund, Failure, Abandoned).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Chặn các chuyển đổi vi phạm quy luật monotonic (ví dụ: cấm chuyển lùi từ `PROCESSING (4)` về `INITIATED (1)`).
  - [ ] **AC-2:** Chặn chuyển trạng thái từ một Terminal state sang một Terminal state khác trong luồng chuẩn (ví dụ: từ `FAILED` không thể sang `COMPLETED`).
  - [ ] **AC-3:** Trạng thái `REFUNDED (6)` chỉ có thể sinh ra khi và chỉ khi trạng thái trước đó là `COMPLETED (5)`.
  - [ ] **AC-4:** Test coverage của `test_lifecycle.py` đạt $\ge 90\%$.
- **Verification Method:**
  ```powershell
  uv run pytest tests/unit/test_lifecycle.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Lộ trình chuyển trạng thái không bao quát hết các kịch bản thực tế của ngân hàng (ví dụ hủy giao dịch ngay sau khi chọn phương thức thanh toán).
  - *Mitigation:* Cấu hình ma trận chuyển đổi (Transition Matrix) tường minh dưới dạng Dictionary/Graph có thể mở rộng.

---

### [WBS-M1-T05] Deterministic Seed Generator & Controlled Fault Injection

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 5 hours
- **Prerequisites / Dependencies:** `WBS-M1-T04`.
- **Input Artifacts:**
  - `config/config.yaml` (các tham số `simulator.rates.*`).
  - Cấu hình seed mặc định: `20260930`.
- **Detailed Implementation Steps:**
  1. Cài đặt Generator Engine trong `src/simulator/generator.py`:
     - Khởi tạo `self.rng = random.Random(seed)`. Đảm bảo toàn bộ các hàm sinh ngẫu nhiên (`random()`, `randint()`, `choice()`, `uniform()`) đều sử dụng instance `self.rng` này, tuyệt đối không dùng module `random` toàn cục.
  2. Xây dựng module tiêm lỗi `src/simulator/fault_injector.py`:
     - Cài đặt 7 kịch bản tiêm lỗi dựa trên xác suất cấu hình:
       1. **Duplicate Event (tỷ lệ 0.5%):** Nhân đôi một event trong vòng đời với cùng `event_id` và cùng nội dung, phát lại sau đó vài giây.
       2. **Late Event Short (tỷ lệ 1.0%):** Dịch lùi `event_time` trễ từ 5s đến 120s (nằm trong Watermark 2 phút).
       3. **Late Event Long (tỷ lệ 0.2%):** Dịch lùi `event_time` trễ từ 3 phút đến 15 phút (vượt quá Watermark 2 phút, kích hoạt đẩy vào `silver_late_events_dlq`).
       4. **Out-of-Order Delivery (tỷ lệ 0.5%):** Đảo lộn thứ tự gửi 2 event liên tiếp của cùng 1 transaction (ví dụ gửi `AUTHORIZED` trước `PAYMENT_METHOD_SELECTED`).
       5. **Missing Terminal / Stuck Transaction (tỷ lệ 0.3%):** Ngắt ngang vòng đời giao dịch tại trạng thái `PROCESSING`, không phát event kết thúc, làm giao dịch bị treo quá 5 phút.
       6. **Poison Record (tỷ lệ 0.2%):** Cố tình sinh giá trị `amount_minor < 0` hoặc gán enum sai (ví dụ `payment_channel: "UNKNOWN_CHANNEL"`) để kiểm tra cơ chế tách bản ghi Quarantine và kích hoạt 5% Circuit Breaker khi cần.
       7. **Conflicting Terminal States (tỷ lệ 0.1%):** Sau khi phát event `COMPLETED`, cố tình phát thêm event `FAILED` cho cùng một `transaction_id` để kiểm tra bảng `silver_lifecycle_exceptions`.
  3. Tích hợp tiêm lỗi cận biên múi giờ EOD (Cutoff boundary testing):
     - Sinh các giao dịch có `event_time` rơi vào `16:59:58 UTC` và `17:00:02 UTC` (xung quanh mốc `23:59:59 UTC+07`).
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/generator.py`, `src/simulator/fault_injector.py`.
  - Test suite: `tests/unit/test_fault_injector.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t05/`
  - **Artifacts:**
    - `logs/pytest_test_fault_injector.log` (Log kết quả kiểm thử 7 kịch bản tiêm lỗi và tính tất định).
    - `logs/seed_parity_hash_comparison.log` (Output so sánh SHA-256 hash của 2 lần sinh với cùng seed 20260930).
    - `data_samples/fault_distribution_report.txt` (Thống kê phân phối tỷ lệ 7 loại lỗi trên 10,000 txns).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1 (Determinism):** Hai lần chạy độc lập với cùng `--seed 20260930` và cùng `--count 5000` tạo ra dãy sự kiện giống nhau từng byte (kiểm tra hash SHA-256).
  - [ ] **AC-2 (Fault Distribution):** Tỷ lệ xuất hiện thực tế của 7 loại lỗi nằm trong khoảng sai số cho phép $\pm 0.3\%$ so với cấu hình trong `config.yaml`.
  - [ ] **AC-3:** Mỗi lỗi được tiêm đều gắn kèm nhãn nhận diện (fault metadata) để truyền sang model Ground Truth.
- **Verification Method:**
  ```powershell
  # Chạy test kiểm chứng tính tất định và tỷ lệ tiêm lỗi
  uv run pytest tests/unit/test_fault_injector.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Việc tiêm lỗi làm hỏng toàn bộ pipeline sinh dữ liệu khiến generator bị crash giữa chừng.
  - *Mitigation:* Bọc từng kịch bản tiêm lỗi trong generator wrapper độc lập; nếu lỗi tiêm không thành công, fallback về luồng chuẩn và ghi log cảnh báo.

---

### [WBS-M1-T06] Ground Truth (`G0`) Exporter & Seed Parity Check

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 3 hours
- **Prerequisites / Dependencies:** `WBS-M1-T05`.
- **Input Artifacts:**
  - Model `GroundTruthRecord` từ `WBS-M1-T03`.
  - Generator Engine từ `WBS-M1-T05`.
- **Detailed Implementation Steps:**
  1. Xây dựng module xuất tệp Ground Truth `src/simulator/ground_truth.py`:
     - Khi mỗi giao dịch hoàn thành tiến trình sinh (bao gồm cả các event và các lỗi đã tiêm), hệ thống tổng hợp một bản ghi `GroundTruthRecord` tương ứng.
     - Xác định rõ:
       * `expected_final_state`: Trạng thái cuối cùng hợp lệ trên thực tế (ví dụ `COMPLETED`).
       * `expected_operational_status`: `NORMAL`, `STUCK` (nếu rơi vào kịch bản missing terminal), `QUARANTINED` (nếu là poison), hoặc `EXCEPTION` (nếu là terminal conflict).
       * `was_ever_stuck_expected`: True nếu giao dịch từng bị kẹt ở `PROCESSING` quá 5 phút trước khi nhận terminal muộn.
       * `injected_faults`: Danh sách các mã lỗi đã tiêm (ví dụ `["POISON_NEGATIVE_AMOUNT"]`).
  2. Ghi dữ liệu Ground Truth dạng JSON Lines (`.jsonl`):
     - Đường dẫn đầu ra: `data/ground_truth/G0_events_seed_{seed}.jsonl`.
     - Đồng thời hỗ trợ upload trực tiếp lên MinIO bucket `s3://ground-truth/` thông qua MinIO SDK hoặc script đồng bộ.
  3. Viết script kiểm tra Seed Parity (`tests/integration/test_seed_parity.py`):
     - Tự động chạy 2 tiến trình generator với cùng seed và kiểm tra tính tương đồng tuyệt đối của cả luồng event lẫn tệp `G0`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/ground_truth.py`.
  - Integration test: `tests/integration/test_seed_parity.py`.
  - Tệp mẫu Ground Truth: `data/ground_truth/G0_events_seed_20260930.jsonl`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t06/`
  - **Artifacts:**
    - `logs/pytest_test_seed_parity.log` (Log chạy test seed parity cho cả event stream và file G0).
    - `logs/g0_file_hash_verification.log` (Lệnh `Get-FileHash` kiểm tra SHA-256 hash của G0 trùng khớp tuyệt đối).
    - `data_samples/g0_sample_100_records.jsonl` (Trích xuất 100 dòng mẫu Ground Truth G0 chứng minh phân loại NORMAL, STUCK, POISON, TERMINAL_CONFLICT).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Tỷ lệ khớp 1:1 giữa số lượng `transaction_id` sinh ra và số lượng bản ghi trong tệp `G0`.
  - [ ] **AC-2:** File `G0` ghi nhận chính xác 100% các giao dịch bị tiêm lỗi `STUCK`, `POISON`, `TERMINAL_CONFLICT`.
  - [ ] **AC-3:** SHA-256 hash của tệp `G0` hoàn toàn trùng khớp khi chạy lại cùng seed.
- **Verification Method:**
  ```powershell
  # Chạy test seed parity
  uv run pytest tests/integration/test_seed_parity.py -v

  # Kiểm tra tính toàn vẹn của file G0
  uv run python -m src.simulator.main --seed 20260930 --count 1000 --export-g0-only
  powershell -Command "Get-Item data/ground_truth/G0_events_seed_20260930.jsonl | Select-Object Length"
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Dung lượng file `G0` phình to khi chạy benchmark với quy mô 1M giao dịch.
  - *Mitigation:* Cấu hình xuất file nén `jsonl.gz` khi số lượng giao dịch vượt quá 100.000 bản ghi.

---

### [WBS-M1-T07] Resilient Kafka Producer with Rate Throttling

- **Classification:** `[D2-Core]`
- **Estimated Effort:** 2 hours
- **Prerequisites / Dependencies:** `WBS-M1-T05`.
- **Input Artifacts:**
  - Cấu hình Kafka trong `config/config.yaml` (`bootstrap_servers: "localhost:9092"`).
  - Generator Engine từ `WBS-M1-T05`.
- **Detailed Implementation Steps:**
  1. Cài đặt Kafka Producer Client trong `src/simulator/kafka_producer.py`:
     - Sử dụng thư viện `confluent-kafka`. Cấu hình các tham số nhà sản xuất tin cậy:
       * `bootstrap.servers`: `localhost:9092` (máy host kết nối tới container Kafka).
       * `acks`: `"all"` hoặc `1`.
       * `linger.ms`: 20.
       * `compression.type`: `"snappy"`.
       * `queue.buffering.max.messages`: 100000.
  2. Cơ chế điều tiết tốc độ phát (Rate Limiter / Throttling):
     - Hỗ trợ chế độ Baseline: `5 events/s`.
     - Hỗ trợ chế độ Burst: `50 events/s`.
     - Hỗ trợ chế độ Max Speed (dành cho benchmark nạp dữ liệu nhanh).
     - Gán giá trị thời gian hệ thống vào trường `ingestion_time` ngay tại thời điểm gọi `producer.produce()`.
  3. Callback & Error Handling:
     - Định nghĩa `delivery_report(err, msg)`: Log lỗi nếu gửi thất bại, tăng metric `events_produced_success` hoặc `events_produced_failure`.
     - Xử lý signal ngắt đột ngột (Graceful shutdown trên `SIGINT` / `SIGTERM`) gọi `producer.flush(timeout=5)`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/kafka_producer.py`.
  - Integration test: `tests/integration/test_kafka_producer.py`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t07/`
  - **Artifacts:**
    - `logs/pytest_test_kafka_producer.log` (Log chạy test gửi message thành công 0% drop).
    - `logs/producer_rate_throttling_benchmark.log` (Log đo lường throughput 5 events/s và burst 50 events/s).
    - `screenshots/kafka_ui_messages_live.png` (Ảnh giao diện Kafka UI hiển thị payload JSON trực tiếp trong topic payment.events.raw).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Producer kết nối ổn định tới topic `payment.events.raw` trên broker local `localhost:9092`.
  - [ ] **AC-2:** Gửi thành công 1,000 events với tỷ lệ thất bại delivery bằng **0%**.
  - [ ] **AC-3:** Kafka UI tại `http://localhost:8080` hiển thị chính xác các message mới được đẩy vào topic với payload JSON hợp lệ.
  - [ ] **AC-4:** Cơ chế rate throttling hoạt động chính xác trong dung sai $\pm 10\%$ số events/giây mong muốn.
- **Verification Method:**
  ```powershell
  # Chạy test gửi message vào Kafka local
  uv run pytest tests/integration/test_kafka_producer.py -v
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Bộ đệm producer bị nghẽn (Queue full) khi phát ở tốc độ cao mà Kafka không kịp commit.
  - *Mitigation:* Cấu hình phương thức `producer.poll(0)` đều đặn sau mỗi batch produce và xử lý `BufferError` bằng retry exponential backoff.

---

### [WBS-M1-T08] Automated E2E Verification Suite & Quality Gate 1 Audit

- **Classification:** `[D1-Implement+Test]`
- **Estimated Effort:** 2 hours
- **Prerequisites / Dependencies:** `WBS-M1-T06`, `WBS-M1-T07`.
- **Input Artifacts:**
  - Tiêu chí nghiệm thu Gate 1 trong `docs/wbs/README.md`.
  - Toàn bộ deliverables của Milestone 1.
- **Detailed Implementation Steps:**
  1. Xây dựng CLI Entrypoint hoàn chỉnh cho simulator trong `src/simulator/main.py`:
     - Hỗ trợ các tham số: `--seed`, `--count`, `--rate`, `--mode` (`kafka` | `file` | `both`), `--export-g0`.
  2. Viết kịch bản kiểm thử tích hợp đầu-cuối E2E (`tests/integration/test_milestone_1_gate.py`):
     - Bước 1: Kiểm tra trạng thái health check của các container Docker (`kafka`, `minio`).
     - Bước 2: Chạy simulator phát 2,000 sự kiện vào Kafka và đồng thời xuất file `G0`.
     - Bước 3: Đọc ngược lại từ Kafka topic `payment.events.raw` bằng consumer tạm, kiểm tra số lượng và schema của các event đã nhận.
     - Bước 4: So sánh tệp `G0` và xác nhận có đầy đủ các nhóm lỗi được tiêm.
  3. Lập Báo cáo Nghiệm thu Gate 1 (Evidence Checklist) đính kèm log thực thi vào `docs/reports/gate_1_signoff.md`.
- **Deliverables / Output Artifacts:**
  - Mã nguồn: `src/simulator/main.py`.
  - Test suite E2E: `tests/integration/test_milestone_1_gate.py`.
  - Tài liệu nghiệm thu: `docs/reports/gate_1_signoff.md`.
- **Evidence to Capture & Storage Directory:**
  - **Directory:** `docs/evidence/m1/wbs-m1-t08/`
  - **Artifacts:**
    - `logs/gate_1_e2e_verification.log` (Log chạy full kịch bản test tích hợp đầu cuối Gate 1).
    - `logs/pytest_full_milestone_1.log` (Log kết quả chạy toàn bộ test suite `pytest tests/` pass 100%).
    - `data_samples/e2e_simulation_run_summary.json` (Báo cáo tổng kết số lượng events sinh, gửi và kiểm chứng).
- **Definition of Done (DoD) & Acceptance Criteria (AC):**
  - [ ] **AC-1:** Toàn bộ test suite của Milestone 1 (`pytest tests/`) pass 100% không có lỗi hoặc warning nghiêm trọng.
  - [ ] **AC-2:** Vượt qua 100% các tiêu chí của **Gate 1 (Milestone 1 $\rightarrow$ Milestone 2)** đã cam kết trong `docs/wbs/README.md`.
  - [ ] **AC-3:** Có tệp log thực thi thực tế chứng minh Simulator chạy thành công với `--seed 20260930`.
- **Verification Method:**
  ```powershell
  # Chạy kiểm thử toàn bộ Gate 1
  uv run pytest tests/integration/test_milestone_1_gate.py -v -s

  # Chạy lệnh CLI mẫu
  uv run python -m src.simulator.main --seed 20260930 --count 1000 --rate 50 --mode both
  ```
- **Potential Risks & Mitigation:**
  - *Risk:* Thời gian chạy test E2E quá lâu do phải chờ Kafka consumer.
  - *Mitigation:* Giới hạn số lượng bản ghi kiểm thử trong test suite ở mức 500 bản ghi và thiết lập consumer timeout hợp lý.

---

## 4. MILESTONE 1 QUALITY GATE 1 CHECKLIST (TIÊU CHÍ CHỐT SỔ M1)

> **Thư mục Lưu trữ Bằng chứng Nghiệm thu:** `docs/evidence/m1/gate_1_signoff/`  
> Toàn bộ artifacts nghiệm thu phải được tập hợp đầy đủ trước phiên review.

Trước khi ký nghiệm thu (Sign-off) Milestone 1 để chuyển sang Milestone 2, TPM và Học viên bắt buộc phải đánh dấu kiểm tra đủ toàn bộ các mục sau:

- [ ] **Gate 1.1 (Infra):** Cụm Docker Compose khởi động sạch không lỗi, Kafka UI hiển thị đủ 2 topics, MinIO hiển thị đủ 5 buckets. *(Artifacts: `docs/evidence/m1/gate_1_signoff/docker_ps.log`, `kafka_ui_clusters.png`, `minio_buckets.png`)*
- [ ] **Gate 1.2 (Determinism):** Simulator chạy 2 lần với cùng một seed sinh ra hai bộ output có hash SHA-256 trùng khớp 100%. *(Artifacts: `docs/evidence/m1/gate_1_signoff/sha256_parity_audit.txt`)*
- [ ] **Gate 1.3 (Monotonic Lifecycle):** Máy trạng thái chuyển đổi tuần tự từ Rank 1 đến Rank 6, không có chuyển lùi trạng thái và không có chuyển đổi trái phép. *(Artifacts: `docs/evidence/m1/gate_1_signoff/lifecycle_transition_test.log`)*
- [ ] **Gate 1.4 (Fault Injection):** 7 kịch bản tiêm lỗi hoạt động chuẩn xác theo xác suất định sẵn, các sự kiện cận biên múi giờ EOD được sinh đúng. *(Artifacts: `docs/evidence/m1/gate_1_signoff/fault_injection_rates.txt`)*
- [ ] **Gate 1.5 (Ground Truth G0):** File `G0` ghi nhận đầy đủ bản chất thật của 100% giao dịch, sẵn sàng làm cơ sở đo lường tự động Precision/Recall. *(Artifacts: `docs/evidence/m1/gate_1_signoff/g0_sample_audit.jsonl`)*
- [ ] **Gate 1.6 (Code Quality):** 100% mã nguồn tuân thủ Type Hints, không dùng `print()`, log cấu trúc JSON, pytest pass 100%. *(Artifacts: `docs/evidence/m1/gate_1_signoff/pytest_final_m1.log`, `ruff_check.log`)*

---

## 5. SCHEDULE RISK, CONTINGENCY BUFFER & REMINDER

> [!WARNING] Cảnh báo Nguy cơ Vượt Ngân sách Thời gian (Schedule Overrun Risk)
> - **Ngân sách Kế hoạch:** **30 giờ**.
> - **Khu vực có rủi ro cao gây trễ tiến độ:**
>   1. **`WBS-M1-T04` (State Machine) & `WBS-M1-T05` (Fault Injection):** Việc xử lý tỉ mỉ 7 loại lỗi và điều phối các nhánh rẽ trạng thái có thể phát sinh thêm 2–4 giờ so với dự toán ban đầu nếu gặp các ca kiểm thử biên (edge cases) phức tạp.
>   2. **`WBS-M1-T01` (Docker Networking on Windows):** Các vấn đề về port binding, WSL2 Docker desktop engine hoặc MinIO S3 credentials handshake trên hệ điều hành Windows có thể tiêu tốn thêm thời gian gỡ lỗi (debug).
> - **Nguyên tắc Đánh đổi của TPM (Quality Leverage Policy):**
>   * **Ưu tiên hàng đầu là Chất lượng Sản phẩm Kỹ thuật & Độ chính xác của Simulator.** Bộ simulator và tệp Ground Truth `G0` là vũ khí chứng minh năng lực xuyên suốt dự án; nếu làm ẩu ở bước này, toàn bộ tầng Silver DQ và Batch Reconciliation phía sau sẽ bị sụp đổ.
>   * Cho phép tổng thời gian thực tế của Milestone 1 có thể dao động đến **34–36 giờ** (+15–20% Contingency Buffer) nếu thời gian vượt trội đó dùng để củng cố độ bao phủ của bộ test và bảo đảm tính tất định tuyệt đối của Ground Truth `G0`.

