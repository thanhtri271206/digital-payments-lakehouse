# PROJECT EVIDENCE & AUDIT REPOSITORY

> **Hệ thống:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Quy chuẩn kỹ thuật WBS:** [`docs/wbs/00_wbs_dictionary_standard.md`](../wbs/00_wbs_dictionary_standard.md)  
> **Tài liệu tham chiếu gốc (SSOT):** [`docs/DE_Project2_Master_Handoff.md`](../DE_Project2_Master_Handoff.md)  
> **Tôn chỉ hoạt động:** *"Evidence Over Confidence"* — Không chấp nhận bất kỳ tính năng hoặc chất lượng nào được công nhận nếu không có bằng chứng khách quan (Execution logs, test results, UI snapshots, data samples).

---

## 1. MỤC ĐÍCH & Ý NGHĨA

Thư mục `docs/evidence/` là nơi lưu trữ tập trung toàn bộ các hiện vật kiểm chứng (Evidence Artifacts) thu thập được trong quá trình thực thi dự án từ Milestone 1 đến Milestone 5. 

Các hiện vật này phục vụ 3 mục đích chiến lược:
1. **Nghiệm thu Quality Gates:** Cung cấp dữ liệu thực chứng để ký duyệt chuyển pha giữa các Milestones (Gate 1 $\rightarrow$ Gate 5).
2. **Xây dựng Portfolio Đẳng cấp:** Trích xuất các hình ảnh đồ thị, snapshot Spark UI, Kafka UI, Metabase và số liệu benchmark đưa vào file `README.md` chính của repository.
3. **Phỏng vấn Tuyển dụng Tuyệt đối Thuyết phục:** Làm vũ khí thực chiến trong các vòng phỏng vấn kỹ thuật với Tech Leads / Hiring Managers, chứng minh 100% mã nguồn và kiến trúc đã được chạy thực tế, đo kiểm và chịu tải.

---

## 2. CẤU TRÚC PHÂN CẤP THƯ MỤC

```text
docs/evidence/
├── m1/                               # Milestone 1: Foundation Infra & Simulator
│   ├── wbs-m1-t01/                   # Task M1-T01: Docker infra logs & screenshot
│   ├── wbs-m1-t02/                   # Task M1-T02: Virtualenv & Config test logs
│   ├── wbs-m1-t03/                   # Task M1-T03: Pydantic test & contract outputs
│   ├── wbs-m1-t04/                   # Task M1-T04: State machine lifecycle test logs
│   ├── wbs-m1-t05/                   # Task M1-T05: Generator & fault injection logs
│   ├── wbs-m1-t06/                   # Task M1-T06: Ground Truth G0 samples & hash checks
│   ├── wbs-m1-t07/                   # Task M1-T07: Kafka producer throttling logs & UI snapshots
│   ├── wbs-m1-t08/                   # Task M1-T08: E2E Verification logs
│   └── gate_1_signoff/               # Bằng chứng nghiệm thu Quality Gate 1
│
├── m2/                               # Milestone 2: Walking Skeleton (Stream to Bronze)
│   ├── wbs-m2-t01/                   # Spark container & S3A extensions logs
│   ├── wbs-m2-t02/                   # MinIO S3A handshake logs
│   ├── wbs-m2-t03/                   # Kafka streaming reader test logs
│   ├── wbs-m2-t04/                   # Bronze schema & metadata test outputs
│   ├── wbs-m2-t05/                   # Bronze Delta sink & Spark UI active query snapshots
│   ├── wbs-m2-t06/                   # Delta history inspection logs
│   ├── wbs-m2-t07/                   # Crash & Outage Recovery Drill (C07) catch-up logs
│   ├── wbs-m2-t08/                   # Gate 2 verification test outputs
│   └── gate_2_signoff/               # Bằng chứng nghiệm thu Quality Gate 2
│
├── m3/                               # Milestone 3: Stateful Streaming & Silver DQ
│   ├── wbs-m3-t01/                   # foreachBatch dispatcher execution logs
│   ├── wbs-m3-t02/                   # 5% Circuit Breaker trip logs & quarantine sample
│   ├── wbs-m3-t03/                   # Watermark 2m dedup & late DLQ sample
│   ├── wbs-m3-t04/                   # History table append-only test outputs
│   ├── wbs-m3-t05/                   # Pre-aggregated MERGE logs & Delta plan explain
│   ├── wbs-m3-t06/                   # Dynamic timer & Stuck status audit logs
│   ├── wbs-m3-t07/                   # Terminal conflict exception sample
│   ├── wbs-m3-t08/                   # Multi-window RT metrics samples (1m/5m)
│   ├── wbs-m3-t09/                   # Hysteresis 2-window degradation logs
│   ├── wbs-m3-t10/                   # Gold RT metrics table samples & view test
│   ├── wbs-m3-t11/                   # Real-time failure drill logs & screenshots
│   ├── wbs-m3-t12/                   # Gate 3 verification & Freshness SLO benchmark
│   └── gate_3_signoff/               # Bằng chứng nghiệm thu Quality Gate 3
│
├── m4/                               # Milestone 4: Batch EOD & 3-Way Reconciliation
│   ├── wbs-m4-t01/                   # S2 Wallet ledger generator test logs & sample
│   ├── wbs-m4-t02/                   # S3 Merchant SCD2 generator test logs & sample
│   ├── wbs-m4-t03/                   # S4 Partner CSV multi-version diff & samples
│   ├── wbs-m4-t04/                   # Airflow stack startup logs & UI screenshot
│   ├── wbs-m4-t05/                   # Completeness gate sensor & 06:00 AM SLA logs
│   ├── wbs-m4-t06/                   # Version-aware CSV manifest logs & sample
│   ├── wbs-m4-t07/                   # dbt staging models test logs
│   ├── wbs-m4-t08/                   # 3-Way matching engine (Level 1 & 2) logs
│   ├── wbs-m4-t09/                   # 7 Break Types distribution & run_version sample
│   ├── wbs-m4-t10/                   # Databricks Cloud showcase lineage & run logs
│   ├── wbs-m4-t11/                   # G0 Precision/Recall >= 99.5% evaluation report
│   ├── wbs-m4-t12/                   # Gate 4 verification & end-to-end test outputs
│   └── gate_4_signoff/               # Bằng chứng nghiệm thu Quality Gate 4
│
└── m5/                               # Milestone 5: Serving, Benchmark & Portfolio
    ├── wbs-m5-t01/                   # Metabase setup API logs & connection screenshot
    ├── wbs-m5-t02/                   # Dashboard 1: Payment Ops Near-RT screenshot & JSON export
    ├── wbs-m5-t03/                   # Dashboard 2: Finance Reconciliation screenshot & JSON export
    ├── wbs-m5-t04/                   # 1M–5M scale benchmark throughput logs & charts
    ├── wbs-m5-t05/                   # Delta OPTIMIZE & VACUUM before/after comparison
    ├── wbs-m5-t06/                   # ADRs markdown syntax check logs
    ├── wbs-m5-t07/                   # Pytest full coverage report (>= 85%) & dbt test log
    ├── wbs-m5-t08/                   # README verification & 1-command demo screenshot/GIF
    ├── wbs-m5-t09/                   # Final project audit log & interview defense kit review
    └── gate_5_signoff/               # Bằng chứng nghiệm thu Final Launch Sign-Off
```

---

## 3. QUY TẮC ĐẶT TÊN & ĐỊNH DẠNG TỆP

1. **Log Files (`*.log`, `*.txt`):** Lưu trữ output terminal hoặc log ứng dụng, định danh rõ ràng: `{task_code}_{action_or_component}.log` (ví dụ: `wbs_m1_t01_docker_compose_ps.log`).
2. **Screenshots (`*.png`):** Ảnh chụp màn hình chất lượng cao ($\ge 1080p$), định danh theo ngữ cảnh: `{ui_source}_{view_context}.png` (ví dụ: `spark_ui_streaming_input_rate.png`, `metabase_payment_ops_live.png`).
3. **Data Samples (`*.jsonl`, `*.csv`, `*.json`):** Trích xuất mẫu nhỏ đại diện (từ 50 đến 200 dòng), tuyệt đối không lưu các file dữ liệu gigabytes vào git repo.
