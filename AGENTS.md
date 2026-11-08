# AGENTS.md — Repository-Local Engineering & QA/QC Governance

> **Project:** Digital Payments Monitoring & 3-Way Reconciliation Lakehouse Platform  
> **Repository:** `digital-payments-lakehouse`  
> **Target Audience:** All AI Coding Agents & Human Engineers operating in this codebase.  
> **Authority:** Mandated Technical Standards based on [`docs/DE_Project2_Master_Handoff.md`](file:///c:/DE-project/digital-payments-lakehouse/docs/DE_Project2_Master_Handoff.md) and [`DE_AI_QA_QC_Universal_Framework_and_Agent_Prompt.md`](file:///c:/DE-project/digital-payments-lakehouse/DE_AI_QA_QC_Universal_Framework_and_Agent_Prompt.md).

---

## 1. Core Operating Principles

1. **Evidence Over Confidence:**
   - Statements like "looks good", "implemented successfully", or "all tests pass" without verifiable command logs are strictly prohibited.
   - Every verification must cite exact command invocations, exit codes, and output summaries.
   - Checks are strictly classified as `PASS`, `FAIL`, `NOT RUN`, or `NOT APPLICABLE`.
   - Never fabricate logs, counts, metrics, or test outputs.

2. **Risk-Based Review & Centrality (Leaf / Branch / Trunk):**
   - Review depth depends on blast radius, data/financial criticality, dependency centrality, and reversibility—not diff line count.
   - **Leaf:** Isolated components (e.g., pure parsing functions, local CLI utilities).
   - **Branch:** Bounded components (e.g., Kafka source adapter, specific dbt staging model).
   - **Trunk:** Core shared foundations (e.g., `config/config.yaml`, Pydantic models, Delta Lake schema definitions, Monotonic State progression logic, 3-Way reconciliation engine). Trunk changes demand deep adversarial review and regression evidence.

3. **Separate Implementation from Adversarial Review:**
   - When reviewing code, adopt an adversarial stance: assume edge cases are unhandled until proven otherwise.
   - Search for failure scenarios: out-of-order events, network retries, duplicate delivery, schema drift, partial batch failures, integer overflow, timezone boundary mismatches.

4. **Separate Merge-Ready from Launch-Ready:**
   - **Merge-Ready:** Code compiles, passes linters/type-checks, unit/integration tests pass with real evidence, blocker/high findings resolved.
   - **Launch-Ready:** Verified against end-to-end data volume, failure recovery tested, observability dashboards functional, SLOs validated, runbooks in place.

5. **Human Accountability & Non-Negotiable Boundaries:**
   - AI agents must NEVER silently waive failed quality gates, alter business definitions, modify accounting invariants, or self-approve high-risk changes.
   - All high-risk decisions and gate sign-offs require explicit human confirmation.

---

## 2. Lakehouse Technical Invariants (Strict Enforcement)

Every agent proposing, generating, or reviewing code MUST verify compliance with these project invariants:

| Category | Non-Negotiable Invariant | Violation Severity |
| :--- | :--- | :--- |
| **Financial / Money** | `amount_minor` must be `BIGINT` (integer minor units, e.g., VND). **NEVER use `FLOAT` or `DOUBLE`** for monetary values. | **BLOCKER** |
| **Ledger Integrity** | Internal Wallet Ledger (`S2`) is **strictly Append-only**. No physical `UPDATE` or `DELETE`. Corrections require compensating `REVERSAL` or `ADJUSTMENT`. | **BLOCKER** |
| **Lifecycle State** | State progression follows **Monotonic State Rank (1 -> 6)** with **Terminal Lock**. Lower rank events cannot regress higher rank state. Conflicting terminal events must go to `silver_lifecycle_exceptions`. | **BLOCKER** |
| **Time Semantics** | Strict separation between `event_time` (business event occurrence) and `ingestion_time` (lakehouse arrival). Business KPIs must derive from `event_time`. | **HIGH** |
| **Timezone Standard** | Internal storage/Delta tables strictly normalized to `UTC`. EOD Cutoff (`23:59:59`) and presentation layers use `Asia/Ho_Chi_Minh (UTC+07:00)`. | **HIGH** |
| **Streaming SLO & DQ** | Streaming Freshness SLO: p95 <= 30s, p99 <= 60s. Watermark = 2 minutes. Events arriving > 2m late route to `silver_late_events_dlq`. | **HIGH** |
| **Circuit Breaker** | Micro-batch error rate > 5% (with batch size >= 50) MUST trip the **Circuit Breaker** and abort the job to protect downstream Delta tables. | **BLOCKER** |
| **Stuck Detection** | Transactions lingering in `PROCESSING` > 5 minutes with no valid progress event receive `operational_status = STUCK` via dynamic timer; `lifecycle_state` remains `PROCESSING`. | **MEDIUM** |
| **3-Way Reconciliation** | EOD Cutoff at `23:59:59 UTC+07`. Missing sources mark run as `INCOMPLETE`. Break types strictly categorized into 7 standard groups. | **HIGH** |
| **Ground Truth (G0)** | Simulator output must be reproducible using deterministic seed (`seed = 20260930`). DQ and Reconciliation accuracy must benchmark against `G0`. | **HIGH** |

---

## 3. Directory Layout for Agent Capabilities

All AI agent workflows, skills, rules, and review templates reside in `.agents/`:

```text
.agents/
├── README.md                           # Documentation & invocation guide for the dual-engine system
├── rules/                              # Execution guardrails & technical invariants
│   ├── system-safety-bounds.md         # PreToolUse bounds (protects Docker volumes & data)
│   ├── lakehouse-invariants-enforcement.md # Strict enforcement of BIGINT, Ledger & Monotonic rank
│   └── code-quality-and-style.md       # PEP 8, Pydantic v2, PySpark idioms, SQL & dbt conventions
├── skills/                             # Focused, progressively disclosed capabilities
│   ├── de-architect-mentor/            # Deep-dive engine mechanics, naive traps & trade-off ADRs
│   ├── de-implementer/                 # Senior engineering builder (production code & test runner)
│   ├── lakehouse-streaming-engine/     # Kafka KRaft, Spark Structured Streaming & Delta Lake
│   ├── financial-reconciliation-dbt/   # 3-Way Reconciliation, 7 Break Types & Airflow gates
│   ├── de-troubleshooter-and-debug/    # Fast diagnostic playbook for Docker, Kafka, MinIO, Spark
│   ├── de-qa-qc-orchestrator/          # Master coordinator for change review & gate verification
│   ├── de-engineering-reviewer/        # Adversarial code & architecture reviewer
│   ├── de-data-correctness-qa/         # Lakehouse, semantic, and financial reconciliation QA
│   ├── de-operational-readiness/       # Streaming SLO, recovery, idempotency & Airflow ops
│   └── ... (existing project skills)
├── workflows/                          # Standard operating procedures
│   ├── teach-decide-and-build.md       # [FLAGSHIP] 4-phase Mentorship + Implementation workflow
│   ├── interview-prep-and-defense.md   # Mock technical interview drill & viva-voce prep
│   ├── failure-drill-and-recovery.md   # Chaos injection, outage catch-up & recovery testing
│   ├── milestone-gate-signoff.md       # Milestone transition audit (Gate 1 -> Gate 5)
│   ├── review-change.md                # Standard end-to-end change review workflow
│   ├── streaming-dq-review.md          # Kafka / PySpark Structured Streaming / Silver dual-table review
│   ├── reconciliation-batch-review.md  # Batch EOD / Airflow / dbt 3-Way Reconciliation review
│   └── launch-readiness-review.md      # Milestone transition and Cloud Showcase readiness check
└── templates/                          # Standardized artifact outputs
    ├── task-adr-and-tradeoffs.md       # Standardized ADR, options & trade-off matrix template
    ├── interview-defense-card.md       # Interview talking points & edge-case challenge card
    ├── execution-evidence-log.md       # Verifiable command and test execution log
    ├── failure-drill-report.md         # Chaos testing & recovery trace report
    ├── change-review-report.md         # Full change review report template
    ├── finding-card.md                 # Single finding report structure
    ├── g0-evaluation-report.md         # Precision / Recall benchmark template against G0
    └── milestone-gate-report.md        # Milestone quality gate sign-off template
```

---

## 4. Invocation Guidelines

Depending on your objective, invoke the corresponding workflow:

1. **Developing a New Task (Mentorship & Implementation):**
   - Execute [`teach-decide-and-build.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/teach-decide-and-build.md). The Agent will deep-dive engine mechanics, compare trade-offs, implement code, and deliver interview talking points.
2. **Reviewing an Existing Change / PR:**
   - Execute [`review-change.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/review-change.md) with `de-qa-qc-orchestrator` and `de-engineering-reviewer`.
3. **Practicing Technical Interviews:**
   - Execute [`interview-prep-and-defense.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/interview-prep-and-defense.md) to drill down on architectural choices and failure scenarios.
4. **Validating Resilience & Chaos:**
   - Execute [`failure-drill-and-recovery.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/failure-drill-and-recovery.md) for outage catch-up, circuit breaker trips, and late-event DLQ.
5. **Evaluating Milestone Quality Gates:**
   - Execute [`milestone-gate-signoff.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/workflows/milestone-gate-signoff.md) to audit DoD and sign off transitions (e.g. Gate 1 -> Gate 2).
