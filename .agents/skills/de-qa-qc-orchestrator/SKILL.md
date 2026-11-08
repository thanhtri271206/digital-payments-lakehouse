---
name: DE QA/QC Orchestrator
description: Coordinate end-to-end AI-assisted QA, QC, and engineering reviews. Classify risk (Leaf/Branch/Trunk), select proportionate checks, dispatch adversarial and data-correctness reviews, and consolidate evidence for merge and launch decisions.
---

# DE QA/QC Orchestrator

The **DE QA/QC Orchestrator** is the lead coordinator for assessing code changes, data pipeline deliverables, and milestone releases in the `digital-payments-lakehouse` platform.

## 1. When to Invoke

Invoke this skill whenever:
- A new feature, bug fix, or refactor diff is ready for review.
- A WBS task deliverable (from `docs/wbs/`) needs verification against its Definition of Done (DoD).
- Evaluating readiness to transition between Milestones (e.g., Gate 1 -> Gate 2).
- Validating end-to-end data pipelines against Ground Truth `G0`.

## 2. Operating Procedure (10-Step Workflow)

When orchestrating a review, execute the following standardized steps:

```text
[Input Diff / Task Spec]
          │
          ▼
1. Scope & Intent Analysis ──► Understand requirement & invariants
          │
          ▼
2. Dependency Discovery   ──► Map Leaf / Branch / Trunk & affected callers
          │
          ▼
3. Risk Classification    ──► Low / Medium / High / Critical & Blast Radius
          │
          ▼
4. Validation Selection   ──► Pick proportionate automated checks
          │
          ▼
5. Evidence Execution     ──► Run CLI / Pytest / Docker commands (Collect logs)
          │
          ▼
6. Adversarial Review     ──► Dispatch to `de-engineering-reviewer`
          │
          ▼
7. Data Correctness Check ──► Dispatch to `de-data-correctness-qa` (if data/pipeline)
          │
          ▼
8. Operational Check      ──► Dispatch to `de-operational-readiness` (if streaming/infra)
          │
          ▼
9. Finding Consolidation  ──► Classify findings (Blocker/High/Med/Low/Observation)
          │
          ▼
10. Readiness Decision    ──► Formulate Merge-Ready & Launch-Ready Verdicts
```

### Step 1: Understand Scope & Acceptance Criteria
- Identify the exact WBS Task ID (e.g., `WBS-M1-T02`) or issue description.
- Identify the explicit Acceptance Criteria (ACs) and core technical invariants from `AGENTS.md` and `docs/DE_Project2_Master_Handoff.md`.
- Ensure requirements are clear; do not guess missing financial or business semantics.

### Step 2: Discover Impact & Centrality
- Classify the target component into the Dependency Tree:
  - **Leaf:** Isolated utility, standalone CLI parser, helper function with zero downstream dependents.
  - **Branch:** Source adapters, specific streaming transform, isolated dbt model, individual Airflow task.
  - **Trunk:** Shared foundational models (`src/simulator/models.py`), Delta Lake schema definitions, Monotonic State progression logic, 3-Way Reconciliation matching engine, `config/config.yaml`.
- Identify upstream feeds and downstream consumers affected by the diff.

### Step 3: Classify Risk Level
- Combine Blast Radius, Centrality, and Reversibility:
  - **Low:** Leaf component, fully reversible, no financial or state impact. Focused diff review + unit tests.
  - **Medium:** Bounded Branch component, standard logic change. Unit + component tests + data validation.
  - **High:** Trunk component, affects core lifecycle state, schema contracts, watermark, or reconciliation logic. Deep adversarial review + regression tests + failure simulation.
  - **Critical:** Destructive operations, financial ledger modifications, security credentials, irreversible production migrations. Requires human mentor sign-off.

### Step 4 & 5: Execute & Collect Proportionate Evidence
- Select tests proportionate to risk. Never run unnecessary test suites just to inflate metrics.
- Collect deterministic command logs, exit codes, and output summaries.
- Mark all checks strictly as:
  - `PASS`: Executed and verified with output evidence.
  - `FAIL`: Executed and produced an error or mismatch.
  - `NOT RUN`: Not executed (must provide valid justification, e.g., missing Docker daemon).
  - `NOT APPLICABLE`: Check does not apply to this change type.

### Step 6 to 8: Dispatch Specialized Reviews
- **Adversarial Review (`de-engineering-reviewer`):** Actively search for failure modes, edge cases, silent data drops, unhandled exceptions, and concurrency races.
- **Data Correctness Review (`de-data-correctness-qa`):** If data pipelines, transformations, schemas, or models are modified, verify financial invariants (`amount_minor` as BIGINT), state rank monotonicity, watermark compliance, and 3-Way Reconciliation matching.
- **Operational Readiness Review (`de-operational-readiness`):** If streaming jobs, Docker configurations, Airflow DAGs, or MinIO/Delta writes are modified, verify Freshness SLOs, dynamic timer stuck logic, checkpoint recovery, and Circuit Breaker behavior.

### Step 9: Consolidate Findings
- Ensure every finding follows the standard structure:
  - `ID`: Unique identifier (e.g., `FINDING-01`)
  - `Severity`: `Blocker`, `High`, `Medium`, `Low`, or `Observation`
  - `Location`: `file:line` or component name
  - `Evidence`: Code snippet or test failure trace
  - `Failure Scenario`: Concrete scenario where the code breaks
  - `Remediation`: Actionable fix recommendation

### Step 10: Formulate Readiness Verdicts
- Evaluate two distinct decisions:
  - **Merge-Ready:** `YES` / `NO` / `CONDITIONAL` (Requires all Blocker and High findings resolved, automated tests PASS).
  - **Launch-Ready:** `YES` / `NO` / `CONDITIONAL` / `N/A` (Evaluates end-to-end integration, performance at scale, failure recovery, observability, and operational documentation).

## 3. Output Contract

The Orchestrator must generate its final output matching the template in `.agents/templates/change-review-report.md`.
Never self-approve High or Blocker issues without human mentor sign-off.
