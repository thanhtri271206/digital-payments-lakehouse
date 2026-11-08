# Task ADR & Trade-Off Analysis Specification

> **Task ID:** `[e.g., WBS-M1-T04]`  
> **Task Name:** `[e.g., Payment Lifecycle State Machine]`  
> **Domain:** `[Streaming / Delta Lake / dbt / Airflow / Data Quality]`  
> **Status:** `[PROPOSED / APPROVED / IMPLEMENTED]`

---

## 1. Engine & Mechanics Deep-Dive (Under The Hood)

*Explain the foundational computer science and distributed system principles behind this component before presenting engineering options.*

- **Underlying Mechanism:**  
  *How does the underlying technology (Spark Micro-batch engine, Kafka Partition log, Delta Transaction Log, or OS memory) actually process this data under the hood?*
- **The "Naive Approach" Trap:**  
  *What happens if an engineer builds this naively (e.g., simple SQL update, ignoring watermark, streaming without pre-aggregation)? Where and why will it silently corrupt data or crash in production?*

---

## 2. Architecture Options & Trade-Off Evaluation

*Compare at least 2–3 viable engineering options to solve this task.*

### 2.1. Option Summary
- **Option A:** `[Brief description of Option A]`
- **Option B:** `[Brief description of Option B]`
- **Option C:** `[Brief description of Option C]`

### 2.2. Comprehensive Trade-off Matrix

| Evaluation Dimension | Option A: `[Name]` | Option B: `[Name]` | Option C: `[Name]` |
| :--- | :--- | :--- | :--- |
| **Throughput & Latency** | | | |
| **Compute & Memory Cost** | | | |
| **Storage & I/O Overhead** | | | |
| **Code & Ops Complexity** | | | |
| **Invariants Compliance** | | | |
| **Failure Recovery Ease** | | | |

---

## 3. Optimal Engineering Decision & Rationale

- **Selected Decision:** `[Option A / B / C]`
- **Concrete Technical Rationale:**  
  *Why is this option superior for our specific constraints (Local 32GB RAM, < 100k VND, Dual-track topology, Financial invariants)?*
- **Known Trade-offs Accepted:**  
  *What compromises are we intentionally making, and how do we monitor/mitigate them?*
- **Invariants Safeguards:**  
  *How does this decision guarantee zero violation of the Blocker invariants in `AGENTS.md`?*
