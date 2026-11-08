# Interview Defense Card & Talking Points

> **Task Reference:** `[e.g., WBS-M3-T04: Dual-Table Silver Medallion]`  
> **Topic Domain:** `[Streaming / Lakehouse ACID / Financial Reconciliation]`  
> **Target Audience:** Technical Interviewers at Banks, Fintechs, & Enterprise Tech (VPBank, Techcombank, Momo, FPT, Viettel).

---

## 1. Executive Talking Points (30-Second Elevator Pitch)

*How to summarize your technical decision clearly and decisively when an interviewer asks: "How did you design this component and why?"*

> **Elevator Response:**  
> `"In this module, I designed [Solution Name] to address [Core Problem]. While a naive approach would use [Naive Alternative], that leads to [Critical Failure Mode]. Instead, by leveraging [Core Technique/Option], we achieved [Key Metric/SLO] while preserving strict compliance with [Invariant/ACID guarantee]."`

---

## 2. Deep-Dive Defense (Why X instead of Y?)

*Prepared responses to common adversarial interview questions.*

### Question 1: "Why didn't you use [Alternative Technology/Pattern]?"
- **Interviewer Intent:** Testing whether you understand the trade-offs or simply copy-pasted a tutorial.
- **Your Defensive Answer:**
  - *Point 1 (Context):* Explain why the alternative works in simpler scenarios but breaks at our scale/requirements.
  - *Point 2 (Failure Scenario):* Cite the exact failure mode (e.g., out-of-order terminal conflict, state store memory leak, small files explosion).
  - *Point 3 (Production Evidence):* Cite your project benchmark numbers (e.g., p95 latency <= 30s, 0 duplicate records).

### Question 2: "What happens if this component crashes mid-execution?"
- **Interviewer Intent:** Testing fault tolerance, idempotency, and recovery comprehension.
- **Your Defensive Answer:**
  - *Recovery Flow:* Checkpoint recovery -> Exactly-once / At-least-once with idempotent downstream MERGE -> Catch-up rate.

---

## 3. Edge-Case Mastery Challenge (Self-Assessment)

*Try answering these 2 hard scenarios before reading the solution.*

### Scenario A: `[Edge Case Scenario Name]`
- **The Drill:** `[e.g., What happens if a COMPLETED event arrives 3 hours late for a transaction already marked as STUCK and reconciled as missing in partner EOD?]`
- **Model Answer:** `[Detailed breakdown of how the system handles it safely without breaking accounting invariants]`

### Scenario B: `[Edge Case Scenario Name]`
- **The Drill:** `[e.g., Spark micro-batch fails at row 48 out of 50 due to an unhandled enum. Does the batch retry indefinitely?]`
- **Model Answer:** `[How the Circuit Breaker trips and Quarantine branch isolates the record]`
