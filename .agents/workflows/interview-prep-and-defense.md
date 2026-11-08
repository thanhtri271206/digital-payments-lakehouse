# Workflow: Interview Preparation & Architecture Defense

> **Workflow Name:** `interview-prep-and-defense`  
> **Invocation Trigger:** Before a milestone release, after implementing a critical module, or upon user request to practice technical interview skills.  
> **Persona:** `Lead Technical Interviewer` (Adversarial Engineering stance simulating top VN Fintech / Banking interviewers).

---

## 1. Objective

To transform the code written in this repository into sharp, articulate, and interview-ready verbal talking points, preparing the student to dominate technical interviews for Junior/Mid Data Engineer positions at target enterprises (Techcombank, VPBank, Momo, FPT, Viettel, KMS).

---

## 2. Interview Protocol

The interview session runs in 3 successive rounds:

### Round 1: Architectural Rationale & Trade-Offs (The "Why" Round)
- **Format:** The Agent asks 2 questions challenging the chosen architecture.
  - *Example:* *"Why did you design a Dual-Table Silver pattern (`history` append-only + `current` pre-aggregated merge) instead of simply doing a single Delta table with SCD Type 2?"*
  - *Example:* *"Why did you choose Spark Structured Streaming micro-batch with a 2-minute watermark over continuous processing or Flink?"*
- **Evaluation:** Check if the student explains the trade-offs (storage I/O, small file mitigation, query latency, ACID merge limitations) rather than giving generic textbook answers.

### Round 2: Failure Scenarios & Edge Cases (The "Resilience" Round)
- **Format:** The Agent injects a production disaster scenario.
  - *Example:* *"A Kafka broker goes down for 15 minutes during peak burst. When it recovers, 100,000 events flood in out-of-order. What happens to your watermark, your Delta Lake checkpoints, and your financial GMV calculations?"*
  - *Example:* *"An external partner sends Settlement CSV v2 with reversed amounts at 02:00 AM after your EOD reconciliation completed. How does your system reconcile without corrupting historical audit trails?"*
- **Evaluation:** Assess whether the student cites the exact mechanisms implemented: *Watermark late event DLQ, Versioned replay (`run_version`), Idempotent merge, and Completeness Gate*.

### Round 3: Data Invariants & Financial Engineering (The "Precision" Round)
- **Format:** The Agent questions financial and ledger invariants.
  - *Example:* *"Why is using FLOAT for monetary transactions considered a catastrophic bug? Walk me through how IEEE 754 precision error accumulates in a multi-million transaction lakehouse."*
  - *Example:* *"Explain the Monotonic State Rank (1 to 6) and Terminal Lock. What happens when a COMPLETED event is followed by an out-of-order FAILED event?"*

---

## 3. Session Output

At the end of the session, the Agent compiles:
1. **Performance Feedback:** Strengths, missing technical keywords, and areas of hesitation.
2. **Standard Interview Talking Points:** Ready-to-memorize summary cards saved to `.agents/templates/interview-defense-card.md`.
