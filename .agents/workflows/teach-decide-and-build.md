# Workflow: Teach, Decide, and Build (Code-Driven Mentorship)

> **Workflow Name:** `teach-decide-and-build`  
> **Invocation Trigger:** Starting any new WBS task, architectural component, or milestone feature.  
> **Primary Persona:** `DE Architect & Mentor` $\rightarrow$ `DE Implementer` $\rightarrow$ `DE Adversarial Reviewer`  
> **Goal:** Accelerate development by having the Agent write production code while empowering the human student to deeply master system mechanics, architectural trade-offs, and interview defense points.

---

## Operating Flow Overview

```text
       [WBS Task Input]
              │
              ▼
  ┌──────────────────────┐
  │  PHASE 1: DEEP-DIVE  │ ──► Under-the-hood engine mechanics & Naive approach failure traps
  └──────────────────────┘
              │
              ▼
  ┌──────────────────────┐
  │  PHASE 2: ADR MATRIX │ ──► Compare 2-3 Options, Trade-offs & Invariants selection
  └──────────────────────┘
              │
              ▼
  ┌──────────────────────┐
  │  PHASE 3: CODE & RUN │ ──► Production implementation, automated execution & logs
  └──────────────────────┘
              │
              ▼
  ┌──────────────────────┐
  │  PHASE 4: INTERVIEW  │ ──► Executive Talking Points & Hard Edge-Case Challenges
  └──────────────────────┘
              │
              ▼
  [Student Sign-Off / Acceptance]
```

---

## Phase-by-Phase Execution Instructions

### Phase 1: Deep-Dive Engine Mechanics (Under The Hood)
1. **Analyze Task Scope & Invariants:** Read corresponding task from `docs/wbs/` and invariants from `AGENTS.md`.
2. **Deconstruct the Engine Internals:**
   - Explain how the core engine (Kafka partition log, Spark micro-batch state store, Delta Lake transaction log, or OS memory allocator) handles this workload physically.
3. **Expose the Naive Failure Trap:**
   - Explicitly demonstrate what goes wrong if an inexperienced engineer builds this with a naive approach (e.g., small files explosion, deadlocks, multiple-row merge exceptions, late event dropping).

### Phase 2: Architecture Decision Record (ADR) & Trade-Off Matrix
1. **Formulate 2–3 Realistic Engineering Options:**
   - Present distinct design options (e.g., Option A: Naive/Standard, Option B: Custom Micro-batch Collapse, Option C: Stateful Arbitrary Mapping).
2. **Construct the Trade-off Matrix:**
   - Compare options across: *Throughput/Latency*, *Memory/Compute*, *Storage/IO*, *Complexity*, and *Invariants Compliance*.
3. **Declare the Optimal Decision & Rationale:**
   - State clearly which option is chosen and justify why it fits the project's exact constraints (Local 32GB Docker, Financial invariants).
4. **Auto-Log to Architectural Decisions (`decisions.md`):**
   - Append the approved ADR entry into [`.agents/memory/decisions.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/memory/decisions.md) to preserve the design rationale permanently across sessions.

### Phase 3: Production Implementation & Evidence Execution
1. **Implement Production-Grade Code:**
   - Write 100% complete, clean code with Pydantic v2 validation, strict type hints, and inline architectural comments explaining the "why".
2. **Automated Test Harness:**
   - Write corresponding Pytest unit/integration tests covering normal paths, edge cases, and fault injections.
3. **Execute & Capture Real Evidence:**
   - Run the execution/test commands in the terminal (Docker, Pytest, Python).
   - Capture real exit codes and log snippets into `.agents/templates/execution-evidence-log.md`.
   - Never fabricate test logs!

### Phase 4: Interview Defense & Mastery Reinforcement
1. **Executive Talking Points (30s Pitch):**
   - Provide the concise answer the student can deliver when asked by a technical interviewer at a bank/fintech: *"How did you design this and why?"*.
2. **Edge-Case Challenge:**
   - Present 1-2 realistic failure scenarios (e.g., late terminal event conflict, consumer group lag spike) and walk through the exact defensive mechanism.
3. **Student Check-In & Formal Approval:**
   - Confirm student comprehension before marking the task complete.
4. **Auto-Sync Active Context (`active-context.md`):**
   - Upon task completion sign-off, update [`.agents/memory/active-context.md`](file:///c:/DE-project/digital-payments-lakehouse/.agents/memory/active-context.md): mark task status as `COMPLETED`, unlock the next task in the critical path as `ACTIVE / READY TO START`, and record environment state changes.
