---
name: DE Memory & Context Synchronization
description: Manages cross-session persistent memory, context compression, ADR decision logging, and active WBS task tracking in .agents/memory/.
---

# DE Memory & Context Synchronization

The **DE Memory & Context Synchronization** skill ensures that the AI Agent maintains continuous, lossless, and strictly aligned context across chat sessions, context compactions, and server restarts.

---

## 1. When to Invoke

- **Session Start / Resume:** At the beginning of any new chat session to reconstruct current project state.
- **Task Completion (Post-DoD):** Automatically after completing a WBS task to sync `.agents/memory/active-context.md`.
- **Architectural Decision (Post-ADR):** Automatically when a new engineering design choice is approved to log into `.agents/memory/decisions.md`.
- **Context Compaction:** Before or after context window truncation to preserve critical decisions and state.

---

## 2. Memory Architecture & Directory Layout

```text
.agents/memory/
├── MEMORY.md          # Long-term knowledge: SSOT anchor, hardware bounds, constraints, invariants.
├── decisions.md       # Decision log: All confirmed ADRs, options compared, rejected alternatives.
└── active-context.md  # Short-term scratchpad: Current Milestone, Active Task ID, next immediate actions.
```

---

## 3. Standard Operating Protocols

### Protocol 1: Session Resume & Context Reconstruction
1. Read `.agents/memory/active-context.md`.
2. Identify:
   - Current Active Milestone (e.g., `Milestone 1 — Foundation Infrastructure`).
   - Current Active Task ID (e.g., `WBS-M1-T01`).
   - Infrastructure & Container status.
   - Immediate next action.
3. Provide a brief 2-sentence status confirmation to the human engineer:
   *"Active context loaded: Currently executing [Task ID] in [Milestone]. Next step is [Action]."*

### Protocol 2: ADR Auto-Logging (To `decisions.md`)
Whenever Phase 2 of `teach-decide-and-build` concludes:
1. Extract:
   - Decision ID (e.g., `ADR-M1-01`).
   - Component / Topic.
   - Selected Option vs Rejected Alternatives.
   - Core Rationale & Accepted Trade-offs.
2. Append the new row to `.agents/memory/decisions.md` under the active milestone table.

### Protocol 3: Task State Auto-Sync (To `active-context.md`)
Whenever a task achieves Definition of Done (DoD) in Phase 4:
1. Mark the completed task status as `COMPLETED` with timestamp.
2. Identify the next task in the critical path from `docs/wbs/dependency_graph_and_critical_path.md`.
3. Set the next task status to `ACTIVE / READY TO START`.
4. Update the "Immediate Next Step" section.
