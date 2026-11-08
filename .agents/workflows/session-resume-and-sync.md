# Workflow: Session Resume & Context Synchronization

> **Workflow Name:** `session-resume-and-sync`  
> **Invocation Trigger:** Starting a new chat session, after server restart, or when the user asks: *"Where did we leave off?"* / *"Tiếp tục từ đâu?"*.  
> **Target Skill:** `DE Memory & Context Synchronization`

---

## 1. Operating Procedure

When invoked, the Agent executes the following 4-step sequence:

```text
[New Session Started / Resume Prompt]
                 │
                 ▼
 1. Read Active Context ──────────► Inspect .agents/memory/active-context.md
                 │
                 ▼
 2. Verify Local Stack State ─────► Run `docker compose ps` (check running containers)
                 │
                 ▼
 3. Check Last Evidence Log ──────► Identify last executed task & exit code
                 │
                 ▼
 4. Formulate Resume Brief ───────► Present 3-point briefing & prompt user to start
```

---

## 2. Standard Briefing Output Contract

The Agent outputs a crisp briefing following this exact structure:

> ### 🟢 Session State Restored
> - **Active Milestone:** `[Milestone Name & Progress %]`
> - **Active Task:** `[Task ID: Task Name]`
> - **Current Environment Status:** `[Docker services up/down, Python venv ready]`
> - **Immediate Next Action:** `[Exact Phase/Action to execute now]`
> 
> *Ready to proceed with [Task ID]? Type "proceed" or provide guidance.*
