### [FINDING-{ID}] {Short Descriptive Title}

- **Severity:** `BLOCKER` | `HIGH` | `MEDIUM` | `LOW` | `OBSERVATION`
- **Category:** `Data Correctness` | `Reliability & Idempotency` | `Storage & Performance` | `Security` | `Observability`
- **Location:** `{file_path}:{line_number}` or `{component_name}`
- **Evidence:**
  ```python
  # Code snippet, error stack trace, or test assertion failure
  ```
- **Failure Scenario:**
  {Step-by-step description of how and when this failure manifests in production or local runtime}
- **Impact:**
  {Silent data loss, duplicate financial transaction, pipeline crash, memory leak, security exposure, etc.}
- **Why Existing Checks Missed It:**
  {Why unit tests or compiler checks did not catch this flaw}
- **Recommended Remediation:**
  ```python
  # Concrete drop-in code fix or architectural correction
  ```
- **Validation Required:**
  ```bash
  # Command or test to verify the fix resolves the issue without regressions
  ```
- **Review Confidence:** `HIGH` | `MEDIUM` | `LOW`
