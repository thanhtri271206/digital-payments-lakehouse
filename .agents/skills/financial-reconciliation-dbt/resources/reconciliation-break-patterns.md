# 3-Way Reconciliation Break Patterns & Diagnostic Rules

> **Scope:** Exact categorization logic for dbt reconciliation models.

---

## The 7 Standard Break Types

| Break Code | Business Name | Trigger Condition | Operational Action |
| :--- | :--- | :--- | :--- |
| `B01` | `MATCHED` | Keys match, `amount_minor` identical in Ledger, Partner, and Bank. | Auto-cleared; marked settled. |
| `B02` | `MISSING_IN_PARTNER` | Recorded in Internal Ledger + Bank, absent in Partner Clearing CSV. | Generate inquiry ticket to Partner Gateway. |
| `B03` | `MISSING_IN_BANK` | Recorded in Internal Ledger + Partner, absent in Bank Statement. | Flag bank transfer pending / transit float. |
| `B04` | `ORPHAN_EXTERNAL` | Appears in Partner or Bank, but no matching `transaction_id` in Internal Ledger. | Critical risk; investigate unrecorded transaction or fraud. |
| `B05` | `DUPLICATE` | More than 1 settlement entry matching single internal ledger record. | Reverse duplicate settlement; flag partner bug. |
| `B06` | `AMOUNT_MISMATCH` | Reference keys match, but `amount_minor` differs (e.g. Ledger: 100,000 VND vs Partner: 90,000 VND). | Post ledger adjustment / fee variance. |
| `B07` | `TIMING_CUTOFF_MISMATCH` | Internal transaction occurred before 23:59:59 UTC+07, but partner settled after 00:00:00 UTC+07 next day. | Hold in transit; roll over to T+2 reconciliation. |
