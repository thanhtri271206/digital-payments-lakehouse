# Ground Truth (G0) Automated Evaluation Report

> **Dataset:** Simulator Ground Truth vs Pipeline Output  
> **Simulation Seed:** `20260930`  
> **Workload Size:** `[e.g., 100,000 transactions]`  
> **Evaluation Date:** YYYY-MM-DD  
> **Auditor:** `de-data-correctness-qa`

---

## 1. Executive Summary

| Evaluation Domain | Target Invariant | Precision | Recall | F1-Score | Status |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Data Quality (DQ)** | Poison record quarantine | 99.8% | 99.5% | 99.6% | `PASS` |
| **Watermark Lateness** | Late event DLQ routing (>2m) | 100.0% | 98.7% | 99.3% | `PASS` |
| **Stuck Detection** | 5-minute timeout flag | 98.5% | 98.2% | 98.3% | `PASS` |
| **3-Way Reconciliation** | Break type classification (7 types) | 99.9% | 99.4% | 99.6% | `PASS` |

---

## 2. Confusion Matrix Details

### 2.1. Poison Records vs Quarantine (`silver_quarantine_events`)
- **True Positives ($TP$):** Injected invalid events correctly quarantined: `[count]`
- **False Positives ($FP$):** Valid events mistakenly quarantined: `[count]`
- **False Negatives ($FN$):** Poison records leaked into valid history: `[count]`
- **True Negatives ($TN$):** Valid events correctly passed: `[count]`

$$\text{Precision} = \frac{TP}{TP + FP} = \text{X.X}\%$$

$$\text{Recall} = \frac{TP}{TP + FN} = \text{X.X}\%$$

### 2.2. Reconciliation Breaks vs `gold_eod_reconciliation_details`

| Injected Error Type | Expected Break Category | Detected Correctly ($TP$) | Misclassified ($FP$) | Missed ($FN$) | Accuracy |
| :--- | :--- | :---: | :---: | :---: | :---: |
| Amount Mismatch | `AMOUNT_MISMATCH` | 148 | 0 | 2 | 98.7% |
| Missing in Partner | `MISSING_IN_PARTNER` | 50 | 0 | 0 | 100.0% |
| Missing in Bank | `MISSING_IN_BANK` | 45 | 1 | 0 | 97.8% |
| Orphan Partner/Bank | `ORPHAN_EXTERNAL` | 30 | 0 | 0 | 100.0% |
| Duplicate Event | `DUPLICATE` | 80 | 0 | 1 | 98.8% |
| Cutoff Spillover | `TIMING_CUTOFF_MISMATCH` | 25 | 1 | 0 | 96.0% |

---

## 3. Discrepancy Analysis & Findings

- **Root Cause of False Negatives:**
  - [Explain any leakages or missed breaks]
- **Root Cause of False Positives:**
  - [Explain any clean records erroneously flagged]

---

## 4. Correctness Verdict

- **Automated Score:** `PASS` (All metrics $\ge 98\%$) | `FAIL`
- **Recommendations:**
  - [Tune tolerance window, adjust regex, or refine matching rule]
