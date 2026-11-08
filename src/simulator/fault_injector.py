"""Controlled Fault Injection Engine for Lakehouse Streaming & Reconciliation Testing.

Injects 7 standardized failure scenarios:
1. DUPLICATE_EVENT (0.5%): Same event_id repeated shortly after.
2. LATE_EVENT_SHORT (1.0%): Event delayed 5s-120s (within 2m watermark).
3. LATE_EVENT_LONG (0.2%): Event delayed > 120s (exceeds watermark, routes to silver_late_events_dlq).
4. OUT_OF_ORDER (0.5%): Swaps sequential events for the same transaction.
5. MISSING_TERMINAL_STUCK (0.3%): Truncates at PROCESSING, triggering STUCK operational status > 5m.
6. POISON_RECORD (0.2%): Injects negative amount or invalid channel to test Quarantine and Circuit Breaker.
7. TERMINAL_CONFLICT (0.1%): Emits FAILED after COMPLETED, testing silver_lifecycle_exceptions.
"""
import copy
import random
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional, Tuple
from simulator.models import EventType, OperationalStatus, PaymentEvent


class FaultType(str, Enum):
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    LATE_EVENT_SHORT = "LATE_EVENT_SHORT"
    LATE_EVENT_LONG = "LATE_EVENT_LONG"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    MISSING_TERMINAL_STUCK = "MISSING_TERMINAL_STUCK"
    POISON_RECORD = "POISON_RECORD"
    TERMINAL_CONFLICT = "TERMINAL_CONFLICT"


class FaultInjector:
    """Applies controlled probabilistic data anomalies to a transaction's event stream."""

    def __init__(
        self,
        rng: random.Random,
        duplicate_rate: float = 0.005,
        late_short_rate: float = 0.010,
        late_long_rate: float = 0.002,
        out_of_order_rate: float = 0.005,
        missing_terminal_rate: float = 0.003,
        poison_rate: float = 0.002,
        terminal_conflict_rate: float = 0.001,
    ):
        self.rng = rng
        self.duplicate_rate = duplicate_rate
        self.late_short_rate = late_short_rate
        self.late_long_rate = late_long_rate
        self.out_of_order_rate = out_of_order_rate
        self.missing_terminal_rate = missing_terminal_rate
        self.poison_rate = poison_rate
        self.terminal_conflict_rate = terminal_conflict_rate

    def apply_faults(
        self,
        events: List[PaymentEvent],
        terminal_outcome: str,
    ) -> Tuple[List[PaymentEvent], List[str], str, str, bool]:
        """Inject faults into event list and return modified events along with ground truth metadata.

        Returns:
            Tuple: (
                modified_events: List[PaymentEvent],
                injected_faults: List[str],
                expected_final_state: str,
                expected_operational_status: str,
                was_ever_stuck_expected: bool
            )
        """
        injected_faults: List[str] = []
        expected_final_state = terminal_outcome
        expected_operational_status = OperationalStatus.NORMAL.value
        was_ever_stuck = False
        out_events = [copy.deepcopy(e) for e in events]

        # 1. Fault: MISSING_TERMINAL_STUCK (Transaction stuck in PROCESSING)
        if self.rng.random() < self.missing_terminal_rate:
            proc_idx = -1
            for idx, ev in enumerate(out_events):
                if ev.event_type == EventType.PROCESSING.value:
                    proc_idx = idx
                    break
            if proc_idx != -1:
                out_events = out_events[: proc_idx + 1]
                injected_faults.append(FaultType.MISSING_TERMINAL_STUCK.value)
                expected_final_state = EventType.PROCESSING.value
                expected_operational_status = OperationalStatus.STUCK.value
                was_ever_stuck = True

        # 2. Fault: TERMINAL_CONFLICT (COMPLETED followed by FAILED)
        if (
            FaultType.MISSING_TERMINAL_STUCK.value not in injected_faults
            and terminal_outcome == EventType.COMPLETED.value
            and self.rng.random() < self.terminal_conflict_rate
        ):
            injected_faults.append(FaultType.TERMINAL_CONFLICT.value)
            expected_operational_status = OperationalStatus.EXCEPTION.value
            last_ev = out_events[-1]
            last_time = datetime.fromisoformat(last_ev.event_time.replace("Z", "+00:00"))
            conflict_time = (last_time + timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            conflict_event = PaymentEvent(
                event_id=f"evt-conflict-{self.rng.randint(100000, 999999)}",
                transaction_id=last_ev.transaction_id,
                event_type=EventType.FAILED.value,
                event_time=conflict_time,
                user_id=last_ev.user_id,
                merchant_id=last_ev.merchant_id,
                payment_channel=last_ev.payment_channel,
                payment_method=last_ev.payment_method,
                amount_minor=last_ev.amount_minor,
                currency=last_ev.currency,
                status_reason="Conflicting terminal event injected for exception testing",
                is_poison=False,
            )
            out_events.append(conflict_event)

        # 3. Fault: POISON_RECORD (negative amount or invalid channel/method)
        if self.rng.random() < self.poison_rate:
            injected_faults.append(FaultType.POISON_RECORD.value)
            expected_operational_status = OperationalStatus.QUARANTINED.value
            target_idx = self.rng.randint(0, len(out_events) - 1)
            target = out_events[target_idx]
            # Create a poisoned record
            poisoned_event = PaymentEvent(
                event_id=target.event_id,
                transaction_id=target.transaction_id,
                event_type=target.event_type,
                event_time=target.event_time,
                user_id=target.user_id,
                merchant_id=target.merchant_id,
                payment_channel="UNKNOWN_CHANNEL",
                payment_method=target.payment_method,
                amount_minor=-abs(target.amount_minor),  # Negative amount
                currency=target.currency,
                status_reason="Poison record injected for DLQ/Circuit Breaker testing",
                is_poison=True,
            )
            out_events[target_idx] = poisoned_event

        # 4. Fault: OUT_OF_ORDER (Swap adjacent events)
        if len(out_events) >= 3 and self.rng.random() < self.out_of_order_rate:
            injected_faults.append(FaultType.OUT_OF_ORDER.value)
            # Swap index 1 and 2
            out_events[1], out_events[2] = out_events[2], out_events[1]

        # 5. Fault: LATE_EVENT_SHORT (Delay event by 5s - 120s within watermark)
        if self.rng.random() < self.late_short_rate:
            injected_faults.append(FaultType.LATE_EVENT_SHORT.value)
            delay_sec = self.rng.randint(5, 120)
            target_idx = self.rng.randint(0, len(out_events) - 1)
            target = out_events[target_idx]
            orig_dt = datetime.fromisoformat(target.event_time.replace("Z", "+00:00"))
            shifted_dt = orig_dt - timedelta(seconds=delay_sec)
            target.event_time = shifted_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # 6. Fault: LATE_EVENT_LONG (Delay event > 120s beyond watermark -> triggers DLQ)
        if self.rng.random() < self.late_long_rate:
            injected_faults.append(FaultType.LATE_EVENT_LONG.value)
            delay_sec = self.rng.randint(180, 900)  # 3m to 15m
            target_idx = self.rng.randint(0, len(out_events) - 1)
            target = out_events[target_idx]
            orig_dt = datetime.fromisoformat(target.event_time.replace("Z", "+00:00"))
            shifted_dt = orig_dt - timedelta(seconds=delay_sec)
            target.event_time = shifted_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # 7. Fault: DUPLICATE_EVENT (Exact duplicate event emitted shortly after)
        if self.rng.random() < self.duplicate_rate:
            injected_faults.append(FaultType.DUPLICATE_EVENT.value)
            dup_target = copy.deepcopy(self.rng.choice(out_events))
            out_events.append(dup_target)

        return (
            out_events,
            injected_faults,
            expected_final_state,
            expected_operational_status,
            was_ever_stuck,
        )

