"""Deterministic Payment Event Generator Engine with Seed Control.

Guarantees 100% byte-for-byte reproducibility across runs with the same seed.
Supports EOD cutoff boundary events and probabilistic fault injection.
"""
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from digital_payments_lakehouse.config.settings import AppConfig, get_config
from simulator.fault_injector import FaultInjector
from simulator.lifecycle import PaymentStateMachine
from simulator.models import (
    EventType,
    GroundTruthRecord,
    PaymentChannel,
    PaymentEvent,
    PaymentMethod,
)


class PaymentGenerator:
    """Deterministic transaction generator driving Lakehouse streaming and reconciliation test sets."""

    def __init__(
        self,
        seed: int = 20260930,
        config: Optional[AppConfig] = None,
    ):
        self.seed = seed
        self.rng = random.Random(seed)
        self.config = config or get_config()

        # Initialize fault injector with seed-tied RNG
        rates = self.config.simulator.rates
        self.fault_injector = FaultInjector(
            rng=self.rng,
            duplicate_rate=rates.duplicate_event_rate,
            late_short_rate=rates.late_event_short_rate,
            late_long_rate=rates.late_event_long_rate,
            out_of_order_rate=rates.out_of_order_rate,
            missing_terminal_rate=rates.missing_terminal_rate,
            poison_rate=rates.poison_record_rate,
            terminal_conflict_rate=rates.terminal_conflict_rate,
        )

        # Entity pools for realistic distribution
        self.user_pool = [f"usr_{i:04d}" for i in range(1, 501)]
        self.merchant_pool = [f"merch_{i:03d}" for i in range(1, 51)]
        self.channels = [c.value for c in PaymentChannel]
        self.methods = [m.value for m in PaymentMethod]

        # Terminal outcomes distribution (sum = 1.0)
        self.outcome_choices = [
            EventType.COMPLETED.value,
            EventType.FAILED.value,
            EventType.CANCELLED.value,
            EventType.EXPIRED.value,
            EventType.REFUNDED.value,
        ]
        self.outcome_weights = [0.85, 0.07, 0.05, 0.02, 0.01]

    def _generate_amount(self) -> int:
        """Generate realistic integer minor currency amounts (VND BIGINT).

        Ranges from 10,000 VND to 10,000,000 VND.
        """
        tier = self.rng.random()
        if tier < 0.60:
            # Low amount: 10,000 to 100,000 VND
            return self.rng.randint(10, 100) * 1000
        elif tier < 0.90:
            # Medium amount: 100,000 to 1,000,000 VND
            return self.rng.randint(10, 100) * 10000
        else:
            # High amount: 1,000,000 to 10,000,000 VND
            return self.rng.randint(10, 100) * 100000

    def generate_transaction(
        self,
        base_time: Optional[datetime] = None,
        txn_index: int = 1,
    ) -> Tuple[List[PaymentEvent], GroundTruthRecord]:
        """Generate a complete payment transaction lifecycle with faults and ground truth record."""
        txn_id = f"txn-{self.seed}-{txn_index:07d}"
        user_id = self.rng.choice(self.user_pool)
        merchant_id = self.rng.choice(self.merchant_pool)
        channel = self.rng.choice(self.channels)
        method = self.rng.choice(self.methods)
        amount = self._generate_amount()

        # Select terminal outcome
        outcome = self.rng.choices(self.outcome_choices, weights=self.outcome_weights, k=1)[0]

        # Generate lifecycle path
        path = PaymentStateMachine.generate_lifecycle_path(outcome, rng=self.rng)

        # Base timestamp: if not provided, default to canonical reference date
        if base_time is None:
            # Reference simulation base: 2026-10-15 08:00:00 UTC
            base_time = datetime(2026, 10, 15, 8, 0, 0, tzinfo=timezone.utc)

        # Attach sequential timestamps
        timed_path = PaymentStateMachine.generate_event_timestamps(
            path=path,
            start_time=base_time,
            rng=self.rng,
        )

        # Construct standard PaymentEvent list
        events: List[PaymentEvent] = []
        for step_idx, (step_type, ts_str) in enumerate(timed_path):
            evt_id = f"evt-{txn_id}-{step_idx:02d}"
            ev = PaymentEvent(
                event_id=evt_id,
                transaction_id=txn_id,
                event_type=step_type.value,
                event_time=ts_str,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=amount,
                currency="VND",
                producer_version="v1.0",
                is_poison=False,
            )
            events.append(ev)

        # Inject controlled faults
        (
            final_events,
            injected_faults,
            expected_final_state,
            expected_operational_status,
            was_ever_stuck,
        ) = self.fault_injector.apply_faults(events, terminal_outcome=outcome)

        # Build Ground Truth G0 record
        lifecycle_seq = [e.event_type for e in final_events]
        gt_record = GroundTruthRecord(
            transaction_id=txn_id,
            user_id=user_id,
            merchant_id=merchant_id,
            payment_channel=channel,
            payment_method=method,
            amount_minor=amount,
            currency="VND",
            lifecycle_sequence=lifecycle_seq,
            expected_final_state=expected_final_state,
            expected_operational_status=expected_operational_status,
            was_ever_stuck_expected=was_ever_stuck,
            injected_faults=injected_faults,
            created_at=timed_path[0][1],
        )

        return final_events, gt_record

    def generate_batch(
        self,
        count: int,
        start_time: Optional[datetime] = None,
        eod_boundary: bool = False,
    ) -> Tuple[List[PaymentEvent], List[GroundTruthRecord]]:
        """Generate a batch of transactions and ground truth records.

        If eod_boundary is True, start timestamps are centered around 16:59:59 UTC (23:59:59 UTC+07).
        """
        all_events: List[PaymentEvent] = []
        all_ground_truth: List[GroundTruthRecord] = []

        if start_time is None:
            if eod_boundary:
                # 16:59:50 UTC - exactly 9 seconds before 23:59:59 UTC+07
                curr_time = datetime(2026, 10, 15, 16, 59, 50, tzinfo=timezone.utc)
            else:
                curr_time = datetime(2026, 10, 15, 8, 0, 0, tzinfo=timezone.utc)
        else:
            curr_time = start_time

        for i in range(1, count + 1):
            events, gt = self.generate_transaction(base_time=curr_time, txn_index=i)
            all_events.extend(events)
            all_ground_truth.append(gt)

            # Advance current time incrementally (0.1s to 1.5s per transaction)
            curr_time += timedelta(milliseconds=self.rng.randint(100, 1500))

        return all_events, all_ground_truth

