"""Unit tests for Payment Lifecycle State Machine (Rank 1 -> 6)."""
import random
from datetime import datetime, timezone
import pytest
from simulator.models import EventType
from simulator.lifecycle import (
    STATE_RANK_MAP,
    TERMINAL_STATES,
    PaymentStateMachine,
)


def test_state_rank_mapping_completeness():
    """Verify all defined EventType members have a strictly defined rank 1..6."""
    for event_type in EventType:
        rank = PaymentStateMachine.get_rank(event_type)
        assert 1 <= rank <= 6
    assert PaymentStateMachine.get_rank(EventType.INITIATED) == 1
    assert PaymentStateMachine.get_rank(EventType.PROCESSING) == 4
    assert PaymentStateMachine.get_rank(EventType.COMPLETED) == 5
    assert PaymentStateMachine.get_rank(EventType.FAILED) == 5
    assert PaymentStateMachine.get_rank(EventType.REFUNDED) == 6


def test_monotonic_rank_regression_strictly_forbidden():
    """Verify AC-1: Transitioning backward to a lower rank is strictly prohibited."""
    assert not PaymentStateMachine.is_transition_allowed(EventType.PROCESSING, EventType.INITIATED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.PROCESSING, EventType.AUTHORIZED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.COMPLETED, EventType.PROCESSING)
    assert not PaymentStateMachine.is_transition_allowed(EventType.REFUNDED, EventType.INITIATED)


def test_terminal_to_terminal_transitions_forbidden():
    """Verify AC-2: Transition between two terminal states is forbidden."""
    assert not PaymentStateMachine.is_transition_allowed(EventType.FAILED, EventType.COMPLETED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.CANCELLED, EventType.COMPLETED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.EXPIRED, EventType.FAILED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.COMPLETED, EventType.FAILED)


def test_refunded_only_allowed_from_completed():
    """Verify AC-3: REFUNDED (6) is only permitted immediately after COMPLETED (5)."""
    assert PaymentStateMachine.is_transition_allowed(EventType.COMPLETED, EventType.REFUNDED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.FAILED, EventType.REFUNDED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.CANCELLED, EventType.REFUNDED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.EXPIRED, EventType.REFUNDED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.PROCESSING, EventType.REFUNDED)
    assert not PaymentStateMachine.is_transition_allowed(EventType.INITIATED, EventType.REFUNDED)


def test_valid_next_states():
    """Verify legally reachable states from each node."""
    next_states_init = PaymentStateMachine.get_valid_next_states(EventType.INITIATED)
    assert EventType.PAYMENT_METHOD_SELECTED in next_states_init

    next_states_proc = PaymentStateMachine.get_valid_next_states(EventType.PROCESSING)
    assert EventType.COMPLETED in next_states_proc
    assert EventType.FAILED in next_states_proc

    next_states_failed = PaymentStateMachine.get_valid_next_states(EventType.FAILED)
    assert len(next_states_failed) == 0


def test_generate_lifecycle_path_outcomes():
    """Verify generation of valid deterministic sequences for all outcome types."""
    rng = random.Random(42)

    # Success Path
    path_success = PaymentStateMachine.generate_lifecycle_path(EventType.COMPLETED.value, rng)
    assert path_success == [
        EventType.INITIATED,
        EventType.PAYMENT_METHOD_SELECTED,
        EventType.AUTHORIZED,
        EventType.PROCESSING,
        EventType.COMPLETED,
    ]

    # Refund Path
    path_refund = PaymentStateMachine.generate_lifecycle_path(EventType.REFUNDED.value, rng)
    assert path_refund[-2:] == [EventType.COMPLETED, EventType.REFUNDED]

    # Failed Path
    path_failed = PaymentStateMachine.generate_lifecycle_path(EventType.FAILED.value, rng)
    assert path_failed[-1] == EventType.FAILED
    assert path_failed[0] == EventType.INITIATED

    # Cancelled Path
    path_cancel = PaymentStateMachine.generate_lifecycle_path(EventType.CANCELLED.value, rng)
    assert path_cancel[-1] == EventType.CANCELLED

    # Expired Path
    path_expired = PaymentStateMachine.generate_lifecycle_path(EventType.EXPIRED.value, rng)
    assert path_expired[-1] == EventType.EXPIRED


def test_generate_event_timestamps_monotonicity():
    """Verify timestamp generation produces chronologically ascending ISO-8601 UTC strings."""
    rng = random.Random(123)
    start_dt = datetime(2026, 10, 15, 10, 0, 0, tzinfo=timezone.utc)
    path = [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.AUTHORIZED, EventType.PROCESSING, EventType.COMPLETED]

    timed_events = PaymentStateMachine.generate_event_timestamps(path, start_dt, rng, min_delta_ms=1000, max_delta_ms=2000)
    assert len(timed_events) == 5

    prev_dt = start_dt
    for step, ts_str in timed_events:
        assert ts_str.endswith("Z")
        curr_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        assert curr_dt >= prev_dt
        prev_dt = curr_dt

