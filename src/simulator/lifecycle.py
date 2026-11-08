"""Payment Lifecycle State Machine and Monotonic Rank Transition Engine.

Enforces:
1. Monotonic State Progression: State rank can only advance (1 -> 6). Lower rank events cannot regress higher rank state.
2. Terminal Lock: Once a transaction reaches a Terminal State (Rank 5: COMPLETED, FAILED, CANCELLED, EXPIRED),
   no further state changes are allowed, except COMPLETED -> REFUNDED (Rank 6).
"""
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple, Union
from simulator.models import EventType


STATE_RANK_MAP: Dict[str, int] = {
    EventType.INITIATED.value: 1,
    EventType.PAYMENT_METHOD_SELECTED.value: 2,
    EventType.AUTHORIZED.value: 3,
    EventType.PROCESSING.value: 4,
    EventType.COMPLETED.value: 5,
    EventType.FAILED.value: 5,
    EventType.CANCELLED.value: 5,
    EventType.EXPIRED.value: 5,
    EventType.REFUNDED.value: 6,
}

TERMINAL_STATES: Set[str] = {
    EventType.COMPLETED.value,
    EventType.FAILED.value,
    EventType.CANCELLED.value,
    EventType.EXPIRED.value,
}

VALID_TRANSITIONS: Dict[str, Set[str]] = {
    EventType.INITIATED.value: {
        EventType.PAYMENT_METHOD_SELECTED.value,
        EventType.CANCELLED.value,
        EventType.EXPIRED.value,
    },
    EventType.PAYMENT_METHOD_SELECTED.value: {
        EventType.AUTHORIZED.value,
        EventType.FAILED.value,
        EventType.CANCELLED.value,
        EventType.EXPIRED.value,
    },
    EventType.AUTHORIZED.value: {
        EventType.PROCESSING.value,
        EventType.FAILED.value,
        EventType.CANCELLED.value,
    },
    EventType.PROCESSING.value: {
        EventType.COMPLETED.value,
        EventType.FAILED.value,
    },
    EventType.COMPLETED.value: {
        EventType.REFUNDED.value,  # Only post-terminal transition allowed
    },
    EventType.FAILED.value: set(),      # Strict terminal lock
    EventType.CANCELLED.value: set(),   # Strict terminal lock
    EventType.EXPIRED.value: set(),     # Strict terminal lock
    EventType.REFUNDED.value: set(),    # Strict terminal lock
}


class PaymentStateMachine:
    """Deterministic finite state machine governing payment transitions."""

    @staticmethod
    def get_rank(state: Union[EventType, str]) -> int:
        """Get the monotonic integer rank (1 to 6) for a given event type."""
        key = state.value if isinstance(state, EventType) else state
        if key not in STATE_RANK_MAP:
            raise ValueError(f"Unknown state '{key}'. Cannot determine rank.")
        return STATE_RANK_MAP[key]

    @staticmethod
    def is_terminal(state: Union[EventType, str]) -> bool:
        """Check if a state is terminal (Rank 5) or final post-terminal (Rank 6)."""
        key = state.value if isinstance(state, EventType) else state
        return key in TERMINAL_STATES or key == EventType.REFUNDED.value

    @staticmethod
    def get_valid_next_states(current_state: Union[EventType, str]) -> List[EventType]:
        """Return the list of legally reachable states from current_state."""
        key = current_state.value if isinstance(current_state, EventType) else current_state
        if key not in VALID_TRANSITIONS:
            raise ValueError(f"Unknown state '{key}'.")
        return [EventType(s) for s in VALID_TRANSITIONS[key]]

    @classmethod
    def is_transition_allowed(
        cls,
        from_state: Union[EventType, str],
        to_state: Union[EventType, str],
    ) -> bool:
        """Verify if transition is permissible under monotonic rank and terminal lock rules."""
        from_key = from_state.value if isinstance(from_state, EventType) else from_state
        to_key = to_state.value if isinstance(to_state, EventType) else to_state

        from_rank = cls.get_rank(from_key)
        to_rank = cls.get_rank(to_key)

        # Monotonic Rank Invariant: to_rank must be strictly >= from_rank
        if to_rank < from_rank:
            return False

        # If from_state is terminal, only COMPLETED -> REFUNDED is valid
        if from_key in TERMINAL_STATES:
            if from_key == EventType.COMPLETED.value and to_key == EventType.REFUNDED.value:
                return True
            return False

        # Post-terminal REFUNDED has no outgoing transitions
        if from_key == EventType.REFUNDED.value:
            return False

        return to_key in VALID_TRANSITIONS.get(from_key, set())

    @classmethod
    def generate_lifecycle_path(
        cls,
        terminal_type: str = EventType.COMPLETED.value,
        rng: Optional[random.Random] = None,
    ) -> List[EventType]:
        """Generate a valid, ordered sequence of lifecycle states terminating at the specified outcome."""
        r = rng or random.Random()

        if terminal_type == EventType.COMPLETED.value:
            return [
                EventType.INITIATED,
                EventType.PAYMENT_METHOD_SELECTED,
                EventType.AUTHORIZED,
                EventType.PROCESSING,
                EventType.COMPLETED,
            ]
        elif terminal_type == EventType.REFUNDED.value:
            return [
                EventType.INITIATED,
                EventType.PAYMENT_METHOD_SELECTED,
                EventType.AUTHORIZED,
                EventType.PROCESSING,
                EventType.COMPLETED,
                EventType.REFUNDED,
            ]
        elif terminal_type == EventType.FAILED.value:
            # Failure can happen at payment method selection, authorization, or processing
            failure_points = [
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.FAILED],
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.AUTHORIZED, EventType.FAILED],
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.AUTHORIZED, EventType.PROCESSING, EventType.FAILED],
            ]
            return r.choice(failure_points)
        elif terminal_type == EventType.CANCELLED.value:
            cancel_points = [
                [EventType.INITIATED, EventType.CANCELLED],
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.CANCELLED],
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.AUTHORIZED, EventType.CANCELLED],
            ]
            return r.choice(cancel_points)
        elif terminal_type == EventType.EXPIRED.value:
            expire_points = [
                [EventType.INITIATED, EventType.EXPIRED],
                [EventType.INITIATED, EventType.PAYMENT_METHOD_SELECTED, EventType.EXPIRED],
            ]
            return r.choice(expire_points)
        else:
            raise ValueError(f"Unsupported terminal outcome: {terminal_type}")

    @staticmethod
    def generate_event_timestamps(
        path: List[EventType],
        start_time: datetime,
        rng: Optional[random.Random] = None,
        min_delta_ms: int = 500,
        max_delta_ms: int = 4000,
    ) -> List[Tuple[EventType, str]]:
        """Attach sequential, forward-moving ISO-8601 UTC timestamps to each step in the path."""
        r = rng or random.Random()
        current_time = start_time
        timed_path: List[Tuple[EventType, str]] = []

        for step in path:
            iso_str = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            timed_path.append((step, iso_str))
            delta_ms = r.randint(min_delta_ms, max_delta_ms)
            current_time += timedelta(milliseconds=delta_ms)

        return timed_path

