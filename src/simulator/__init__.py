"""Payment Simulator Package."""

from typing import Any

from simulator.models import (
    LIFECYCLE_RANKS,
    AnomalyType,
    EventType,
    GroundTruthRecord,
    PaymentChannel,
    PaymentEvent,
    PaymentMethod,
)

__all__ = [
    "LIFECYCLE_RANKS",
    "AnomalyType",
    "EventType",
    "GroundTruthRecord",
    "PaymentChannel",
    "PaymentEvent",
    "PaymentMethod",
    "PaymentSimulator",
]


def __getattr__(name: str) -> Any:
    if name == "PaymentSimulator":
        from simulator.payment_generator import PaymentSimulator

        return PaymentSimulator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
