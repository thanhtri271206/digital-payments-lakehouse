"""Payment Simulator Package."""

from typing import Any
from simulator.models import (
    AnomalyType,
    EventType,
    GroundTruthRecord,
    LIFECYCLE_RANKS,
    PaymentChannel,
    PaymentEvent,
    PaymentMethod,
)

__all__ = [
    "AnomalyType",
    "EventType",
    "GroundTruthRecord",
    "LIFECYCLE_RANKS",
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
