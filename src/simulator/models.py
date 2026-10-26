"""Data models for payment simulation, event streaming, and ground truth tracking."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventType(str, Enum):
    """Lifecycle event types according to SRS Section 3.2."""

    INITIATED = "INITIATED"
    PAYMENT_METHOD_SELECTED = "PAYMENT_METHOD_SELECTED"
    AUTHORIZED = "AUTHORIZED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REFUNDED = "REFUNDED"


LIFECYCLE_RANKS: dict[str, int] = {
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


class PaymentChannel(str, Enum):
    """Payment channels / partner gateways."""

    BANK_A = "BANK_A"
    BANK_B = "BANK_B"
    GATEWAY_A = "GATEWAY_A"
    GATEWAY_B = "GATEWAY_B"
    MOMO_PAY = "MOMO_PAY"


class PaymentMethod(str, Enum):
    """Payment methods supported by the wallet platform."""

    BANK_TRANSFER = "BANK_TRANSFER"
    CARD = "CARD"
    WALLET_BALANCE = "WALLET_BALANCE"
    QR = "QR"


class AnomalyType(str, Enum):
    """Injection anomaly types configured in config.yaml and SRS."""

    NONE = "NONE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    DUPLICATE = "DUPLICATE"
    LATE_SHORT = "LATE_SHORT"
    LATE_LONG = "LATE_LONG"
    MISSING_TERMINAL = "MISSING_TERMINAL"
    POISON_RECORD = "POISON_RECORD"
    TERMINAL_CONFLICT = "TERMINAL_CONFLICT"


class PaymentEvent(BaseModel):
    """S1 - Payment Gateway Event Stream Schema."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    event_id: str = Field(description="Unique UUID for this specific event")
    transaction_id: str = Field(
        description="Correlation UUID across the lifecycle of a payment"
    )
    event_type: str = Field(description="Current status transition event type")
    event_time: datetime = Field(description="Timestamp when event occurred (UTC)")
    ingestion_time: datetime = Field(
        description="Timestamp when platform/Kafka ingested the event (UTC)"
    )
    user_id: str = Field(
        description="Unique identifier for the user initiating the payment"
    )
    merchant_id: str = Field(description="Merchant receiving the payment")
    payment_channel: str = Field(description="External payment channel or bank partner")
    payment_method: str = Field(description="Method used: BANK_TRANSFER, CARD, etc.")
    amount_minor: int = Field(
        description="Integer minor unit amount, e.g. 100000 VND (BIGINT)"
    )
    currency: str = Field(default="VND", description="ISO currency code, default VND")
    status_reason: str | None = Field(
        default=None, description="Detailed failure or cancellation reason"
    )
    producer_version: str = Field(
        default="v1.0", description="Simulator / producer semantic version"
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert model to a JSON-compatible dictionary with ISO 8601 timestamps."""
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Serialize model to JSON string."""
        return self.model_dump_json()


class GroundTruthRecord(BaseModel):
    """G0 - Simulator Ground Truth Record for automated DQ and Reconciliation validation."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    transaction_id: str = Field(description="Unique correlation ID of transaction")
    user_id: str = Field(description="User ID")
    merchant_id: str = Field(description="Merchant ID")
    payment_channel: str = Field(description="Payment channel")
    payment_method: str = Field(description="Payment method")
    amount_minor: int = Field(description="Intended true amount in minor units")
    currency: str = Field(default="VND")
    initiated_at: datetime = Field(description="Timestamp when initiated")
    final_state: str = Field(
        description="Expected final business state: COMPLETED, FAILED, PROCESSING, etc."
    )
    is_anomaly: bool = Field(description="Whether any fault was deliberately injected")
    anomaly_types: list[str] = Field(
        default_factory=list, description="List of anomaly types injected"
    )
    expected_resolution: str = Field(
        description="Anticipated handling resolution (e.g., NORMAL_SETTLEMENT, ROUTED_TO_DLQ, ROUTED_TO_QUARANTINE, STUCK_PROCESSING, LIFECYCLE_EXCEPTION, WATERMARK_DEDUPLICATED)"
    )
    event_count: int = Field(
        description="Total number of events emitted for this transaction"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary audit details about the anomaly"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert model to a JSON-compatible dictionary."""
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Serialize model to JSON string."""
        return self.model_dump_json()
