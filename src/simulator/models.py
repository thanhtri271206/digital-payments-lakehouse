"""Domain Models and Strict Data Contracts for Payment Events & Ground Truth G0.

Enforces Lakehouse Technical Invariants:
1. amount_minor is strictly BIGINT (integer). Floats are rejected to prevent precision loss.
2. event_time is strictly ISO-8601 UTC.
3. Strict separation between event_time (business occurrence) and ingestion_time (broker arrival).
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    INITIATED = "INITIATED"
    PAYMENT_METHOD_SELECTED = "PAYMENT_METHOD_SELECTED"
    AUTHORIZED = "AUTHORIZED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REFUNDED = "REFUNDED"


class PaymentChannel(str, Enum):
    BANK_A = "BANK_A"
    GATEWAY_A = "GATEWAY_A"
    GATEWAY_B = "GATEWAY_B"


class PaymentMethod(str, Enum):
    BANK_TRANSFER = "BANK_TRANSFER"
    CARD = "CARD"
    WALLET_BALANCE = "WALLET_BALANCE"
    QR = "QR"


class OperationalStatus(str, Enum):
    NORMAL = "NORMAL"
    STUCK = "STUCK"
    QUARANTINED = "QUARANTINED"
    EXCEPTION = "EXCEPTION"


class PaymentEvent(BaseModel):
    """Pydantic v2 contract for payment event streaming into Kafka topic 'payment.events.raw'."""

    event_id: str = Field(..., description="Unique UUID for this event")
    transaction_id: str = Field(..., description="Global business transaction ID")
    event_type: str = Field(..., description="Payment lifecycle state event")
    event_time: str = Field(..., description="Business event occurrence time in ISO-8601 UTC")
    ingestion_time: Optional[str] = Field(
        default=None,
        description="Lakehouse/Broker arrival time in ISO-8601 UTC. Null when generated.",
    )
    user_id: str = Field(..., description="User identifier")
    merchant_id: str = Field(..., description="Merchant identifier")
    payment_channel: str = Field(..., description="Originating channel (e.g. BANK_A, GATEWAY_A)")
    payment_method: str = Field(..., description="Selected method (e.g. QR, CARD, WALLET_BALANCE)")
    amount_minor: int = Field(
        ...,
        description="Monetary amount in smallest minor currency unit (e.g., integer VND). Never float.",
    )
    currency: str = Field(default="VND", description="ISO 4217 Currency Code")
    status_reason: Optional[str] = Field(default=None, description="Optional failure or operational reason")
    producer_version: str = Field(default="v1.0", description="Simulator generator code version")
    is_poison: bool = Field(default=False, description="Flag indicating deliberate fault injection")

    @field_validator("amount_minor", mode="before")
    @classmethod
    def validate_amount_minor(cls, v: Any) -> int:
        if isinstance(v, float):
            raise ValueError(
                f"Financial Invariant Violation: 'amount_minor' must be integer (BIGINT), float is prohibited: {v}"
            )
        if not isinstance(v, int):
            raise ValueError(f"'amount_minor' must be integer, got {type(v).__name__}")
        return v

    @field_validator("event_time")
    @classmethod
    def validate_utc_iso8601(cls, v: str) -> str:
        try:
            normalized = v.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
                raise ValueError("event_time must include UTC timezone offset ('Z' or '+00:00')")
        except Exception as e:
            raise ValueError(f"Invalid ISO-8601 UTC timestamp '{v}': {e}")
        return v

    @field_validator("ingestion_time")
    @classmethod
    def validate_ingestion_time_utc(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        try:
            normalized = v.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
                raise ValueError("ingestion_time must include UTC timezone offset ('Z' or '+00:00')")
        except Exception as e:
            raise ValueError(f"Invalid ISO-8601 UTC ingestion_time '{v}': {e}")
        return v

    @model_validator(mode="after")
    def validate_business_invariants(self) -> "PaymentEvent":
        # Unless explicitly flagged as poison record, amount_minor must be > 0
        if not self.is_poison:
            if self.amount_minor <= 0:
                raise ValueError(
                    f"Financial Invariant Violation: 'amount_minor' must be > 0 for standard events, got {self.amount_minor}"
                )
            # Validate enum values if not poison
            valid_types = {e.value for e in EventType}
            if self.event_type not in valid_types:
                raise ValueError(f"Invalid event_type: '{self.event_type}'. Must be one of {valid_types}")
            valid_channels = {c.value for c in PaymentChannel}
            if self.payment_channel not in valid_channels:
                raise ValueError(f"Invalid payment_channel: '{self.payment_channel}'. Must be one of {valid_channels}")
            valid_methods = {m.value for m in PaymentMethod}
            if self.payment_method not in valid_methods:
                raise ValueError(f"Invalid payment_method: '{self.payment_method}'. Must be one of {valid_methods}")
        return self

    def to_json_bytes(self) -> bytes:
        """Serialize event to UTF-8 bytes for low-overhead Kafka streaming."""
        return self.model_dump_json().encode("utf-8")


class GroundTruthRecord(BaseModel):
    """Ground Truth G0 entity representing the uncorrupted objective reality of a transaction."""

    transaction_id: str = Field(..., description="Unique transaction ID")
    user_id: str = Field(..., description="User ID")
    merchant_id: str = Field(..., description="Merchant ID")
    payment_channel: str = Field(..., description="Channel name")
    payment_method: str = Field(..., description="Method name")
    amount_minor: int = Field(..., description="True transaction amount in integer minor units")
    currency: str = Field(default="VND", description="ISO currency code")
    lifecycle_sequence: List[str] = Field(
        default_factory=list,
        description="Chronological sequence of states generated for this transaction",
    )
    expected_final_state: str = Field(..., description="Expected terminal or highest monotonic state")
    expected_operational_status: str = Field(
        default=OperationalStatus.NORMAL.value,
        description="Expected operational classification: NORMAL, STUCK, QUARANTINED, EXCEPTION",
    )
    was_ever_stuck_expected: bool = Field(
        default=False,
        description="Whether transaction lingered in PROCESSING > 5m during execution",
    )
    injected_faults: List[str] = Field(
        default_factory=list,
        description="List of fault injection labels applied to this transaction",
    )
    created_at: str = Field(..., description="Ground truth record timestamp in ISO-8601 UTC")
