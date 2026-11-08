"""Unit tests for PaymentEvent and GroundTruthRecord Pydantic v2 data contracts."""
import json
import pytest
from pydantic import ValidationError
from simulator.models import (
    EventType,
    OperationalStatus,
    PaymentChannel,
    PaymentEvent,
    PaymentMethod,
    GroundTruthRecord,
)


def test_valid_payment_event_creation():
    """Verify normal valid event instantiation and JSON serialization."""
    event = PaymentEvent(
        event_id="evt-001",
        transaction_id="txn-001",
        event_type=EventType.INITIATED.value,
        event_time="2026-10-15T03:00:00.000Z",
        user_id="usr-123",
        merchant_id="merch-456",
        payment_channel=PaymentChannel.BANK_A.value,
        payment_method=PaymentMethod.QR.value,
        amount_minor=50000,
        currency="VND",
    )
    assert event.amount_minor == 50000
    assert event.event_type == "INITIATED"
    assert event.ingestion_time is None

    # Test to_json_bytes
    json_bytes = event.to_json_bytes()
    assert isinstance(json_bytes, bytes)
    data = json.loads(json_bytes.decode("utf-8"))
    assert data["amount_minor"] == 50000
    assert data["event_id"] == "evt-001"


def test_amount_minor_float_strictly_rejected():
    """Verify AC-1: Floats are strictly prohibited to prevent IEEE 754 precision loss."""
    with pytest.raises(ValidationError) as exc_info:
        PaymentEvent(
            event_id="evt-002",
            transaction_id="txn-002",
            event_type=EventType.INITIATED.value,
            event_time="2026-10-15T03:00:00.000Z",
            user_id="usr-123",
            merchant_id="merch-456",
            payment_channel=PaymentChannel.BANK_A.value,
            payment_method=PaymentMethod.CARD.value,
            amount_minor=50000.75,  # FLOAT
        )
    assert "Financial Invariant Violation" in str(exc_info.value)
    assert "float is prohibited" in str(exc_info.value)


def test_amount_minor_large_bigint_preserves_precision():
    """Verify AC-3: Extremely large minor unit amounts preserve exact integer precision."""
    large_val = 9_000_000_000_000_000  # 9 quadrillion VND
    event = PaymentEvent(
        event_id="evt-003",
        transaction_id="txn-003",
        event_type=EventType.COMPLETED.value,
        event_time="2026-10-15T03:00:00.000Z",
        user_id="usr-123",
        merchant_id="merch-456",
        payment_channel=PaymentChannel.GATEWAY_A.value,
        payment_method=PaymentMethod.BANK_TRANSFER.value,
        amount_minor=large_val,
    )
    assert event.amount_minor == large_val
    dumped = json.loads(event.to_json_bytes().decode("utf-8"))
    assert dumped["amount_minor"] == large_val


def test_negative_amount_minor_rejected_unless_poison():
    """Verify negative amount is rejected for normal events but allowed when is_poison=True."""
    with pytest.raises(ValidationError) as exc_info:
        PaymentEvent(
            event_id="evt-004",
            transaction_id="txn-004",
            event_type=EventType.INITIATED.value,
            event_time="2026-10-15T03:00:00.000Z",
            user_id="usr-123",
            merchant_id="merch-456",
            payment_channel=PaymentChannel.BANK_A.value,
            payment_method=PaymentMethod.WALLET_BALANCE.value,
            amount_minor=-1000,
            is_poison=False,
        )
    assert "Financial Invariant Violation" in str(exc_info.value)

    # When marked as poison, it should be constructible for testing DLQ/Circuit Breakers
    poison_event = PaymentEvent(
        event_id="evt-004-poison",
        transaction_id="txn-004",
        event_type=EventType.INITIATED.value,
        event_time="2026-10-15T03:00:00.000Z",
        user_id="usr-123",
        merchant_id="merch-456",
        payment_channel="UNKNOWN_CHANNEL",
        payment_method="UNKNOWN_METHOD",
        amount_minor=-1000,
        is_poison=True,
    )
    assert poison_event.is_poison is True
    assert poison_event.amount_minor == -1000


def test_invalid_event_time_timezone_rejected():
    """Verify AC-2: event_time without UTC offset is rejected."""
    # Non-UTC timezone (e.g. +07:00)
    with pytest.raises(ValidationError) as exc_info:
        PaymentEvent(
            event_id="evt-005",
            transaction_id="txn-005",
            event_type=EventType.INITIATED.value,
            event_time="2026-10-15T10:00:00+07:00",  # Not UTC!
            user_id="usr-123",
            merchant_id="merch-456",
            payment_channel=PaymentChannel.BANK_A.value,
            payment_method=PaymentMethod.CARD.value,
            amount_minor=10000,
        )
    assert "event_time must include UTC timezone offset" in str(exc_info.value)

    # Naive timestamp without timezone
    with pytest.raises(ValidationError):
        PaymentEvent(
            event_id="evt-006",
            transaction_id="txn-006",
            event_type=EventType.INITIATED.value,
            event_time="2026-10-15 10:00:00",
            user_id="usr-123",
            merchant_id="merch-456",
            payment_channel=PaymentChannel.BANK_A.value,
            payment_method=PaymentMethod.CARD.value,
            amount_minor=10000,
        )


def test_ground_truth_record():
    """Verify GroundTruthRecord instantiation and default properties."""
    gt = GroundTruthRecord(
        transaction_id="txn-010",
        user_id="usr-123",
        merchant_id="merch-456",
        payment_channel="BANK_A",
        payment_method="QR",
        amount_minor=100000,
        lifecycle_sequence=["INITIATED", "PAYMENT_METHOD_SELECTED", "AUTHORIZED", "PROCESSING", "COMPLETED"],
        expected_final_state="COMPLETED",
        expected_operational_status=OperationalStatus.NORMAL.value,
        was_ever_stuck_expected=False,
        injected_faults=[],
        created_at="2026-10-15T03:00:05.000Z",
    )
    assert gt.expected_final_state == "COMPLETED"
    assert gt.expected_operational_status == "NORMAL"
    assert len(gt.lifecycle_sequence) == 5

