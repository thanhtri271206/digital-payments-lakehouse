"""Unit tests for Deterministic Generator and Fault Injector Engine."""
import hashlib
import json
import pytest
from simulator.fault_injector import FaultInjector, FaultType
from simulator.generator import PaymentGenerator
from simulator.models import OperationalStatus


def test_seed_determinism_sha256_parity():
    """Verify AC-1: Two generator instances with identical seed produce 100% byte-identical output."""
    seed = 20260930
    count = 500

    gen1 = PaymentGenerator(seed=seed)
    events1, gt1 = gen1.generate_batch(count=count)
    bytes1 = "".join([e.model_dump_json() for e in events1]).encode("utf-8")
    hash1 = hashlib.sha256(bytes1).hexdigest()

    gen2 = PaymentGenerator(seed=seed)
    events2, gt2 = gen2.generate_batch(count=count)
    bytes2 = "".join([e.model_dump_json() for e in events2]).encode("utf-8")
    hash2 = hashlib.sha256(bytes2).hexdigest()

    assert hash1 == hash2
    assert len(events1) == len(events2)
    assert len(gt1) == len(gt2)


def test_fault_injection_metadata_recording():
    """Verify AC-3: Injected faults are accurately tagged in GroundTruthRecord."""
    # Run with high fault probabilities to ensure all faults trigger
    from digital_payments_lakehouse.config.settings import get_config
    cfg = get_config()
    cfg_copy = cfg.model_copy(deep=True)
    cfg_copy.simulator.rates.poison_record_rate = 1.0  # Force poison

    gen = PaymentGenerator(seed=12345, config=cfg_copy)
    events, gt = gen.generate_transaction()

    assert FaultType.POISON_RECORD.value in gt.injected_faults
    assert gt.expected_operational_status == OperationalStatus.QUARANTINED.value
    # Ensure one event has is_poison=True and negative amount
    poison_events = [e for e in events if e.is_poison]
    assert len(poison_events) >= 1
    assert poison_events[0].amount_minor < 0


def test_missing_terminal_stuck_status():
    """Verify missing terminal injection leaves transaction in PROCESSING with STUCK status."""
    from digital_payments_lakehouse.config.settings import get_config
    cfg = get_config()
    cfg_copy = cfg.model_copy(deep=True)
    cfg_copy.simulator.rates.missing_terminal_rate = 1.0  # Force stuck

    gen = PaymentGenerator(seed=999, config=cfg_copy)
    events, gt = gen.generate_transaction()

    assert FaultType.MISSING_TERMINAL_STUCK.value in gt.injected_faults
    assert gt.expected_operational_status == OperationalStatus.STUCK.value
    assert gt.was_ever_stuck_expected is True
    assert gt.expected_final_state == "PROCESSING"
    assert events[-1].event_type == "PROCESSING"


def test_eod_boundary_generation():
    """Verify transactions generated around EOD boundary (16:59:59 UTC)."""
    gen = PaymentGenerator(seed=20260930)
    events, gt = gen.generate_batch(count=10, eod_boundary=True)

    # First event should start around 16:59:50 UTC
    first_time = events[0].event_time
    assert "16:59:" in first_time

