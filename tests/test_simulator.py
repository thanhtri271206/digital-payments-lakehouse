"""Unit and Integration Tests for PaymentSimulator."""

from datetime import datetime, timezone
import json
from pathlib import Path
import pyarrow.parquet as pq
import pytest

from simulator.models import (
    AnomalyType,
    EventType,
    GroundTruthRecord,
    PaymentEvent,
)
from simulator.payment_generator import PaymentSimulator


@pytest.fixture
def simulator() -> PaymentSimulator:
    """Fixture providing a deterministic PaymentSimulator instance."""
    return PaymentSimulator(config_path="config/config.yaml", seed=20260930)


def test_simulator_initialization(simulator: PaymentSimulator) -> None:
    """Verify simulator initialization with default seed and rates from config."""
    assert simulator.seed == 20260930
    assert simulator.rng is not None
    assert simulator.duplicate_rate == 0.005
    assert simulator.late_short_rate == 0.010
    assert simulator.late_long_rate == 0.002
    assert simulator.out_of_order_rate == 0.005
    assert simulator.missing_terminal_rate == 0.003
    assert simulator.poison_record_rate == 0.002
    assert simulator.terminal_conflict_rate == 0.001


def test_determinism_with_same_seed() -> None:
    """Verify that identical seeds produce 100% reproducible transactions and events."""
    sim1 = PaymentSimulator(seed=20260930)
    sim2 = PaymentSimulator(seed=20260930)

    events1, gt1 = sim1.generate_batch(n_transactions=5, dry_run=False, as_dict=True)
    events2, gt2 = sim2.generate_batch(n_transactions=5, dry_run=False, as_dict=True)

    assert len(events1) == len(events2)
    assert len(gt1) == len(gt2)

    for e1, e2 in zip(events1, events2):
        assert e1["event_id"] == e2["event_id"]
        assert e1["transaction_id"] == e2["transaction_id"]
        assert e1["event_type"] == e2["event_type"]
        assert e1["amount_minor"] == e2["amount_minor"]

    for g1, g2 in zip(gt1, gt2):
        assert g1.transaction_id == g2.transaction_id
        assert g1.amount_minor == g2.amount_minor
        assert g1.anomaly_types == g2.anomaly_types


def test_generate_transaction_normal(simulator: PaymentSimulator) -> None:
    """Verify standard transaction follows monotonic lifecycle rank 1 -> 2 -> 3 -> 4 -> 5."""
    events, gt = simulator.generate_transaction(force_anomaly="NONE")

    assert isinstance(gt, GroundTruthRecord)
    assert not gt.is_anomaly
    assert gt.anomaly_types == []
    assert gt.amount_minor > 0
    assert gt.expected_resolution == "NORMAL_SETTLEMENT"

    # Monotonic rank progression
    event_types = [e.event_type for e in events]
    assert event_types[0] == EventType.INITIATED.value
    assert event_types[1] == EventType.PAYMENT_METHOD_SELECTED.value
    assert event_types[2] == EventType.AUTHORIZED.value
    assert event_types[3] == EventType.PROCESSING.value
    assert event_types[4] in {
        EventType.COMPLETED.value,
        EventType.FAILED.value,
        EventType.CANCELLED.value,
        EventType.EXPIRED.value,
    }


def test_generate_transaction_out_of_order(simulator: PaymentSimulator) -> None:
    """Verify OUT_OF_ORDER swaps positions of 2 events in emission order."""
    events, gt = simulator.generate_transaction(force_anomaly="OUT_OF_ORDER")

    assert gt.is_anomaly
    assert AnomalyType.OUT_OF_ORDER.value in gt.anomaly_types
    assert gt.expected_resolution == "WATERMARK_REORDERED"
    assert "swapped_events" in gt.metadata
    assert "swapped_indices" in gt.metadata

    i, j = gt.metadata["swapped_indices"]
    # The swapped event type matches metadata
    assert events[i].event_type == gt.metadata["swapped_events"][1]
    assert events[j].event_type == gt.metadata["swapped_events"][0]


def test_generate_transaction_duplicate(simulator: PaymentSimulator) -> None:
    """Verify DUPLICATE replicates an event with the exact same event_id."""
    events, gt = simulator.generate_transaction(force_anomaly="DUPLICATE")

    assert gt.is_anomaly
    assert AnomalyType.DUPLICATE.value in gt.anomaly_types
    assert gt.expected_resolution == "WATERMARK_DEDUPLICATED"
    assert "duplicated_event_id" in gt.metadata

    event_ids = [e.event_id for e in events]
    dup_id = gt.metadata["duplicated_event_id"]
    assert event_ids.count(dup_id) == 2


def test_generate_transaction_late_short(simulator: PaymentSimulator) -> None:
    """Verify LATE_SHORT shifts event_time back ~30s-60s vs ingestion_time (within watermark)."""
    events, gt = simulator.generate_transaction(force_anomaly="LATE_SHORT")

    assert gt.is_anomaly
    assert AnomalyType.LATE_SHORT.value in gt.anomaly_types
    assert gt.expected_resolution == "WITHIN_WATERMARK_PROCESSED"
    assert gt.metadata["within_watermark"] is True

    diff = (events[0].ingestion_time - events[0].event_time).total_seconds()
    assert 28.0 <= diff <= 65.0
    # Must be <= 120s (watermark)
    assert diff <= 120.0


def test_generate_transaction_late_long(simulator: PaymentSimulator) -> None:
    """Verify LATE_LONG shifts event_time back > 150s (triggers DLQ)."""
    events, gt = simulator.generate_transaction(force_anomaly="LATE_LONG")

    assert gt.is_anomaly
    assert AnomalyType.LATE_LONG.value in gt.anomaly_types
    assert gt.expected_resolution == "ROUTED_TO_DLQ"
    assert gt.metadata["within_watermark"] is False

    diff = (events[0].ingestion_time - events[0].event_time).total_seconds()
    assert diff > 150.0


def test_generate_transaction_missing_terminal(simulator: PaymentSimulator) -> None:
    """Verify MISSING_TERMINAL stops event chain at PROCESSING (triggers STUCK > 5m)."""
    events, gt = simulator.generate_transaction(force_anomaly="MISSING_TERMINAL")

    assert gt.is_anomaly
    assert AnomalyType.MISSING_TERMINAL.value in gt.anomaly_types
    assert gt.final_state == EventType.PROCESSING.value
    assert gt.expected_resolution == "STUCK_PROCESSING"

    event_types = [e.event_type for e in events]
    assert event_types[-1] == EventType.PROCESSING.value
    assert EventType.COMPLETED.value not in event_types
    assert EventType.FAILED.value not in event_types


def test_generate_transaction_poison_record(simulator: PaymentSimulator) -> None:
    """Verify POISON_RECORD assigns amount_minor = -50000."""
    events, gt = simulator.generate_transaction(force_anomaly="POISON_RECORD")

    assert gt.is_anomaly
    assert AnomalyType.POISON_RECORD.value in gt.anomaly_types
    assert gt.expected_resolution == "ROUTED_TO_QUARANTINE"

    for ev in events:
        assert ev.amount_minor == -50000
    assert gt.amount_minor > 0  # Ground truth records intended genuine amount


def test_generate_transaction_terminal_conflict(simulator: PaymentSimulator) -> None:
    """Verify TERMINAL_CONFLICT emits both COMPLETED and FAILED for the same transaction_id."""
    events, gt = simulator.generate_transaction(force_anomaly="TERMINAL_CONFLICT")

    assert gt.is_anomaly
    assert AnomalyType.TERMINAL_CONFLICT.value in gt.anomaly_types
    assert gt.expected_resolution == "LIFECYCLE_EXCEPTION"
    assert gt.final_state == "CONFLICT_COMPLETED_FAILED"

    types = [e.event_type for e in events]
    assert EventType.COMPLETED.value in types
    assert EventType.FAILED.value in types

    # Both have the exact same transaction_id
    tx_ids = {e.transaction_id for e in events}
    assert len(tx_ids) == 1


def test_generate_batch_export_json_and_parquet(tmp_path: Path, simulator: PaymentSimulator) -> None:
    """Verify batch generation and export to both JSON and Parquet formats."""
    json_path = tmp_path / "ground_truth.json"
    parquet_path = tmp_path / "ground_truth.parquet"

    # Export to JSON
    events, gt_json = simulator.generate_batch(
        n_transactions=15,
        export_gt_path=json_path,
        dry_run=False,
    )
    assert json_path.exists()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert len(data) == 15

    # Export to Parquet
    _, gt_parquet = simulator.generate_batch(
        n_transactions=15,
        export_gt_path=parquet_path,
        dry_run=False,
    )
    assert parquet_path.exists()
    table = pq.read_table(parquet_path)
    assert len(table) == 15
    assert "transaction_id" in table.column_names
    assert "amount_minor" in table.column_names
    assert "anomaly_types" in table.column_names


def test_dry_run_stdout(capsys: pytest.CaptureFixture[str], simulator: PaymentSimulator) -> None:
    """Verify dry_run prints clean execution summary to stdout."""
    simulator.generate_batch(n_transactions=10, dry_run=True)
    captured = capsys.readouterr()
    assert "PAYMENT SIMULATOR [DRY-RUN EXECUTION SUMMARY]" in captured.out
    assert "Total Transactions: 10" in captured.out
    assert "Anomaly Breakdown:" in captured.out
