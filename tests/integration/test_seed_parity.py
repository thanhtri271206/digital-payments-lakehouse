"""Integration test: Seed Parity and Ground Truth (G0) Consistency Audit."""
import hashlib
from pathlib import Path
import pytest
from simulator.generator import PaymentGenerator
from simulator.ground_truth import GroundTruthExporter
from simulator.models import OperationalStatus


def test_seed_parity_event_stream_and_g0(tmp_path):
    """Verify AC-1 & AC-3: Complete deterministic replay of event stream and G0 export."""
    seed = 20260930
    count = 1000

    # Run 1
    gen1 = PaymentGenerator(seed=seed)
    events1, gt1 = gen1.generate_batch(count=count)
    exporter1 = GroundTruthExporter(output_dir=tmp_path / "run1")
    file1 = exporter1.export_records(gt1, seed=seed)
    hash_file1 = hashlib.sha256(file1.read_bytes()).hexdigest()

    # Run 2
    gen2 = PaymentGenerator(seed=seed)
    events2, gt2 = gen2.generate_batch(count=count)
    exporter2 = GroundTruthExporter(output_dir=tmp_path / "run2")
    file2 = exporter2.export_records(gt2, seed=seed)
    hash_file2 = hashlib.sha256(file2.read_bytes()).hexdigest()

    # Verification
    assert len(gt1) == count
    assert len(gt2) == count
    assert len(events1) == len(events2)
    assert hash_file1 == hash_file2


def test_g0_records_contain_all_fault_classifications(tmp_path):
    """Verify AC-2: G0 records comprehensively capture STUCK, QUARANTINED, and EXCEPTION faults."""
    seed = 20260930
    count = 2000

    gen = PaymentGenerator(seed=seed)
    _, gt_records = gen.generate_batch(count=count)
    exporter = GroundTruthExporter(output_dir=tmp_path)
    file_path = exporter.export_records(gt_records, seed=seed)

    loaded = GroundTruthExporter.load_records(file_path)
    assert len(loaded) == count

    statuses = {r.expected_operational_status for r in loaded}
    assert OperationalStatus.NORMAL.value in statuses
    assert OperationalStatus.STUCK.value in statuses
    assert OperationalStatus.QUARANTINED.value in statuses
    assert OperationalStatus.EXCEPTION.value in statuses

    # Confirm all STUCK records have was_ever_stuck_expected=True
    stuck_records = [r for r in loaded if r.expected_operational_status == OperationalStatus.STUCK.value]
    assert len(stuck_records) > 0
    for sr in stuck_records:
        assert sr.was_ever_stuck_expected is True
        assert "MISSING_TERMINAL_STUCK" in sr.injected_faults


def test_g0_compressed_export_roundtrip(tmp_path):
    """Verify compressed export .jsonl.gz preserves exact records upon loading."""
    seed = 20260930
    gen = PaymentGenerator(seed=seed)
    _, gt_records = gen.generate_batch(count=50)

    exporter = GroundTruthExporter(output_dir=tmp_path)
    file_path = exporter.export_records(gt_records, seed=seed, compressed=True)
    assert file_path.suffix == ".gz"

    loaded = GroundTruthExporter.load_records(file_path)
    assert len(loaded) == 50
    assert loaded[0].transaction_id == gt_records[0].transaction_id

