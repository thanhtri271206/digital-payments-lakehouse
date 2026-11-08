"""End-to-End Quality Gate 1 Verification Test Suite."""
import json
from pathlib import Path
import pytest
from typer.testing import CliRunner
from simulator.ground_truth import GroundTruthExporter
from simulator.main import app

runner = CliRunner()


def test_gate_1_e2e_cli_simulation_run(tmp_path):
    """Verify AC-1 & AC-3: Full CLI execution generating events and G0 file."""
    g0_dir = tmp_path / "g0"
    events_dir = tmp_path / "events"

    result = runner.invoke(
        app,
        [
            "--seed", "20260930",
            "--count", "200",
            "--rate", "0",  # Unthrottled for rapid testing
            "--mode", "both",
            "--dry-run",
            "--output-dir", str(g0_dir),
            "--events-dir", str(events_dir),
        ],
    )
    assert result.exit_code == 0

    # 1. Verify G0 file
    g0_file = g0_dir / "G0_events_seed_20260930.jsonl"
    assert g0_file.exists()
    records = GroundTruthExporter.load_records(g0_file)
    assert len(records) == 200

    # 2. Verify raw events file
    events_file = events_dir / "events_seed_20260930.jsonl"
    assert events_file.exists()
    with open(events_file, "r", encoding="utf-8") as f:
        event_lines = [json.loads(line) for line in f if line.strip()]
    assert len(event_lines) >= 200

    # 3. Check Financial and Time Invariants on all generated events
    for ev in event_lines:
        assert isinstance(ev["amount_minor"], int)
        assert ev["event_time"].endswith("Z")
        assert ev["currency"] == "VND"


def test_gate_1_export_g0_only(tmp_path):
    """Verify --export-g0-only generates solely the ground truth artifact."""
    g0_dir = tmp_path / "g0_only"

    result = runner.invoke(
        app,
        [
            "--seed", "20260930",
            "--count", "50",
            "--export-g0-only",
            "--output-dir", str(g0_dir),
        ],
    )
    assert result.exit_code == 0
    g0_file = g0_dir / "G0_events_seed_20260930.jsonl"
    assert g0_file.exists()
    records = GroundTruthExporter.load_records(g0_file)
    assert len(records) == 50

