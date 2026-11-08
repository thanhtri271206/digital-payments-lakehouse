"""Unit tests for Centralized Config Loader and Structured Logger."""
import json
import pytest
from pydantic import ValidationError
from digital_payments_lakehouse.config.settings import (
    AppConfig,
    load_config,
    get_config,
)
from digital_payments_lakehouse.utils.logger import setup_logger


def test_load_valid_config():
    """Verify loading real config/config.yaml parses without error."""
    config = load_config("config/config.yaml")
    assert isinstance(config, AppConfig)
    assert config.project.canonical_timezone == "UTC"
    assert config.project.presentation_timezone == "Asia/Ho_Chi_Minh"
    assert config.streaming_slo.freshness_p95_seconds == 30
    assert config.streaming_slo.watermark_lateness_seconds == 120
    assert config.streaming_slo.stuck_timeout_seconds == 300
    assert config.data_quality.circuit_breaker_error_rate == 0.05
    assert config.simulator.default_seed == 20260930
    assert config.simulator.rates.duplicate_event_rate == 0.005
    assert config.simulator.rates.terminal_conflict_rate == 0.001


def test_missing_config_raises_filenotfound():
    """Verify non-existent config path raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_config("config/non_existent_file.yaml")


def test_invalid_canonical_timezone_raises_validation_error(tmp_path):
    """Verify that a canonical_timezone other than UTC violates invariant."""
    bad_yaml = """
project:
  name: "bad-tz-proj"
  canonical_timezone: "Asia/Bangkok"
  presentation_timezone: "Asia/Ho_Chi_Minh"
streaming_slo:
  freshness_p95_seconds: 30
partner_degradation:
  short_window_seconds: 60
data_quality:
  circuit_breaker_error_rate: 0.05
reconciliation_eod:
  cutoff_time_local: "23:59:59"
simulator:
  default_seed: 20260930
"""
    bad_config_file = tmp_path / "bad_config.yaml"
    bad_config_file.write_text(bad_yaml, encoding="utf-8")

    with pytest.raises(ValidationError) as exc_info:
        load_config(bad_config_file)
    assert "canonical_timezone must strictly be 'UTC'" in str(exc_info.value)


def test_rates_out_of_bounds_raises_validation_error(tmp_path):
    """Verify rates > 1.0 trigger Pydantic validation error."""
    bad_rates_yaml = """
project:
  name: "bad-rates-proj"
  canonical_timezone: "UTC"
  presentation_timezone: "Asia/Ho_Chi_Minh"
streaming_slo:
  freshness_p95_seconds: 30
partner_degradation:
  short_window_seconds: 60
data_quality:
  circuit_breaker_error_rate: 0.05
reconciliation_eod:
  cutoff_time_local: "23:59:59"
simulator:
  default_seed: 20260930
  rates:
    duplicate_event_rate: 1.5
"""
    bad_rates_file = tmp_path / "bad_rates.yaml"
    bad_rates_file.write_text(bad_rates_yaml, encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(bad_rates_file)


def test_logger_structured_output(tmp_path):
    """Verify logger writes parseable JSON records to disk."""
    log_file = tmp_path / "test_sim.log"
    test_logger = setup_logger(log_file_path=str(log_file), level="INFO", json_stdout=False)

    test_logger.info("Simulator initialization started", extra={"event_code": "INIT_001"})
    test_logger.complete()  # Ensure buffered logs flush

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8").strip()
    assert len(content) > 0

    first_line = content.splitlines()[0]
    record = json.loads(first_line)
    assert record["level"] == "INFO"
    assert record["message"] == "Simulator initialization started"
    assert "timestamp" in record
    assert record["extra"]["event_code"] == "INIT_001"

