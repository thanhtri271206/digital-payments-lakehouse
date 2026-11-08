"""Centralized Configuration Engine with Pydantic v2 validation.

Loads and enforces strict typing and range invariants on config/config.yaml.
"""
from pathlib import Path
from typing import Optional, Union
import yaml
from pydantic import BaseModel, Field, field_validator


class ProjectSettings(BaseModel):
    name: str = Field(..., description="Project system name")
    canonical_timezone: str = Field(default="UTC", description="Internal canonical storage timezone")
    presentation_timezone: str = Field(default="Asia/Ho_Chi_Minh", description="Local business timezone")

    @field_validator("canonical_timezone")
    @classmethod
    def validate_utc(cls, v: str) -> str:
        if v.upper() != "UTC":
            raise ValueError(f"canonical_timezone must strictly be 'UTC', got '{v}'")
        return v


class StreamingSLOSettings(BaseModel):
    freshness_p95_seconds: int = Field(default=30, gt=0)
    freshness_p99_seconds: int = Field(default=60, gt=0)
    baseline_events_per_sec: int = Field(default=5, gt=0)
    burst_events_per_sec: int = Field(default=50, gt=0)
    watermark_lateness_seconds: int = Field(default=120, gt=0)
    stuck_timeout_seconds: int = Field(default=300, gt=0)


class PartnerDegradationSettings(BaseModel):
    short_window_seconds: int = Field(default=60, gt=0)
    long_window_seconds: int = Field(default=300, gt=0)
    min_volume_short: int = Field(default=20, gt=0)
    min_volume_long: int = Field(default=100, gt=0)
    alert_success_rate_threshold: float = Field(default=0.90, ge=0.0, le=1.0)
    alert_baseline_drop_threshold: float = Field(default=0.08, ge=0.0, le=1.0)
    recovery_success_rate_threshold: float = Field(default=0.95, ge=0.0, le=1.0)
    recovery_consecutive_windows: int = Field(default=2, gt=0)


class DataQualitySettings(BaseModel):
    circuit_breaker_error_rate: float = Field(default=0.05, ge=0.0, le=1.0)
    circuit_breaker_min_batch_size: int = Field(default=50, gt=0)


class ReconciliationEODSettings(BaseModel):
    cutoff_time_local: str = Field(default="23:59:59")
    initial_run_local: str = Field(default="01:00:00")
    grace_period_deadline_local: str = Field(default="06:00:00")


class SimulatorRatesSettings(BaseModel):
    duplicate_event_rate: float = Field(default=0.005, ge=0.0, le=1.0)
    late_event_short_rate: float = Field(default=0.010, ge=0.0, le=1.0)
    late_event_long_rate: float = Field(default=0.002, ge=0.0, le=1.0)
    out_of_order_rate: float = Field(default=0.005, ge=0.0, le=1.0)
    missing_terminal_rate: float = Field(default=0.003, ge=0.0, le=1.0)
    poison_record_rate: float = Field(default=0.002, ge=0.0, le=1.0)
    terminal_conflict_rate: float = Field(default=0.001, ge=0.0, le=1.0)


class SimulatorSettings(BaseModel):
    default_seed: int = Field(default=20260930)
    scenario: str = Field(default="normal")
    rates: SimulatorRatesSettings = Field(default_factory=SimulatorRatesSettings)


class AppConfig(BaseModel):
    project: ProjectSettings
    streaming_slo: StreamingSLOSettings
    partner_degradation: PartnerDegradationSettings
    data_quality: DataQualitySettings
    reconciliation_eod: ReconciliationEODSettings
    simulator: SimulatorSettings


_GLOBAL_CONFIG: Optional[AppConfig] = None


def load_config(config_path: Optional[Union[str, Path]] = None) -> AppConfig:
    """Load, parse, and validate application configuration from YAML file.

    Raises:
        FileNotFoundError: If the specified config file does not exist.
        ValueError: If YAML syntax is invalid.
        pydantic.ValidationError: If schema types or invariants fail.
    """
    global _GLOBAL_CONFIG
    if config_path is None:
        # Default path lookup relative to repo root or cwd
        default_candidates = [
            Path("config/config.yaml"),
            Path(__file__).resolve().parents[3] / "config" / "config.yaml",
        ]
        chosen_path: Optional[Path] = None
        for cand in default_candidates:
            if cand.exists():
                chosen_path = cand
                break
        if chosen_path is None:
            raise FileNotFoundError("config/config.yaml could not be found.")
        target_path = chosen_path
    else:
        target_path = Path(config_path)
        if not target_path.exists():
            raise FileNotFoundError(f"Config file not found at: {target_path}")

    with open(target_path, "r", encoding="utf-8") as f:
        raw_dict = yaml.safe_load(f)

    if not isinstance(raw_dict, dict):
        raise ValueError(f"Invalid YAML content in {target_path}; expected a mapping/dict.")

    config = AppConfig.model_validate(raw_dict)
    _GLOBAL_CONFIG = config
    return config


def get_config() -> AppConfig:
    """Get the cached global configuration, or load default if not yet loaded."""
    global _GLOBAL_CONFIG
    if _GLOBAL_CONFIG is None:
        _GLOBAL_CONFIG = load_config()
    return _GLOBAL_CONFIG

