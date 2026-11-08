"""Structured JSON Logging Utility using Loguru."""
import json
import sys
from pathlib import Path
from typing import Any
from loguru import logger


def _serialize_record(record: dict) -> None:
    """Serialize log record into custom JSON payload in extra['serialized']."""
    extra_fields = {}
    for k, v in record["extra"].items():
        if k == "serialized":
            continue
        if k == "extra" and isinstance(v, dict):
            extra_fields.update(v)
        else:
            extra_fields[k] = v

    payload = {
        "timestamp": record["time"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "level": record["level"].name,
        "message": record["message"],
        "module": record["name"],
        "function": record["function"],
        "line": record["line"],
        "extra": extra_fields,
    }
    record["extra"]["serialized"] = json.dumps(payload, ensure_ascii=False) + "\n"


_IS_CONFIGURED = False


def setup_logger(
    log_file_path: str = "logs/simulator.log",
    level: str = "INFO",
    json_stdout: bool = True,
) -> Any:
    """Configure Loguru to emit structured logs to stdout and a rotating file."""
    global _IS_CONFIGURED
    logger.remove()

    # Create logs directory if not exists
    file_path = Path(log_file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Patch logger with JSON serialization helper
    configured_logger = logger.patch(_serialize_record)

    if json_stdout:
        configured_logger.add(
            sys.stdout,
            level=level,
            format="{extra[serialized]}",
        )
    else:
        configured_logger.add(
            sys.stdout,
            level=level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        )

    # Rotating file sink with structured JSON
    configured_logger.add(
        str(file_path),
        level=level,
        format="{extra[serialized]}",
        rotation="10 MB",
        retention="7 days",
        enqueue=False,
        encoding="utf-8",
    )

    _IS_CONFIGURED = True
    return configured_logger


def get_logger() -> Any:
    """Retrieve logger instance, configuring with defaults if uninitialized."""
    global _IS_CONFIGURED
    if not _IS_CONFIGURED:
        return setup_logger()
    return logger.patch(_serialize_record)
