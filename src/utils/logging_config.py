"""Structured JSON logging for pipeline-wide auditability."""

import logging
import sys
from datetime import UTC, datetime


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line for SIEM compatibility."""

    def format(self, record: logging.LogRecord) -> str:
        import json

        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "pipeline_stage"):
            payload["pipeline_stage"] = record.pipeline_stage
        return json.dumps(payload)


def get_logger(name: str, stage: str | None = None) -> logging.Logger:
    """Return a logger that emits structured JSON to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    if stage:
        logger = logging.LoggerAdapter(logger, {"pipeline_stage": stage})  # type: ignore[assignment]
    return logger
