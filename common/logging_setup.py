"""Structured JSON logging with redaction of sensitive values."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

SENSITIVE_KEYS = re.compile(
    r"(pass(word)?|secret|token|api[_-]?key|authorization|card[_-]?number|pan|cvv|cvc|iban|sasl)", re.I
)
# Long digit runs that look like card numbers (13-19 digits) are masked wherever they appear.
CARD_LIKE = re.compile(r"\b\d(?:[ -]?\d){12,18}\b")
ACCOUNT_KEYS = {"account_ref"}

_STANDARD_ATTRS = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


def redact_value(key: str, value: Any) -> Any:
    if SENSITIVE_KEYS.search(key):
        return "***REDACTED***"
    if key in ACCOUNT_KEYS and isinstance(value, str) and len(value) > 4:
        return "***" + value[-4:]
    if isinstance(value, str):
        return CARD_LIKE.sub("***REDACTED-NUMBER***", value)
    if isinstance(value, dict):
        return {k: redact_value(str(k), v) for k, v in value.items()}
    return value


class JsonFormatter(logging.Formatter):
    def __init__(self, service_name: str, environment: str) -> None:
        super().__init__()
        self.service_name = service_name
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "service_name": self.service_name,
            "environment": self.environment,
            "message": CARD_LIKE.sub("***REDACTED-NUMBER***", record.getMessage()),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                entry[key] = redact_value(key, value)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(service_name: str, environment: str = "local", level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name, environment))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("kafka").setLevel("WARNING")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
