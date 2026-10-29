"""Envelope decoding and schema validation of source events.

Malformed data (this module) is kept separate from legitimate business exceptions such as a
declined payment or a missing billing posting, which are handled by the reconciliation engine.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from common.errors import ErrorCategory
from processing.models import MODEL_BY_SOURCE, SUPPORTED_SCHEMA_VERSIONS, SourceEvent


@dataclass(frozen=True)
class ValidationFailure:
    category: ErrorCategory
    error_code: str
    detail: str


@dataclass(frozen=True)
class TimestampRules:
    max_future_skew_s: int = 300
    max_age_days: int = 400


@dataclass(frozen=True)
class EventMetadata:
    """Best-effort fields extracted from any payload, valid or not. Never trusted for business logic."""

    source_event_id: str | None = None
    event_type: str | None = None
    payment_id: str | None = None
    event_time: datetime | None = None
    schema_version: str | None = None
    source_system: str | None = None
    correlation_id: str | None = None


class DecodeError(ValueError):
    def __init__(self, error_code: str, detail: str) -> None:
        super().__init__(detail)
        self.error_code = error_code
        self.detail = detail


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value}")


def decode_payload(value: bytes | None) -> dict[str, Any]:
    """Decode Kafka message bytes into a dict; JSON numbers are parsed as Decimal, never float."""
    if value is None or len(value) == 0:
        raise DecodeError("EMPTY_PAYLOAD", "message value is empty")
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DecodeError("INVALID_ENCODING", f"payload is not valid UTF-8: {exc.reason}") from exc
    try:
        payload = json.loads(text, parse_float=Decimal, parse_constant=_reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise DecodeError("INVALID_JSON", f"payload is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise DecodeError("NOT_AN_OBJECT", "payload must be a JSON object")
    return payload


def _short_str(value: Any, limit: int = 64) -> str | None:
    if isinstance(value, str) and 0 < len(value) <= limit:
        return value
    return None


def extract_metadata(payload: dict[str, Any] | None) -> EventMetadata:
    if not payload:
        return EventMetadata()
    event_time = None
    raw_time = payload.get("event_time")
    if isinstance(raw_time, str):
        try:
            parsed = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
            event_time = parsed if parsed.tzinfo is not None else None
        except ValueError:
            event_time = None
    return EventMetadata(
        source_event_id=_short_str(payload.get("event_id")),
        event_type=_short_str(payload.get("event_type"), 40),
        payment_id=_short_str(payload.get("payment_id")),
        event_time=event_time,
        schema_version=_short_str(payload.get("schema_version"), 16),
        source_system=_short_str(payload.get("source_system"), 40),
        correlation_id=_short_str(payload.get("correlation_id")),
    )


def _code_for(error: Any) -> str:
    loc = error.get("loc") or ()
    field = str(loc[0]) if loc else ""
    etype = error.get("type", "")
    if etype == "missing":
        return "MISSING_REQUIRED_FIELD"
    if etype == "extra_forbidden":
        return "UNKNOWN_FIELD"
    if field == "currency":
        return "INVALID_CURRENCY"
    if field == "amount":
        return "INVALID_AMOUNT"
    if field == "event_type":
        return "INVALID_EVENT_TYPE"
    if field == "event_time":
        return "INVALID_TIMESTAMP"
    if field in ("source_status", "processor_status", "posting_status", "direction"):
        return "INVALID_STATUS"
    if not field:
        return "BUSINESS_RULE_VIOLATION"
    return "INVALID_FIELD_VALUE"


def _describe(exc: ValidationError) -> str:
    # Only field locations and messages are recorded; input values are omitted to avoid leaking data.
    parts = []
    for err in exc.errors(include_input=False, include_url=False):
        loc = ".".join(str(p) for p in err.get("loc", ())) or "<event>"
        parts.append(f"{loc}: {err.get('msg')}")
    return "; ".join(parts)[:2000]


def validate_event(
    payload: dict[str, Any], expected_source: str, now: datetime, rules: TimestampRules
) -> SourceEvent | ValidationFailure:
    version = payload.get("schema_version")
    if version is None:
        return ValidationFailure(ErrorCategory.VALIDATION, "MISSING_REQUIRED_FIELD", "schema_version: Field required")
    if not isinstance(version, str) or version not in SUPPORTED_SCHEMA_VERSIONS:
        return ValidationFailure(
            ErrorCategory.UNSUPPORTED_SCHEMA,
            "UNSUPPORTED_SCHEMA_VERSION",
            f"schema_version {str(version)[:16]!r} is not supported; supported={sorted(SUPPORTED_SCHEMA_VERSIONS)}",
        )
    declared = payload.get("source_system")
    if declared is not None and declared != expected_source:
        return ValidationFailure(
            ErrorCategory.VALIDATION,
            "SOURCE_TOPIC_MISMATCH",
            f"source_system {str(declared)[:40]!r} does not belong on the topic for {expected_source}",
        )
    model = MODEL_BY_SOURCE[expected_source]
    try:
        event = model.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        return ValidationFailure(ErrorCategory.VALIDATION, _code_for(first), _describe(exc))

    if event.event_time > now + timedelta(seconds=rules.max_future_skew_s):
        return ValidationFailure(
            ErrorCategory.VALIDATION, "TIMESTAMP_IN_FUTURE", "event_time is beyond the allowed clock skew"
        )
    if event.event_time < now - timedelta(days=rules.max_age_days):
        return ValidationFailure(ErrorCategory.VALIDATION, "TIMESTAMP_TOO_OLD", "event_time exceeds the maximum age")
    return event
