"""Error taxonomy shared by ingestion, validation and reconciliation."""

from __future__ import annotations

from enum import StrEnum


class ErrorCategory(StrEnum):
    TRANSIENT = "TRANSIENT"
    VALIDATION = "VALIDATION"
    DATABASE = "DATABASE"
    SERIALIZATION = "SERIALIZATION"
    UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
    PROCESSING = "PROCESSING"


class TransientError(RuntimeError):
    """A failure that is expected to succeed on retry (e.g. database or broker unavailable)."""


class RetryExhaustedError(RuntimeError):
    """Bounded retries for a transient failure were exhausted."""
