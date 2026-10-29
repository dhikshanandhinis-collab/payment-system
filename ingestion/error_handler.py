"""Error classification, bounded retries and dead-letter publishing."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

import psycopg
from psycopg import errors as pg_errors
from psycopg_pool import PoolTimeout
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from common.config import RetrySettings
from common.errors import ErrorCategory, TransientError
from database.repositories import KafkaRecord
from observability import metrics
from processing.validation import DecodeError

log = logging.getLogger(__name__)

TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TransientError,
    PoolTimeout,
    psycopg.OperationalError,  # includes connection loss and admin shutdown
    psycopg.InterfaceError,
    pg_errors.SerializationFailure,
    pg_errors.DeadlockDetected,
    pg_errors.LockNotAvailable,
    ConnectionError,
    TimeoutError,
)


def is_transient(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_EXCEPTIONS)


def classify_exception(exc: BaseException) -> ErrorCategory:
    if is_transient(exc):
        return ErrorCategory.TRANSIENT
    if isinstance(exc, psycopg.Error):
        return ErrorCategory.DATABASE
    if isinstance(exc, DecodeError | UnicodeError | json.JSONDecodeError):
        return ErrorCategory.SERIALIZATION
    return ErrorCategory.PROCESSING


def run_with_retry[T](component: str, settings: RetrySettings, fn: Callable[[], T]) -> T:
    """Run ``fn`` with bounded exponential backoff + jitter for transient failures only."""

    def before_sleep(state: RetryCallState) -> None:
        metrics.RETRIES.labels(component).inc()
        exc = state.outcome.exception() if state.outcome else None
        log.warning(
            "transient failure; retrying",
            extra={
                "processing_stage": component,
                "attempt": state.attempt_number,
                "error_code": type(exc).__name__ if exc else None,
                "outcome": "retry",
            },
        )

    retrying = Retrying(
        retry=retry_if_exception(is_transient),
        stop=stop_after_attempt(settings.max_attempts),
        wait=wait_exponential(multiplier=settings.initial_backoff_s, max=settings.max_backoff_s)
        + wait_random(0, settings.jitter_s),
        before_sleep=before_sleep,
        reraise=True,
    )
    return retrying(fn)


class DeadLetterPublisher:
    """Publishes the *original* message bytes plus error headers to the DLQ topic and waits for the ack."""

    def __init__(self, producer: Any, topic: str, timeout_s: float = 30.0) -> None:
        self.producer = producer
        self.topic = topic
        self.timeout_s = timeout_s

    def publish(
        self, record: KafkaRecord, source_system: str, category: str, error_code: str, detail: str
    ) -> None:
        errors: list[Exception] = []
        delivered: list[bool] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(RuntimeError(str(err)))
            else:
                delivered.append(True)

        headers = [
            ("x-original-topic", record.topic.encode()),
            ("x-original-partition", str(record.partition).encode()),
            ("x-original-offset", str(record.offset).encode()),
            ("x-source-system", source_system.encode()),
            ("x-error-category", category.encode()),
            ("x-error-code", error_code.encode()),
            ("x-error-detail", detail[:1000].encode("utf-8", "replace")),
        ]
        self.producer.produce(
            self.topic, key=record.key, value=record.value, headers=headers, on_delivery=on_delivery
        )
        remaining = self.producer.flush(self.timeout_s)
        if errors or remaining > 0 or not delivered:
            metrics.DLQ_PUBLISHED.labels("failed").inc()
            raise TransientError(f"dead-letter publish not acknowledged: {errors[0] if errors else 'timeout'}")
        metrics.DLQ_PUBLISHED.labels("delivered").inc()
