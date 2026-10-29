"""Transactional processing of one Kafka batch into Bronze, Silver, quarantine and the work queue.

All writes for a batch happen in ONE database transaction; each record runs inside its own
savepoint so a single bad record cannot poison the batch. The consumer commits Kafka offsets only
after this transaction commits. If the process dies between the database commit and the offset
commit, the redelivered records hit the unique (topic, partition, offset) constraint in Bronze and
are skipped, which makes redelivery idempotent (effectively-once into PostgreSQL; the Kafka
delivery itself is at-least-once).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from common.config import TopicSettings
from common.errors import ErrorCategory
from database import repositories as repo
from database.repositories import KafkaRecord
from ingestion.error_handler import classify_exception, is_transient
from processing.deduplication import DedupOutcome, KeyType, register_key
from processing.normalization import normalize, sha256_hex
from processing.rules import ReconciliationRules
from processing.validation import (
    DecodeError,
    EventMetadata,
    ValidationFailure,
    decode_payload,
    extract_metadata,
    validate_event,
)

log = logging.getLogger(__name__)

PROCESSED = "PROCESSED"
DUPLICATE_DELIVERY = "DUPLICATE_DELIVERY"
DUPLICATE_BUSINESS_EVENT = "DUPLICATE_BUSINESS_EVENT"
QUARANTINED = "QUARANTINED"
ALREADY_INGESTED = "ALREADY_INGESTED"


@dataclass(frozen=True)
class RecordResult:
    record: KafkaRecord
    source_system: str
    outcome: str
    payment_id: str | None = None
    source_event_id: str | None = None
    event_time: datetime | None = None
    error_category: str | None = None
    error_code: str | None = None
    error_detail: str | None = None


class _Quarantine(Exception):
    def __init__(self, failure: ValidationFailure) -> None:
        super().__init__(failure.error_code)
        self.failure = failure


@dataclass
class _Decoded:
    payload: dict[str, Any] | None
    text: str | None
    meta: EventMetadata
    payload_hash: str


class EventWriter:
    def __init__(
        self,
        topics: TopicSettings,
        rules: ReconciliationRules,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.source_by_topic = topics.source_topics
        self.rules = rules
        self.clock = clock

    def source_for(self, topic: str) -> str:
        return self.source_by_topic.get(topic, "unknown")

    # ------------------------------------------------------------------ batch

    def process_batch(self, conn: psycopg.Connection, records: Sequence[KafkaRecord]) -> list[RecordResult]:
        """Process records inside the caller's open transaction. Transient errors propagate."""
        results: list[RecordResult] = []
        payments: set[str] = set()
        with conn.cursor() as cur:
            for record in records:
                result = self._process_with_savepoint(conn, cur, record)
                results.append(result)
                if result.outcome == PROCESSED and result.payment_id:
                    payments.add(result.payment_id)
            repo.enqueue_payments(cur, payments, "STREAM")
        return results

    def _process_with_savepoint(self, conn: psycopg.Connection, cur: psycopg.Cursor, record: KafkaRecord) -> RecordResult:
        decoded = self._decode(record)
        try:
            with conn.transaction():
                return self._process_record(cur, record, decoded)
        except _Quarantine as signal:
            with conn.transaction():
                return self._quarantine(cur, record, decoded, signal.failure)
        except Exception as exc:
            if is_transient(exc):
                raise
            category = classify_exception(exc)
            log.exception(
                "record processing failed; quarantining",
                extra={"topic": record.topic, "partition": record.partition, "offset": record.offset,
                       "error_code": type(exc).__name__, "processing_stage": "ingestion"},
            )
            failure = ValidationFailure(
                category if category is not ErrorCategory.TRANSIENT else ErrorCategory.PROCESSING,
                f"{category.value}_ERROR",
                f"{type(exc).__name__}: {str(exc)[:500]}",
            )
            with conn.transaction():
                return self._quarantine(cur, record, decoded, failure)

    # ------------------------------------------------------------------ record

    def _decode(self, record: KafkaRecord) -> _Decoded:
        payload_hash = sha256_hex(record.value or b"")
        try:
            payload = decode_payload(record.value)
        except DecodeError:
            return _Decoded(None, None, EventMetadata(), payload_hash)
        text = (record.value or b"").decode("utf-8")
        # PostgreSQL JSONB cannot store NUL characters; such payloads keep only the raw bytes.
        return _Decoded(payload, None if "\\u0000" in text else text, extract_metadata(payload), payload_hash)

    def _process_record(self, cur: psycopg.Cursor, record: KafkaRecord, decoded: _Decoded) -> RecordResult:
        source_system = self.source_for(record.topic)
        if source_system == "unknown":
            raise _Quarantine(
                ValidationFailure(ErrorCategory.VALIDATION, "UNKNOWN_SOURCE_TOPIC", f"topic {record.topic} is not a source topic")
            )
        if decoded.payload is None:
            try:
                decode_payload(record.value)
            except DecodeError as exc:
                raise _Quarantine(ValidationFailure(ErrorCategory.SERIALIZATION, exc.error_code, exc.detail)) from exc

        validated = validate_event(decoded.payload or {}, source_system, self.clock(), self.rules.timestamp_rules)
        if isinstance(validated, ValidationFailure):
            raise _Quarantine(validated)

        raw_id = repo.insert_raw_event(
            cur, record, source_system, decoded.meta, decoded.text, decoded.payload_hash, PROCESSED
        )
        if raw_id is None:
            return RecordResult(record, source_system, ALREADY_INGESTED, decoded.meta.payment_id)

        norm = normalize(validated)

        def result(outcome: str) -> RecordResult:
            return RecordResult(record, source_system, outcome, norm.payment_id, norm.source_event_id, norm.event_time)

        outcome = register_key(cur, source_system, KeyType.EVENT_ID, norm.source_event_id, norm.content_hash, raw_id)
        if outcome is DedupOutcome.DUPLICATE:
            repo.update_raw_status(cur, raw_id, DUPLICATE_DELIVERY)
            return result(DUPLICATE_DELIVERY)
        if outcome is DedupOutcome.CONFLICT:
            raise _Quarantine(
                ValidationFailure(
                    ErrorCategory.DUPLICATE_CONFLICT,
                    "EVENT_ID_CONFLICT",
                    "event_id was already received with different content",
                )
            )

        if norm.business_key and norm.business_hash:
            outcome = register_key(
                cur, source_system, KeyType.BUSINESS_KEY, norm.business_key, norm.business_hash, raw_id
            )
            if outcome is DedupOutcome.DUPLICATE:
                repo.update_raw_status(cur, raw_id, DUPLICATE_BUSINESS_EVENT)
                return result(DUPLICATE_BUSINESS_EVENT)
            if outcome is DedupOutcome.CONFLICT:
                raise _Quarantine(
                    ValidationFailure(
                        ErrorCategory.DUPLICATE_CONFLICT,
                        "BUSINESS_KEY_CONFLICT",
                        f"transaction id {norm.business_key} was already received with different financial content",
                    )
                )

        for row in norm.rows:
            repo.insert_silver_row(cur, row, raw_id)
        if norm.payment_request is not None:
            repo.upsert_payment_request(cur, norm.payment_request)
        return result(PROCESSED)

    def _quarantine(
        self, cur: psycopg.Cursor, record: KafkaRecord, decoded: _Decoded, failure: ValidationFailure
    ) -> RecordResult:
        source_system = self.source_for(record.topic)
        safe_text = decoded.text if failure.category not in (ErrorCategory.DATABASE, ErrorCategory.PROCESSING) else None
        raw_id = repo.insert_raw_event(
            cur,
            record,
            source_system,
            decoded.meta,
            safe_text,
            decoded.payload_hash,
            QUARANTINED,
            failure.error_code,
            failure.detail[:2000],
        )
        if raw_id is None:
            return RecordResult(record, source_system, ALREADY_INGESTED, decoded.meta.payment_id)
        repo.insert_quarantine(
            cur, raw_id, record, source_system, decoded.meta, failure.category.value, failure.error_code, failure.detail
        )
        return RecordResult(
            record,
            source_system,
            QUARANTINED,
            payment_id=decoded.meta.payment_id,
            source_event_id=decoded.meta.source_event_id,
            event_time=decoded.meta.event_time,
            error_category=failure.category.value,
            error_code=failure.error_code,
            error_detail=failure.detail,
        )
