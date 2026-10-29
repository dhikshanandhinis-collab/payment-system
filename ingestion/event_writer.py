"""Transactional processing of one Kafka batch into Bronze, Silver, quarantine and the work queue.

All writes for a batch happen in ONE database transaction. The consumer commits Kafka offsets only
after this transaction commits. If the process dies between the database commit and the offset
commit, the redelivered records hit the unique (topic, partition, offset) constraint in Bronze and
are skipped, which makes redelivery idempotent (effectively-once into PostgreSQL; the Kafka
delivery itself is at-least-once).

Two write paths produce identical results:
  * bulk (default): records are decoded, validated and deduplicated in memory, then written with a
    handful of pipelined statements per batch inside one savepoint.
  * per-record: each record runs inside its own savepoint so a single bad record cannot poison the
    batch. Used when bulk is disabled, or as a fallback when the bulk savepoint fails for any
    non-transient reason (including a concurrent consumer registering the same idempotency key).
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
from observability import metrics
from processing.deduplication import DedupOutcome, KeyType, register_key
from processing.models import SourceEvent
from processing.normalization import NormalizedEvent, normalize, sha256_hex
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


_EVENT_ID_CONFLICT = ValidationFailure(
    ErrorCategory.DUPLICATE_CONFLICT, "EVENT_ID_CONFLICT", "event_id was already received with different content"
)


def _business_key_conflict(business_key: str) -> ValidationFailure:
    return ValidationFailure(
        ErrorCategory.DUPLICATE_CONFLICT,
        "BUSINESS_KEY_CONFLICT",
        f"transaction id {business_key} was already received with different financial content",
    )


@dataclass
class _Decoded:
    payload: dict[str, Any] | None
    text: str | None
    meta: EventMetadata
    payload_hash: str


class _BulkAbort(Exception):
    """The bulk savepoint must be rolled back and the batch re-run on the per-record path."""


@dataclass
class _BulkItem:
    record: KafkaRecord
    decoded: _Decoded
    source_system: str
    norm: NormalizedEvent | None = None
    failure: ValidationFailure | None = None
    raw_id: int | None = None
    outcome: str = PROCESSED


class EventWriter:
    def __init__(
        self,
        topics: TopicSettings,
        rules: ReconciliationRules,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        bulk: bool = True,
    ) -> None:
        self.source_by_topic = topics.source_topics
        self.rules = rules
        self.clock = clock
        self.bulk = bulk

    def source_for(self, topic: str) -> str:
        return self.source_by_topic.get(topic, "unknown")

    # ------------------------------------------------------------------ batch

    def process_batch(self, conn: psycopg.Connection, records: Sequence[KafkaRecord]) -> list[RecordResult]:
        """Process records inside the caller's open transaction. Transient errors propagate."""
        if self.bulk and records:
            try:
                with conn.transaction():
                    return self._process_bulk(conn, records)
            except _BulkAbort as exc:
                metrics.BULK_FALLBACKS.labels(str(exc)).inc()
                log.info("bulk write aborted; re-running batch per record", extra={"outcome": str(exc)})
            except Exception as exc:
                if is_transient(exc):
                    raise
                metrics.BULK_FALLBACKS.labels("error").inc()
                log.warning("bulk write failed; re-running batch per record",
                            extra={"error_code": type(exc).__name__, "processing_stage": "ingestion"})
        return self._process_per_record(conn, records)

    def _process_per_record(self, conn: psycopg.Connection, records: Sequence[KafkaRecord]) -> list[RecordResult]:
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

    def _validate(self, record: KafkaRecord, decoded: _Decoded) -> tuple[str, SourceEvent]:
        """Checks that need no database access; raises _Quarantine before anything is written."""
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
        return source_system, validated

    # ------------------------------------------------------------------ bulk path

    def _process_bulk(self, conn: psycopg.Connection, records: Sequence[KafkaRecord]) -> list[RecordResult]:
        items: list[_BulkItem] = []
        for record in records:
            decoded = self._decode(record)
            item = _BulkItem(record, decoded, self.source_for(record.topic))
            try:
                _, event = self._validate(record, decoded)
                item.norm = normalize(event)
            except _Quarantine as signal:
                item.failure = signal.failure
                item.outcome = QUARANTINED
            items.append(item)

        with conn.cursor() as cur:
            # 1. Bronze: every record, valid or not, in one pipelined statement.
            raw_params = []
            for it in items:
                if it.failure is None:
                    raw_params.append(repo.raw_event_params(
                        it.record, it.source_system, it.decoded.meta, it.decoded.text, it.decoded.payload_hash, PROCESSED))
                else:
                    raw_params.append(repo.raw_event_params(
                        it.record, it.source_system, it.decoded.meta, self._safe_text(it.decoded, it.failure),
                        it.decoded.payload_hash, QUARANTINED, it.failure.error_code, it.failure.detail[:2000]))
            for it, raw_id in zip(items, repo.insert_raw_events(cur, raw_params), strict=True):
                it.raw_id = raw_id
                if raw_id is None:
                    it.outcome = ALREADY_INGESTED

            # 2. Idempotency keys, resolved in record order exactly like the per-record path.
            fresh = [it for it in items if it.outcome == PROCESSED]
            wanted: set[repo.DedupKey] = set()
            for it in fresh:
                assert it.norm is not None
                wanted.add((it.source_system, KeyType.EVENT_ID.value, it.norm.source_event_id))
                if it.norm.business_key and it.norm.business_hash:
                    wanted.add((it.source_system, KeyType.BUSINESS_KEY.value, it.norm.business_key))
            known = repo.fetch_dedup_hashes(cur, wanted)
            new_keys: list[tuple[str, str, str, str, int]] = []
            for it in fresh:
                self._resolve_keys(it, known, new_keys)
            if repo.insert_dedup_keys(cur, new_keys) != len(new_keys):
                raise _BulkAbort("dedup_race")

            # 3. Status changes, quarantine rows, Silver rows, payment requests, work queue.
            repo.update_raw_statuses(cur, [
                (it.outcome, None, None, it.raw_id) for it in fresh
                if it.outcome in (DUPLICATE_DELIVERY, DUPLICATE_BUSINESS_EVENT) and it.raw_id is not None
            ] + [
                (QUARANTINED, it.failure.error_code, it.failure.detail[:2000], it.raw_id) for it in fresh
                if it.outcome == QUARANTINED and it.failure is not None and it.raw_id is not None
            ])
            repo.insert_quarantines(cur, [
                repo.quarantine_params(it.raw_id, it.record, it.source_system, it.decoded.meta,
                                       it.failure.category.value, it.failure.error_code, it.failure.detail)
                for it in items if it.outcome == QUARANTINED and it.failure is not None
            ])
            processed = [it for it in items if it.outcome == PROCESSED and it.norm is not None and it.raw_id is not None]
            repo.insert_silver_rows(cur, [(row, it.raw_id) for it in processed if it.norm and it.raw_id
                                          for row in it.norm.rows])
            repo.upsert_payment_requests(cur, [it.norm.payment_request for it in processed
                                               if it.norm and it.norm.payment_request is not None])
            repo.enqueue_payments(cur, {it.norm.payment_id for it in processed if it.norm and it.norm.payment_id},
                                  "STREAM")
        return [self._bulk_result(it) for it in items]

    def _resolve_keys(
        self, it: _BulkItem, known: dict[repo.DedupKey, str], new_keys: list[tuple[str, str, str, str, int]]
    ) -> None:
        norm, raw_id = it.norm, it.raw_id
        assert norm is not None and raw_id is not None
        event_key = (it.source_system, KeyType.EVENT_ID.value, norm.source_event_id)
        if event_key in known:
            if known[event_key] == norm.content_hash:
                it.outcome = DUPLICATE_DELIVERY
            else:
                it.outcome, it.failure = QUARANTINED, _EVENT_ID_CONFLICT
            return
        if norm.business_key and norm.business_hash:
            business_key = (it.source_system, KeyType.BUSINESS_KEY.value, norm.business_key)
            if business_key in known and known[business_key] != norm.business_hash:
                # The per-record path rolls back the event-id key too, so it is not registered here.
                it.outcome, it.failure = QUARANTINED, _business_key_conflict(norm.business_key)
                return
            if business_key in known:
                it.outcome = DUPLICATE_BUSINESS_EVENT
            else:
                known[business_key] = norm.business_hash
                new_keys.append((*business_key, norm.business_hash, raw_id))
        known[event_key] = norm.content_hash
        new_keys.append((*event_key, norm.content_hash, raw_id))

    @staticmethod
    def _safe_text(decoded: _Decoded, failure: ValidationFailure) -> str | None:
        return decoded.text if failure.category not in (ErrorCategory.DATABASE, ErrorCategory.PROCESSING) else None

    @staticmethod
    def _bulk_result(it: _BulkItem) -> RecordResult:
        meta = it.decoded.meta
        if it.outcome == ALREADY_INGESTED:
            return RecordResult(it.record, it.source_system, ALREADY_INGESTED, meta.payment_id)
        if it.outcome == QUARANTINED and it.failure is not None:
            return RecordResult(
                it.record, it.source_system, QUARANTINED, payment_id=meta.payment_id,
                source_event_id=meta.source_event_id, event_time=meta.event_time,
                error_category=it.failure.category.value, error_code=it.failure.error_code,
                error_detail=it.failure.detail,
            )
        assert it.norm is not None
        return RecordResult(it.record, it.source_system, it.outcome, it.norm.payment_id, it.norm.source_event_id,
                            it.norm.event_time)

    def _process_record(self, cur: psycopg.Cursor, record: KafkaRecord, decoded: _Decoded) -> RecordResult:
        source_system, validated = self._validate(record, decoded)

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
            raise _Quarantine(_EVENT_ID_CONFLICT)

        if norm.business_key and norm.business_hash:
            outcome = register_key(
                cur, source_system, KeyType.BUSINESS_KEY, norm.business_key, norm.business_hash, raw_id
            )
            if outcome is DedupOutcome.DUPLICATE:
                repo.update_raw_status(cur, raw_id, DUPLICATE_BUSINESS_EVENT)
                return result(DUPLICATE_BUSINESS_EVENT)
            if outcome is DedupOutcome.CONFLICT:
                raise _Quarantine(_business_key_conflict(norm.business_key))

        for row in norm.rows:
            repo.insert_silver_row(cur, row, raw_id)
        if norm.payment_request is not None:
            repo.upsert_payment_request(cur, norm.payment_request)
        return result(PROCESSED)

    def _quarantine(
        self, cur: psycopg.Cursor, record: KafkaRecord, decoded: _Decoded, failure: ValidationFailure
    ) -> RecordResult:
        source_system = self.source_for(record.topic)
        safe_text = self._safe_text(decoded, failure)
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
