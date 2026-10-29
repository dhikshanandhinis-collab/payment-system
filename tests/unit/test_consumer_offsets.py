"""Consumer offset handling and retry behaviour with in-memory fakes (no Kafka/PostgreSQL needed)."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime

import psycopg
import pytest

from common.config import IngestionSettings, KafkaSettings, RetrySettings, TopicSettings
from common.errors import ErrorCategory, TransientError
from database.repositories import KafkaRecord
from ingestion.consumer import IngestionService
from ingestion.error_handler import DeadLetterPublisher, classify_exception, is_transient, run_with_retry
from ingestion.event_writer import RecordResult

FAST_RETRY = RetrySettings(max_attempts=3, initial_backoff_s=0.001, max_backoff_s=0.002, jitter_s=0)


class FakeConsumer:
    def __init__(self, conf):
        self.conf = conf
        self.commits = []
        self.seeks = []
        self.closed = False

    def commit(self, offsets, asynchronous):
        assert asynchronous is True  # a sync commit can block forever without a coordinator
        self.commits.append([(tp.topic, tp.partition, tp.offset) for tp in offsets])

    def seek(self, tp):
        self.seeks.append((tp.topic, tp.partition, tp.offset))

    def close(self):
        self.closed = True


class FakeDatabase:
    def __init__(self, failures: int, exc: Exception):
        self.failures = failures
        self.exc = exc
        self.attempts = 0

    @contextmanager
    def transaction(self):
        self.attempts += 1
        if self.attempts <= self.failures:
            raise self.exc
        yield object()


class FakeWriter:
    def __init__(self):
        self.batches = []

    def process_batch(self, conn, records):
        self.batches.append(list(records))
        return [RecordResult(r, "mybell_app", "PROCESSED", payment_id="P") for r in records]

    def source_for(self, topic):
        return "mybell_app"


def records(*offsets, topic="mybell.payment.events", partition=0):
    return [KafkaRecord(topic, partition, o, b"k", b"{}", datetime.now(UTC)) for o in offsets]


def service(db):
    svc = IngestionService(
        KafkaSettings(), TopicSettings(), IngestionSettings(publish_dlq=False), FAST_RETRY, None, db,  # type: ignore[arg-type]
        consumer_factory=FakeConsumer,
    )
    svc.writer = FakeWriter()  # type: ignore[assignment]
    svc._assigned = {("mybell.payment.events", 0), ("mybell.payment.events", 1)}
    return svc


def test_consumer_config_disables_auto_commit():
    svc = service(FakeDatabase(0, RuntimeError()))
    assert svc.consumer.conf["enable.auto.commit"] is False
    assert svc.consumer.conf["enable.auto.offset.store"] is False
    assert svc.consumer.conf["on_commit"] == svc._on_commit


def test_async_commit_results_are_counted():
    from observability import metrics

    svc = service(FakeDatabase(0, RuntimeError()))
    ok = metrics.OFFSET_COMMITS.labels("committed")._value.get()
    failed = metrics.OFFSET_COMMITS.labels("failed")._value.get()
    svc._on_commit(None, [])
    svc._on_commit("NOT_COORDINATOR", [])
    assert metrics.OFFSET_COMMITS.labels("committed")._value.get() == ok + 1
    assert metrics.OFFSET_COMMITS.labels("failed")._value.get() == failed + 1


def test_offsets_committed_only_after_successful_db_transaction():
    svc = service(FakeDatabase(0, RuntimeError()))
    batch = records(5, 6, 7) + records(3, partition=1)
    svc.handle_batch(batch)
    assert svc.consumer.commits == [[("mybell.payment.events", 0, 8), ("mybell.payment.events", 1, 4)]]


def test_transient_db_failure_is_retried_then_committed():
    db = FakeDatabase(2, psycopg.OperationalError("connection lost"))
    svc = service(db)
    svc.handle_batch(records(10))
    assert db.attempts == 3
    assert svc.consumer.commits == [[("mybell.payment.events", 0, 11)]]


def test_exhausted_retries_rewind_without_commit():
    db = FakeDatabase(99, psycopg.OperationalError("database down"))
    svc = service(db)
    assert svc.handle_batch(records(20, 21, 22)) is None
    assert svc.consumer.commits == []
    assert svc.consumer.seeks == [("mybell.payment.events", 0, 20)]
    assert db.attempts == FAST_RETRY.max_attempts
    assert svc.health.status()[0] is False


def test_error_classification():
    assert is_transient(psycopg.OperationalError())
    assert is_transient(TransientError())
    assert classify_exception(psycopg.errors.UniqueViolation()) is ErrorCategory.DATABASE
    assert classify_exception(ValueError()) is ErrorCategory.PROCESSING
    assert classify_exception(psycopg.errors.DeadlockDetected()) is ErrorCategory.TRANSIENT


def test_retry_does_not_retry_permanent_errors():
    calls = []

    def boom():
        calls.append(1)
        raise ValueError("bad payload")

    with pytest.raises(ValueError):
        run_with_retry("t", FAST_RETRY, boom)
    assert len(calls) == 1


class FakeProducer:
    def __init__(self, fail: bool):
        self.fail = fail
        self.sent = []
        self._cb = None

    def produce(self, topic, key, value, headers, on_delivery):
        self.sent.append((topic, key, value, dict(headers)))
        self._cb = on_delivery

    def flush(self, timeout):
        self._cb("broker down" if self.fail else None, None)
        return 0


def test_dead_letter_preserves_original_bytes_and_requires_ack():
    rec = records(1)[0]
    ok = FakeProducer(fail=False)
    DeadLetterPublisher(ok, "dlq").publish(rec, "mybell_app", "VALIDATION", "INVALID_JSON", "bad")
    topic, key, value, headers = ok.sent[0]
    assert topic == "dlq" and value == rec.value and headers["x-original-offset"] == b"1"
    with pytest.raises(TransientError):
        DeadLetterPublisher(FakeProducer(fail=True), "dlq").publish(rec, "mybell_app", "V", "C", "d")
