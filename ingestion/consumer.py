"""Continuously running Kafka ingestion service.

Delivery guarantee (documented in docs/architecture.md):
  * Kafka -> service: at-least-once (manual offset commits, auto-commit disabled).
  * service -> PostgreSQL: idempotent. A batch is written in one transaction; offsets are committed
    only after that transaction commits. Redelivered records are detected by the
    unique (topic, partition, offset) constraint and by event-id / business-key deduplication.
  * The offset commit is sent asynchronously (result handled in ``_on_commit``). A synchronous commit
    blocks indefinitely while the group coordinator is unavailable; the main loop then stops polling,
    exceeds max.poll.interval.ms and can never rejoin the group (observed on the local broker).
    A lost or failed commit only causes redelivery, which the Bronze constraints absorb.
  * This is NOT distributed exactly-once: Kafka offsets and PostgreSQL are not committed atomically.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from functools import partial
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from common.config import (
    BusinessSettings,
    DatabaseSettings,
    IngestionSettings,
    KafkaSettings,
    ReconciliationSettings,
    RetrySettings,
    SchemaRegistrySettings,
    ServiceSettings,
    TopicSettings,
    load,
)
from common.errors import ErrorCategory
from common.kafka_config import consumer_config, producer_config
from common.logging_setup import configure_logging
from common.schema_registry import AvroDecoder, SchemaRegistryClient
from common.signals import install_signal_handlers
from database.connection import Database
from database.repositories import KafkaRecord
from ingestion.error_handler import DeadLetterPublisher, classify_exception, is_transient, run_with_retry
from ingestion.event_writer import (
    ALREADY_INGESTED,
    DUPLICATE_BUSINESS_EVENT,
    DUPLICATE_DELIVERY,
    PROCESSED,
    QUARANTINED,
    EventWriter,
    RecordResult,
)
from observability import metrics
from processing.rules import ReconciliationRules, load_rules

log = logging.getLogger("ingestion.consumer")


def to_record(msg: Any) -> KafkaRecord:
    ts_type, ts_ms = msg.timestamp()
    timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=UTC) if ts_type != 0 and ts_ms > 0 else None
    headers = tuple((k, v) for k, v in (msg.headers() or []))
    return KafkaRecord(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value(), timestamp, headers)


class IngestionService:
    def __init__(
        self,
        kafka: KafkaSettings,
        topics: TopicSettings,
        ingestion: IngestionSettings,
        retry: RetrySettings,
        rules: ReconciliationRules,
        database: Database,
        health: metrics.HealthState | None = None,
        consumer_factory: Callable[[dict], Any] = Consumer,
        dlq_producer: Any | None = None,
        client_id: str = "ingestion",
        avro_decoder: AvroDecoder | None = None,
    ) -> None:
        self.kafka = kafka
        self.topics = topics
        self.settings = ingestion
        self.retry = retry
        self.db = database
        self.health = health or metrics.HealthState()
        self.writer = EventWriter(topics, rules, bulk=ingestion.bulk_writes, avro_decoder=avro_decoder)
        conf = consumer_config(kafka, ingestion, client_id)
        conf["on_commit"] = self._on_commit
        self.consumer = consumer_factory(conf)
        producer = dlq_producer
        if producer is None and ingestion.publish_dlq:
            producer = Producer(producer_config(kafka, f"{client_id}-dlq", linger_ms=0))
        self.dlq = DeadLetterPublisher(producer, topics.dlq) if producer is not None else None
        self.stop_event = threading.Event()
        self._assigned: set[tuple[str, int]] = set()
        self._last_lag_report = 0.0
        self._subscribed = False
        self._closed = False
        self.batches_committed = 0

    # ------------------------------------------------------------------ rebalance callbacks
    # Batches are processed and committed synchronously between consume() calls, so no batch is in
    # flight when a rebalance callback runs: every processed record's offset is already committed.

    def _on_assign(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        self._assigned.update((p.topic, p.partition) for p in partitions)
        log.info("partitions assigned", extra={"partitions": [f"{p.topic}:{p.partition}" for p in partitions]})
        self.health.set_ready(True, "assigned")

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        for p in partitions:
            self._assigned.discard((p.topic, p.partition))
            metrics.CONSUMER_LAG.labels(p.topic, str(p.partition)).set(0)
        log.info("partitions revoked", extra={"partitions": [f"{p.topic}:{p.partition}" for p in partitions]})

    def _on_lost(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        log.warning("partitions lost", extra={"partitions": [f"{p.topic}:{p.partition}" for p in partitions]})
        self._on_revoke(consumer, partitions)

    # ------------------------------------------------------------------ main loop

    def start(self) -> None:
        if self._subscribed:
            return
        topics = list(self.topics.source_topics)
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_lost)
        self._subscribed = True
        log.info("ingestion started", extra={"topics": topics, "consumer_group": self.settings.consumer_group})

    def run(self) -> None:
        self.start()
        try:
            while not self.stop_event.is_set():
                self.poll_once()
        finally:
            self.close()

    def poll_once(self) -> int:
        self.start()
        messages = self.consumer.consume(num_messages=self.settings.batch_size, timeout=self.settings.poll_timeout_s)
        records: list[KafkaRecord] = []
        for msg in messages or []:
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.fatal():
                    raise KafkaException(err)
                log.warning("kafka consumer error", extra={"error_code": err.name(), "outcome": "ignored"})
                continue
            records.append(to_record(msg))
            metrics.EVENTS_CONSUMED.labels(msg.topic()).inc()
        if records:
            self.handle_batch(records)
        self._maybe_report_lag()
        return len(records)

    def handle_batch(self, records: Sequence[KafkaRecord]) -> list[RecordResult] | None:
        started = time.perf_counter()
        try:
            results = run_with_retry("ingestion_batch", self.retry, lambda: self._write(records))
        except Exception as exc:
            category = classify_exception(exc)
            metrics.BATCH_FAILURES.labels(
                ErrorCategory.RETRY_EXHAUSTED.value if is_transient(exc) else category.value
            ).inc()
            if is_transient(exc):
                # Database/broker still unavailable after bounded retries: do NOT commit offsets.
                # Rewind so the same records are re-polled (consume() keeps the group membership alive).
                self.health.set_ready(False, "database unavailable")
                log.error(
                    "batch failed after retries; rewinding without committing offsets",
                    extra={"error_code": type(exc).__name__, "outcome": "rewind",
                           "processing_stage": "ingestion", "records": len(records)},
                )
                self._rewind(records)
                return None
            log.exception("non-transient batch failure; falling back to per-record dead-lettering",
                          extra={"error_code": type(exc).__name__})
            results = self._fallback_per_record(records)
        self.health.set_ready(True, "processing")

        self._publish_dead_letters(results)
        self._commit(records)
        self._record_metrics(results)
        metrics.CONSUMER_PROCESSING_SECONDS.observe(time.perf_counter() - started)
        self.batches_committed += 1
        return results

    def _write(self, records: Sequence[KafkaRecord]) -> list[RecordResult]:
        started = time.perf_counter()
        with self.db.transaction() as conn:
            results = self.writer.process_batch(conn, records)
        metrics.DB_WRITE_SECONDS.labels("ingestion").observe(time.perf_counter() - started)
        return results

    def _fallback_per_record(self, records: Sequence[KafkaRecord]) -> list[RecordResult]:
        """Last resort when even quarantine persistence fails: the original bytes go to the DLQ topic.

        A record's offset is committed only if it was either stored in PostgreSQL or acknowledged by
        the DLQ topic; otherwise the exception propagates and nothing is committed.
        """
        results: list[RecordResult] = []
        for record in records:
            try:
                results.extend(run_with_retry("ingestion_record", self.retry, partial(self._write, [record])))
            except Exception as exc:
                if is_transient(exc) or self.dlq is None:
                    raise
                source = self.writer.source_for(record.topic)
                self.dlq.publish(record, source, ErrorCategory.PROCESSING.value, "PERSISTENCE_FAILED", str(exc))
                results.append(
                    RecordResult(record, source, "DEAD_LETTERED", error_category="PROCESSING",
                                 error_code="PERSISTENCE_FAILED", error_detail=str(exc)[:500])
                )
        return results

    def _publish_dead_letters(self, results: Sequence[RecordResult]) -> None:
        # Quarantined events are already durable in PostgreSQL; the DLQ topic is a notification stream
        # for downstream tooling. A failed DLQ publish is logged and counted but cannot lose the event.
        if self.dlq is None:
            return
        for r in results:
            if r.outcome != QUARANTINED:
                continue
            try:
                self.dlq.publish(r.record, r.source_system, r.error_category or "", r.error_code or "", r.error_detail or "")
            except Exception:  # noqa: BLE001
                log.warning("dead-letter notification failed; event is retained in quarantined_events",
                            extra={"topic": r.record.topic, "partition": r.record.partition, "offset": r.record.offset})

    def _commit(self, records: Sequence[KafkaRecord]) -> None:
        highest: dict[tuple[str, int], int] = {}
        for r in records:
            key = (r.topic, r.partition)
            highest[key] = max(highest.get(key, -1), r.offset)
        offsets = [TopicPartition(t, p, o + 1) for (t, p), o in highest.items()]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=True)
        except KafkaException as exc:
            # Safe: the records are durable and will be skipped as ALREADY_INGESTED on redelivery.
            metrics.OFFSET_COMMITS.labels("failed").inc()
            log.warning("offset commit failed; records will be redelivered and skipped idempotently",
                        extra={"error_code": str(exc.args[0]) if exc.args else "commit_failed"})

    def _on_commit(self, err: Any, partitions: list[TopicPartition]) -> None:
        """Delivery report for async commits (served from consume())."""
        if err is None:
            metrics.OFFSET_COMMITS.labels("committed").inc()
            return
        metrics.OFFSET_COMMITS.labels("failed").inc()
        log.warning("offset commit failed; records will be redelivered and skipped idempotently",
                    extra={"error_code": str(err), "partitions": [f"{p.topic}:{p.partition}" for p in partitions or []]})

    def _rewind(self, records: Sequence[KafkaRecord]) -> None:
        lowest: dict[tuple[str, int], int] = {}
        for r in records:
            key = (r.topic, r.partition)
            lowest[key] = min(lowest.get(key, r.offset), r.offset)
        for (topic, partition), offset in lowest.items():
            if (topic, partition) not in self._assigned:
                continue
            try:
                self.consumer.seek(TopicPartition(topic, partition, offset))
            except KafkaException:
                log.warning("seek failed; partition will resume from committed offset after rebalance",
                            extra={"topic": topic, "partition": partition, "offset": offset})

    def _record_metrics(self, results: Sequence[RecordResult]) -> None:
        now = datetime.now(UTC)
        for r in results:
            src = r.source_system
            if r.outcome == ALREADY_INGESTED:
                metrics.REDELIVERED_RECORDS.labels(r.record.topic).inc()
                continue
            metrics.EVENTS_PERSISTED.labels(src).inc()
            if r.outcome in (PROCESSED, DUPLICATE_DELIVERY, DUPLICATE_BUSINESS_EVENT):
                metrics.EVENTS_VALIDATED.labels(src).inc()
            if r.outcome == DUPLICATE_DELIVERY:
                metrics.DUPLICATES_DETECTED.labels(src, "delivery").inc()
            elif r.outcome == DUPLICATE_BUSINESS_EVENT:
                metrics.DUPLICATES_DETECTED.labels(src, "business_event").inc()
            elif r.outcome == QUARANTINED:
                metrics.EVENTS_REJECTED.labels(src, r.error_category or "", r.error_code or "").inc()
                metrics.EVENTS_QUARANTINED.labels(src).inc()
            if r.outcome == PROCESSED and r.event_time is not None:
                metrics.EVENT_AGE_SECONDS.labels(src).observe(max((now - r.event_time).total_seconds(), 0))
            log.debug(
                "event processed",
                extra={
                    "source_system": src, "topic": r.record.topic, "partition": r.record.partition,
                    "offset": r.record.offset, "event_id": r.source_event_id, "payment_id": r.payment_id,
                    "processing_stage": "ingestion", "outcome": r.outcome, "error_code": r.error_code,
                },
            )

    def _maybe_report_lag(self) -> None:
        now = time.monotonic()
        if now - self._last_lag_report < self.settings.lag_report_interval_s or not self._assigned:
            return
        self._last_lag_report = now
        try:
            partitions = [TopicPartition(t, p) for t, p in sorted(self._assigned)]
            committed = self.consumer.committed(partitions, timeout=5)
            for tp in committed:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                position = tp.offset if tp.offset >= 0 else low
                metrics.CONSUMER_LAG.labels(tp.topic, str(tp.partition)).set(max(high - position, 0))
        except KafkaException as exc:
            log.warning("lag report failed", extra={"error_code": str(exc)})

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        log.info("ingestion shutting down")
        try:
            self.consumer.close()  # leaves the group cleanly; offsets were committed after each batch
        except (KafkaException, RuntimeError):
            pass
        if self.dlq is not None:
            self.dlq.producer.flush(10)


def avro_decoder(settings: SchemaRegistrySettings) -> AvroDecoder | None:
    """Avro is decoded whenever a registry is configured; JSON is always accepted."""
    return AvroDecoder(SchemaRegistryClient(settings)) if settings.url else None


def main() -> None:
    service = load(ServiceSettings)
    configure_logging(service.service_name or "ingestion", service.environment, service.log_level)
    kafka, topics = load(KafkaSettings), load(TopicSettings)
    ingestion, retry = load(IngestionSettings), load(RetrySettings)
    db_settings = load(DatabaseSettings)
    load(BusinessSettings)
    rules = load_rules(load(ReconciliationSettings).rules_path)

    health = metrics.HealthState()
    if service.metrics_enabled:
        metrics.start_http_server(service.metrics_port, health)
    database = Database(db_settings, name="ingestion")
    run_with_retry("db_connect", retry, lambda: database.open(wait=True, timeout=30))
    health.probe = database.ping

    svc = IngestionService(kafka, topics, ingestion, retry, rules, database, health, avro_decoder=avro_decoder(
        load(SchemaRegistrySettings)))
    install_signal_handlers(svc.stop_event)
    try:
        svc.run()
    finally:
        database.close()


if __name__ == "__main__":
    main()
