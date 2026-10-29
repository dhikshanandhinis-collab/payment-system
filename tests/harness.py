"""Test harness that runs the real ingestion service and reconciliation worker in-process."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer, Producer

from common.config import IngestionSettings, KafkaSettings, ReconciliationSettings, RetrySettings, TopicSettings
from common.kafka_config import producer_config
from database.connection import Database
from ingestion.consumer import IngestionService
from processing.reconciliation_service import ReconciliationWorker
from processing.rules import ReconciliationRules

TZ = ZoneInfo("America/Toronto")
FAST_RETRY = RetrySettings(max_attempts=3, initial_backoff_s=0.05, max_backoff_s=0.2, jitter_s=0.01)


def recent_past(hours: float = 2.0) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)


class Harness:
    def __init__(
        self,
        database: Database,
        topics: TopicSettings,
        rules: ReconciliationRules,
        group: str,
        batch_size: int = 200,
        retry: RetrySettings = FAST_RETRY,
    ) -> None:
        self.database = database
        self.topics = topics
        self.rules = rules
        self.kafka = KafkaSettings()
        self.producer = Producer(producer_config(self.kafka, "test-producer", linger_ms=0))
        self.group = group
        self.batch_size = batch_size
        self.retry = retry
        self.ingestion = self.new_ingestion()
        self.worker = ReconciliationWorker(
            database, rules, TZ, ReconciliationSettings(worker_batch_size=500), self.producer, topics.reconciliation_events
        )

    def new_ingestion(self) -> IngestionService:
        return IngestionService(
            self.kafka,
            self.topics,
            IngestionSettings(consumer_group=self.group, batch_size=self.batch_size, poll_timeout_s=0.5,
                              session_timeout_ms=10_000),
            self.retry,
            self.rules,
            self.database,
        )

    def topic_for(self, payload: dict[str, Any]) -> str:
        return {
            "mybell_app": self.topics.app_events,
            "payment_processor": self.topics.processor_events,
            "billing_ledger": self.topics.ledger_events,
        }[payload["source_system"]]

    def publish(self, items: Iterable[dict[str, Any] | tuple[str, bytes, bytes | None]]) -> int:
        n = 0
        for item in items:
            if isinstance(item, dict):
                key = (item.get("payment_id") or item.get("account_ref") or "").encode()
                self.producer.produce(self.topic_for(item), key=key, value=json.dumps(item).encode())
            else:
                topic, value, key = item
                self.producer.produce(topic, key=key, value=value)
            n += 1
        assert self.producer.flush(30) == 0
        return n

    def raw_count(self) -> int:
        with self.database.transaction() as conn:
            row = conn.execute(
                "SELECT count(*) FROM raw_payment_events WHERE source_topic = ANY(%s)",
                (list(self.topics.source_topics),),
            ).fetchone()
        return int(row[0])

    def ingest_until(self, expected_raw: int, timeout: float = 90.0, service: IngestionService | None = None) -> None:
        svc = service or self.ingestion
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            svc.poll_once()
            if self.raw_count() >= expected_raw:
                return
        raise AssertionError(f"timed out: raw={self.raw_count()} expected={expected_raw}")

    def reconcile_all(self) -> int:
        total = 0
        while True:
            n = self.worker.process_queue_once()
            total += n
            if n == 0:
                return total

    def run(self, expected_raw: int, timeout: float = 90.0) -> None:
        self.ingest_until(expected_raw, timeout)
        self.reconcile_all()

    def close(self) -> None:
        self.ingestion.close()
        self.producer.flush(10)


def read_topic(kafka: KafkaSettings, topic: str, expected: int, timeout: float = 30.0) -> list[Any]:
    consumer = Consumer({**kafka.client_config(), "group.id": f"reader-{topic}-{time.time()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    out: list[Any] = []
    deadline = time.monotonic() + timeout
    try:
        while len(out) < expected and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is not None and msg.error() is None:
                out.append(msg)
    finally:
        consumer.close()
    return out
