"""Idempotently create (or verify) the Kafka topics.

Usage: python -m scripts.create_topics

Local single-broker defaults: replication factor 1, min.insync.replicas 1. A production multi-broker
cluster should use replication factor 3 and min.insync.replicas 2 (see docs/architecture.md).
"""

from __future__ import annotations

import logging
import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KafkaSettings, RetrySettings, ServiceSettings, TopicSettings, load
from common.errors import TransientError
from common.logging_setup import configure_logging
from ingestion.error_handler import run_with_retry

log = logging.getLogger("scripts.create_topics")


def topic_specs(topics: TopicSettings) -> list[NewTopic]:
    common = {
        "min.insync.replicas": str(topics.min_insync_replicas),
        "cleanup.policy": "delete",
    }
    specs = []
    for name in (topics.app_events, topics.processor_events, topics.ledger_events, topics.reconciliation_events):
        specs.append(
            NewTopic(name, num_partitions=topics.partitions, replication_factor=topics.replication_factor,
                     config={**common, "retention.ms": str(topics.retention_ms)})
        )
    specs.append(
        NewTopic(topics.dlq, num_partitions=max(1, topics.partitions // 2), replication_factor=topics.replication_factor,
                 config={**common, "retention.ms": str(topics.dlq_retention_ms)})
    )
    return specs


def create_topics(kafka: KafkaSettings, topics: TopicSettings, retry: RetrySettings) -> dict[str, str]:
    admin = AdminClient(kafka.client_config())

    def existing() -> set[str]:
        try:
            return set(admin.list_topics(timeout=10).topics)
        except KafkaException as exc:
            raise TransientError(str(exc)) from exc

    present = run_with_retry("kafka_admin", retry, existing)
    results: dict[str, str] = {}
    to_create = [spec for spec in topic_specs(topics) if spec.topic not in present]
    for spec in topic_specs(topics):
        if spec.topic in present:
            results[spec.topic] = "exists"
    if to_create:
        futures = admin.create_topics(to_create, operation_timeout=30)
        for name, future in futures.items():
            try:
                future.result()
                results[name] = "created"
            except KafkaException as exc:
                if exc.args and exc.args[0].name() == "TOPIC_ALREADY_EXISTS":
                    results[name] = "exists"
                else:
                    raise
    # Newly created topics can take a moment to appear in broker metadata; wait until all are visible.
    def visible() -> dict[str, int]:
        topics_meta = admin.list_topics(timeout=10).topics
        missing = [name for name in results if name not in topics_meta]
        if missing:
            raise TransientError(f"topics not yet visible in metadata: {missing}")
        return {name: len(topics_meta[name].partitions) for name in results}

    for name, partitions in run_with_retry("kafka_admin", retry, visible).items():
        log.info("topic ready", extra={"topic": name, "outcome": results[name], "partitions": partitions})
    return results


def main() -> int:
    service = load(ServiceSettings)
    configure_logging("create-topics", service.environment, service.log_level)
    results = create_topics(load(KafkaSettings), load(TopicSettings), load(RetrySettings))
    for name, outcome in sorted(results.items()):
        print(f"{name}: {outcome}")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
