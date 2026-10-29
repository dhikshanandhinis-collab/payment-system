"""Kafka client configuration builders shared by producers and consumers."""

from __future__ import annotations

from common.config import IngestionSettings, KafkaSettings


def producer_config(kafka: KafkaSettings, client_id: str, linger_ms: int = 20) -> dict[str, object]:
    conf: dict[str, object] = dict(kafka.client_config())
    conf.update(
        {
            "client.id": f"{kafka.client_id_prefix}-{client_id}",
            # acks=all + idempotence prevents producer-side duplicates caused by internal retries.
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 2_147_483_647,
            "delivery.timeout.ms": 120_000,
            "linger.ms": linger_ms,
            "compression.type": "lz4",
        }
    )
    return conf


def consumer_config(kafka: KafkaSettings, ingestion: IngestionSettings, client_id: str) -> dict[str, object]:
    conf: dict[str, object] = dict(kafka.client_config())
    conf.update(
        {
            "client.id": f"{kafka.client_id_prefix}-{client_id}",
            "group.id": ingestion.consumer_group,
            "auto.offset.reset": ingestion.auto_offset_reset,
            # Offsets are committed manually, only after the database transaction commits.
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": ingestion.max_poll_interval_ms,
            "session.timeout.ms": ingestion.session_timeout_ms,
            "isolation.level": "read_committed",
            "enable.partition.eof": False,
        }
    )
    return conf
