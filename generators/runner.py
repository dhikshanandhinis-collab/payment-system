"""Real-time producer loop shared by the three independent source simulators.

Each producer process derives intents from wall-clock time and the shared epoch, simulates only its
own source, and publishes events when their simulated arrival time is reached. A producer that
starts late or is interrupted simply misses/delays its own events, like a real independent system.
"""

from __future__ import annotations

import heapq
import json
import logging
import random
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Producer

from common.config import (
    KafkaSettings,
    SchemaRegistrySettings,
    ServiceSettings,
    SimulationSettings,
    TopicSettings,
    load,
)
from common.kafka_config import producer_config
from common.logging_setup import configure_logging
from common.schema_registry import SchemaRegistryClient, serializers_for
from common.signals import install_signal_handlers
from generators.simulation import ScheduledEvent, build_source_events, index_at, intent_time
from observability import metrics

log = logging.getLogger("generators")


class SourceProducer:
    def __init__(
        self,
        source: str,
        sim: SimulationSettings,
        topics: TopicSettings,
        producer: Any,
        clock: Any = lambda: datetime.now(UTC),
        serializers: dict[str, Callable[[dict[str, Any]], bytes]] | None = None,
    ) -> None:
        self.source = source
        self.sim = sim
        self.topics = topics
        self.producer = producer
        self.clock = clock
        self.serializers = serializers or {}
        self.heap: list[ScheduledEvent] = []
        self.next_index = index_at(sim, clock())
        self.stop_event = threading.Event()
        self.paused_until: datetime | None = None
        self.interrupt_rng = random.Random(f"{sim.seed}|{source}|interruptions")  # noqa: S311
        self._last_interrupt_check = clock()
        self.produced = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            metrics.PRODUCE_ERRORS.labels(self.source).inc()
            log.error("delivery failed", extra={"source_system": self.source, "error_code": str(err)})

    def _maybe_interrupt(self, now: datetime) -> None:
        if self.sim.interruption_probability_per_minute <= 0:
            return
        if (now - self._last_interrupt_check).total_seconds() < 60:
            return
        self._last_interrupt_check = now
        if self.interrupt_rng.random() < self.sim.interruption_probability_per_minute:
            self.paused_until = now + timedelta(seconds=self.sim.interruption_duration_s)
            log.warning("simulated source interruption", extra={"source_system": self.source,
                                                                "duration_s": self.sim.interruption_duration_s})

    def _encode(self, ev: ScheduledEvent) -> bytes:
        """Valid events in the configured format; injected malformed payloads stay raw bytes."""
        serializer = self.serializers.get(ev.topic)
        if serializer is None or ev.kind.startswith("invalid_"):
            return ev.value
        return serializer(json.loads(ev.value))

    def step(self) -> int:
        now = self.clock()
        while intent_time(self.sim, self.next_index) <= now:
            for ev in build_source_events(self.sim, self.topics, self.source, self.next_index):
                heapq.heappush(self.heap, ev)
            self.next_index += 1
        self._maybe_interrupt(now)
        if self.paused_until is not None:
            if now < self.paused_until:
                self.producer.poll(0)
                return 0
            self.paused_until = None
        sent = 0
        while self.heap and self.heap[0].emit_at <= now:
            ev = heapq.heappop(self.heap)
            self.producer.produce(ev.topic, key=ev.key, value=self._encode(ev), on_delivery=self._on_delivery)
            metrics.PRODUCED.labels(self.source, ev.kind).inc()
            sent += 1
        self.producer.poll(0)
        self.produced += sent
        return sent

    def run(self) -> None:
        log.info("producer started", extra={"source_system": self.source, "start_index": self.next_index,
                                            "payments_per_second": self.sim.payments_per_second})
        try:
            while not self.stop_event.is_set():
                self.step()
                self.stop_event.wait(0.05)
        finally:
            remaining = self.producer.flush(30)
            log.info("producer stopped", extra={"source_system": self.source, "produced": self.produced,
                                                "undelivered": remaining})


def main(source: str) -> None:
    service = load(ServiceSettings)
    configure_logging(service.service_name or f"{source}-producer", service.environment, service.log_level)
    sim, topics, kafka = load(SimulationSettings), load(TopicSettings), load(KafkaSettings)
    health = metrics.HealthState()
    if service.metrics_enabled:
        metrics.start_http_server(service.metrics_port, health)
    producer = Producer(producer_config(kafka, f"{source}-producer"))
    registry = load(SchemaRegistrySettings)
    serializers: dict[str, Callable[[dict[str, Any]], bytes]] | None = None
    if registry.serialization == "avro":
        serializers = dict(serializers_for(SchemaRegistryClient(registry), topics, registry.auto_register))
    runner = SourceProducer(source, sim, topics, producer, serializers=serializers)
    install_signal_handlers(runner.stop_event)
    health.set_ready(True, "producing")
    runner.run()
