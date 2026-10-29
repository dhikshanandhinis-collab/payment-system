"""Real-time producer loop: emits events only when due, pauses during simulated interruptions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from common.config import SimulationSettings, TopicSettings
from generators.runner import SourceProducer
from generators.simulation import APP


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


def test_events_are_emitted_when_due():
    start = datetime(2026, 10, 28, 12, tzinfo=UTC)
    sim = SimulationSettings(epoch=start, payments_per_second=2, delayed_event_rate=0, invalid_payload_rate=0,
                             missing_app_rate=0, duplicate_delivery_rate=0)
    clock = Clock(start)
    producer = FakeProducer()
    runner = SourceProducer(APP, sim, TopicSettings(), producer, clock=clock)
    runner.step()
    first = len(producer.messages)
    clock.now = start + timedelta(seconds=30)
    runner.step()
    assert len(producer.messages) > first
    assert all(topic == "mybell.payment.events" for topic, _, _ in producer.messages)


def test_interruption_pauses_then_flushes_backlog():
    start = datetime(2026, 10, 28, 12, tzinfo=UTC)
    sim = SimulationSettings(epoch=start, payments_per_second=2, interruption_probability_per_minute=1.0,
                             interruption_duration_s=30, delayed_event_rate=0)
    clock = Clock(start)
    producer = FakeProducer()
    runner = SourceProducer(APP, sim, TopicSettings(), producer, clock=clock)
    clock.now = start + timedelta(seconds=61)
    assert runner.step() == 0 and runner.paused_until is not None
    clock.now = start + timedelta(seconds=95)
    assert runner.step() > 0
