"""Bulk publishing of pre-built synthetic events (seeding, end-to-end tests, load tests)."""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from generators.simulation import ScheduledEvent


@dataclass
class PublishStats:
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    duration_s: float = 0.0

    @property
    def rate(self) -> float:
        return self.delivered / self.duration_s if self.duration_s > 0 else 0.0


def publish(
    producer: Any,
    events: Iterable[ScheduledEvent],
    events_per_second: float | None = None,
    burst: tuple[float, float] | None = None,
) -> PublishStats:
    """Publish events in order. Optional pacing: steady events/s, with an optional (rate, seconds) burst first."""
    stats = PublishStats()

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is None:
            stats.delivered += 1
        else:
            stats.failed += 1

    started = time.perf_counter()
    burst_rate, burst_seconds = burst if burst else (0.0, 0.0)
    burst_events = burst_rate * burst_seconds
    for ev in events:
        if events_per_second or burst:
            n = stats.attempted
            if n < burst_events:
                due = n / burst_rate
            else:
                due = burst_seconds + (n - burst_events) / (events_per_second or burst_rate)
            elapsed = time.perf_counter() - started
            if due > elapsed:
                producer.poll(0)
                time.sleep(due - elapsed)
        while True:
            try:
                producer.produce(ev.topic, key=ev.key, value=ev.value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)
        stats.attempted += 1
        if stats.attempted % 1000 == 0:
            producer.poll(0)
    producer.flush(120)
    stats.duration_s = time.perf_counter() - started
    return stats
