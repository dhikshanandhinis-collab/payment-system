"""Publish a finite, deterministic demo dataset to the source topics.

Events are generated with event times in the past (default: starting 2 hours ago) so that waiting
periods have already expired and the resulting exceptions are visible immediately. Arrival order
(including simulated delays and out-of-order delivery) is preserved.

Usage:
    python -m scripts.seed_demo_data --payments 500
    python -m scripts.seed_demo_data --payments 200 --run-id demo2 --hours-ago 30
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from common.config import KafkaSettings, SimulationSettings, TopicSettings, load
from common.kafka_config import producer_config
from generators.publisher import publish
from generators.simulation import build_all_events


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed deterministic synthetic demo data")
    parser.add_argument("--payments", type=int, default=500)
    parser.add_argument("--run-id", default="demo")
    parser.add_argument("--hours-ago", type=float, default=2.0)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)
    if not 1 <= args.payments <= 1_000_000:
        parser.error("--payments must be between 1 and 1,000,000")

    overrides: dict = {"run_id": args.run_id, "payments_per_second": 5.0}
    if args.seed is not None:
        overrides["seed"] = args.seed
    sim = load(SimulationSettings, **overrides)
    topics = load(TopicSettings)
    start = datetime.now(UTC) - timedelta(hours=args.hours_ago)
    events = build_all_events(sim, topics, 0, args.payments, start)
    producer = Producer(producer_config(load(KafkaSettings), "seed-demo"))
    stats = publish(producer, events)
    kinds = Counter(ev.kind for ev in events)
    print(  # noqa: T201
        f"published {stats.delivered}/{stats.attempted} events for {args.payments} payments "
        f"(run_id={args.run_id}, seed={sim.seed}) in {stats.duration_s:.1f}s; failed={stats.failed}; kinds={dict(kinds)}"
    )
    return 0 if stats.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
