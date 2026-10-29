"""Run named report queries from analytics/reports.sql and print them as text tables.

Usage:
    python -m scripts.report --list
    python -m scripts.report daily_summary oldest_unresolved_exceptions
    python -m scripts.report --all
    python -m scripts.report --kafka-lag
"""

from __future__ import annotations

import argparse
import re
from typing import Any

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common.config import REPO_ROOT, DatabaseSettings, IngestionSettings, KafkaSettings, TopicSettings, load
from database.connection import connect

REPORTS_SQL = REPO_ROOT / "analytics" / "reports.sql"


def load_reports() -> dict[str, tuple[str, str]]:
    """Return {name: (description, sql)} parsed from reports.sql."""
    text = REPORTS_SQL.read_text(encoding="utf-8")
    reports: dict[str, tuple[str, str]] = {}
    for block in re.split(r"^-- name: ", text, flags=re.M)[1:]:
        name, _, body = block.partition("\n")
        lines = body.strip().splitlines()
        description = " ".join(line[len("-- metric: "):] for line in lines if line.startswith("-- metric: "))
        query = "\n".join(line for line in lines if not line.startswith("--")).strip().rstrip(";")
        reports[name.strip()] = (description, query)
    return reports


def format_table(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    line = " | ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))
    sep = "-+-".join("-" * w for w in widths)
    body = "\n".join(" | ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)) for r in cells)
    return f"{line}\n{sep}\n{body}" if body else f"{line}\n{sep}\n(no rows)"


def kafka_lag() -> list[tuple[str, int, int, int, int]]:
    kafka, topics, ingestion = load(KafkaSettings), load(TopicSettings), load(IngestionSettings)
    admin = AdminClient(kafka.client_config())
    metadata = admin.list_topics(timeout=10)
    partitions = [
        TopicPartition(t, p) for t in topics.source_topics if t in metadata.topics for p in metadata.topics[t].partitions
    ]
    future = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(ingestion.consumer_group, partitions)])
    committed = {(tp.topic, tp.partition): tp.offset for tp in future[ingestion.consumer_group].result().topic_partitions}
    from confluent_kafka import Consumer

    probe = Consumer({**kafka.client_config(), "group.id": f"{ingestion.consumer_group}-lag-probe"})
    rows = []
    try:
        for tp in partitions:
            low, high = probe.get_watermark_offsets(tp, timeout=10)
            offset = committed.get((tp.topic, tp.partition), -1)
            lag = high - (offset if offset >= 0 else low)
            rows.append((tp.topic, tp.partition, offset, high, max(lag, 0)))
    finally:
        probe.close()
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print reconciliation reports")
    parser.add_argument("names", nargs="*")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--kafka-lag", action="store_true")
    args = parser.parse_args(argv)
    reports = load_reports()
    if args.list:
        for name, (desc, _) in reports.items():
            print(f"{name}: {desc}")  # noqa: T201
        return 0
    if args.kafka_lag:
        print(format_table(["topic", "partition", "committed", "high_watermark", "lag"], kafka_lag()))  # noqa: T201
        if not args.names and not args.all:
            return 0
    names = list(reports) if args.all else args.names
    unknown = [n for n in names if n not in reports]
    if unknown:
        parser.error(f"unknown report(s): {', '.join(unknown)}; use --list")
    with connect(load(DatabaseSettings), autocommit=True) as conn:
        for name in names:
            desc, query = reports[name]
            cur = conn.execute(query)  # static SQL from the repository
            columns = [d.name for d in cur.description or []]
            print(f"\n== {name} ==\n{desc}\n")  # noqa: T201
            print(format_table(columns, cur.fetchall()))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
