"""Settlement-file adapter: validates processor settlement CSV files and publishes one event per line.

File-level integrity is checked here (exact header, column count, TRAILER record count and net total);
a file failing it is moved to ``rejected/`` with an ``.error.txt`` and nothing is published. Field-level
validation of each line is left to the ingestion service, so a bad line is quarantined like any other
invalid event while the rest of the file is processed.

Re-processing a file is harmless: event ids and event times are derived from the file content, so
re-published lines are detected as duplicate deliveries by ingestion. A file is moved to
``processed/`` only after every line was acknowledged by Kafka (acks=all, idempotent producer).

    python -m ingestion.settlement_adapter                 # poll SETTLEMENT_INBOX every SETTLEMENT_POLL_S
    python -m ingestion.settlement_adapter --inbox DIR --once
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from common.config import BusinessSettings, KafkaSettings, SchemaRegistrySettings, ServiceSettings, TopicSettings, load
from common.kafka_config import producer_config
from common.logging_setup import configure_logging
from common.schema_registry import SchemaRegistryClient, serializers_for
from common.signals import install_signal_handlers
from generators.settlement_files import HEADER
from observability import metrics

log = logging.getLogger("ingestion.settlement_adapter")


class FileRejected(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code


def _event_time(settlement_date: str, tz: ZoneInfo) -> str:
    try:
        day = date.fromisoformat(settlement_date)
    except ValueError:
        return settlement_date  # left for ingestion validation to quarantine
    return datetime.combine(day, time.min, tzinfo=tz).astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_settlement_file(text: str, tz: ZoneInfo) -> list[dict[str, Any]]:
    """Validate file-level integrity and return one event payload per line."""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or tuple(rows[0]) != HEADER:
        raise FileRejected("INVALID_HEADER", f"expected header {','.join(HEADER)}")
    if len(rows) < 2 or not rows[-1] or rows[-1][0] != "TRAILER" or len(rows[-1]) != 3:
        raise FileRejected("MISSING_TRAILER", "last record must be TRAILER,<line_count>,<net_total>")
    body = rows[1:-1]
    for number, row in enumerate(body, start=2):
        if len(row) != len(HEADER):
            raise FileRejected("WRONG_COLUMN_COUNT", f"line {number} has {len(row)} columns, expected {len(HEADER)}")
    try:
        count, total = int(rows[-1][1]), Decimal(rows[-1][2])
        net_sum = sum((Decimal(row[HEADER.index("net_amount")]) for row in body), Decimal("0"))
    except (ValueError, InvalidOperation) as exc:
        raise FileRejected("UNPARSABLE_CONTROL_VALUE", str(exc)) from exc
    if count != len(body):
        raise FileRejected("CONTROL_COUNT_MISMATCH", f"trailer says {count} lines, file has {len(body)}")
    if net_sum != total:
        raise FileRejected("CONTROL_TOTAL_MISMATCH", f"trailer net total {total}, lines sum to {net_sum}")

    events = []
    for row in body:
        fields = dict(zip(HEADER, row, strict=True))
        events.append({
            "event_id": f"STL-EVT-{fields['settlement_id']}",
            "event_type": "SETTLEMENT_LINE",
            "schema_version": "1.0",
            "source_system": "processor_settlement",
            "event_time": _event_time(fields["settlement_date"], tz),
            "correlation_id": fields["settlement_batch_id"],
            **fields,
        })
    return events


class SettlementAdapter:
    def __init__(
        self,
        inbox: Path,
        topics: TopicSettings,
        producer: Any,
        tz: ZoneInfo,
        serializer: Callable[[dict[str, Any]], bytes] | None = None,
    ) -> None:
        self.inbox = inbox
        self.processed = inbox.parent / "processed"
        self.rejected = inbox.parent / "rejected"
        self.topic = topics.settlement_events
        self.producer = producer
        self.tz = tz
        self.encode = serializer or (lambda event: json.dumps(event, separators=(",", ":")).encode())

    def run_once(self) -> int:
        """Process every complete file currently in the inbox; returns the number of files handled."""
        handled = 0
        for path in sorted(self.inbox.glob("*.csv")):
            self.process_file(path)
            handled += 1
        return handled

    def process_file(self, path: Path) -> str:
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()[:12]
        try:
            events = parse_settlement_file(raw.decode("utf-8-sig"), self.tz)
        except (FileRejected, UnicodeDecodeError) as exc:
            code = exc.code if isinstance(exc, FileRejected) else "INVALID_ENCODING"
            self._move(path, self.rejected, digest)
            (self.rejected / f"{path.stem}.{digest}.error.txt").write_text(f"{code}\n{exc}\n", encoding="utf-8")
            metrics.SETTLEMENT_FILES.labels("rejected").inc()
            log.error("settlement file rejected", extra={"file": path.name, "error_code": code})
            return "rejected"

        errors: list[str] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(str(err))

        for event in events:
            key = str(event.get("payment_id") or "").encode() or None
            self.producer.produce(self.topic, key=key, value=self.encode(event), on_delivery=on_delivery)
            self.producer.poll(0)
        undelivered = self.producer.flush(60)
        if errors or undelivered:
            # Left in the inbox: the next attempt republishes; ingestion deduplicates what already arrived.
            metrics.SETTLEMENT_FILES.labels("retry").inc()
            log.error("settlement publish incomplete; will retry",
                      extra={"file": path.name, "error_code": errors[0] if errors else "FLUSH_TIMEOUT"})
            return "retry"
        self._move(path, self.processed, digest)
        metrics.SETTLEMENT_FILES.labels("processed").inc()
        metrics.SETTLEMENT_LINES.inc(len(events))
        log.info("settlement file published", extra={"file": path.name, "lines": len(events)})
        return "processed"

    @staticmethod
    def _move(path: Path, folder: Path, digest: str) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        os.replace(path, folder / f"{path.stem}.{digest}{path.suffix}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish processor settlement files to Kafka")
    parser.add_argument("--inbox", type=Path, default=Path(os.environ.get("SETTLEMENT_INBOX", "data/settlement/inbox")))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    service = load(ServiceSettings)
    configure_logging(service.service_name or "settlement-adapter", service.environment, service.log_level)
    kafka, topics, tz = load(KafkaSettings), load(TopicSettings), load(BusinessSettings).tz
    args.inbox.mkdir(parents=True, exist_ok=True)
    registry = load(SchemaRegistrySettings)
    serializer = None
    if registry.serialization == "avro":
        serializer = serializers_for(SchemaRegistryClient(registry), topics, registry.auto_register)[
            topics.settlement_events]
    adapter = SettlementAdapter(args.inbox, topics, Producer(producer_config(kafka, "settlement-adapter", 0)), tz,
                                serializer)
    if args.once:
        adapter.run_once()
        return 0

    health = metrics.HealthState()
    if service.metrics_enabled:
        metrics.start_http_server(service.metrics_port, health)
    stop = threading.Event()
    install_signal_handlers(stop)
    health.set_ready(True, "polling")
    poll_s = float(os.environ.get("SETTLEMENT_POLL_S", "30"))
    while not stop.is_set():
        adapter.run_once()
        stop.wait(poll_s)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
