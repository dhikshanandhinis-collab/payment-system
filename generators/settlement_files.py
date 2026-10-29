"""Synthetic processor settlement (funding) files, derived from the same deterministic simulation.

The processor settles what it actually executed: the lines come from the processor simulator's own
payloads (before feed faults such as invalid payloads or duplicates), plus fees and independent
settlement-file faults (missing line, wrong gross amount). Files follow the format validated by
ingestion/settlement_adapter.py (header, one line per settled transaction, TRAILER control record).

    python -m generators.settlement_files --from 2026-10-29T09:00:00Z --to 2026-10-29T10:00:00Z --out DIR
    python -m generators.settlement_files --loop      # every SETTLEMENT_INTERVAL_S, until stopped
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from common.config import BusinessSettings, ServiceSettings, SimulationSettings, TopicSettings, load
from common.logging_setup import configure_logging
from common.money import ZERO, to_money
from common.signals import install_signal_handlers
from generators.simulation import (
    PROCESSOR,
    _rng,
    _SourceBuilder,
    index_at,
    intent_time,
    make_intent,
    processor_events,
    world_outcome,
)

log = logging.getLogger("generators.settlement_files")

HEADER = (
    "settlement_date", "settlement_batch_id", "settlement_id", "processor_transaction_id", "payment_id",
    "transaction_type", "gross_amount", "fee_amount", "net_amount", "currency",
)
SIGN = {"CAPTURE": 1, "CHARGEBACK_REVERSAL": 1, "REFUND": -1, "REVERSAL": -1, "CHARGEBACK": -1}
CENT = Decimal("0.01")
# Synthetic merchant pricing: (percentage, fixed) on captures; a flat fee per chargeback.
CAPTURE_FEES = {
    "CREDIT_CARD": (Decimal("0.0245"), Decimal("0.10")),
    "DEBIT_CARD": (Decimal("0.0050"), Decimal("0.05")),
    "PRE_AUTHORIZED_DEBIT": (Decimal("0"), Decimal("0.25")),
    "ONLINE_BANKING": (Decimal("0"), Decimal("0.35")),
}
CHARGEBACK_FEE = Decimal("15.00")


def fee_for(method: str, transaction_type: str, gross: Decimal) -> Decimal:
    if transaction_type == "CHARGEBACK":
        return CHARGEBACK_FEE
    if transaction_type != "CAPTURE":
        return ZERO
    rate, fixed = CAPTURE_FEES[method]
    return (gross * rate + fixed).quantize(CENT, rounding=ROUND_HALF_UP)


def settlement_lines(settings: SimulationSettings, topics: TopicSettings, index: int) -> list[dict[str, Any]]:
    """Settlement lines (with transaction time) for one simulated payment."""
    intent = make_intent(settings, index)
    world = world_outcome(settings, intent)
    if not world.approved:
        return []
    builder = _SourceBuilder(settings, topics, PROCESSOR, intent)
    processor_events(builder, world)
    settled: dict[str, dict[str, Any]] = {}
    for payload in builder.originals:
        if payload["event_type"] in SIGN:
            settled.setdefault(payload["processor_transaction_id"], payload)
    rng = _rng(settings, index, "settlement")
    lines = []
    for txn_id, payload in settled.items():
        missing, wrong_amount = rng.random(), rng.random()
        if missing < settings.settlement_missing_rate:
            continue
        gross = to_money(payload["amount"])
        if wrong_amount < settings.settlement_amount_mismatch_rate:
            gross = max(gross - Decimal(rng.randint(1, 300)) / 100, CENT)
        kind = payload["event_type"]
        fee = fee_for(intent.payment_method, kind, gross)
        lines.append({
            "transaction_time": datetime.fromisoformat(payload["event_time"].replace("Z", "+00:00")),
            "settlement_id": f"STL-{txn_id}",
            "processor_transaction_id": txn_id,
            "payment_id": intent.payment_id,
            "transaction_type": kind,
            "gross_amount": gross,
            "fee_amount": fee,
            "net_amount": SIGN[kind] * gross - fee,
            "currency": intent.currency,
        })
    return lines


def render_file(rows: Sequence[dict[str, Any]], batch_id: str, tz: Any) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(HEADER)
    for r in rows:
        writer.writerow([
            r["transaction_time"].astimezone(tz).date().isoformat(), batch_id, r["settlement_id"],
            r["processor_transaction_id"], r["payment_id"], r["transaction_type"], f"{r['gross_amount']:.2f}",
            f"{r['fee_amount']:.2f}", f"{r['net_amount']:.2f}", r["currency"],
        ])
    writer.writerow(["TRAILER", len(rows), f"{sum((r['net_amount'] for r in rows), ZERO):.2f}"])
    return out.getvalue()


def write_settlement_file(
    settings: SimulationSettings, topics: TopicSettings, tz: Any, start: datetime, end: datetime, out_dir: Path
) -> Path | None:
    """Settle transactions of payments initiated in [start, end) whose transaction time is before end."""
    rows: list[dict[str, Any]] = []
    for index in range(index_at(settings, start), index_at(settings, end) + 1):
        if not start <= intent_time(settings, index) < end:
            continue
        rows.extend(r for r in settlement_lines(settings, topics, index) if r["transaction_time"] < end)
    if not rows:
        return None
    rows.sort(key=lambda r: (r["transaction_time"], r["settlement_id"]))
    batch_id = f"STB-{settings.run_id}-{start:%Y%m%d%H%M%S}-{end:%H%M%S}"
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"settlement_{batch_id}.csv"
    tmp = target.with_suffix(".csv.tmp")
    tmp.write_text(render_file(rows, batch_id, tz), encoding="utf-8")
    os.replace(tmp, target)  # the adapter only picks up complete *.csv files
    log.info("settlement file written", extra={"file": target.name, "lines": len(rows)})
    return target


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamps must include a timezone, e.g. 2026-10-29T09:00:00Z")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write synthetic processor settlement files")
    parser.add_argument("--from", dest="start", type=_parse_time)
    parser.add_argument("--to", dest="end", type=_parse_time)
    parser.add_argument("--out", type=Path, default=Path(os.environ.get("SETTLEMENT_INBOX", "data/settlement/inbox")))
    parser.add_argument("--loop", action="store_true", help="write a file every SETTLEMENT_INTERVAL_S until stopped")
    args = parser.parse_args(argv)

    service = load(ServiceSettings)
    configure_logging(service.service_name or "settlement-producer", service.environment, service.log_level)
    settings, topics, tz = load(SimulationSettings), load(TopicSettings), load(BusinessSettings).tz
    # Producers emit an event up to ~90 s after its business time; settling only older transactions
    # avoids settlement lines for events that are still in flight.
    lag = timedelta(seconds=float(os.environ.get("SETTLEMENT_LAG_S", "120")))

    if not args.loop:
        if args.start is None:
            parser.error("--from is required without --loop")
        write_settlement_file(settings, topics, tz, args.start, args.end or datetime.now(UTC) - lag, args.out)
        return 0

    interval = float(os.environ.get("SETTLEMENT_INTERVAL_S", "300"))
    stop = threading.Event()
    install_signal_handlers(stop)
    last = args.start or datetime.now(UTC)
    while True:
        stop.wait(interval)
        end = datetime.now(UTC) - lag
        if end > last:
            write_settlement_file(settings, topics, tz, last, end, args.out)
            last = end
        if stop.is_set():  # the final iteration flushes everything settled so far
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
