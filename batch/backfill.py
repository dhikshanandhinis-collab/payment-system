"""Historical backfill: re-run reconciliation for a range of business dates.

Each date is an independent, idempotent run (see batch.daily_reconciliation), so a backfill can be
interrupted and restarted safely; already-processed dates are simply re-evaluated.

Usage:
    python -m batch.backfill --start-date 2026-10-01 --end-date 2026-10-07
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from batch.daily_reconciliation import run_business_date, summary_dict
from common.config import BusinessSettings, DatabaseSettings, ReconciliationSettings, ServiceSettings, load
from common.logging_setup import configure_logging
from database.connection import connect
from processing.rules import load_rules

MAX_DAYS = 400


def date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end date must not be before start date")
    days = (end - start).days + 1
    if days > MAX_DAYS:
        raise ValueError(f"backfill range is limited to {MAX_DAYS} days")
    return [start + timedelta(days=i) for i in range(days)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill reconciliation for a range of business dates")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--triggered-by", default="backfill-cli")
    args = parser.parse_args(argv)

    service = load(ServiceSettings)
    configure_logging(service.service_name or "backfill", service.environment, service.log_level)
    business = load(BusinessSettings)
    rules = load_rules(load(ReconciliationSettings).rules_path)
    failures = 0
    with connect(load(DatabaseSettings)) as conn:
        for day in date_range(args.start_date, args.end_date):
            try:
                summary = run_business_date(conn, rules, business.tz, day, "BACKFILL", args.triggered_by)
                print(json.dumps(summary_dict(summary), default=str))  # noqa: T201
            except Exception as exc:  # noqa: BLE001 - continue with remaining dates, report at the end
                failures += 1
                print(json.dumps({"business_date": day.isoformat(), "status": "FAILED", "error": str(exc)}))  # noqa: T201
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
