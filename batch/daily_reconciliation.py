"""Business-date reconciliation run (scheduled daily, rerunnable, used by backfills).

Safety properties
-----------------
* Identifiable: every run gets a run_id row in reconciliation_run_history (RUNNING -> SUCCEEDED/FAILED).
* Exclusive: a session advisory lock per business date prevents two runs for the same date at once.
* Idempotent: payments are re-evaluated from the full Silver snapshot (history rows are written only on
  material change), control totals for the date are replaced atomically, and quality results are
  keyed by run_id. Re-running a date - including after a partial failure - never duplicates cases
  or inflates totals.
* Deterministic: payments are evaluated in payment_id order using the same rule version and engine
  as the streaming path.

Usage:
    python -m batch.daily_reconciliation                       # previous business date
    python -m batch.daily_reconciliation --business-date 2026-10-27
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

from batch import control_totals as ct
from common.config import BusinessSettings, DatabaseSettings, ReconciliationSettings, ServiceSettings, load
from common.logging_setup import configure_logging
from database.connection import connect
from database.repositories import register_rule_version
from observability import metrics
from processing.case_manager import reconcile_payment
from processing.reconciliation_service import record_outcome_metrics
from processing.rules import ReconciliationRules, load_rules

log = logging.getLogger("batch.daily_reconciliation")

PAYMENTS_IN_WINDOW = """
    SELECT payment_id FROM payment_requests WHERE initiated_at >= %(s)s AND initiated_at < %(e)s
    UNION SELECT payment_id FROM application_payment_events WHERE event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM processor_transactions WHERE event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM payment_refunds WHERE event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM payment_reversals WHERE event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM payment_chargebacks WHERE event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM billing_ledger_entries
          WHERE payment_id IS NOT NULL AND event_time >= %(s)s AND event_time < %(e)s
    UNION SELECT payment_id FROM reconciliation_cases WHERE business_date = %(d)s
    ORDER BY 1
"""


class RunInProgressError(RuntimeError):
    pass


@dataclass
class RunSummary:
    run_id: str
    business_date: date
    run_type: str
    status: str
    payments_evaluated: int = 0
    status_changes: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)
    failed_quality_checks: list[str] = field(default_factory=list)
    comparisons: dict[str, Any] = field(default_factory=dict)
    duration_s: float = 0.0


def business_window(business_date: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """UTC [start, end) of a local business day; correct across DST changes (23h/25h days)."""
    start = datetime.combine(business_date, dtime.min, tzinfo=tz).astimezone(UTC)
    end = datetime.combine(business_date + timedelta(days=1), dtime.min, tzinfo=tz).astimezone(UTC)
    return start, end


def previous_business_date(tz: ZoneInfo, now: datetime | None = None) -> date:
    return ((now or datetime.now(UTC)).astimezone(tz) - timedelta(days=1)).date()


def _lock_key(business_date: date) -> int:
    return int(business_date.strftime("%Y%m%d"))


def run_business_date(
    conn: psycopg.Connection,
    rules: ReconciliationRules,
    tz: ZoneInfo,
    business_date: date,
    run_type: str = "DAILY",
    triggered_by: str = "scheduler",
    chunk_size: int = 200,
    now: datetime | None = None,
) -> RunSummary:
    """Run reconciliation for one business date on a dedicated (non-pooled) connection."""
    started = time.perf_counter()
    run_id = str(uuid.uuid4())
    start, end = business_window(business_date, tz)
    trigger = "BACKFILL" if run_type == "BACKFILL" else "BATCH"

    # Autocommit mode: every `with conn.transaction()` block below is a real, independently committed
    # transaction (evaluation chunks, totals replacement, run status), and the advisory lock is session-level.
    conn.autocommit = True
    got = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", (4242, _lock_key(business_date))).fetchone()
    if not got or not got[0]:
        raise RunInProgressError(f"a reconciliation run for {business_date} is already in progress")
    summary = RunSummary(run_id, business_date, run_type, "RUNNING")
    try:
        with conn.transaction():
            register_rule_version(conn, rules)
            conn.execute(
                """
                INSERT INTO reconciliation_run_history (run_id, run_type, business_date, business_timezone,
                    window_start_utc, window_end_utc, status, rule_version, triggered_by)
                VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING', %s, %s)
                """,
                (run_id, run_type, business_date, str(tz), start, end, rules.rule_version, triggered_by),
            )
        log.info("batch run started", extra={"run_id": run_id, "business_date": str(business_date), "run_type": run_type})
        try:
            payment_ids = [
                r[0] for r in conn.execute(PAYMENTS_IN_WINDOW, {"s": start, "e": end, "d": business_date}).fetchall()
            ]
            for i in range(0, len(payment_ids), chunk_size):
                with conn.transaction():
                    for payment_id in payment_ids[i : i + chunk_size]:
                        outcome = reconcile_payment(conn, payment_id, rules, tz, trigger, run_id=run_id, now=now)
                        summary.payments_evaluated += 1
                        if outcome.changed or outcome.created:
                            summary.status_changes += 1
                            record_outcome_metrics(outcome, trigger)

            totals = ct.compute_control_totals(conn, start, end)
            ct.replace_control_totals(conn, business_date, run_id, totals)
            summary.comparisons = ct.compare_control_totals(totals)

            checks = ct.run_quality_checks(conn, business_date, start, end, rules)
            ct.store_quality_results(conn, run_id, business_date, checks)
            summary.failed_quality_checks = [c.name for c in checks if not c.passed]
            for c in checks:
                if not c.passed:
                    log.warning(
                        "data quality threshold exceeded",
                        extra={"run_id": run_id, "check": c.name, "observed": str(c.observed),
                               "threshold": str(c.threshold), "comparison": c.comparison},
                    )

            counts = conn.execute(
                "SELECT reconciliation_status, count(*) FROM reconciliation_cases WHERE business_date = %s GROUP BY 1",
                (business_date,),
            ).fetchall()
            summary.status_counts = {status: int(n) for status, n in counts}
            summary.status = "SUCCEEDED"
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE reconciliation_run_history SET status = 'SUCCEEDED', finished_at = clock_timestamp(),
                        payments_evaluated = %s, status_changes = %s, status_counts = %s, summary = %s
                    WHERE run_id = %s
                    """,
                    (
                        summary.payments_evaluated,
                        summary.status_changes,
                        Jsonb(summary.status_counts),
                        Jsonb({"control_total_comparisons": summary.comparisons,
                               "failed_quality_checks": summary.failed_quality_checks}),
                        run_id,
                    ),
                )
        except Exception as exc:
            summary.status = "FAILED"
            with conn.transaction():
                conn.execute(
                    "UPDATE reconciliation_run_history SET status = 'FAILED', finished_at = clock_timestamp(), "
                    "payments_evaluated = %s, error_message = %s WHERE run_id = %s",
                    (summary.payments_evaluated, f"{type(exc).__name__}: {str(exc)[:1000]}", run_id),
                )
            raise
    finally:
        summary.duration_s = time.perf_counter() - started
        metrics.BATCH_RUNS.labels(run_type, summary.status).inc()
        metrics.BATCH_RUN_SECONDS.labels(run_type, summary.status).observe(summary.duration_s)
        try:
            conn.execute("SELECT pg_advisory_unlock(%s, %s)", (4242, _lock_key(business_date)))
        except psycopg.Error:
            log.warning("advisory unlock failed; lock is released when the session ends")
        log.info(
            "batch run finished",
            extra={"run_id": run_id, "business_date": str(business_date), "outcome": summary.status,
                   "duration_ms": round(summary.duration_s * 1000), "payments_evaluated": summary.payments_evaluated},
        )
    return summary


def summary_dict(summary: RunSummary) -> dict[str, Any]:
    return {
        "run_id": summary.run_id,
        "business_date": summary.business_date.isoformat(),
        "run_type": summary.run_type,
        "status": summary.status,
        "payments_evaluated": summary.payments_evaluated,
        "status_changes": summary.status_changes,
        "status_counts": dict(Counter(summary.status_counts)),
        "failed_quality_checks": summary.failed_quality_checks,
        "control_total_comparisons": summary.comparisons,
        "duration_seconds": round(summary.duration_s, 3),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run business-date payment reconciliation")
    parser.add_argument("--business-date", type=date.fromisoformat, help="YYYY-MM-DD (default: previous business date)")
    parser.add_argument("--triggered-by", default="cli")
    args = parser.parse_args(argv)

    service = load(ServiceSettings)
    configure_logging(service.service_name or "batch", service.environment, service.log_level)
    business = load(BusinessSettings)
    rules = load_rules(load(ReconciliationSettings).rules_path)
    business_date = args.business_date or previous_business_date(business.tz)
    with connect(load(DatabaseSettings)) as conn:
        summary = run_business_date(conn, rules, business.tz, business_date, "DAILY", args.triggered_by)
    print(json.dumps(summary_dict(summary), indent=2, default=str))  # noqa: T201 - CLI output
    return 0 if summary.status == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
