"""Source control totals and data-quality checks for one business-date window.

Control totals are reported per source and are NOT financially interchangeable:
  * mybell_app.PAYMENT_REQUESTS       - amounts customers asked to pay (intent, not money movement)
  * payment_processor.CAPTURES        - money actually captured by the processor
  * payment_processor.REFUNDS/REVERSALS - money returned / captures cancelled
  * billing_ledger.PAYMENT_CREDITS_EFFECTIVE - credits effectively posted to customer accounts
Comparisons between them are explanatory: declines, refunds, pending postings and day-boundary
timing legitimately create differences. Per-payment correctness is established by the case-level
reconciliation, not by these aggregates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from processing.rules import ReconciliationRules

CONTROL_TOTAL_QUERIES: dict[tuple[str, str], str] = {
    ("mybell_app", "PAYMENT_REQUESTS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM payment_requests
        WHERE initiated_at >= %(s)s AND initiated_at < %(e)s GROUP BY currency""",
    ("mybell_app", "APP_EVENTS"): """
        SELECT currency, count(*), 0 FROM application_payment_events
        WHERE event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("mybell_app", "APP_REPORTED_SUCCEEDED"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM (
            SELECT DISTINCT ON (payment_id) payment_id, currency, amount FROM application_payment_events
            WHERE event_time >= %(s)s AND event_time < %(e)s AND source_status = 'SUCCEEDED'
            ORDER BY payment_id, event_time) x GROUP BY currency""",
    ("payment_processor", "AUTHORIZATIONS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM processor_transactions
        WHERE transaction_type = 'AUTHORIZATION' AND event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("payment_processor", "CAPTURES"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM processor_transactions
        WHERE transaction_type = 'CAPTURE' AND event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("payment_processor", "DECLINES"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM processor_transactions
        WHERE transaction_type = 'DECLINE' AND event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("payment_processor", "REFUNDS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM payment_refunds
        WHERE event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("payment_processor", "REVERSALS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM payment_reversals
        WHERE event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("billing_ledger", "PAYMENT_CREDITS_EFFECTIVE"): """
        SELECT l.currency, count(*), coalesce(sum(l.amount), 0) FROM billing_ledger_entries l
        WHERE l.entry_type = 'PAYMENT_CREDIT' AND l.posting_status <> 'REJECTED'
          AND (l.posting_status = 'POSTED' OR EXISTS (SELECT 1 FROM billing_ledger_entries c
               WHERE c.entry_type = 'POSTING_CONFIRMATION' AND c.original_ledger_entry_id = l.ledger_entry_id))
          AND l.event_time >= %(s)s AND l.event_time < %(e)s GROUP BY l.currency""",
    ("billing_ledger", "PAYMENT_CREDITS_PENDING"): """
        SELECT l.currency, count(*), coalesce(sum(l.amount), 0) FROM billing_ledger_entries l
        WHERE l.entry_type = 'PAYMENT_CREDIT' AND l.posting_status = 'PENDING'
          AND NOT EXISTS (SELECT 1 FROM billing_ledger_entries c
               WHERE c.entry_type = 'POSTING_CONFIRMATION' AND c.original_ledger_entry_id = l.ledger_entry_id)
          AND l.event_time >= %(s)s AND l.event_time < %(e)s GROUP BY l.currency""",
    ("billing_ledger", "REFUND_DEBITS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM billing_ledger_entries
        WHERE entry_type = 'REFUND_DEBIT' AND posting_status = 'POSTED' AND event_time >= %(s)s AND event_time < %(e)s
        GROUP BY currency""",
    ("billing_ledger", "REVERSAL_DEBITS"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM billing_ledger_entries
        WHERE entry_type = 'REVERSAL' AND posting_status = 'POSTED' AND event_time >= %(s)s AND event_time < %(e)s
        GROUP BY currency""",
    ("billing_ledger", "ADJUSTMENTS_NET"): """
        SELECT currency, count(*), coalesce(sum(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)
        FROM billing_ledger_entries WHERE entry_type = 'ADJUSTMENT' AND posting_status = 'POSTED'
          AND event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
    ("billing_ledger", "CHARGES"): """
        SELECT currency, count(*), coalesce(sum(amount), 0) FROM billing_ledger_entries
        WHERE entry_type = 'CHARGE' AND event_time >= %(s)s AND event_time < %(e)s GROUP BY currency""",
}

# Bronze delivery metrics use ingestion_time because invalid events may have no usable event_time.
BRONZE_QUERY = """
    SELECT source_system, processing_status, count(*) FROM raw_payment_events
    WHERE ingestion_time >= %(s)s AND ingestion_time < %(e)s GROUP BY source_system, processing_status
"""


@dataclass(frozen=True)
class ControlTotal:
    source_system: str
    metric_name: str
    currency: str
    record_count: int
    amount: Decimal


def compute_control_totals(conn: psycopg.Connection, start: datetime, end: datetime) -> list[ControlTotal]:
    params = {"s": start, "e": end}
    totals: list[ControlTotal] = []
    for (source, metric), query in CONTROL_TOTAL_QUERIES.items():
        for currency, count, amount in conn.execute(query, params).fetchall():
            totals.append(ControlTotal(source, metric, currency, int(count), Decimal(amount)))
    for source, status, count in conn.execute(BRONZE_QUERY, params).fetchall():
        totals.append(ControlTotal(source, f"RAW_{status}", "XXX", int(count), Decimal("0.00")))
    return totals


def replace_control_totals(conn: psycopg.Connection, business_date: Any, run_id: str, totals: list[ControlTotal]) -> None:
    """Atomically replace the date's totals so reruns never inflate them."""
    with conn.transaction():
        conn.execute("DELETE FROM source_control_totals WHERE business_date = %s", (business_date,))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO source_control_totals (business_date, source_system, metric_name, currency, record_count, "
                "amount, run_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [(business_date, t.source_system, t.metric_name, t.currency, t.record_count, t.amount, run_id) for t in totals],
            )


def compare_control_totals(totals: list[ControlTotal]) -> dict[str, Any]:
    def amount(source: str, metric: str) -> Decimal:
        return sum((t.amount for t in totals if t.source_system == source and t.metric_name == metric), Decimal("0.00"))

    def count(source: str, metric: str) -> int:
        return sum(t.record_count for t in totals if t.source_system == source and t.metric_name == metric)

    captures = amount("payment_processor", "CAPTURES")
    credits = amount("billing_ledger", "PAYMENT_CREDITS_EFFECTIVE")
    return {
        "app_requests_vs_processor_captures": {
            "app_request_count": count("mybell_app", "PAYMENT_REQUESTS"),
            "app_request_amount": str(amount("mybell_app", "PAYMENT_REQUESTS")),
            "capture_count": count("payment_processor", "CAPTURES"),
            "capture_amount": str(captures),
            "decline_amount": str(amount("payment_processor", "DECLINES")),
            "note": "Requests include declined/failed payments; difference is not by itself an error.",
        },
        "processor_captures_vs_ledger_credits": {
            "capture_amount": str(captures),
            "effective_credit_amount": str(credits),
            "pending_credit_amount": str(amount("billing_ledger", "PAYMENT_CREDITS_PENDING")),
            "difference": str(credits - captures),
            "note": "Timing at day boundaries and pending postings create expected differences.",
        },
        "refunds_vs_ledger_refund_debits": {
            "processor_refund_amount": str(amount("payment_processor", "REFUNDS")),
            "ledger_refund_debit_amount": str(amount("billing_ledger", "REFUND_DEBITS")),
        },
        "reversals_vs_ledger_reversal_debits": {
            "processor_reversal_amount": str(amount("payment_processor", "REVERSALS")),
            "ledger_reversal_amount": str(amount("billing_ledger", "REVERSAL_DEBITS")),
        },
    }


@dataclass(frozen=True)
class QualityCheck:
    name: str
    observed: Decimal | None
    threshold: Decimal
    comparison: str
    numerator: int
    denominator: int

    @property
    def passed(self) -> bool:
        if self.observed is None:
            return True  # no data -> nothing to fail
        return self.observed <= self.threshold if self.comparison == "<=" else self.observed >= self.threshold


def _ratio(num: int, den: int) -> Decimal | None:
    return None if den == 0 else (Decimal(num) / Decimal(den)).quantize(Decimal("0.000001"))


def run_quality_checks(
    conn: psycopg.Connection, business_date: Any, start: datetime, end: datetime, rules: ReconciliationRules
) -> list[QualityCheck]:
    t = rules.dq_thresholds
    p = {"s": start, "e": end, "d": business_date, "late": int(rules.late_event_threshold.total_seconds())}
    raw = conn.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE processing_status = 'QUARANTINED'),
               count(*) FILTER (WHERE processing_status IN ('DUPLICATE_DELIVERY', 'DUPLICATE_BUSINESS_EVENT')),
               count(*) FILTER (WHERE processing_status <> 'QUARANTINED' AND event_time IS NOT NULL),
               count(*) FILTER (WHERE processing_status <> 'QUARANTINED' AND event_time IS NOT NULL
                                  AND ingestion_time - event_time > make_interval(secs => %(late)s))
        FROM raw_payment_events WHERE ingestion_time >= %(s)s AND ingestion_time < %(e)s
        """,
        p,
    ).fetchone()
    total, quarantined, duplicates, valid, late = (int(x) for x in raw)  # type: ignore[union-attr]
    cases = conn.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE reconciliation_status IN ('AMOUNT_MISMATCH', 'STATUS_MISMATCH',
                   'MISSING_PROCESSOR_RECORD', 'MISSING_BILLING_POSTING', 'MISSING_APPLICATION_RECORD',
                   'DUPLICATE_FINANCIAL_EFFECT', 'DATA_QUALITY_ERROR', 'UNDER_REVIEW'))
        FROM reconciliation_cases WHERE business_date = %(d)s
        """,
        p,
    ).fetchone()
    case_total, open_exceptions = (int(x) for x in cases)  # type: ignore[union-attr]
    comp = conn.execute(
        """
        SELECT count(*) FILTER (WHERE expected_amount IS NOT NULL AND reason_code <> 'APP_FAILED_NO_PROCESSOR_ACTIVITY'),
               count(*) FILTER (WHERE expected_amount IS NOT NULL AND processor_outcome IN ('CAPTURED', 'DECLINED')),
               count(*) FILTER (WHERE processor_outcome = 'CAPTURED'),
               count(*) FILTER (WHERE processor_outcome = 'CAPTURED'
                                  AND jsonb_array_length(source_refs -> 'ledger_entry_ids') > 0)
        FROM reconciliation_cases WHERE business_date = %(d)s
        """,
        p,
    ).fetchone()
    app_den, proc_num, cap_den, ledger_num = (int(x) for x in comp)  # type: ignore[union-attr]
    return [
        QualityCheck("quarantine_rate", _ratio(quarantined, total), t["max_quarantine_rate"], "<=", quarantined, total),
        QualityCheck(
            "duplicate_delivery_rate", _ratio(duplicates, total), t["max_duplicate_delivery_rate"], "<=", duplicates, total
        ),
        QualityCheck("late_event_rate", _ratio(late, valid), t["max_late_event_rate"], "<=", late, valid),
        QualityCheck(
            "open_exception_rate", _ratio(open_exceptions, case_total), t["max_open_exception_rate"], "<=",
            open_exceptions, case_total,
        ),
        QualityCheck(
            "processor_completeness", _ratio(proc_num, app_den), t["min_source_completeness"], ">=", proc_num, app_den
        ),
        QualityCheck(
            "ledger_completeness", _ratio(ledger_num, cap_den), t["min_source_completeness"], ">=", ledger_num, cap_den
        ),
    ]


def store_quality_results(conn: psycopg.Connection, run_id: str, business_date: Any, checks: list[QualityCheck]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO data_quality_results (run_id, business_date, check_name, observed_value, threshold, comparison, "
            "passed, numerator, denominator, details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (run_id, business_date, c.name, c.observed, c.threshold, c.comparison, c.passed, c.numerator,
                 c.denominator, Jsonb({}))
                for c in checks
            ],
        )
