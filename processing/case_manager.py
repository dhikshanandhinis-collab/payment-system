"""Persisting reconciliation outcomes: case row locking, lifecycle timestamps, audit history and outbox.

Concurrency control: every evaluation runs inside the caller's transaction and holds a row lock on
the payment's case (``SELECT ... FOR UPDATE``). The case row is created with
``INSERT ... ON CONFLICT DO NOTHING`` on the unique ``payment_id``, so concurrent workers, the
timeout sweeper, batch jobs and admin actions serialize per payment and can never create two cases
or interleave conflicting outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import psycopg

from database import repositories as repo
from processing.lifecycle import CaseTimestamps, ReconciliationStatus, apply_transition
from processing.reconciliation_engine import ReconciliationResult, evaluate
from processing.rules import ReconciliationRules

AMOUNT_FIELDS = (
    "expected_amount",
    "processor_amount",
    "refunded_amount",
    "reversed_amount",
    "processor_net_amount",
    "billing_amount",
    "billing_pending_amount",
    "difference_amount",
)


@dataclass(frozen=True)
class ReconcileOutcome:
    payment_id: str
    case_id: str | None
    previous_status: str | None
    new_status: str | None
    reason_code: str | None
    created: bool
    changed: bool


def business_date(ts: datetime, tz: ZoneInfo) -> date:
    return ts.astimezone(tz).date()


def _material_change(case: dict[str, Any], result: ReconciliationResult, effective: str) -> bool:
    if case["reconciliation_status"] != effective or case["computed_status"] != result.status.value:
        return True
    if case["reason_code"] != result.reason_code or case["currency"] != result.currency:
        return True
    return any(case[f] != getattr(result, f) for f in AMOUNT_FIELDS)


def reconcile_payment(
    conn: psycopg.Connection,
    payment_id: str,
    rules: ReconciliationRules,
    tz: ZoneInfo,
    trigger_type: str,
    run_id: str | None = None,
    actor: str = "system",
    note: str | None = None,
    now: datetime | None = None,
    force_history: bool = False,
    review_hold_override: bool | None = None,
) -> ReconcileOutcome:
    now = now or datetime.now(UTC)
    with conn.cursor() as cur:
        case = repo.lock_case(cur, payment_id)
        created = False
        if case is None:
            probe = evaluate(repo.load_snapshot(cur, payment_id), now, rules)
            if probe is None:
                return ReconcileOutcome(payment_id, None, None, None, None, False, False)
            repo.insert_case_placeholder(
                cur, payment_id, business_date(probe.reference_time, tz), probe.reference_time, rules.rule_version
            )
            case = repo.lock_case(cur, payment_id)
            assert case is not None
            created = case["evaluation_count"] == 0

        # Reload after acquiring the lock so the evaluation sees every committed record.
        result = evaluate(repo.load_snapshot(cur, payment_id), now, rules)
        if result is None:
            return ReconcileOutcome(payment_id, str(case["reconciliation_case_id"]), None, None, None, False, False)

        previous_status = None if created else case["reconciliation_status"]
        hold = case["review_hold"] if review_hold_override is None else review_hold_override
        effective = ReconciliationStatus.UNDER_REVIEW if hold else result.status
        timestamps = apply_transition(
            ReconciliationStatus(previous_status) if previous_status else None,
            effective,
            result.reason_code,
            CaseTimestamps(case["exception_created_at"], case["resolved_at"], case["resolution_reason"]),
            now,
        )
        material = created or force_history or _material_change(case, result, effective.value)

        values: dict[str, Any] = {
            "business_date": business_date(result.reference_time, tz),
            "payment_method": result.payment_method,
            "currency": result.currency,
            "reconciliation_status": effective.value,
            "computed_status": result.status.value,
            "reason_code": result.reason_code,
            "app_status": result.app_status,
            "processor_outcome": result.processor_outcome,
            "source_refs": result.source_refs,
            "findings": list(result.findings),
            "reference_time": result.reference_time,
            "last_evaluated_at": now,
            "next_evaluation_at": result.next_evaluation_at,
            "exception_created_at": timestamps.exception_created_at,
            "resolved_at": timestamps.resolved_at,
            "resolution_reason": timestamps.resolution_reason,
            "rule_version": result.rule_version,
            "review_hold": hold,
            "evaluation_count": case["evaluation_count"] + 1,
        }
        values.update({f: getattr(result, f) for f in AMOUNT_FIELDS})
        repo.update_case(cur, case["reconciliation_case_id"], values)

        if material:
            repo.insert_history(
                cur,
                {
                    "reconciliation_case_id": case["reconciliation_case_id"],
                    "payment_id": payment_id,
                    "previous_status": previous_status,
                    "new_status": effective.value,
                    "previous_reason_code": None if created else case["reason_code"],
                    "reason_code": result.reason_code,
                    "computed_status": result.status.value,
                    "rule_version": result.rule_version,
                    "trigger_type": trigger_type,
                    "run_id": run_id,
                    "actor": actor,
                    "source_refs": result.source_refs,
                    "amounts": result.amounts(),
                    "findings": list(result.findings),
                    "note": note,
                    "evaluated_at": now,
                },
            )
            repo.insert_outbox(
                cur,
                case["reconciliation_case_id"],
                payment_id,
                "RECONCILIATION_CASE_UPDATED",
                {
                    "schema_version": "1.0",
                    "reconciliation_case_id": str(case["reconciliation_case_id"]),
                    "payment_id": payment_id,
                    "previous_status": previous_status,
                    "reconciliation_status": effective.value,
                    "reason_code": result.reason_code,
                    "rule_version": result.rule_version,
                    "business_date": values["business_date"].isoformat(),
                    "amounts": result.amounts(),
                    "trigger_type": trigger_type,
                    "evaluated_at": now.isoformat(),
                },
            )
        return ReconcileOutcome(
            payment_id,
            str(case["reconciliation_case_id"]),
            previous_status,
            effective.value,
            result.reason_code,
            created,
            material and previous_status != effective.value,
        )
