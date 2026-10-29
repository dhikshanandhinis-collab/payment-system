"""Reconciliation case statuses, categories and transition bookkeeping.

The source transaction's own processing status (e.g. processor CAPTURED, app FAILED) is kept in the
Silver tables. The *reconciliation* status below describes whether the sources agree.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ReconciliationStatus(StrEnum):
    PENDING_MATCH = "PENDING_MATCH"
    REFUND_PENDING = "REFUND_PENDING"
    REVERSAL_PENDING = "REVERSAL_PENDING"
    MATCHED = "MATCHED"
    NO_FINANCIAL_EFFECT = "NO_FINANCIAL_EFFECT"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    STATUS_MISMATCH = "STATUS_MISMATCH"
    MISSING_PROCESSOR_RECORD = "MISSING_PROCESSOR_RECORD"
    MISSING_BILLING_POSTING = "MISSING_BILLING_POSTING"
    MISSING_APPLICATION_RECORD = "MISSING_APPLICATION_RECORD"
    DUPLICATE_FINANCIAL_EFFECT = "DUPLICATE_FINANCIAL_EFFECT"
    DATA_QUALITY_ERROR = "DATA_QUALITY_ERROR"
    UNDER_REVIEW = "UNDER_REVIEW"


S = ReconciliationStatus

PENDING_STATUSES = frozenset({S.PENDING_MATCH, S.REFUND_PENDING, S.REVERSAL_PENDING})
RESOLVED_STATUSES = frozenset({S.MATCHED, S.NO_FINANCIAL_EFFECT})
EXCEPTION_STATUSES = frozenset(
    {
        S.AMOUNT_MISMATCH,
        S.STATUS_MISMATCH,
        S.MISSING_PROCESSOR_RECORD,
        S.MISSING_BILLING_POSTING,
        S.MISSING_APPLICATION_RECORD,
        S.DUPLICATE_FINANCIAL_EFFECT,
        S.DATA_QUALITY_ERROR,
    }
)
MANUAL_STATUSES = frozenset({S.UNDER_REVIEW})

assert PENDING_STATUSES | RESOLVED_STATUSES | EXCEPTION_STATUSES | MANUAL_STATUSES == set(S)


def category(status: ReconciliationStatus) -> str:
    if status in PENDING_STATUSES:
        return "PENDING"
    if status in RESOLVED_STATUSES:
        return "RESOLVED"
    if status in EXCEPTION_STATUSES:
        return "EXCEPTION"
    return "MANUAL"


@dataclass(frozen=True)
class CaseTimestamps:
    exception_created_at: datetime | None
    resolved_at: datetime | None
    resolution_reason: str | None


def apply_transition(
    previous_status: ReconciliationStatus | None,
    new_status: ReconciliationStatus,
    new_reason: str,
    current: CaseTimestamps,
    now: datetime,
) -> CaseTimestamps:
    """Derive exception/resolution timestamps for a status change.

    * exception_created_at is set when a case enters an exception status from a non-exception status
      and is retained afterwards, so a later resolution still shows that an exception existed.
    * resolved_at is set when a case reaches a resolved status and cleared if it later leaves it
      (e.g. a late duplicate capture turns a MATCHED case into an exception).
    """
    exception_created_at = current.exception_created_at
    resolved_at = current.resolved_at
    resolution_reason = current.resolution_reason

    entering_exception = new_status in EXCEPTION_STATUSES and (
        previous_status is None or previous_status not in EXCEPTION_STATUSES
    )
    if entering_exception:
        exception_created_at = now

    if new_status in RESOLVED_STATUSES:
        if previous_status is not None and previous_status in RESOLVED_STATUSES and resolved_at is not None:
            # Still resolved (e.g. FULL_MATCH -> FULLY_REFUNDED): keep the original resolution time.
            if resolution_reason is None or not resolution_reason.startswith("RESOLVED_FROM_"):
                resolution_reason = new_reason
        else:
            resolved_at = now
            if previous_status in EXCEPTION_STATUSES or previous_status in MANUAL_STATUSES:
                resolution_reason = f"RESOLVED_FROM_{previous_status}:{new_reason}"
            else:
                resolution_reason = new_reason
    else:
        resolved_at = None
        resolution_reason = None

    return CaseTimestamps(exception_created_at, resolved_at, resolution_reason)
