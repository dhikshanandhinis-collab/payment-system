"""Status taxonomy and lifecycle timestamp transitions."""

from __future__ import annotations

from datetime import timedelta

from processing.lifecycle import (
    EXCEPTION_STATUSES,
    PENDING_STATUSES,
    RESOLVED_STATUSES,
    CaseTimestamps,
    apply_transition,
    category,
)
from processing.lifecycle import (
    ReconciliationStatus as S,
)
from tests.factories import NOW

EMPTY = CaseTimestamps(None, None, None)


def test_required_statuses_exist():
    required = {"PENDING_MATCH", "MATCHED", "AMOUNT_MISMATCH", "STATUS_MISMATCH", "MISSING_PROCESSOR_RECORD",
                "MISSING_BILLING_POSTING", "MISSING_APPLICATION_RECORD", "REFUND_PENDING", "REVERSAL_PENDING",
                "DATA_QUALITY_ERROR", "UNDER_REVIEW"}
    assert required <= {s.value for s in S}
    assert not (PENDING_STATUSES & RESOLVED_STATUSES) and not (EXCEPTION_STATUSES & RESOLVED_STATUSES)
    assert category(S.UNDER_REVIEW) == "MANUAL"


def test_pending_to_exception_sets_exception_time():
    ts = apply_transition(S.PENDING_MATCH, S.MISSING_BILLING_POSTING, "PAYMENT_CREDIT_MISSING", EMPTY, NOW)
    assert ts.exception_created_at == NOW and ts.resolved_at is None


def test_exception_to_matched_records_resolution_and_keeps_exception_time():
    start = CaseTimestamps(NOW - timedelta(hours=1), None, None)
    ts = apply_transition(S.MISSING_BILLING_POSTING, S.MATCHED, "FULL_MATCH", start, NOW)
    assert ts.exception_created_at == start.exception_created_at
    assert ts.resolved_at == NOW
    assert ts.resolution_reason == "RESOLVED_FROM_MISSING_BILLING_POSTING:FULL_MATCH"


def test_exception_to_other_exception_keeps_original_time():
    start = CaseTimestamps(NOW - timedelta(hours=1), None, None)
    ts = apply_transition(S.MISSING_BILLING_POSTING, S.AMOUNT_MISMATCH, "X", start, NOW)
    assert ts.exception_created_at == start.exception_created_at


def test_matched_case_reopened_clears_resolution():
    start = CaseTimestamps(None, NOW - timedelta(hours=1), "FULL_MATCH")
    ts = apply_transition(S.MATCHED, S.DUPLICATE_FINANCIAL_EFFECT, "DUPLICATE_CAPTURE", start, NOW)
    assert ts.resolved_at is None and ts.resolution_reason is None and ts.exception_created_at == NOW


def test_resolved_to_resolved_keeps_resolution_time():
    start = CaseTimestamps(None, NOW - timedelta(hours=1), "FULL_MATCH")
    ts = apply_transition(S.MATCHED, S.MATCHED, "FULLY_REFUNDED", start, NOW)
    assert ts.resolved_at == start.resolved_at and ts.resolution_reason == "FULLY_REFUNDED"


def test_new_case_directly_matched():
    ts = apply_transition(None, S.MATCHED, "FULL_MATCH", EMPTY, NOW)
    assert ts.resolved_at == NOW and ts.resolution_reason == "FULL_MATCH" and ts.exception_created_at is None
