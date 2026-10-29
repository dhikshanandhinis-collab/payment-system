"""Reconciliation engine: statuses, financial correctness, timeouts and late-event re-evaluation."""

from __future__ import annotations

from datetime import timedelta

import pytest

from processing.lifecycle import ReconciliationStatus as S
from processing.reconciliation_engine import evaluate
from tests.factories import NOW, OLD, D, app, build_snapshot, ledger, matched_payment, proc

P = "PAY-T-000001"


def run(payloads, rules, now=NOW):
    return evaluate(build_snapshot(P, payloads), now, rules)


def test_empty_snapshot_returns_none(rules):
    assert evaluate(build_snapshot(P, []), NOW, rules) is None


def test_fully_matched_payment(rules):
    r = run(matched_payment(P, "125.50"), rules)
    assert r.status is S.MATCHED and r.reason_code == "FULL_MATCH"
    assert (r.expected_amount, r.processor_amount, r.processor_net_amount, r.billing_amount) == (
        D("125.50"), D("125.50"), D("125.50"), D("125.50"))
    assert r.difference_amount == D("0.00")
    assert r.next_evaluation_at is None
    assert r.source_refs["processor_transaction_ids"] == ["A-T-000001", "C-T-000001"]


def test_event_order_does_not_matter(rules):
    events = matched_payment(P)
    assert run(list(reversed(events)), rules).status is S.MATCHED


def test_processor_amount_mismatch_uses_exact_decimal(rules):
    events = matched_payment(P, "100.00")
    events[2] = proc(P, "CAPTURE", "C-T-000001", "100.01", OLD, original="A-T-000001")
    r = run(events, rules)
    assert r.status is S.AMOUNT_MISMATCH and r.reason_code == "APP_PROCESSOR_AMOUNT_MISMATCH"
    assert r.difference_amount == D("0.01")


def test_ledger_amount_mismatch(rules):
    events = matched_payment(P, "100.00")
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-T-000001", "90.00", OLD)
    r = run(events, rules)
    assert r.status is S.AMOUNT_MISMATCH and r.reason_code == "PROCESSOR_LEDGER_AMOUNT_MISMATCH"
    assert r.difference_amount == D("-10.00")


def test_ledger_adjustment_resolves_mismatch(rules):
    events = matched_payment(P, "100.00")
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-T-000001", "90.00", OLD)
    events.append(ledger(P, "ADJUSTMENT", "L-AJ-1", "10.00", OLD, direction="CREDIT", original="L-T-000001",
                         adjustment_reason="Posting correction"))
    r = run(events, rules)
    assert r.status is S.MATCHED and r.reason_code == "MATCHED_WITH_ADJUSTMENT"


def test_missing_billing_posting_only_after_waiting_period(rules):
    events = matched_payment(P, t0=NOW - timedelta(seconds=30))[:-1]
    pending = run(events, rules)
    assert pending.status is S.PENDING_MATCH and pending.reason_code == "AWAITING_BILLING_POSTING"
    assert pending.next_evaluation_at is not None and pending.next_evaluation_at > NOW
    expired = run(events, rules, now=pending.next_evaluation_at + timedelta(seconds=1))
    assert expired.status is S.MISSING_BILLING_POSTING and expired.reason_code == "PAYMENT_CREDIT_MISSING"
    assert expired.difference_amount == D("-100.00")


def test_late_counterpart_reevaluation_resolves_case(rules):
    events = matched_payment(P)
    assert run(events[:-1], rules).status is S.MISSING_BILLING_POSTING
    assert run(events, rules).status is S.MATCHED


def test_pending_posting_counts_only_after_confirmation(rules):
    events = matched_payment(P, t0=NOW - timedelta(seconds=20))
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-1", "100.00", NOW - timedelta(seconds=10), posting_status="PENDING")
    r = run(events, rules)
    assert r.status is S.PENDING_MATCH and r.reason_code == "AWAITING_POSTING_CONFIRMATION"
    assert r.billing_amount == D("0.00") and r.billing_pending_amount == D("100.00")
    events.append(ledger(P, "POSTING_CONFIRMATION", "L-CF-1", "100.00", NOW, original="L-1"))
    assert run(events, rules).status is S.MATCHED


def test_unconfirmed_posting_becomes_missing_after_wait(rules):
    events = matched_payment(P)
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-1", "100.00", OLD, posting_status="PENDING")
    r = run(events, rules)
    assert r.status is S.MISSING_BILLING_POSTING and r.reason_code == "PAYMENT_CREDIT_UNCONFIRMED"


def test_missing_processor_record(rules):
    events = [app(P, "PAYMENT_INITIATED"), app(P, "PAYMENT_SUCCEEDED"), ledger(P, "PAYMENT_CREDIT", "L-1")]
    r = run(events, rules)
    assert r.status is S.MISSING_PROCESSOR_RECORD and r.reason_code == "PROCESSOR_RECORD_MISSING"
    within = run([app(P, "PAYMENT_INITIATED", t=NOW)], rules)
    assert within.status is S.PENDING_MATCH and within.reason_code == "AWAITING_PROCESSOR_OUTCOME"


def test_waiting_period_depends_on_payment_method(rules):
    t0 = NOW - timedelta(seconds=400)  # beyond card wait (180s), within PAD wait (900s)
    card = run([app(P, "PAYMENT_INITIATED", t=t0, method="CREDIT_CARD")], rules)
    pad = run([app(P, "PAYMENT_INITIATED", t=t0, method="PRE_AUTHORIZED_DEBIT")], rules)
    assert card.status is S.MISSING_PROCESSOR_RECORD
    assert pad.status is S.PENDING_MATCH


def test_authorization_without_capture_is_not_a_capture(rules):
    events = [app(P, "PAYMENT_INITIATED"), proc(P, "AUTHORIZATION", "A-1")]
    r = run(events, rules)
    assert r.processor_amount == D("0.00")
    assert r.status is S.MISSING_PROCESSOR_RECORD and r.reason_code == "CAPTURE_MISSING"


def test_missing_application_record(rules):
    events = [proc(P, "AUTHORIZATION", "A-1"), proc(P, "CAPTURE", "C-1", original="A-1"), ledger(P, "PAYMENT_CREDIT", "L-1")]
    assert run(events, rules).status is S.MISSING_APPLICATION_RECORD
    recent = [proc(P, "CAPTURE", "C-1", t=NOW)]
    assert run(recent, rules).status is S.PENDING_MATCH


def test_consistent_decline_has_no_financial_effect(rules):
    events = [app(P), app(P, "PAYMENT_FAILED"), proc(P, "DECLINE", "D-1")]
    r = run(events, rules)
    assert r.status is S.NO_FINANCIAL_EFFECT and r.reason_code == "DECLINED_CONSISTENT"
    assert r.processor_outcome == "DECLINED" and r.processor_net_amount == D("0.00")


def test_decline_with_app_success_is_status_mismatch(rules):
    r = run([app(P), app(P, "PAYMENT_SUCCEEDED"), proc(P, "DECLINE", "D-1")], rules)
    assert r.status is S.STATUS_MISMATCH and r.reason_code == "APP_SUCCEEDED_BUT_PROCESSOR_DECLINED"


def test_decline_with_ledger_credit_is_status_mismatch(rules):
    r = run([app(P), app(P, "PAYMENT_FAILED"), proc(P, "DECLINE", "D-1"), ledger(P, "PAYMENT_CREDIT", "L-1")], rules)
    assert r.status is S.STATUS_MISMATCH and r.reason_code == "LEDGER_POSTED_FOR_DECLINED_PAYMENT"
    assert r.difference_amount == D("100.00")


def test_app_failed_but_captured(rules):
    events = matched_payment(P)
    events[3] = app(P, "PAYMENT_FAILED")
    assert run(events, rules).status is S.STATUS_MISMATCH


def test_ambiguous_app_result_resolved_by_processor(rules):
    events = matched_payment(P)
    events[3] = app(P, "PAYMENT_TIMEOUT")
    r = run(events, rules)
    assert r.status is S.MATCHED
    assert "APP_OUTCOME_RESOLVED_BY_PROCESSOR" in r.findings


def test_app_failed_without_processor_activity(rules):
    r = run([app(P), app(P, "PAYMENT_FAILED")], rules)
    assert r.status is S.NO_FINANCIAL_EFFECT and r.reason_code == "APP_FAILED_NO_PROCESSOR_ACTIVITY"


def test_full_refund_matched_without_double_counting(rules):
    events = matched_payment(P, "80.00") + [
        proc(P, "REFUND", "R-1", "80.00", OLD + timedelta(minutes=5), original="C-T-000001"),
        ledger(P, "REFUND_DEBIT", "LR-1", "80.00", OLD + timedelta(minutes=6), processor_reference="R-1"),
    ]
    r = run(events + [events[-2]], rules)  # repeated refund delivery collapses to one Silver row
    assert r.status is S.MATCHED and r.reason_code == "FULLY_REFUNDED"
    assert (r.processor_amount, r.refunded_amount, r.processor_net_amount, r.billing_amount) == (
        D("80.00"), D("80.00"), D("0.00"), D("0.00"))


def test_multiple_partial_refunds(rules):
    events = matched_payment(P, "100.00") + [
        proc(P, "REFUND", "R-1", "30.00", OLD + timedelta(minutes=5), original="C-T-000001"),
        proc(P, "REFUND", "R-2", "20.25", OLD + timedelta(minutes=7), original="C-T-000001"),
        ledger(P, "REFUND_DEBIT", "LR-1", "30.00", OLD + timedelta(minutes=6), processor_reference="R-1"),
        ledger(P, "REFUND_DEBIT", "LR-2", "20.25", OLD + timedelta(minutes=8), processor_reference="R-2"),
    ]
    r = run(events, rules)
    assert r.status is S.MATCHED and r.reason_code == "PARTIALLY_REFUNDED"
    assert r.refunded_amount == D("50.25") and r.processor_net_amount == D("49.75") and r.billing_amount == D("49.75")


def test_refund_pending_then_missing(rules):
    t_ref = NOW - timedelta(seconds=30)
    events = matched_payment(P) + [proc(P, "REFUND", "R-1", "40.00", t_ref, original="C-T-000001")]
    pending = run(events, rules)
    assert pending.status is S.REFUND_PENDING and pending.next_evaluation_at == t_ref + rules.refund_posting_grace
    missing = run(events, rules, now=NOW + rules.refund_posting_grace)
    assert missing.status is S.MISSING_BILLING_POSTING and missing.reason_code == "REFUND_POSTING_MISSING"


def test_refund_posted_with_wrong_amount(rules):
    events = matched_payment(P) + [
        proc(P, "REFUND", "R-1", "40.00", OLD, original="C-T-000001"),
        ledger(P, "REFUND_DEBIT", "LR-1", "45.00", OLD, processor_reference="R-1"),
    ]
    r = run(events, rules)
    assert r.status is S.AMOUNT_MISMATCH and r.reason_code == "REFUND_AMOUNT_MISMATCH"
    assert r.difference_amount == D("5.00")


def test_refunds_exceeding_capture(rules):
    events = matched_payment(P, "50.00") + [
        proc(P, "REFUND", "R-1", "30.00", OLD, original="C-T-000001"),
        proc(P, "REFUND", "R-2", "30.00", OLD, original="C-T-000001"),
    ]
    r = run(events, rules)
    assert r.status is S.AMOUNT_MISMATCH and r.reason_code == "REFUNDS_EXCEED_CAPTURE"


def test_reversal_matched(rules):
    events = matched_payment(P, "60.00") + [
        proc(P, "REVERSAL", "V-1", "60.00", OLD + timedelta(minutes=3), original="C-T-000001"),
        ledger(P, "REVERSAL", "LV-1", "60.00", OLD + timedelta(minutes=4), original="L-T-000001", processor_reference="V-1"),
    ]
    r = run(events, rules)
    assert r.status is S.MATCHED and r.reason_code == "REVERSED"
    assert r.reversed_amount == D("60.00") and r.processor_net_amount == D("0.00") and r.billing_amount == D("0.00")


def test_reversal_pending(rules):
    events = matched_payment(P) + [proc(P, "REVERSAL", "V-1", "100.00", NOW - timedelta(seconds=5), original="C-T-000001")]
    assert run(events, rules).status is S.REVERSAL_PENDING


def test_repeated_reversal_is_flagged_and_counted_once(rules):
    events = matched_payment(P) + [
        proc(P, "REVERSAL", "V-1", "100.00", OLD + timedelta(minutes=1), original="C-T-000001"),
        proc(P, "REVERSAL", "V-2", "100.00", OLD + timedelta(minutes=2), original="C-T-000001"),
    ]
    r = run(events, rules)
    assert r.status is S.DUPLICATE_FINANCIAL_EFFECT and r.reason_code == "DUPLICATE_REVERSAL"
    assert r.reversed_amount == D("100.00")


def test_reversal_of_authorization_is_invalid(rules):
    events = matched_payment(P) + [proc(P, "REVERSAL", "V-1", "100.00", OLD, original="A-T-000001")]
    assert run(events, rules).reason_code == "INVALID_REVERSAL_TARGET"


def test_orphan_refund_reference(rules):
    events = matched_payment(P) + [proc(P, "REFUND", "R-1", "10.00", NOW - timedelta(seconds=5), original="C-UNKNOWN")]
    assert run(events, rules).status is S.REFUND_PENDING
    late = run(events, rules, now=NOW + rules.orphan_reference_grace)
    assert late.status is S.DATA_QUALITY_ERROR and late.reason_code == "ORPHAN_REFUND_REFERENCE"


def test_refund_before_capture_waits_for_capture(rules):
    events = [app(P, t=NOW), proc(P, "REFUND", "R-1", "10.00", NOW, original="C-1")]
    assert run(events, rules).status is S.PENDING_MATCH


def test_distinct_duplicate_capture_is_duplicate_financial_effect(rules):
    events = matched_payment(P) + [proc(P, "CAPTURE", "C-DUP", "100.00", OLD, original="A-T-000001")]
    r = run(events, rules)
    assert r.status is S.DUPLICATE_FINANCIAL_EFFECT and r.reason_code == "DUPLICATE_CAPTURE"
    assert r.processor_amount == D("200.00") and r.difference_amount == D("100.00")


def test_duplicate_ledger_credit(rules):
    events = matched_payment(P) + [ledger(P, "PAYMENT_CREDIT", "L-DUP", "100.00", OLD)]
    assert run(events, rules).reason_code == "DUPLICATE_LEDGER_CREDIT"


def test_redelivered_capture_same_id_not_double_counted(rules):
    events = matched_payment(P)
    r = run(events + [events[2], events[4]], rules)
    assert r.status is S.MATCHED and r.processor_amount == D("100.00") and r.billing_amount == D("100.00")


def test_currency_mismatch_is_data_quality_error(rules):
    events = matched_payment(P)
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-1", "100.00", OLD, currency="USD")
    assert run(events, rules).reason_code == "CURRENCY_MISMATCH"


def test_capture_and_decline_conflict(rules):
    events = matched_payment(P) + [proc(P, "DECLINE", "D-1")]
    assert run(events, rules).reason_code == "PROCESSOR_CAPTURE_AND_DECLINE"


def test_rejected_posting_is_not_effective(rules):
    events = matched_payment(P)
    events[4] = ledger(P, "PAYMENT_CREDIT", "L-1", "100.00", OLD, posting_status="REJECTED")
    assert run(events, rules).status is S.MISSING_BILLING_POSTING


@pytest.mark.parametrize("amount", ["0.01", "999999999999.99", "19.99"])
def test_decimal_extremes_match(rules, amount):
    assert run(matched_payment(P, amount), rules).status is S.MATCHED
