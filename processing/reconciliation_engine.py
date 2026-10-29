"""Pure, deterministic reconciliation engine.

``evaluate`` derives the reconciliation status of one payment from *all* records currently known for
that payment. Because the status is recomputed from the full snapshot every time, event arrival
order does not matter and a late event simply triggers another evaluation.

Matching is performed exclusively through the shared ``payment_id`` and explicit transaction
references (``original_processor_transaction_id``, ``processor_reference``,
``original_ledger_entry_id``). Records are never matched on similar amounts or timestamps.

Financial model (all amounts are positive magnitudes, Decimal only):
  gross_captured   = sum of distinct CAPTURE transactions
  total_refunded   = sum of distinct REFUNDs referencing a capture of this payment
  total_reversed   = sum of the first REVERSAL per capture (a processor reversal cancels a capture)
  net_chargeback   = CHARGEBACKs referencing a capture - CHARGEBACK_REVERSALs referencing those chargebacks
  net_captured     = gross_captured - total_refunded - total_reversed - net_chargeback
  billing_amount   = signed sum of *effective* ledger entries for the payment
                     (CREDIT +, DEBIT -). An entry is effective when posting_status is POSTED or a
                     POSTING_CONFIRMATION references it; PENDING and REJECTED entries are excluded.

Waiting periods of bank-settled payment methods elapse on business days only (rules calendar).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from common.money import ZERO, money_sum
from processing.lifecycle import ReconciliationStatus as S
from processing.rules import ReconciliationRules

STATUS_PRECEDENCE = {"INITIATED": 0, "UNKNOWN": 1, "FAILED": 2, "SUCCEEDED": 3}


@dataclass(frozen=True)
class PaymentRequest:
    payment_id: str
    account_ref: str
    amount: Decimal
    currency: str
    payment_method: str
    initiated_at: datetime
    initiated_event_id: str | None


@dataclass(frozen=True)
class AppEvent:
    source_event_id: str
    event_type: str
    source_status: str
    amount: Decimal
    currency: str
    event_time: datetime


@dataclass(frozen=True)
class ProcessorRecord:
    """Processor transaction, refund, reversal or chargeback (``kind`` distinguishes them)."""

    transaction_id: str
    payment_id: str
    kind: str  # AUTHORIZATION | CAPTURE | DECLINE | REFUND | REVERSAL | CHARGEBACK | CHARGEBACK_REVERSAL
    original_transaction_id: str | None
    amount: Decimal
    currency: str
    event_time: datetime


@dataclass(frozen=True)
class LedgerRecord:
    ledger_entry_id: str
    payment_id: str | None
    entry_type: str
    direction: str
    amount: Decimal
    currency: str
    posting_status: str
    original_ledger_entry_id: str | None
    processor_reference: str | None
    event_time: datetime


@dataclass(frozen=True)
class SettlementRecord:
    settlement_id: str
    processor_transaction_id: str
    transaction_type: str
    gross_amount: Decimal
    fee_amount: Decimal
    net_amount: Decimal
    currency: str
    settlement_date: date
    event_time: datetime


@dataclass(frozen=True)
class SettlementOutcome:
    """Cash (settlement-file) reconciliation, reported alongside the operational status."""

    status: str  # NOT_APPLICABLE | PENDING | SETTLED | UNSETTLED | SETTLEMENT_MISMATCH
    reason: str | None = None
    settled_net: Decimal = ZERO
    fees: Decimal = ZERO
    difference: Decimal = ZERO
    next_eval: datetime | None = None


@dataclass(frozen=True)
class PaymentSnapshot:
    payment_id: str
    request: PaymentRequest | None = None
    app_events: tuple[AppEvent, ...] = ()
    processor_records: tuple[ProcessorRecord, ...] = ()
    ledger_records: tuple[LedgerRecord, ...] = ()
    # Processor transactions referenced by this payment's refunds/reversals that belong to *other* payments.
    foreign_references: tuple[ProcessorRecord, ...] = ()
    settlements: tuple[SettlementRecord, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not (self.request or self.app_events or self.processor_records or self.ledger_records
                    or self.settlements)


@dataclass(frozen=True)
class ReconciliationResult:
    payment_id: str
    status: S
    reason_code: str
    rule_version: str
    currency: str | None
    payment_method: str | None
    app_status: str | None
    processor_outcome: str
    expected_amount: Decimal | None
    processor_amount: Decimal
    refunded_amount: Decimal
    reversed_amount: Decimal
    processor_net_amount: Decimal
    billing_amount: Decimal
    billing_pending_amount: Decimal
    difference_amount: Decimal
    reference_time: datetime
    next_evaluation_at: datetime | None
    source_refs: dict[str, list[str]]
    chargeback_amount: Decimal = ZERO
    settlement: SettlementOutcome = field(default_factory=lambda: SettlementOutcome("NOT_APPLICABLE"))
    findings: tuple[str, ...] = field(default_factory=tuple)

    def amounts(self) -> dict[str, Any]:
        def s(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        return {
            "expected_amount": s(self.expected_amount),
            "processor_amount": s(self.processor_amount),
            "refunded_amount": s(self.refunded_amount),
            "reversed_amount": s(self.reversed_amount),
            "chargeback_amount": s(self.chargeback_amount),
            "processor_net_amount": s(self.processor_net_amount),
            "billing_amount": s(self.billing_amount),
            "billing_pending_amount": s(self.billing_pending_amount),
            "difference_amount": s(self.difference_amount),
            "settlement_status": self.settlement.status,
            "settled_net_amount": s(self.settlement.settled_net),
            "settlement_fee_amount": s(self.settlement.fees),
            "settlement_difference_amount": s(self.settlement.difference),
            "currency": self.currency,
        }


def _sum(records: Iterable[Any]) -> Decimal:
    return money_sum(r.amount for r in records)


def latest_app_status(events: Iterable[AppEvent]) -> str | None:
    """Most recent application-reported status; terminal statuses win timestamp ties."""
    best: AppEvent | None = None
    for ev in events:
        if best is None or (ev.event_time, STATUS_PRECEDENCE[ev.source_status]) > (
            best.event_time,
            STATUS_PRECEDENCE[best.source_status],
        ):
            best = ev
    return best.source_status if best else None


class _Decision(Exception):
    def __init__(self, status: S, reason: str, difference: Decimal = ZERO, next_eval: datetime | None = None):
        super().__init__(reason)
        self.status = status
        self.reason = reason
        self.difference = difference
        self.next_eval = next_eval


def evaluate(snapshot: PaymentSnapshot, now: datetime, rules: ReconciliationRules) -> ReconciliationResult | None:
    if snapshot.is_empty:
        return None
    req = snapshot.request
    findings: list[str] = []

    # ---------- processor side ----------
    proc = snapshot.processor_records
    captures = sorted((p for p in proc if p.kind == "CAPTURE"), key=lambda p: (p.event_time, p.transaction_id))
    auths = [p for p in proc if p.kind == "AUTHORIZATION"]
    declines = [p for p in proc if p.kind == "DECLINE"]
    refunds = sorted((p for p in proc if p.kind == "REFUND"), key=lambda p: (p.event_time, p.transaction_id))
    reversals = sorted((p for p in proc if p.kind == "REVERSAL"), key=lambda p: (p.event_time, p.transaction_id))
    own_ids = {p.transaction_id: p for p in proc}
    capture_ids = {c.transaction_id for c in captures}
    foreign = {p.transaction_id: p for p in snapshot.foreign_references}

    valid_refunds = [r for r in refunds if r.original_transaction_id in capture_ids]
    orphan_refunds = [r for r in refunds if r.original_transaction_id not in capture_ids]

    effective_reversals: dict[str, ProcessorRecord] = {}
    duplicate_reversals: list[ProcessorRecord] = []
    orphan_reversals: list[ProcessorRecord] = []
    for rv in reversals:
        target = rv.original_transaction_id or ""
        if target in capture_ids:
            if target in effective_reversals:
                duplicate_reversals.append(rv)
            else:
                effective_reversals[target] = rv
        else:
            orphan_reversals.append(rv)

    gross = _sum(captures)
    refunded = _sum(valid_refunds)
    reversed_ = _sum(effective_reversals.values())

    chargebacks = sorted((p for p in proc if p.kind == "CHARGEBACK"), key=lambda p: (p.event_time, p.transaction_id))
    chargeback_reversals = sorted(
        (p for p in proc if p.kind == "CHARGEBACK_REVERSAL"), key=lambda p: (p.event_time, p.transaction_id)
    )
    valid_chargebacks = [c for c in chargebacks if c.original_transaction_id in capture_ids]
    orphan_chargebacks = [c for c in chargebacks if c.original_transaction_id not in capture_ids]
    chargeback_ids = {c.transaction_id for c in valid_chargebacks}
    valid_cb_reversals = [r for r in chargeback_reversals if r.original_transaction_id in chargeback_ids]
    orphan_cb_reversals = [r for r in chargeback_reversals if r.original_transaction_id not in chargeback_ids]
    net_chargeback = _sum(valid_chargebacks) - _sum(valid_cb_reversals)

    net = gross - refunded - reversed_ - net_chargeback
    settlement = _settle(
        snapshot,
        captures + valid_refunds + list(effective_reversals.values()) + valid_chargebacks + valid_cb_reversals,
        now,
        rules,
    )

    if captures:
        processor_outcome = "CAPTURED"
    elif declines:
        processor_outcome = "DECLINED"
    elif auths:
        processor_outcome = "AUTHORIZED_ONLY"
    elif proc:
        processor_outcome = "ADJUSTMENTS_ONLY"
    else:
        processor_outcome = "NONE"

    # ---------- ledger side ----------
    ledger = [e for e in snapshot.ledger_records if e.entry_type != "CHARGE"]
    confirmed_ids = {e.original_ledger_entry_id for e in ledger if e.entry_type == "POSTING_CONFIRMATION"}
    financial = [e for e in ledger if e.entry_type != "POSTING_CONFIRMATION"]

    def effective(e: LedgerRecord) -> bool:
        if e.posting_status == "REJECTED":
            return False
        return e.posting_status == "POSTED" or e.ledger_entry_id in confirmed_ids

    effective_entries = [e for e in financial if effective(e)]
    pending_entries = [e for e in financial if e.posting_status == "PENDING" and not effective(e)]
    billing_amount = money_sum(e.amount if e.direction == "CREDIT" else -e.amount for e in effective_entries)
    billing_pending = money_sum(e.amount if e.direction == "CREDIT" else -e.amount for e in pending_entries)
    credits = [e for e in financial if e.entry_type == "PAYMENT_CREDIT" and e.posting_status != "REJECTED"]
    effective_credits = [e for e in credits if effective(e)]
    pending_credits = [e for e in credits if not effective(e)]
    adjustments = [e for e in effective_entries if e.entry_type == "ADJUSTMENT"]

    # ---------- reference time / identity ----------
    all_times = [e.event_time for e in snapshot.app_events] + [p.event_time for p in proc] + [
        e.event_time for e in snapshot.ledger_records
    ] + [s.event_time for s in snapshot.settlements]
    if req is not None:
        all_times.append(req.initiated_at)
    reference_time = req.initiated_at if req is not None else min(all_times)
    method = req.payment_method if req is not None else None
    app_status = latest_app_status(snapshot.app_events)
    expected = req.amount if req is not None else None

    currencies = (
        {req.currency} if req else set()
    ) | {e.currency for e in snapshot.app_events} | {p.currency for p in proc} | {e.currency for e in financial} | {
        s.currency for s in snapshot.settlements
    }
    currency = req.currency if req is not None else (sorted(currencies)[0] if currencies else None)

    source_refs = {
        "application_event_ids": sorted(e.source_event_id for e in snapshot.app_events),
        "processor_transaction_ids": sorted(
            p.transaction_id for p in proc if p.kind not in ("REFUND", "REVERSAL", "CHARGEBACK", "CHARGEBACK_REVERSAL")
        ),
        "refund_ids": sorted(r.transaction_id for r in refunds),
        "reversal_ids": sorted(r.transaction_id for r in reversals),
        "chargeback_ids": sorted(c.transaction_id for c in chargebacks + chargeback_reversals),
        "ledger_entry_ids": sorted(e.ledger_entry_id for e in ledger),
    }

    def deadline(start: datetime, grace: timedelta) -> datetime:
        return rules.calendar.add(start, grace, method)

    try:
        # 1. Data quality problems that make any financial conclusion unsafe.
        if len(currencies) > 1:
            raise _Decision(S.DATA_QUALITY_ERROR, "CURRENCY_MISMATCH")
        if req is not None:
            conflicting = [e for e in snapshot.app_events if e.amount != req.amount]
            if conflicting:
                raise _Decision(S.DATA_QUALITY_ERROR, "APP_AMOUNT_CONFLICT", conflicting[0].amount - req.amount)
        for ref in refunds + reversals + chargebacks + chargeback_reversals:
            target = ref.original_transaction_id or ""
            if target in foreign:
                raise _Decision(S.DATA_QUALITY_ERROR, "REFERENCE_PAYMENT_MISMATCH")
            expected_kind = "CHARGEBACK" if ref.kind == "CHARGEBACK_REVERSAL" else "CAPTURE"
            if target in own_ids and own_ids[target].kind != expected_kind:
                raise _Decision(S.DATA_QUALITY_ERROR, f"INVALID_{ref.kind}_TARGET")

        # 2. Duplicate financial effects (distinct identifiers carrying the same economic event).
        if len(captures) > 1:
            raise _Decision(S.DUPLICATE_FINANCIAL_EFFECT, "DUPLICATE_CAPTURE", gross - (expected or gross))
        if len(credits) > 1:
            raise _Decision(S.DUPLICATE_FINANCIAL_EFFECT, "DUPLICATE_LEDGER_CREDIT")
        if duplicate_reversals:
            raise _Decision(S.DUPLICATE_FINANCIAL_EFFECT, "DUPLICATE_REVERSAL", -_sum(duplicate_reversals))

        # 3. Application record missing.
        if req is None:
            due = deadline(reference_time, rules.application_record_grace)
            if now < due:
                raise _Decision(S.PENDING_MATCH, "AWAITING_APPLICATION_RECORD", next_eval=due)
            raise _Decision(S.MISSING_APPLICATION_RECORD, "APPLICATION_RECORD_MISSING", billing_amount or net)

        assert expected is not None
        if captures and declines:
            raise _Decision(S.STATUS_MISMATCH, "PROCESSOR_CAPTURE_AND_DECLINE")

        ledger_has_effect = bool(effective_entries)

        # 4. No capture yet.
        if not captures:
            if declines:
                if ledger_has_effect:
                    raise _Decision(S.STATUS_MISMATCH, "LEDGER_POSTED_FOR_DECLINED_PAYMENT", billing_amount)
                if app_status == "SUCCEEDED":
                    raise _Decision(S.STATUS_MISMATCH, "APP_SUCCEEDED_BUT_PROCESSOR_DECLINED")
                if app_status in ("UNKNOWN", "INITIATED", None):
                    findings.append("APP_OUTCOME_RESOLVED_BY_PROCESSOR")
                raise _Decision(S.NO_FINANCIAL_EFFECT, "DECLINED_CONSISTENT")
            due = deadline(reference_time, rules.processor_grace(method))
            if now < due:
                raise _Decision(S.PENDING_MATCH, "AWAITING_PROCESSOR_OUTCOME", next_eval=due)
            if app_status == "FAILED" and not proc and not ledger_has_effect:
                raise _Decision(S.NO_FINANCIAL_EFFECT, "APP_FAILED_NO_PROCESSOR_ACTIVITY")
            reason = "CAPTURE_MISSING" if auths else "PROCESSOR_RECORD_MISSING"
            raise _Decision(S.MISSING_PROCESSOR_RECORD, reason, -expected)

        capture = captures[0]

        # 5. Application vs processor.
        if gross != expected:
            raise _Decision(S.AMOUNT_MISMATCH, "APP_PROCESSOR_AMOUNT_MISMATCH", gross - expected)
        if app_status == "FAILED":
            raise _Decision(S.STATUS_MISMATCH, "APP_FAILED_BUT_CAPTURED", gross)
        if app_status in ("UNKNOWN", "INITIATED"):
            findings.append("APP_OUTCOME_RESOLVED_BY_PROCESSOR")
        if refunded + reversed_ + net_chargeback > gross:
            reason = "CHARGEBACKS_EXCEED_CAPTURE" if net_chargeback > ZERO else "REFUNDS_EXCEED_CAPTURE"
            raise _Decision(S.AMOUNT_MISMATCH, reason, gross - refunded - reversed_ - net_chargeback)

        # 6. Refund/reversal/chargeback references that are not (yet) resolvable.
        orphan_groups = (
            (orphan_refunds, S.REFUND_PENDING, "ORPHAN_REFUND_REFERENCE"),
            (orphan_reversals, S.REVERSAL_PENDING, "ORPHAN_REVERSAL_REFERENCE"),
            (orphan_chargebacks + orphan_cb_reversals, S.CHARGEBACK_PENDING, "ORPHAN_CHARGEBACK_REFERENCE"),
        )
        orphans = [o for group, _, _ in orphan_groups for o in group]
        if orphans:
            pending_status, code = next((status, c) for group, status, c in orphan_groups if group)
            due = min(deadline(o.event_time, rules.orphan_reference_grace) for o in orphans)
            if now < due:
                raise _Decision(pending_status, "AWAITING_ORIGINAL_TRANSACTION", next_eval=due)
            raise _Decision(S.DATA_QUALITY_ERROR, code, -_sum(orphans))

        # 7. Processor vs billing ledger.
        if not effective_credits:
            due = deadline(capture.event_time, rules.billing_grace(method))
            if now < due:
                reason = "AWAITING_POSTING_CONFIRMATION" if pending_credits else "AWAITING_BILLING_POSTING"
                raise _Decision(S.PENDING_MATCH, reason, next_eval=due)
            reason = "PAYMENT_CREDIT_UNCONFIRMED" if pending_credits else "PAYMENT_CREDIT_MISSING"
            raise _Decision(S.MISSING_BILLING_POSTING, reason, billing_amount - net)

        def counterpart_check(
            items: list[ProcessorRecord], entry_type: str, grace: timedelta, pending: S, label: str
        ) -> None:
            missing_due: list[datetime] = []
            for item in items:
                matches = [
                    e for e in effective_entries if e.entry_type == entry_type and e.processor_reference == item.transaction_id
                ]
                if not matches:
                    missing_due.append(deadline(item.event_time, grace))
                    continue
                posted = _sum(matches)
                if posted != item.amount:
                    raise _Decision(S.AMOUNT_MISMATCH, f"{label}_AMOUNT_MISMATCH", posted - item.amount)
            if missing_due:
                if all(now < d for d in missing_due):
                    raise _Decision(pending, f"AWAITING_{label}_POSTING", next_eval=min(missing_due))
                raise _Decision(S.MISSING_BILLING_POSTING, f"{label}_POSTING_MISSING", billing_amount - net)

        counterpart_check(valid_refunds, "REFUND_DEBIT", rules.refund_posting_grace, S.REFUND_PENDING, "REFUND")
        counterpart_check(
            list(effective_reversals.values()), "REVERSAL", rules.reversal_posting_grace, S.REVERSAL_PENDING, "REVERSAL"
        )
        counterpart_check(
            valid_chargebacks, "CHARGEBACK_DEBIT", rules.chargeback_posting_grace, S.CHARGEBACK_PENDING, "CHARGEBACK"
        )
        counterpart_check(
            valid_cb_reversals, "CHARGEBACK_CREDIT", rules.chargeback_posting_grace, S.CHARGEBACK_PENDING,
            "CHARGEBACK_REVERSAL",
        )

        if billing_amount != net:
            raise _Decision(S.AMOUNT_MISMATCH, "PROCESSOR_LEDGER_AMOUNT_MISMATCH", billing_amount - net)

        if effective_reversals:
            reason = "REVERSED"
        elif valid_chargebacks:
            reason = "CHARGEBACK_REVERSED" if net_chargeback == ZERO else "CHARGED_BACK"
        elif valid_refunds and net == ZERO:
            reason = "FULLY_REFUNDED"
        elif valid_refunds:
            reason = "PARTIALLY_REFUNDED"
        elif adjustments:
            reason = "MATCHED_WITH_ADJUSTMENT"
        else:
            reason = "FULL_MATCH"
        raise _Decision(S.MATCHED, reason)
    except _Decision as decision:
        return ReconciliationResult(
            payment_id=snapshot.payment_id,
            status=decision.status,
            reason_code=decision.reason,
            rule_version=rules.rule_version,
            currency=currency,
            payment_method=method,
            app_status=app_status,
            processor_outcome=processor_outcome,
            expected_amount=expected,
            processor_amount=gross,
            refunded_amount=refunded,
            reversed_amount=reversed_,
            processor_net_amount=net,
            billing_amount=billing_amount,
            billing_pending_amount=billing_pending,
            difference_amount=decision.difference,
            reference_time=reference_time,
            next_evaluation_at=min(
                (t for t in (decision.next_eval, settlement.next_eval) if t is not None), default=None
            ),
            source_refs=source_refs,
            chargeback_amount=net_chargeback,
            settlement=settlement,
            findings=tuple(findings),
        )
    raise AssertionError("unreachable: every path produces a decision")  # pragma: no cover


_SETTLEMENT_SIGN = {"CAPTURE": 1, "CHARGEBACK_REVERSAL": 1, "REFUND": -1, "REVERSAL": -1, "CHARGEBACK": -1}


def _settle(
    snapshot: PaymentSnapshot, expected: list[ProcessorRecord], now: datetime, rules: ReconciliationRules
) -> SettlementOutcome:
    """Every effective processor money movement needs exactly one settlement line with the same gross amount.

    Settlement waits always elapse on bank business days (settlement files are produced on banking days).
    """
    lines = snapshot.settlements
    if not expected and not lines:
        return SettlementOutcome("NOT_APPLICABLE")
    fees = money_sum(line.fee_amount for line in lines)
    settled_net = money_sum(line.net_amount for line in lines)

    def outcome(
        status: str, reason: str | None, difference: Decimal = ZERO, next_eval: datetime | None = None
    ) -> SettlementOutcome:
        return SettlementOutcome(status, reason, settled_net, fees, difference, next_eval)

    known = {p.transaction_id for p in snapshot.processor_records}
    unknown = [line for line in lines if line.processor_transaction_id not in known]
    if unknown:
        due = min(line.event_time + rules.orphan_reference_grace for line in unknown)
        if now < due:
            return outcome("PENDING", "AWAITING_PROCESSOR_TRANSACTION", next_eval=due)
        return outcome("SETTLEMENT_MISMATCH", "SETTLED_TRANSACTION_UNKNOWN", money_sum(line.net_amount for line in unknown))

    by_txn: dict[str, list[SettlementRecord]] = {}
    for line in lines:
        by_txn.setdefault(line.processor_transaction_id, []).append(line)
    expected_ids = {item.transaction_id for item in expected}
    unexpected = [line for line in lines if line.processor_transaction_id not in expected_ids]
    if unexpected:
        return outcome("SETTLEMENT_MISMATCH", "UNEXPECTED_SETTLEMENT", money_sum(line.net_amount for line in unexpected))

    missing: list[ProcessorRecord] = []
    for item in expected:
        found = by_txn.get(item.transaction_id, [])
        if not found:
            missing.append(item)
        elif len(found) > 1:
            return outcome("SETTLEMENT_MISMATCH", "DUPLICATE_SETTLEMENT", money_sum(f.gross_amount for f in found[1:]))
        elif found[0].transaction_type != item.kind:
            return outcome("SETTLEMENT_MISMATCH", "SETTLEMENT_TYPE_MISMATCH")
        elif found[0].gross_amount != item.amount:
            return outcome("SETTLEMENT_MISMATCH", "SETTLEMENT_AMOUNT_MISMATCH", found[0].gross_amount - item.amount)
    if missing:
        dues = [rules.calendar.add(item.event_time, rules.settlement_grace, None, business_days=True) for item in missing]
        if all(now < d for d in dues):
            return outcome("PENDING", "AWAITING_SETTLEMENT", next_eval=min(dues))
        return outcome("UNSETTLED", "SETTLEMENT_MISSING",
                       -money_sum(_SETTLEMENT_SIGN[item.kind] * item.amount for item in missing))
    return outcome("SETTLED", None)
