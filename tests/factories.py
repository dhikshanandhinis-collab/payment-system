"""Builders for synthetic source events and engine snapshots used across the test suite."""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from processing.models import AppPaymentEvent, BillingLedgerEvent, ProcessorTransactionEvent
from processing.reconciliation_engine import AppEvent, LedgerRecord, PaymentRequest, PaymentSnapshot, ProcessorRecord

APP_TOPIC = "mybell.payment.events"
PROC_TOPIC = "processor.transaction.events"
LEDGER_TOPIC = "billing.ledger.events"

_counter = itertools.count(1)
NOW = datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
OLD = NOW - timedelta(hours=2)


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _eid(prefix: str = "EVT") -> str:
    return f"{prefix}-TEST-{next(_counter):08d}"


def app(
    payment_id: str,
    event_type: str = "PAYMENT_INITIATED",
    status: str | None = None,
    amount: str = "100.00",
    t: datetime = OLD,
    method: str = "CREDIT_CARD",
    currency: str = "CAD",
    account_ref: str = "SYN-ACCT-00000001",
    event_id: str | None = None,
) -> dict[str, Any]:
    default_status = {
        "PAYMENT_INITIATED": "INITIATED",
        "PAYMENT_SUCCEEDED": "SUCCEEDED",
        "PAYMENT_FAILED": "FAILED",
        "PAYMENT_TIMEOUT": "UNKNOWN",
        "PAYMENT_STATUS_UPDATED": "SUCCEEDED",
    }[event_type]
    return {
        "event_id": event_id or _eid(),
        "event_type": event_type,
        "schema_version": "1.0",
        "source_system": "mybell_app",
        "event_time": iso(t),
        "correlation_id": f"COR-{payment_id}",
        "payment_id": payment_id,
        "account_ref": account_ref,
        "amount": amount,
        "currency": currency,
        "payment_method": method,
        "source_status": status or default_status,
    }


def proc(
    payment_id: str,
    event_type: str,
    txn_id: str,
    amount: str = "100.00",
    t: datetime = OLD,
    original: str | None = None,
    currency: str = "CAD",
    event_id: str | None = None,
) -> dict[str, Any]:
    status = {"AUTHORIZATION": "AUTHORIZED", "CAPTURE": "CAPTURED", "DECLINE": "DECLINED", "REVERSAL": "REVERSED",
              "REFUND": "REFUNDED", "CHARGEBACK": "CHARGED_BACK", "CHARGEBACK_REVERSAL": "CHARGEBACK_REVERSED"}[event_type]
    payload = {
        "event_id": event_id or _eid(),
        "event_type": event_type,
        "schema_version": "1.0",
        "source_system": "payment_processor",
        "event_time": iso(t),
        "correlation_id": f"COR-{payment_id}",
        "processor_transaction_id": txn_id,
        "payment_id": payment_id,
        "amount": amount,
        "currency": currency,
        "processor_status": status,
    }
    if original:
        payload["original_processor_transaction_id"] = original
    return payload


def ledger(
    payment_id: str | None,
    event_type: str,
    entry_id: str,
    amount: str = "100.00",
    t: datetime = OLD,
    direction: str | None = None,
    posting_status: str = "POSTED",
    original: str | None = None,
    processor_reference: str | None = None,
    adjustment_reason: str | None = None,
    currency: str = "CAD",
    account_ref: str = "SYN-ACCT-00000001",
    event_id: str | None = None,
) -> dict[str, Any]:
    default_direction = {"CHARGE": "DEBIT", "PAYMENT_CREDIT": "CREDIT", "REFUND_DEBIT": "DEBIT",
                         "POSTING_CONFIRMATION": "CREDIT", "REVERSAL": "DEBIT", "ADJUSTMENT": "CREDIT",
                         "CHARGEBACK_DEBIT": "DEBIT", "CHARGEBACK_CREDIT": "CREDIT"}[event_type]
    payload: dict[str, Any] = {
        "event_id": event_id or _eid(),
        "event_type": event_type,
        "schema_version": "1.0",
        "source_system": "billing_ledger",
        "event_time": iso(t),
        "correlation_id": f"COR-{payment_id or account_ref}",
        "ledger_entry_id": entry_id,
        "account_ref": account_ref,
        "amount": amount,
        "currency": currency,
        "direction": direction or default_direction,
        "posting_status": posting_status,
    }
    if payment_id:
        payload["payment_id"] = payment_id
    if original:
        payload["original_ledger_entry_id"] = original
    if processor_reference:
        payload["processor_reference"] = processor_reference
    if adjustment_reason:
        payload["adjustment_reason"] = adjustment_reason
    return payload


def matched_payment(pid: str, amount: str = "100.00", t0: datetime = OLD, method: str = "CREDIT_CARD") -> list[dict]:
    s = pid.split("-", 1)[-1]
    return [
        app(pid, "PAYMENT_INITIATED", amount=amount, t=t0, method=method),
        proc(pid, "AUTHORIZATION", f"A-{s}", amount, t0 + timedelta(seconds=1)),
        proc(pid, "CAPTURE", f"C-{s}", amount, t0 + timedelta(seconds=2), original=f"A-{s}"),
        app(pid, "PAYMENT_SUCCEEDED", amount=amount, t=t0 + timedelta(seconds=3), method=method),
        ledger(pid, "PAYMENT_CREDIT", f"L-{s}", amount, t0 + timedelta(seconds=10)),
    ]


def topic_for(payload: dict[str, Any]) -> str:
    return {"mybell_app": APP_TOPIC, "payment_processor": PROC_TOPIC, "billing_ledger": LEDGER_TOPIC}[
        payload["source_system"]
    ]


def encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode()


def build_snapshot(payment_id: str, payloads: list[dict[str, Any]]) -> PaymentSnapshot:
    """Validate payloads with the production models and convert them into an engine snapshot.

    Mirrors Silver semantics: the first PAYMENT_INITIATED is authoritative for the request, duplicate
    processor/ledger transaction ids are collapsed.
    """
    request: PaymentRequest | None = None
    first_app: AppPaymentEvent | None = None
    app_events: list[AppEvent] = []
    proc_records: dict[str, ProcessorRecord] = {}
    ledger_records: dict[str, LedgerRecord] = {}
    for p in payloads:
        if p["source_system"] == "mybell_app":
            e = AppPaymentEvent.model_validate(p)
            app_events.append(AppEvent(e.event_id, e.event_type.value, e.source_status.value, e.amount, e.currency, e.event_time))
            if first_app is None:
                first_app = e
            if e.event_type.value == "PAYMENT_INITIATED" and (request is None or request.initiated_event_id is None):
                request = PaymentRequest(e.payment_id, e.account_ref, e.amount, e.currency, e.payment_method.value,
                                         e.event_time, e.event_id)
        elif p["source_system"] == "payment_processor":
            e2 = ProcessorTransactionEvent.model_validate(p)
            proc_records.setdefault(
                e2.processor_transaction_id,
                ProcessorRecord(e2.processor_transaction_id, e2.payment_id, e2.event_type.value,
                                e2.original_processor_transaction_id, e2.amount, e2.currency, e2.event_time),
            )
        else:
            e3 = BillingLedgerEvent.model_validate(p)
            ledger_records.setdefault(
                e3.ledger_entry_id,
                LedgerRecord(e3.ledger_entry_id, e3.payment_id, e3.event_type.value, e3.direction.value, e3.amount,
                             e3.currency, e3.posting_status.value, e3.original_ledger_entry_id, e3.processor_reference,
                             e3.event_time),
            )
    if request is None and first_app is not None:
        request = PaymentRequest(first_app.payment_id, first_app.account_ref, first_app.amount, first_app.currency,
                                 first_app.payment_method.value, first_app.event_time, None)
    return PaymentSnapshot(payment_id, request, tuple(app_events), tuple(proc_records.values()),
                           tuple(ledger_records.values()))


def D(value: str) -> Decimal:  # noqa: N802 - short helper for readability in assertions
    return Decimal(value)
