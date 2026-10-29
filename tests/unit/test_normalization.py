"""Normalization, idempotency keys and money helpers."""

from __future__ import annotations

from decimal import Decimal

import pytest

from common.money import MoneyError, money_sum, to_money
from processing.models import AppPaymentEvent, BillingLedgerEvent, ProcessorTransactionEvent
from processing.normalization import normalize
from tests.factories import app, ledger, proc


def test_app_event_normalization():
    n = normalize(AppPaymentEvent.model_validate(app("PAY-1", amount="10.50")))
    assert n.rows[0].table == "application_payment_events"
    assert n.rows[0].values["amount"] == Decimal("10.50")
    assert n.payment_request is not None and n.payment_request["is_initiation"] is True
    assert n.business_key is None


@pytest.mark.parametrize(
    ("event_type", "table", "key"),
    [("CAPTURE", "processor_transactions", "processor_transaction_id"), ("REFUND", "payment_refunds", "refund_id"),
     ("REVERSAL", "payment_reversals", "reversal_id")],
)
def test_processor_event_routing(event_type, table, key):
    original = "C-0001" if event_type != "CAPTURE" else None
    n = normalize(ProcessorTransactionEvent.model_validate(proc("PAY-1", event_type, "T-0001", original=original)))
    assert n.rows[0].table == table and n.rows[0].values[key] == "T-0001"
    assert n.business_key == "T-0001"


def test_ledger_normalization():
    n = normalize(BillingLedgerEvent.model_validate(ledger("PAY-1", "REFUND_DEBIT", "L-0001", processor_reference="R-0001")))
    assert n.rows[0].table == "billing_ledger_entries"
    assert n.rows[0].values["processor_reference"] == "R-0001"


def test_content_hash_is_stable_and_business_hash_ignores_delivery_fields():
    p1 = proc("PAY-1", "CAPTURE", "C-0001", event_id="EVT-AAAA0001")
    p2 = {**p1, "event_id": "EVT-AAAA0002", "event_time": "2026-10-28T12:00:00.000Z"}
    n1 = normalize(ProcessorTransactionEvent.model_validate(p1))
    n1b = normalize(ProcessorTransactionEvent.model_validate(dict(p1)))
    n2 = normalize(ProcessorTransactionEvent.model_validate(p2))
    assert n1.content_hash == n1b.content_hash
    assert n1.content_hash != n2.content_hash
    assert n1.business_hash == n2.business_hash  # same transaction re-sent under a new event id
    p3 = {**p2, "amount": "99.99"}
    assert normalize(ProcessorTransactionEvent.model_validate(p3)).business_hash != n1.business_hash


def test_money_helpers():
    assert to_money("1.5") == Decimal("1.50")
    assert to_money(7) == Decimal("7.00")
    assert money_sum([Decimal("0.10")] * 3) == Decimal("0.30")
    for bad in (0.1, True, "abc", "1.234", "NaN"):
        with pytest.raises(MoneyError):
            to_money(bad)
