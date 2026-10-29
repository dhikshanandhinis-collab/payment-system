"""Envelope decoding, schema validation and error classification."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from common.errors import ErrorCategory
from processing.models import AppPaymentEvent, BillingLedgerEvent, ProcessorTransactionEvent
from processing.validation import (
    DecodeError,
    TimestampRules,
    ValidationFailure,
    decode_payload,
    extract_metadata,
    validate_event,
)
from tests.factories import NOW, app, iso, ledger, proc

RULES = TimestampRules(max_future_skew_s=300, max_age_days=400)


def check(payload, source="mybell_app"):
    return validate_event(payload, source, NOW, RULES)


def test_valid_events_for_each_source():
    assert isinstance(check(app("PAY-1")), AppPaymentEvent)
    assert isinstance(check(proc("PAY-1", "CAPTURE", "C-0001"), "payment_processor"), ProcessorTransactionEvent)
    assert isinstance(check(ledger("PAY-1", "PAYMENT_CREDIT", "L-0001"), "billing_ledger"), BillingLedgerEvent)
    charge = ledger(None, "CHARGE", "L-CH-0001")
    assert isinstance(check(charge, "billing_ledger"), BillingLedgerEvent)


def test_decode_parses_numbers_as_decimal_never_float():
    payload = decode_payload(b'{"amount": 0.1, "n": 3}')
    assert isinstance(payload["amount"], Decimal) and payload["amount"] == Decimal("0.1")


@pytest.mark.parametrize(
    ("raw", "code"),
    [(b"", "EMPTY_PAYLOAD"), (b"\xff\xfe", "INVALID_ENCODING"), (b"{bad", "INVALID_JSON"), (b"[1,2]", "NOT_AN_OBJECT"),
     (b'{"a": NaN}', "INVALID_JSON")],
)
def test_decode_errors(raw, code):
    with pytest.raises(DecodeError) as exc:
        decode_payload(raw)
    assert exc.value.error_code == code


def test_unsupported_schema_version_is_classified_separately():
    payload = app("PAY-1")
    payload["schema_version"] = "2.0"
    result = check(payload)
    assert isinstance(result, ValidationFailure)
    assert result.category is ErrorCategory.UNSUPPORTED_SCHEMA and result.error_code == "UNSUPPORTED_SCHEMA_VERSION"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda p: p.pop("amount"), "MISSING_REQUIRED_FIELD"),
        (lambda p: p.update(amount="-5.00"), "INVALID_AMOUNT"),
        (lambda p: p.update(amount="0.00"), "INVALID_AMOUNT"),
        (lambda p: p.update(amount="12.345"), "INVALID_AMOUNT"),
        (lambda p: p.update(amount=12.5), "INVALID_AMOUNT"),
        (lambda p: p.update(currency="C$D"), "INVALID_CURRENCY"),
        (lambda p: p.update(currency="XYZ"), "INVALID_CURRENCY"),
        (lambda p: p.update(event_type="SOMETHING"), "INVALID_EVENT_TYPE"),
        (lambda p: p.update(event_time="2026-10-28T10:00:00"), "INVALID_TIMESTAMP"),
        (lambda p: p.update(extra_field="x"), "UNKNOWN_FIELD"),
        (lambda p: p.update(source_status="SUCCEEDED"), "BUSINESS_RULE_VIOLATION"),
        (lambda p: p.update(source_system="payment_processor"), "SOURCE_TOPIC_MISMATCH"),
        (lambda p: p.pop("schema_version"), "MISSING_REQUIRED_FIELD"),
    ],
)
def test_invalid_application_payloads(mutate, code):
    payload = app("PAY-1")
    mutate(payload)
    result = check(payload)
    assert isinstance(result, ValidationFailure), result
    assert result.error_code == code
    assert "SYN-ACCT" not in result.detail  # input values are not echoed into error details


def test_timestamp_reasonableness():
    future = app("PAY-1", t=NOW + timedelta(hours=1))
    old = app("PAY-1", t=NOW - timedelta(days=500))
    assert check(future).error_code == "TIMESTAMP_IN_FUTURE"
    assert check(old).error_code == "TIMESTAMP_TOO_OLD"
    assert isinstance(check(app("PAY-1", t=NOW + timedelta(seconds=60))), AppPaymentEvent)


def test_processor_rules():
    refund = proc("PAY-1", "REFUND", "R-0001")
    assert check(refund, "payment_processor").error_code == "BUSINESS_RULE_VIOLATION"  # original required
    auth = proc("PAY-1", "AUTHORIZATION", "A-0001", original="X-0001")
    assert check(auth, "payment_processor").error_code == "BUSINESS_RULE_VIOLATION"
    wrong_status = proc("PAY-1", "CAPTURE", "C-0001")
    wrong_status["processor_status"] = "AUTHORIZED"
    assert check(wrong_status, "payment_processor").error_code == "BUSINESS_RULE_VIOLATION"


def test_ledger_rules():
    bad_direction = ledger("PAY-1", "PAYMENT_CREDIT", "L-0001", direction="DEBIT")
    assert check(bad_direction, "billing_ledger").error_code == "BUSINESS_RULE_VIOLATION"
    no_payment = ledger(None, "PAYMENT_CREDIT", "L-0002")
    assert check(no_payment, "billing_ledger").error_code == "BUSINESS_RULE_VIOLATION"
    refund_without_ref = ledger("PAY-1", "REFUND_DEBIT", "L-0003")
    assert check(refund_without_ref, "billing_ledger").error_code == "BUSINESS_RULE_VIOLATION"
    adjustment = ledger("PAY-1", "ADJUSTMENT", "L-0004")
    assert check(adjustment, "billing_ledger").error_code == "BUSINESS_RULE_VIOLATION"
    confirmation = ledger("PAY-1", "POSTING_CONFIRMATION", "L-0005", original="L-0001", posting_status="PENDING")
    assert check(confirmation, "billing_ledger").error_code == "BUSINESS_RULE_VIOLATION"


def test_event_time_normalized_to_utc():
    payload = app("PAY-1")
    payload["event_time"] = "2026-10-28T09:00:00-04:00"
    event = check(payload)
    assert event.event_time.utcoffset() == timedelta(0) and event.event_time.hour == 13


def test_metadata_extraction_is_best_effort():
    assert extract_metadata(None).source_event_id is None
    meta = extract_metadata({"event_id": "x" * 200, "payment_id": 5, "event_time": "garbage"})
    assert meta.source_event_id is None and meta.payment_id is None and meta.event_time is None
    good = extract_metadata(app("PAY-9"))
    assert good.payment_id == "PAY-9" and good.event_time is not None


def test_iso_helper_roundtrip():
    assert iso(NOW).endswith("Z")
