"""End-to-end: known synthetic scenarios through Kafka -> ingestion -> Bronze/Silver -> reconciliation -> Gold.

Every scenario asserts both the reconciliation status and the financial amounts.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from decimal import Decimal

import pytest

from tests.factories import app, ledger, matched_payment, proc
from tests.harness import Harness, read_topic, recent_past

RUN = uuid.uuid4().hex[:6].upper()
T0 = recent_past(2).replace(microsecond=0)


def pid(n: int) -> str:
    return f"PAY-E2E{RUN}-{n:03d}"


def short(p: str) -> str:
    return p.split("-", 1)[1]


def scenarios() -> dict[str, tuple[str, list]]:
    s: dict[str, tuple[str, list]] = {}

    s["fully_matched"] = (pid(1), matched_payment(pid(1), "125.40", T0))

    p = pid(2)
    ev = matched_payment(p, "100.00", T0)
    ev[2] = proc(p, "CAPTURE", f"C-{short(p)}", "100.50", T0 + timedelta(seconds=2), original=f"A-{short(p)}")
    s["processor_amount_mismatch"] = (p, ev)

    p = pid(3)
    s["missing_billing_posting"] = (p, matched_payment(p, "75.00", T0)[:-1])

    p = pid(4)
    s["missing_processor_record"] = (p, [app(p, t=T0), app(p, "PAYMENT_SUCCEEDED", t=T0 + timedelta(seconds=2)),
                                         ledger(p, "PAYMENT_CREDIT", f"L-{short(p)}", t=T0 + timedelta(seconds=9))])

    p = pid(5)
    s["declined"] = (p, [app(p, amount="60.00", t=T0), proc(p, "DECLINE", f"D-{short(p)}", "60.00", T0 + timedelta(seconds=1)),
                         app(p, "PAYMENT_FAILED", amount="60.00", t=T0 + timedelta(seconds=2))])

    p = pid(6)
    ev = matched_payment(p, "90.00", T0)
    resend = {**ev[2], "event_id": f"EVT-RESEND-{RUN}", "event_time": (T0 + timedelta(seconds=40)).isoformat()}
    s["duplicate_delivery"] = (p, ev + [ev[2], ev[4], ev[0], resend])

    p = pid(7)
    s["out_of_order"] = (p, list(reversed(matched_payment(p, "33.33", T0))))

    p = pid(9)
    refund = proc(p, "REFUND", f"R-{short(p)}", "80.00", T0 + timedelta(minutes=5), original=f"C-{short(p)}")
    s["full_refund"] = (p, matched_payment(p, "80.00", T0) + [
        refund, ledger(p, "REFUND_DEBIT", f"LR-{short(p)}", "80.00", T0 + timedelta(minutes=6), processor_reference=refund["processor_transaction_id"]),
        refund])

    p = pid(10)
    r1 = proc(p, "REFUND", f"R1-{short(p)}", "25.00", T0 + timedelta(minutes=5), original=f"C-{short(p)}")
    r2 = proc(p, "REFUND", f"R2-{short(p)}", "10.10", T0 + timedelta(minutes=9), original=f"C-{short(p)}")
    s["partial_refunds"] = (p, matched_payment(p, "100.00", T0) + [
        r1, r2,
        ledger(p, "REFUND_DEBIT", f"LR1-{short(p)}", "25.00", T0 + timedelta(minutes=6), processor_reference=r1["processor_transaction_id"]),
        ledger(p, "REFUND_DEBIT", f"LR2-{short(p)}", "10.10", T0 + timedelta(minutes=10), processor_reference=r2["processor_transaction_id"])])

    p = pid(11)
    rv = proc(p, "REVERSAL", f"V-{short(p)}", "45.00", T0 + timedelta(minutes=3), original=f"C-{short(p)}")
    s["reversal"] = (p, matched_payment(p, "45.00", T0) + [
        rv, ledger(p, "REVERSAL", f"LV-{short(p)}", "45.00", T0 + timedelta(minutes=4), original=f"L-{short(p)}",
                   processor_reference=rv["processor_transaction_id"])])
    return s


EXPECTED = {
    "fully_matched": ("MATCHED", "FULL_MATCH", "125.40", "125.40", "0.00"),
    "processor_amount_mismatch": ("AMOUNT_MISMATCH", "APP_PROCESSOR_AMOUNT_MISMATCH", "100.50", "100.00", "0.50"),
    "missing_billing_posting": ("MISSING_BILLING_POSTING", "PAYMENT_CREDIT_MISSING", "75.00", "0.00", "-75.00"),
    "missing_processor_record": ("MISSING_PROCESSOR_RECORD", "PROCESSOR_RECORD_MISSING", "0.00", "100.00", "-100.00"),
    "declined": ("NO_FINANCIAL_EFFECT", "DECLINED_CONSISTENT", "0.00", "0.00", "0.00"),
    "duplicate_delivery": ("MATCHED", "FULL_MATCH", "90.00", "90.00", "0.00"),
    "out_of_order": ("MATCHED", "FULL_MATCH", "33.33", "33.33", "0.00"),
    "full_refund": ("MATCHED", "FULLY_REFUNDED", "0.00", "0.00", "0.00"),
    "partial_refunds": ("MATCHED", "PARTIALLY_REFUNDED", "64.90", "64.90", "0.00"),
    "reversal": ("MATCHED", "REVERSED", "0.00", "0.00", "0.00"),
}


@pytest.fixture(scope="module")
def pipeline(database, kafka_topics, rules):
    harness = Harness(database, kafka_topics, rules, group=f"e2e-{RUN}")
    data = scenarios()
    published = harness.publish(e for _, events in data.values() for e in events)
    invalid = [
        (kafka_topics.app_events, b"{this is not json", b"bad"),
        (kafka_topics.processor_events, json.dumps({**proc(pid(12), "CAPTURE", "C-BAD-12", t=T0), "amount": "-3.00"}).encode(), b"x"),
    ]
    unsupported = {**app(pid(18), t=T0), "schema_version": "3.1"}
    published += harness.publish(invalid)
    published += harness.publish([unsupported])
    harness.run(expected_raw=published)
    yield harness, data
    harness.close()


def case(harness, payment_id):
    with harness.database.transaction() as conn:
        cur = conn.execute(
            "SELECT reconciliation_status, reason_code, processor_net_amount, billing_amount, difference_amount, "
            "processor_amount, refunded_amount, reversed_amount FROM reconciliation_cases WHERE payment_id = %s",
            (payment_id,),
        )
        return cur.fetchone()


@pytest.mark.parametrize("name", list(EXPECTED))
def test_scenario_status_and_amounts(pipeline, name):
    harness, data = pipeline
    payment_id = data[name][0]
    status, reason, net, billing, diff, *_ = case(harness, payment_id)
    exp_status, exp_reason, exp_net, exp_billing, exp_diff = EXPECTED[name]
    assert (status, reason) == (exp_status, exp_reason)
    assert (net, billing, diff) == (Decimal(exp_net), Decimal(exp_billing), Decimal(exp_diff))


def test_duplicate_delivery_has_no_duplicate_financial_effect(pipeline):
    harness, data = pipeline
    payment_id = data["duplicate_delivery"][0]
    with harness.database.transaction() as conn:
        statuses = dict(conn.execute(
            "SELECT processing_status, count(*) FROM raw_payment_events WHERE payment_id = %s GROUP BY 1", (payment_id,)
        ).fetchall())
        captures = conn.execute(
            "SELECT count(*), sum(amount) FROM processor_transactions WHERE payment_id = %s AND transaction_type = 'CAPTURE'",
            (payment_id,),
        ).fetchone()
    assert statuses == {"PROCESSED": 5, "DUPLICATE_DELIVERY": 3, "DUPLICATE_BUSINESS_EVENT": 1}
    assert captures == (1, Decimal("90.00"))
    assert case(harness, payment_id)[5] == Decimal("90.00")


def test_refund_amounts(pipeline):
    harness, data = pipeline
    full = case(harness, data["full_refund"][0])
    partial = case(harness, data["partial_refunds"][0])
    reversal = case(harness, data["reversal"][0])
    assert (full[5], full[6]) == (Decimal("80.00"), Decimal("80.00"))  # repeated refund delivery counted once
    assert (partial[5], partial[6]) == (Decimal("100.00"), Decimal("35.10"))
    assert (reversal[5], reversal[7]) == (Decimal("45.00"), Decimal("45.00"))


def test_invalid_and_unsupported_events_quarantined_and_dead_lettered(pipeline, kafka_topics):
    harness, _ = pipeline
    with harness.database.transaction() as conn:
        rows = conn.execute(
            "SELECT error_code, error_category FROM quarantined_events WHERE source_topic = ANY(%s) ORDER BY error_code",
            (list(kafka_topics.source_topics),),
        ).fetchall()
    assert rows == [("INVALID_AMOUNT", "VALIDATION"), ("INVALID_JSON", "SERIALIZATION"),
                    ("UNSUPPORTED_SCHEMA_VERSION", "UNSUPPORTED_SCHEMA")]
    dlq = read_topic(harness.kafka, kafka_topics.dlq, expected=3)
    assert len(dlq) == 3
    assert {dict(m.headers())["x-error-code"] for m in dlq} == {b"INVALID_AMOUNT", b"INVALID_JSON", b"UNSUPPORTED_SCHEMA_VERSION"}
    assert b"{this is not json" in {m.value() for m in dlq}  # original bytes preserved


def test_late_arriving_counterpart_resolves_case_with_audit(pipeline):
    harness, data = pipeline
    payment_id = data["missing_billing_posting"][0]
    assert case(harness, payment_id)[0] == "MISSING_BILLING_POSTING"
    before = harness.raw_count()
    harness.publish([ledger(payment_id, "PAYMENT_CREDIT", f"L-{short(payment_id)}", "75.00", T0 + timedelta(minutes=30))])
    harness.run(expected_raw=before + 1)
    status, reason, net, billing, diff, *_ = case(harness, payment_id)
    assert (status, reason, billing, diff) == ("MATCHED", "FULL_MATCH", Decimal("75.00"), Decimal("0.00"))
    with harness.database.transaction() as conn:
        history = conn.execute(
            "SELECT previous_status, new_status, trigger_type FROM reconciliation_history WHERE payment_id = %s "
            "ORDER BY history_id", (payment_id,)
        ).fetchall()
        resolution = conn.execute(
            "SELECT resolution_reason, exception_created_at IS NOT NULL FROM reconciliation_cases WHERE payment_id = %s",
            (payment_id,),
        ).fetchone()
    assert history[0] == (None, "MISSING_BILLING_POSTING", "STREAM")
    assert history[-1][:2] == ("MISSING_BILLING_POSTING", "MATCHED")
    assert resolution == ("RESOLVED_FROM_MISSING_BILLING_POSTING:FULL_MATCH", True)


def test_outbox_published_reconciliation_events(pipeline, kafka_topics):
    harness, data = pipeline
    harness.worker.publish_outbox_once()
    with harness.database.transaction() as conn:
        unpublished = conn.execute("SELECT count(*) FROM reconciliation_outbox WHERE published_at IS NULL").fetchone()[0]
        total = conn.execute("SELECT count(*) FROM reconciliation_outbox").fetchone()[0]
    assert unpublished == 0
    messages = read_topic(harness.kafka, kafka_topics.reconciliation_events, expected=total)
    payloads = [json.loads(m.value()) for m in messages]
    assert len({p["event_id"] for p in payloads}) == total
    matched = [p for p in payloads if p["payment_id"] == data["fully_matched"][0]]
    assert matched and matched[0]["reconciliation_status"] == "MATCHED" and matched[0]["amounts"]["billing_amount"] == "125.40"


def test_gold_reports_match_known_dataset(pipeline):
    harness, data = pipeline
    with harness.database.transaction() as conn:
        summary = conn.execute(
            "SELECT sum(payments), sum(matched_payments), sum(captured_payments), sum(declined_payments), "
            "sum(gross_captured_amount), sum(refunded_amount), sum(reversed_amount), sum(net_captured_amount) "
            "FROM gold_daily_payment_summary"
        ).fetchone()
        refunds = dict(((k, (n, a)) for k, n, a in conn.execute(
            "SELECT kind, sum(transactions), sum(amount) FROM gold_refund_summary GROUP BY kind").fetchall()))
        completeness = conn.execute("SELECT sum(with_capture), sum(captured_with_ledger_record) FROM gold_source_completeness").fetchone()
        exceptions = conn.execute("SELECT count(*) FROM gold_reconciliation_exceptions").fetchone()[0]
    # 10 scenarios: matched=7 (incl. late-resolved #3), captured=8 (all but #4 and #5), declined=1.
    payments, matched, captured, declined, gross, refunded, reversed_, net = summary
    assert (payments, matched, captured, declined) == (10, 7, 8, 1)
    assert gross == Decimal("125.40") + Decimal("100.50") + Decimal("75.00") + Decimal("90.00") + Decimal("33.33") \
        + Decimal("80.00") + Decimal("100.00") + Decimal("45.00")
    assert (refunded, reversed_) == (Decimal("115.10"), Decimal("45.00"))
    assert net == gross - refunded - reversed_
    assert refunds == {"REFUND": (3, Decimal("115.10")), "REVERSAL": (1, Decimal("45.00"))}
    assert completeness == (8, 8)
    assert exceptions == 2  # amount mismatch (#2) and missing processor (#4)
