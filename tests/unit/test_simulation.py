"""Deterministic, independent synthetic sources that conform to the published JSON Schemas."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import jsonschema
import pytest

from common.config import REPO_ROOT, SimulationSettings, TopicSettings
from generators.simulation import (
    APP,
    LEDGER,
    PROCESSOR,
    build_all_events,
    build_source_events,
    make_intent,
    world_outcome,
)
from processing.models import MODEL_BY_SOURCE

TOPICS = TopicSettings()
START = datetime(2026, 10, 28, 12, tzinfo=UTC)
SCHEMAS = {
    APP: json.loads((REPO_ROOT / "schemas" / "application_payment.json").read_text()),
    PROCESSOR: json.loads((REPO_ROOT / "schemas" / "processor_transaction.json").read_text()),
    LEDGER: json.loads((REPO_ROOT / "schemas" / "billing_ledger.json").read_text()),
}
VALIDATORS = {}
for _source, _schema in SCHEMAS.items():
    jsonschema.Draft202012Validator.check_schema(_schema)
    VALIDATORS[_source] = jsonschema.Draft202012Validator(_schema)


def sim(**kw) -> SimulationSettings:
    return SimulationSettings(**kw)


def test_generation_is_deterministic_for_a_seed():
    a = build_all_events(sim(seed=7), TOPICS, 0, 50, START)
    b = build_all_events(sim(seed=7), TOPICS, 0, 50, START)
    c = build_all_events(sim(seed=8), TOPICS, 0, 50, START)
    assert [e.value for e in a] == [e.value for e in b]
    assert [e.value for e in a] != [e.value for e in c]


def test_each_source_can_be_generated_independently():
    s = sim(seed=11, payments_per_second=5.0)
    combined = build_all_events(s, TOPICS, 0, 20, START)
    # Separate "processes": each source derives the same intents/world from the shared seed, no shared state.
    separately = []
    for source in (APP, PROCESSOR, LEDGER):
        for i in range(20):
            when = START + timedelta(seconds=i / s.payments_per_second)
            separately.extend(build_source_events(s, TOPICS, source, i, when))
    assert sorted(separately) == combined
    assert [e.value for e in sorted(separately)] == [e.value for e in combined]


def test_faults_are_injected_independently_per_source():
    s = sim(seed=3, missing_processor_rate=0.5, missing_ledger_rate=0.5, missing_app_rate=0.0, decline_rate=0.0,
            invalid_payload_rate=0, duplicate_delivery_rate=0)
    missing_proc = set()
    missing_ledger = set()
    for i in range(200):
        when = START
        if not build_source_events(s, TOPICS, PROCESSOR, i, when):
            missing_proc.add(i)
        ledger_events = [e for e in build_source_events(s, TOPICS, LEDGER, i, when) if b"PAYMENT_CREDIT" in e.value]
        if not ledger_events:
            missing_ledger.add(i)
    assert 60 < len(missing_proc) < 140 and 60 < len(missing_ledger) < 140
    assert missing_proc != missing_ledger  # not derived from one shared fault decision


def test_valid_events_conform_to_json_schemas_and_models():
    s = sim(seed=5, invalid_payload_rate=0.0, refund_rate=0.3, reversal_rate=0.3, pending_posting_rate=0.3,
            ledger_amount_mismatch_rate=0.2, chargeback_rate=0.3, chargeback_reversal_share=0.5)
    events = build_all_events(s, TOPICS, 0, 150, START)
    kinds = Counter(e.kind for e in events)
    assert kinds["valid"] > 500
    types = Counter()
    for ev in events:
        payload = json.loads(ev.value)
        VALIDATORS[ev.source_system].validate(payload)
        MODEL_BY_SOURCE[ev.source_system].model_validate(payload)
        types[payload["event_type"]] += 1
    for expected in ("PAYMENT_INITIATED", "CAPTURE", "AUTHORIZATION", "PAYMENT_CREDIT", "REFUND", "REFUND_DEBIT",
                     "REVERSAL", "POSTING_CONFIRMATION", "CHARGE", "DECLINE", "CHARGEBACK", "CHARGEBACK_REVERSAL",
                     "CHARGEBACK_DEBIT", "CHARGEBACK_CREDIT"):
        assert types[expected] > 0, expected


def test_chargebacks_do_not_change_previous_outcomes():
    base = build_all_events(sim(seed=13, chargeback_rate=0.0), TOPICS, 0, 40, START)
    with_cb = build_all_events(sim(seed=13, chargeback_rate=1.0), TOPICS, 0, 40, START)
    chargeback_types = (b'"CHARGEBACK', b'"CHARGEBACK_REVERSAL', b'"CHARGEBACK_DEBIT', b'"CHARGEBACK_CREDIT')
    assert [e.value for e in with_cb if not any(t in e.value for t in chargeback_types)] == [e.value for e in base]


def test_invalid_payloads_are_rejected_by_schema():
    s = sim(seed=9, invalid_payload_rate=1.0, duplicate_delivery_rate=0)
    events = build_all_events(s, TOPICS, 0, 30, START)
    assert events and all(e.kind.startswith("invalid_") for e in events)
    for ev in events:
        try:
            payload = json.loads(ev.value)
        except json.JSONDecodeError:
            continue
        with pytest.raises(jsonschema.ValidationError):
            VALIDATORS[ev.source_system].validate(payload)


def test_default_rates_produce_mostly_normal_traffic():
    events = build_all_events(sim(seed=1), TOPICS, 0, 800, START)
    kinds = Counter(e.kind for e in events)
    total = sum(kinds.values())
    assert kinds["valid"] / total > 0.95
    assert kinds["duplicate_delivery"] > 0


def test_amounts_are_exact_decimals_and_refunds_never_exceed_capture():
    s = sim(seed=13, refund_rate=1.0, partial_refund_share=1.0)
    for i in range(200):
        intent = make_intent(s, i, START)
        world = world_outcome(s, intent)
        assert isinstance(intent.amount, Decimal) and intent.amount.as_tuple().exponent == -2
        assert sum((r[0] for r in world.refunds), Decimal("0")) <= intent.amount


def test_messages_keyed_by_payment_id():
    events = build_all_events(sim(seed=2, invalid_payload_rate=0), TOPICS, 0, 20, START)
    for ev in events:
        payload = json.loads(ev.value)
        expected = payload.get("payment_id") or payload["account_ref"]
        assert ev.key == expected.encode()
