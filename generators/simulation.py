"""Deterministic synthetic payment simulation.

Independence model
------------------
1. A *payment intent* (customer, amount, method, time) is derived from (seed, index).
2. A *world outcome* (approved/declined, later refunds/reversal) is derived from (seed, index).
   This is the hidden ground truth the real-world systems would all observe.
3. Each source system then derives its OWN observations from its OWN random stream
   (seed, index, source): missing records, wrong amounts, duplicates, delays, malformed payloads.

Because every source process derives the same intents and world outcome from the shared seed,
the three producers can run as separate processes/containers with no shared state, while their
faults are injected independently. Nothing here looks at a reconciliation result.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from common.config import SimulationSettings, TopicSettings
from common.money import to_money

APP, PROCESSOR, LEDGER = "mybell_app", "payment_processor", "billing_ledger"
SOURCES = (APP, PROCESSOR, LEDGER)
METHODS = ("CREDIT_CARD", "CREDIT_CARD", "CREDIT_CARD", "DEBIT_CARD", "PRE_AUTHORIZED_DEBIT", "ONLINE_BANKING")
INVALID_KINDS = (
    "missing_field",
    "negative_amount",
    "bad_currency",
    "unsupported_schema",
    "not_json",
    "excess_precision",
    "unknown_event_type",
)


@dataclass(frozen=True)
class PaymentIntent:
    index: int
    payment_id: str
    account_ref: str
    amount: Decimal
    currency: str
    payment_method: str
    initiated_at: datetime
    correlation_id: str


@dataclass(frozen=True)
class WorldOutcome:
    approved: bool
    refunds: tuple[tuple[Decimal, float], ...]  # (amount, seconds after capture)
    reversal_after_s: float | None


@dataclass(order=True)
class ScheduledEvent:
    emit_at: datetime
    seq: int
    topic: str = field(compare=False)
    key: bytes | None = field(compare=False)
    value: bytes = field(compare=False)
    source_system: str = field(compare=False)
    kind: str = field(compare=False, default="valid")


def _rng(settings: SimulationSettings, index: int, stream: str) -> random.Random:
    return random.Random(f"{settings.seed}|{settings.run_id}|{index}|{stream}")  # noqa: S311 - simulation only


def _uid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def intent_time(settings: SimulationSettings, index: int) -> datetime:
    return settings.epoch + timedelta(seconds=index / settings.payments_per_second)


def index_at(settings: SimulationSettings, when: datetime) -> int:
    return max(int((when - settings.epoch).total_seconds() * settings.payments_per_second), 0)


def make_intent(settings: SimulationSettings, index: int, initiated_at: datetime | None = None) -> PaymentIntent:
    rng = _rng(settings, index, "intent")
    low = int(to_money(settings.min_amount) * 100)
    high = int(to_money(settings.max_amount) * 100)
    cents = rng.randint(low, high)
    return PaymentIntent(
        index=index,
        payment_id=f"PAY-{settings.run_id}-{index:012d}",
        account_ref=f"SYN-ACCT-{rng.randint(1, 250_000):08d}",
        amount=(Decimal(cents) / 100).quantize(Decimal("0.01")),
        currency=settings.currency,
        payment_method=rng.choice(METHODS),
        initiated_at=initiated_at or intent_time(settings, index),
        correlation_id=f"COR-{_uid(rng)}",
    )


def world_outcome(settings: SimulationSettings, intent: PaymentIntent) -> WorldOutcome:
    rng = _rng(settings, intent.index, "world")
    approved = rng.random() >= settings.decline_rate
    refunds: list[tuple[Decimal, float]] = []
    reversal: float | None = None
    if approved and rng.random() < settings.refund_rate:
        if rng.random() < settings.partial_refund_share:
            # One or two partial refunds that never exceed the captured amount.
            parts = rng.choice((1, 2))
            remaining = intent.amount
            for _ in range(parts):
                cents = int(remaining * 100)
                if cents < 2:
                    break
                amount = (Decimal(rng.randint(1, max(cents // 2, 1))) / 100).quantize(Decimal("0.01"))
                remaining -= amount
                refunds.append((amount, rng.uniform(settings.refund_after_min_s, settings.refund_after_max_s)))
        else:
            refunds.append((intent.amount, rng.uniform(settings.refund_after_min_s, settings.refund_after_max_s)))
    elif approved and rng.random() < settings.reversal_rate:
        reversal = rng.uniform(settings.refund_after_min_s, settings.refund_after_max_s)
    return WorldOutcome(approved, tuple(sorted(refunds, key=lambda r: r[1])), reversal)


def processor_ids(intent: PaymentIntent) -> dict[str, str]:
    """Deterministic processor transaction identifiers (shared knowledge of the 'real world')."""
    suffix = intent.payment_id.split("-", 1)[1]
    return {
        "auth": f"PTX-A-{suffix}",
        "capture": f"PTX-C-{suffix}",
        "decline": f"PTX-D-{suffix}",
        "reversal": f"PTX-V-{suffix}",
    }


def refund_id(intent: PaymentIntent, n: int) -> str:
    return f"PTX-R{n}-{intent.payment_id.split('-', 1)[1]}"


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class _SourceBuilder:
    def __init__(self, settings: SimulationSettings, topics: TopicSettings, source: str, intent: PaymentIntent) -> None:
        self.s = settings
        self.source = source
        self.intent = intent
        self.rng = _rng(settings, intent.index, source)
        self.topic = {APP: topics.app_events, PROCESSOR: topics.processor_events, LEDGER: topics.ledger_events}[source]
        self.events: list[ScheduledEvent] = []
        self.seq = 0
        self.missing = False

    def delay(self) -> float:
        base = self.rng.uniform(0.0, 0.5)
        if self.rng.random() < self.s.delayed_event_rate:
            base += self.rng.uniform(self.s.delay_min_s, self.s.delay_max_s)
        return base

    def emit(self, payload: dict[str, Any], event_time: datetime, extra_delay: float = 0.0) -> None:
        key = (payload.get("payment_id") or payload.get("account_ref") or "").encode() or None
        emit_at = event_time + timedelta(seconds=self.delay() + extra_delay)
        kind = "valid"
        value = json.dumps(payload, separators=(",", ":")).encode()
        if self.rng.random() < self.s.invalid_payload_rate:
            kind, value = corrupt(payload, self.rng)
        self._push(emit_at, key, value, kind)
        if self.rng.random() < self.s.duplicate_delivery_rate:
            self._push(emit_at + timedelta(seconds=self.rng.uniform(0.05, 5.0)), key, value, "duplicate_delivery")

    def _push(self, emit_at: datetime, key: bytes | None, value: bytes, kind: str) -> None:
        self.seq += 1
        seq = self.intent.index * 1000 + {APP: 0, PROCESSOR: 300, LEDGER: 600}[self.source] + self.seq
        self.events.append(ScheduledEvent(emit_at, seq, self.topic, key, value, self.source, kind))

    def envelope(self, event_type: str, event_time: datetime) -> dict[str, Any]:
        return {
            "event_id": f"EVT-{_uid(self.rng)}",
            "event_type": event_type,
            "schema_version": "1.0",
            "source_system": self.source,
            "event_time": _iso(event_time),
            "correlation_id": self.intent.correlation_id,
        }


def corrupt(payload: dict[str, Any], rng: random.Random) -> tuple[str, bytes]:
    kind = rng.choice(INVALID_KINDS)
    bad = dict(payload)
    if kind == "missing_field":
        bad.pop(rng.choice([k for k in ("amount", "currency", "event_id", "event_time") if k in bad]), None)
    elif kind == "negative_amount":
        bad["amount"] = "-" + str(bad.get("amount", "1.00"))
    elif kind == "bad_currency":
        bad["currency"] = "C$D"
    elif kind == "unsupported_schema":
        bad["schema_version"] = "9.0"
    elif kind == "excess_precision":
        bad["amount"] = str(bad.get("amount", "1.00")) + "7"
    elif kind == "unknown_event_type":
        bad["event_type"] = "SOMETHING_ELSE"
    elif kind == "not_json":
        return "invalid_not_json", b"{not-json: " + json.dumps(payload).encode()[:40]
    return f"invalid_{kind}", json.dumps(bad, separators=(",", ":")).encode()


def application_events(b: _SourceBuilder, world: WorldOutcome) -> None:
    s, it, rng = b.s, b.intent, b.rng
    if rng.random() < s.missing_app_rate:
        b.missing = True
        return

    def body(event_type: str, status: str, t: datetime) -> dict[str, Any]:
        return {
            **b.envelope(event_type, t),
            "payment_id": it.payment_id,
            "account_ref": it.account_ref,
            "amount": str(it.amount),
            "currency": it.currency,
            "payment_method": it.payment_method,
            "source_status": status,
        }

    t0 = it.initiated_at
    out_of_order = rng.random() < s.out_of_order_rate
    # Out-of-order: the initiation is held back so the outcome event overtakes it.
    b.emit(body("PAYMENT_INITIATED", "INITIATED", t0), t0, extra_delay=rng.uniform(4, 12) if out_of_order else 0)
    if rng.random() < s.app_retry_rate:
        # Client retry of the same payment: new event_id, same payment_id (duplicate business transaction).
        b.emit(body("PAYMENT_INITIATED", "INITIATED", t0 + timedelta(seconds=0.4)), t0 + timedelta(seconds=0.4))
    t1 = t0 + timedelta(seconds=rng.uniform(1.0, 3.0))
    final = "SUCCEEDED" if world.approved else "FAILED"
    if rng.random() < s.ambiguous_app_result_rate:
        b.emit(body("PAYMENT_TIMEOUT", "UNKNOWN", t1), t1)
        if rng.random() < 0.5:
            t2 = t1 + timedelta(seconds=rng.uniform(20, 60))
            b.emit(body("PAYMENT_STATUS_UPDATED", final, t2), t2)
    else:
        b.emit(body(f"PAYMENT_{final}", final, t1), t1)


def processor_events(b: _SourceBuilder, world: WorldOutcome) -> datetime | None:
    s, it, rng = b.s, b.intent, b.rng
    ids = processor_ids(it)
    if rng.random() < s.missing_processor_rate:
        b.missing = True
        return None

    def body(event_type: str, txn: str, status: str, amount: Decimal, t: datetime, original: str | None) -> dict:
        payload = {
            **b.envelope(event_type, t),
            "processor_transaction_id": txn,
            "payment_id": it.payment_id,
            "amount": str(amount),
            "currency": it.currency,
            "processor_status": status,
        }
        if original:
            payload["original_processor_transaction_id"] = original
        return payload

    t_auth = it.initiated_at + timedelta(seconds=rng.uniform(0.3, 1.2))
    if not world.approved:
        b.emit(body("DECLINE", ids["decline"], "DECLINED", it.amount, t_auth, None), t_auth)
        return None
    b.emit(body("AUTHORIZATION", ids["auth"], "AUTHORIZED", it.amount, t_auth, None), t_auth)
    capture_delay = rng.uniform(1.0, 3.0) if it.payment_method != "PRE_AUTHORIZED_DEBIT" else rng.uniform(5.0, 20.0)
    t_cap = t_auth + timedelta(seconds=capture_delay)
    captured = it.amount
    if rng.random() < s.processor_amount_mismatch_rate:
        captured = max(it.amount + Decimal(rng.choice((-1, 1)) * rng.randint(1, 500)) / 100, Decimal("0.01"))
    capture = body("CAPTURE", ids["capture"], "CAPTURED", captured, t_cap, ids["auth"])
    b.emit(capture, t_cap)
    if rng.random() < s.duplicate_business_event_rate:
        # Processor re-sends the same capture under a new event_id (e.g. webhook redelivery).
        t_dup = t_cap + timedelta(seconds=rng.uniform(5, 30))
        resend = {**capture, **b.envelope("CAPTURE", t_dup)}
        b.emit(resend, t_dup)
    for n, (amount, after) in enumerate(world.refunds, start=1):
        t_ref = t_cap + timedelta(seconds=after)
        b.emit(body("REFUND", refund_id(it, n), "REFUNDED", amount, t_ref, ids["capture"]), t_ref)
    if world.reversal_after_s is not None:
        t_rev = t_cap + timedelta(seconds=world.reversal_after_s)
        b.emit(body("REVERSAL", ids["reversal"], "REVERSED", captured, t_rev, ids["capture"]), t_rev)
    return t_cap


def ledger_events(b: _SourceBuilder, world: WorldOutcome) -> None:
    s, it, rng = b.s, b.intent, b.rng
    suffix = it.payment_id.split("-", 1)[1]
    ids = processor_ids(it)
    if rng.random() < s.billing_charge_rate:
        t_charge = it.initiated_at - timedelta(seconds=rng.uniform(60, 600))
        charge = {
            **b.envelope("CHARGE", t_charge),
            "ledger_entry_id": f"LED-CH-{suffix}",
            "account_ref": it.account_ref,
            "amount": str((Decimal(rng.randint(2000, 25000)) / 100).quantize(Decimal("0.01"))),
            "currency": it.currency,
            "direction": "DEBIT",
            "posting_status": "POSTED",
        }
        b.emit(charge, t_charge)
    if not world.approved:
        return
    if rng.random() < s.missing_ledger_rate:
        b.missing = True
        return

    def entry(event_type: str, entry_id: str, direction: str, amount: Decimal, status: str, t: datetime, **extra) -> dict:
        payload = {
            **b.envelope(event_type, t),
            "ledger_entry_id": entry_id,
            "payment_id": it.payment_id,
            "account_ref": it.account_ref,
            "amount": str(amount),
            "currency": it.currency,
            "direction": direction,
            "posting_status": status,
        }
        payload.update({k: v for k, v in extra.items() if v is not None})
        return payload

    # The ledger learns about the payment independently of the processor's own timing.
    settle = rng.uniform(3.0, 12.0) if it.payment_method != "PRE_AUTHORIZED_DEBIT" else rng.uniform(15.0, 40.0)
    t_credit = it.initiated_at + timedelta(seconds=settle)
    credited = it.amount
    mismatch = rng.random() < s.ledger_amount_mismatch_rate
    if mismatch:
        credited = max(it.amount - Decimal(rng.randint(1, 1000)) / 100, Decimal("0.01"))
    pending = rng.random() < s.pending_posting_rate
    credit_id = f"LED-PC-{suffix}"
    b.emit(entry("PAYMENT_CREDIT", credit_id, "CREDIT", credited, "PENDING" if pending else "POSTED", t_credit), t_credit)
    if pending:
        t_conf = t_credit + timedelta(seconds=rng.uniform(10, 60))
        b.emit(
            entry("POSTING_CONFIRMATION", f"LED-CF-{suffix}", "CREDIT", credited, "POSTED", t_conf,
                  original_ledger_entry_id=credit_id),
            t_conf,
        )
    if mismatch and credited != it.amount and rng.random() < s.ledger_adjustment_fix_rate:
        t_adj = t_credit + timedelta(seconds=rng.uniform(30, 120))
        b.emit(
            entry("ADJUSTMENT", f"LED-AJ-{suffix}", "CREDIT", it.amount - credited, "POSTED", t_adj,
                  original_ledger_entry_id=credit_id, adjustment_reason="Posting amount correction"),
            t_adj,
        )
    for n, (amount, after) in enumerate(world.refunds, start=1):
        t_ref = t_credit + timedelta(seconds=after + rng.uniform(2, 10))
        b.emit(
            entry("REFUND_DEBIT", f"LED-RF{n}-{suffix}", "DEBIT", amount, "POSTED", t_ref,
                  processor_reference=refund_id(it, n)),
            t_ref,
        )
    if world.reversal_after_s is not None:
        t_rev = t_credit + timedelta(seconds=world.reversal_after_s + rng.uniform(2, 10))
        b.emit(
            entry("REVERSAL", f"LED-RV-{suffix}", "DEBIT", credited, "POSTED", t_rev,
                  original_ledger_entry_id=credit_id, processor_reference=ids["reversal"]),
            t_rev,
        )


def build_source_events(
    settings: SimulationSettings, topics: TopicSettings, source: str, index: int, initiated_at: datetime | None = None
) -> list[ScheduledEvent]:
    intent = make_intent(settings, index, initiated_at)
    world = world_outcome(settings, intent)
    builder = _SourceBuilder(settings, topics, source, intent)
    if source == APP:
        application_events(builder, world)
    elif source == PROCESSOR:
        processor_events(builder, world)
    elif source == LEDGER:
        ledger_events(builder, world)
    else:
        raise ValueError(f"unknown source {source}")
    return builder.events


def build_all_events(
    settings: SimulationSettings,
    topics: TopicSettings,
    start_index: int,
    count: int,
    start_time: datetime,
    sources: tuple[str, ...] = SOURCES,
) -> list[ScheduledEvent]:
    """Finite dataset for seeding, tests and load tests, sorted by simulated arrival time."""
    events: list[ScheduledEvent] = []
    for i in range(start_index, start_index + count):
        when = start_time + timedelta(seconds=(i - start_index) / settings.payments_per_second)
        for source in sources:
            events.extend(build_source_events(settings, topics, source, i, when))
    events.sort()
    return events
