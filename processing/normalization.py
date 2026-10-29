"""Normalization of validated source events into Silver rows."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from processing.models import (
    AppPaymentEvent,
    BillingLedgerEvent,
    ProcessorEventType,
    ProcessorTransactionEvent,
    SourceEvent,
)

# Fields that legitimately differ when a source re-sends the same business transaction.
_DELIVERY_FIELDS = {"event_id", "event_time", "correlation_id", "schema_version"}


def canonical_json(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)


def sha256_hex(data: bytes | str) -> str:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class SilverRow:
    table: str
    values: dict[str, Any]


@dataclass(frozen=True)
class NormalizedEvent:
    source_system: str
    source_event_id: str
    event_type: str
    payment_id: str | None
    event_time: datetime
    content_hash: str
    business_key: str | None
    business_hash: str | None
    rows: list[SilverRow] = field(default_factory=list)
    payment_request: dict[str, Any] | None = None


def _common(event: SourceEvent) -> dict[str, Any]:
    return {
        "source_event_id": event.event_id,
        "event_time": event.event_time,
        "correlation_id": event.correlation_id,
        "schema_version": event.schema_version,
        "amount": event.amount,
        "currency": event.currency,
    }


def normalize(event: SourceEvent) -> NormalizedEvent:
    dumped = event.model_dump(mode="json")
    content_hash = sha256_hex(canonical_json(dumped))
    business_hash = sha256_hex(canonical_json({k: v for k, v in dumped.items() if k not in _DELIVERY_FIELDS}))

    if isinstance(event, AppPaymentEvent):
        row = SilverRow(
            "application_payment_events",
            {
                **_common(event),
                "payment_id": event.payment_id,
                "event_type": event.event_type.value,
                "source_status": event.source_status.value,
                "account_ref": event.account_ref,
                "payment_method": event.payment_method.value,
            },
        )
        request = {
            "payment_id": event.payment_id,
            "account_ref": event.account_ref,
            "amount": event.amount,
            "currency": event.currency,
            "payment_method": event.payment_method.value,
            "event_time": event.event_time,
            "source_event_id": event.event_id,
            "is_initiation": event.event_type.value == "PAYMENT_INITIATED",
        }
        return NormalizedEvent(
            source_system=event.source_system,
            source_event_id=event.event_id,
            event_type=event.event_type.value,
            payment_id=event.payment_id,
            event_time=event.event_time,
            content_hash=content_hash,
            business_key=None,
            business_hash=None,
            rows=[row],
            payment_request=request,
        )

    if isinstance(event, ProcessorTransactionEvent):
        base = {
            **_common(event),
            "payment_id": event.payment_id,
            "original_processor_transaction_id": event.original_processor_transaction_id,
            "processor_status": event.processor_status.value,
        }
        if event.event_type is ProcessorEventType.REFUND:
            row = SilverRow("payment_refunds", {**base, "refund_id": event.processor_transaction_id})
        elif event.event_type is ProcessorEventType.REVERSAL:
            row = SilverRow("payment_reversals", {**base, "reversal_id": event.processor_transaction_id})
        elif event.event_type in (ProcessorEventType.CHARGEBACK, ProcessorEventType.CHARGEBACK_REVERSAL):
            row = SilverRow(
                "payment_chargebacks",
                {**base, "chargeback_id": event.processor_transaction_id, "chargeback_type": event.event_type.value},
            )
        else:
            row = SilverRow(
                "processor_transactions",
                {
                    **base,
                    "processor_transaction_id": event.processor_transaction_id,
                    "transaction_type": event.event_type.value,
                },
            )
        return NormalizedEvent(
            source_system=event.source_system,
            source_event_id=event.event_id,
            event_type=event.event_type.value,
            payment_id=event.payment_id,
            event_time=event.event_time,
            content_hash=content_hash,
            business_key=event.processor_transaction_id,
            business_hash=business_hash,
            rows=[row],
        )

    if isinstance(event, BillingLedgerEvent):
        row = SilverRow(
            "billing_ledger_entries",
            {
                **_common(event),
                "ledger_entry_id": event.ledger_entry_id,
                "payment_id": event.payment_id,
                "account_ref": event.account_ref,
                "entry_type": event.event_type.value,
                "direction": event.direction.value,
                "posting_status": event.posting_status.value,
                "original_ledger_entry_id": event.original_ledger_entry_id,
                "processor_reference": event.processor_reference,
                "adjustment_reason": event.adjustment_reason,
            },
        )
        return NormalizedEvent(
            source_system=event.source_system,
            source_event_id=event.event_id,
            event_type=event.event_type.value,
            payment_id=event.payment_id,
            event_time=event.event_time,
            content_hash=content_hash,
            business_key=event.ledger_entry_id,
            business_hash=business_hash,
            rows=[row],
        )

    raise TypeError(f"unsupported event type {type(event)!r}")
