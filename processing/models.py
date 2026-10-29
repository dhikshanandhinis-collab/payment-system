"""Source event contracts (schema version 1.0).

These Pydantic models are the executable form of the JSON Schemas in ``schemas/``.
Amounts are positive magnitudes; the financial direction is carried by the event type
(and, for ledger events, by an explicit ``direction`` field).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from common.money import ISO_4217_CODES

SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})
_AMOUNT_TEXT = re.compile(r"^(0|[1-9][0-9]{0,11})(\.[0-9]{1,2})?$")

Identifier = Annotated[str, Field(min_length=3, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:\-]*$")]
Amount = Annotated[Decimal, Field(gt=Decimal("0"), max_digits=14, decimal_places=2)]


class SourceSystem(StrEnum):
    MYBELL_APP = "mybell_app"
    PAYMENT_PROCESSOR = "payment_processor"
    BILLING_LEDGER = "billing_ledger"


class PaymentMethod(StrEnum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    PRE_AUTHORIZED_DEBIT = "PRE_AUTHORIZED_DEBIT"
    ONLINE_BANKING = "ONLINE_BANKING"


class AppEventType(StrEnum):
    PAYMENT_INITIATED = "PAYMENT_INITIATED"
    PAYMENT_SUCCEEDED = "PAYMENT_SUCCEEDED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT"
    PAYMENT_STATUS_UPDATED = "PAYMENT_STATUS_UPDATED"


class AppStatus(StrEnum):
    INITIATED = "INITIATED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class ProcessorEventType(StrEnum):
    AUTHORIZATION = "AUTHORIZATION"
    CAPTURE = "CAPTURE"
    DECLINE = "DECLINE"
    REVERSAL = "REVERSAL"
    REFUND = "REFUND"


class ProcessorStatus(StrEnum):
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    DECLINED = "DECLINED"
    REVERSED = "REVERSED"
    REFUNDED = "REFUNDED"


class LedgerEntryType(StrEnum):
    CHARGE = "CHARGE"
    PAYMENT_CREDIT = "PAYMENT_CREDIT"
    POSTING_CONFIRMATION = "POSTING_CONFIRMATION"
    REFUND_DEBIT = "REFUND_DEBIT"
    REVERSAL = "REVERSAL"
    ADJUSTMENT = "ADJUSTMENT"


class PostingStatus(StrEnum):
    PENDING = "PENDING"
    POSTED = "POSTED"
    REJECTED = "REJECTED"


class Direction(StrEnum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


APP_STATUS_BY_EVENT: dict[AppEventType, AppStatus | None] = {
    AppEventType.PAYMENT_INITIATED: AppStatus.INITIATED,
    AppEventType.PAYMENT_SUCCEEDED: AppStatus.SUCCEEDED,
    AppEventType.PAYMENT_FAILED: AppStatus.FAILED,
    AppEventType.PAYMENT_TIMEOUT: AppStatus.UNKNOWN,
    AppEventType.PAYMENT_STATUS_UPDATED: None,  # any status allowed
}

PROCESSOR_STATUS_BY_EVENT: dict[ProcessorEventType, ProcessorStatus] = {
    ProcessorEventType.AUTHORIZATION: ProcessorStatus.AUTHORIZED,
    ProcessorEventType.CAPTURE: ProcessorStatus.CAPTURED,
    ProcessorEventType.DECLINE: ProcessorStatus.DECLINED,
    ProcessorEventType.REVERSAL: ProcessorStatus.REVERSED,
    ProcessorEventType.REFUND: ProcessorStatus.REFUNDED,
}

LEDGER_FIXED_DIRECTION: dict[LedgerEntryType, Direction] = {
    LedgerEntryType.CHARGE: Direction.DEBIT,
    LedgerEntryType.PAYMENT_CREDIT: Direction.CREDIT,
    LedgerEntryType.REFUND_DEBIT: Direction.DEBIT,
}


class _Envelope(BaseModel):
    # extra="forbid": unknown fields in a 1.0 payload are rejected rather than silently dropped.
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=False)

    event_id: Identifier
    schema_version: Literal["1.0"]
    event_time: AwareDatetime
    correlation_id: Identifier

    @field_validator("event_time")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


class _Monetary(BaseModel):
    amount: Amount
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]

    @field_validator("amount", mode="before")
    @classmethod
    def _amount_is_string(cls, value: object) -> object:
        # Contract: amounts are decimal strings, so no producer can introduce binary floating point.
        if not isinstance(value, str):
            raise ValueError("amount must be a decimal string such as \"125.50\"")
        if not _AMOUNT_TEXT.match(value):
            raise ValueError("amount must be a positive decimal string with at most 2 decimal places")
        return value

    @field_validator("currency")
    @classmethod
    def _iso(cls, value: str) -> str:
        if value not in ISO_4217_CODES:
            raise ValueError(f"unsupported ISO 4217 currency code {value!r}")
        return value


class AppPaymentEvent(_Envelope, _Monetary):
    source_system: Literal["mybell_app"]
    event_type: AppEventType
    payment_id: Identifier
    account_ref: Identifier
    payment_method: PaymentMethod
    source_status: AppStatus

    @model_validator(mode="after")
    def _status_matches_type(self) -> AppPaymentEvent:
        expected = APP_STATUS_BY_EVENT[self.event_type]
        if expected is not None and self.source_status != expected:
            raise ValueError(
                f"source_status {self.source_status} is inconsistent with event_type {self.event_type}"
            )
        if self.event_type is AppEventType.PAYMENT_STATUS_UPDATED and self.source_status is AppStatus.INITIATED:
            raise ValueError("PAYMENT_STATUS_UPDATED cannot move a payment back to INITIATED")
        return self


class ProcessorTransactionEvent(_Envelope, _Monetary):
    source_system: Literal["payment_processor"]
    event_type: ProcessorEventType
    processor_transaction_id: Identifier
    payment_id: Identifier
    original_processor_transaction_id: Identifier | None = None
    processor_status: ProcessorStatus

    @model_validator(mode="after")
    def _consistency(self) -> ProcessorTransactionEvent:
        if PROCESSOR_STATUS_BY_EVENT[self.event_type] != self.processor_status:
            raise ValueError(
                f"processor_status {self.processor_status} is inconsistent with event_type {self.event_type}"
            )
        needs_original = self.event_type in (ProcessorEventType.REFUND, ProcessorEventType.REVERSAL)
        forbids_original = self.event_type in (ProcessorEventType.AUTHORIZATION, ProcessorEventType.DECLINE)
        if needs_original and not self.original_processor_transaction_id:
            raise ValueError(f"{self.event_type} requires original_processor_transaction_id")
        if forbids_original and self.original_processor_transaction_id:
            raise ValueError(f"{self.event_type} must not reference an original transaction")
        if self.original_processor_transaction_id == self.processor_transaction_id:
            raise ValueError("a transaction cannot reference itself as original")
        return self


class BillingLedgerEvent(_Envelope, _Monetary):
    source_system: Literal["billing_ledger"]
    event_type: LedgerEntryType
    ledger_entry_id: Identifier
    payment_id: Identifier | None = None
    account_ref: Identifier
    direction: Direction
    posting_status: PostingStatus
    original_ledger_entry_id: Identifier | None = None
    processor_reference: Identifier | None = None
    adjustment_reason: Annotated[str, Field(min_length=3, max_length=200)] | None = None

    @model_validator(mode="after")
    def _consistency(self) -> BillingLedgerEvent:
        fixed = LEDGER_FIXED_DIRECTION.get(self.event_type)
        if fixed is not None and self.direction != fixed:
            raise ValueError(f"{self.event_type} must have direction {fixed}")
        if self.event_type is not LedgerEntryType.CHARGE and not self.payment_id:
            raise ValueError(f"{self.event_type} requires payment_id")
        if self.event_type in (LedgerEntryType.POSTING_CONFIRMATION, LedgerEntryType.REVERSAL):
            if not self.original_ledger_entry_id:
                raise ValueError(f"{self.event_type} requires original_ledger_entry_id")
        if self.event_type is LedgerEntryType.POSTING_CONFIRMATION and self.posting_status is not PostingStatus.POSTED:
            raise ValueError("POSTING_CONFIRMATION must have posting_status POSTED")
        if self.event_type is LedgerEntryType.REFUND_DEBIT and not self.processor_reference:
            raise ValueError("REFUND_DEBIT requires processor_reference (the processor refund id)")
        if self.event_type is LedgerEntryType.ADJUSTMENT and not self.adjustment_reason:
            raise ValueError("ADJUSTMENT requires adjustment_reason")
        if self.original_ledger_entry_id == self.ledger_entry_id:
            raise ValueError("a ledger entry cannot reference itself")
        return self


SourceEvent = AppPaymentEvent | ProcessorTransactionEvent | BillingLedgerEvent

MODEL_BY_SOURCE: dict[str, type[AppPaymentEvent] | type[ProcessorTransactionEvent] | type[BillingLedgerEvent]] = {
    SourceSystem.MYBELL_APP.value: AppPaymentEvent,
    SourceSystem.PAYMENT_PROCESSOR.value: ProcessorTransactionEvent,
    SourceSystem.BILLING_LEDGER.value: BillingLedgerEvent,
}
