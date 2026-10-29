"""Loading and validation of the versioned reconciliation rule set."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from processing.models import PaymentMethod
from processing.validation import TimestampRules


class _MethodGrace(BaseModel):
    model_config = ConfigDict(extra="forbid")
    default: int = Field(ge=0)
    CREDIT_CARD: int | None = Field(default=None, ge=0)
    DEBIT_CARD: int | None = Field(default=None, ge=0)
    ONLINE_BANKING: int | None = Field(default=None, ge=0)
    PRE_AUTHORIZED_DEBIT: int | None = Field(default=None, ge=0)


class _Grace(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_record: int = Field(ge=0)
    processor_outcome: _MethodGrace
    billing_posting: _MethodGrace
    refund_posting: int = Field(ge=0)
    reversal_posting: int = Field(ge=0)
    chargeback_posting: int = Field(ge=0)
    settlement: int = Field(ge=0)
    orphan_reference: int = Field(ge=0)


class _Calendar(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timezone: str
    non_business_weekdays: list[int] = Field(default_factory=lambda: [5, 6])  # Monday = 0
    holidays: list[date] = Field(default_factory=list)
    business_day_methods: list[PaymentMethod] = Field(default_factory=list)

    @field_validator("timezone")
    @classmethod
    def _tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown timezone {value!r}") from exc
        return value

    @field_validator("non_business_weekdays")
    @classmethod
    def _weekdays(cls, value: list[int]) -> list[int]:
        if any(d < 0 or d > 6 for d in value) or len(set(value)) >= 7:
            raise ValueError("non_business_weekdays must be 0-6 and leave at least one business day")
        return sorted(set(value))


class _TimestampRules(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_future_skew_seconds: int = Field(ge=0)
    max_age_days: int = Field(ge=1)


class _Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_quarantine_rate: Decimal = Field(ge=0, le=1)
    max_duplicate_delivery_rate: Decimal = Field(ge=0, le=1)
    max_late_event_rate: Decimal = Field(ge=0, le=1)
    max_open_exception_rate: Decimal = Field(ge=0, le=1)
    min_source_completeness: Decimal = Field(ge=0, le=1)


class _RulesFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    rule_version: str = Field(min_length=1, max_length=40)
    description: str
    grace_periods_seconds: _Grace
    business_calendar: _Calendar
    timestamp_rules: _TimestampRules
    late_event_threshold_seconds: int = Field(ge=0)
    data_quality_thresholds: _Thresholds

    @field_validator("rule_version")
    @classmethod
    def _no_spaces(cls, value: str) -> str:
        if value.strip() != value or " " in value:
            raise ValueError("rule_version must not contain spaces")
        return value


@dataclass(frozen=True)
class BusinessCalendar:
    """Bank processing days. Waiting periods of bank-settled methods only elapse on business days."""

    tz: ZoneInfo
    non_business_weekdays: frozenset[int]
    holidays: frozenset[date]
    business_day_methods: frozenset[str]

    def is_business_day(self, day: date) -> bool:
        return day.weekday() not in self.non_business_weekdays and day not in self.holidays

    def add(self, start: datetime, duration: timedelta, method: str | None, business_days: bool = False) -> datetime:
        """start + duration, counting only business-day time for business-day methods (DST-safe, in UTC)."""
        if not business_days and method not in self.business_day_methods:
            return start + duration
        t, remaining = start.astimezone(UTC), duration
        while True:
            local_day = t.astimezone(self.tz).date()
            next_midnight = datetime.combine(local_day + timedelta(days=1), time.min, tzinfo=self.tz).astimezone(UTC)
            if not self.is_business_day(local_day):
                t = next_midnight
                continue
            if t + remaining <= next_midnight:
                return (t + remaining).astimezone(start.tzinfo)
            remaining -= next_midnight - t
            t = next_midnight


@dataclass(frozen=True)
class ReconciliationRules:
    rule_version: str
    description: str
    application_record_grace: timedelta
    processor_grace_default: timedelta
    processor_grace_by_method: dict[str, timedelta]
    billing_grace_default: timedelta
    billing_grace_by_method: dict[str, timedelta]
    refund_posting_grace: timedelta
    reversal_posting_grace: timedelta
    chargeback_posting_grace: timedelta
    settlement_grace: timedelta
    orphan_reference_grace: timedelta
    calendar: BusinessCalendar
    timestamp_rules: TimestampRules
    late_event_threshold: timedelta
    dq_thresholds: dict[str, Decimal]
    config_hash: str
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    def processor_grace(self, method: str | None) -> timedelta:
        return self.processor_grace_by_method.get(method or "", self.processor_grace_default)

    def billing_grace(self, method: str | None) -> timedelta:
        return self.billing_grace_by_method.get(method or "", self.billing_grace_default)


def _by_method(grace: _MethodGrace) -> dict[str, timedelta]:
    out = {}
    for method in PaymentMethod:
        value = getattr(grace, method.value)
        if value is not None:
            out[method.value] = timedelta(seconds=value)
    return out


def rules_from_dict(data: dict[str, Any]) -> ReconciliationRules:
    parsed = _RulesFile.model_validate(data)
    canonical = json.dumps(parsed.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    g = parsed.grace_periods_seconds
    cal = parsed.business_calendar
    return ReconciliationRules(
        rule_version=parsed.rule_version,
        description=parsed.description,
        application_record_grace=timedelta(seconds=g.application_record),
        processor_grace_default=timedelta(seconds=g.processor_outcome.default),
        processor_grace_by_method=_by_method(g.processor_outcome),
        billing_grace_default=timedelta(seconds=g.billing_posting.default),
        billing_grace_by_method=_by_method(g.billing_posting),
        refund_posting_grace=timedelta(seconds=g.refund_posting),
        reversal_posting_grace=timedelta(seconds=g.reversal_posting),
        chargeback_posting_grace=timedelta(seconds=g.chargeback_posting),
        settlement_grace=timedelta(seconds=g.settlement),
        orphan_reference_grace=timedelta(seconds=g.orphan_reference),
        calendar=BusinessCalendar(
            tz=ZoneInfo(cal.timezone),
            non_business_weekdays=frozenset(cal.non_business_weekdays),
            holidays=frozenset(cal.holidays),
            business_day_methods=frozenset(m.value for m in cal.business_day_methods),
        ),
        timestamp_rules=TimestampRules(
            max_future_skew_s=parsed.timestamp_rules.max_future_skew_seconds,
            max_age_days=parsed.timestamp_rules.max_age_days,
        ),
        late_event_threshold=timedelta(seconds=parsed.late_event_threshold_seconds),
        dq_thresholds=dict(parsed.data_quality_thresholds.model_dump()),
        config_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        raw=json.loads(canonical),
    )


def load_rules(path: Path) -> ReconciliationRules:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"rules file {path} must contain a mapping")
    return rules_from_dict(data)
