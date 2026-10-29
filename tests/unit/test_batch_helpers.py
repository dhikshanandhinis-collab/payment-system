"""Batch helpers: business-date windows, control total comparison and quality thresholds."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from batch.backfill import date_range
from batch.control_totals import ControlTotal, QualityCheck, compare_control_totals
from batch.daily_reconciliation import business_window, previous_business_date
from processing.case_manager import business_date

TORONTO = ZoneInfo("America/Toronto")


def test_business_window_regular_day():
    start, end = business_window(date(2026, 10, 27), TORONTO)
    assert start == datetime(2026, 10, 27, 4, tzinfo=UTC) and end == datetime(2026, 10, 28, 4, tzinfo=UTC)


def test_business_window_dst_end_is_25_hours():
    start, end = business_window(date(2026, 11, 1), TORONTO)  # DST ends in Canada on 2026-11-01
    assert (end - start).total_seconds() == 25 * 3600


def test_business_date_derivation_uses_business_timezone():
    late_evening_local = datetime(2026, 10, 28, 2, 30, tzinfo=UTC)  # 22:30 on the 27th in Toronto
    assert business_date(late_evening_local, TORONTO) == date(2026, 10, 27)
    assert previous_business_date(TORONTO, datetime(2026, 10, 28, 3, tzinfo=UTC)) == date(2026, 10, 26)


def test_control_total_comparison():
    totals = [
        ControlTotal("mybell_app", "PAYMENT_REQUESTS", "CAD", 3, Decimal("300.00")),
        ControlTotal("payment_processor", "CAPTURES", "CAD", 2, Decimal("200.00")),
        ControlTotal("payment_processor", "DECLINES", "CAD", 1, Decimal("100.00")),
        ControlTotal("billing_ledger", "PAYMENT_CREDITS_EFFECTIVE", "CAD", 2, Decimal("190.00")),
    ]
    cmp = compare_control_totals(totals)
    assert cmp["processor_captures_vs_ledger_credits"]["difference"] == "-10.00"
    assert cmp["app_requests_vs_processor_captures"]["capture_count"] == 2


def test_quality_check_threshold_direction():
    assert QualityCheck("q", Decimal("0.01"), Decimal("0.02"), "<=", 1, 100).passed
    assert not QualityCheck("q", Decimal("0.03"), Decimal("0.02"), "<=", 3, 100).passed
    assert not QualityCheck("c", Decimal("0.90"), Decimal("0.95"), ">=", 90, 100).passed
    assert QualityCheck("c", None, Decimal("0.95"), ">=", 0, 0).passed


def test_backfill_range_validation():
    assert len(date_range(date(2026, 10, 1), date(2026, 10, 7))) == 7
    with pytest.raises(ValueError):
        date_range(date(2026, 10, 7), date(2026, 10, 1))
