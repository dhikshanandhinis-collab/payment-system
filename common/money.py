"""Exact decimal money helpers. Binary floating point is never used for money."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

MONEY_QUANTUM = Decimal("0.01")
ZERO = Decimal("0.00")

# Subset of ISO 4217 alphabetic codes accepted by the platform's validators.
ISO_4217_CODES = frozenset(
    {
        "AUD", "BRL", "CAD", "CHF", "CNY", "DKK", "EUR", "GBP", "HKD", "INR",
        "JPY", "MXN", "NOK", "NZD", "SEK", "SGD", "USD", "ZAR",
    }
)


class MoneyError(ValueError):
    """Raised when a value cannot be represented as an exact monetary amount."""


def to_money(value: object) -> Decimal:
    """Convert str/int/Decimal to a 2-decimal Decimal. Floats and extra precision are rejected."""
    if isinstance(value, bool) or isinstance(value, float):
        raise MoneyError(f"floating point/boolean values are not accepted for money: {value!r}")
    try:
        amount = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise MoneyError(f"invalid monetary amount: {value!r}") from exc
    if not amount.is_finite():
        raise MoneyError(f"non-finite monetary amount: {value!r}")
    exponent = amount.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -2:
        raise MoneyError(f"monetary amount has more than 2 decimal places: {value!r}")
    return amount.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_EVEN)


def money_sum(values: Iterable[Decimal]) -> Decimal:
    total = ZERO
    for value in values:
        total += value
    return total.quantize(MONEY_QUANTUM)


def money_str(value: Decimal | None) -> str | None:
    return None if value is None else str(value.quantize(MONEY_QUANTUM))
