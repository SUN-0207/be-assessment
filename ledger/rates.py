import csv
from datetime import date
from decimal import ROUND_HALF_DOWN, ROUND_HALF_UP, Decimal
from typing import NamedTuple

TWO_PLACES = Decimal("0.01")


class RateRow(NamedTuple):
    rate_date: date
    currency: str
    rate: Decimal


def to_usd(plus: Decimal, minus: Decimal, rate: Decimal) -> Decimal:
    return (plus - minus) * rate


def from_usd(usd: Decimal, rate: Decimal) -> Decimal:
    return usd / rate


def round_money(value: Decimal) -> Decimal:
    """Round to 2 decimal places, half toward positive infinity (ROUND_HALF_UP for non-negative,
    ROUND_HALF_DOWN for negative — net effect: ties always round toward +inf)."""
    if value >= 0:
        return value.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    return value.quantize(TWO_PLACES, rounding=ROUND_HALF_DOWN)


def read_rates(path: str) -> list[RateRow]:
    rows: list[RateRow] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(
                RateRow(
                    date.fromisoformat(r["date"]),
                    r["currency"].upper(),
                    Decimal(r["rate"]),
                )
            )
    return rows


def build_rate_map(rates: list[RateRow]) -> dict[tuple[str, date], Decimal]:
    return {(r.currency, r.rate_date): r.rate for r in rates}
