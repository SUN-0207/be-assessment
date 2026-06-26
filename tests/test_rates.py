from datetime import date
from decimal import Decimal

from ledger.rates import (
    RateRow,
    build_rate_map,
    from_usd,
    read_rates,
    round_money,
    to_usd,
)


def test_to_usd_multiplies():
    assert to_usd(Decimal("250.00"), Decimal("120.50"), Decimal("1.0")) == Decimal("129.50")
    assert to_usd(Decimal("80.00"), Decimal("0.00"), Decimal("1.0832")) == Decimal("86.656000")


def test_from_usd_divides():
    assert from_usd(Decimal("129.50"), Decimal("1.0")) == Decimal("129.50")
    # 100 USD at rate 0.00642 (JPY) -> 100 / 0.00642
    assert round_money(from_usd(Decimal("100"), Decimal("0.00642"))) == Decimal("15576.32")


def test_round_money_half_up():
    assert round_money(Decimal("1.005")) == Decimal("1.01")
    assert round_money(Decimal("2.675")) == Decimal("2.68")
    assert round_money(Decimal("-1.005")) == Decimal("-1.00")  # HALF_UP rounds toward +inf at .5


def test_read_rates_and_map(tmp_path):
    p = tmp_path / "rates.csv"
    p.write_text("date,currency,rate\n2026-06-15,usd,1.0\n2026-06-15,EUR,1.0832\n")
    rows = read_rates(str(p))
    assert RateRow(date(2026, 6, 15), "USD", Decimal("1.0")) in rows
    rate_map = build_rate_map(rows)
    assert rate_map[("EUR", date(2026, 6, 15))] == Decimal("1.0832")
