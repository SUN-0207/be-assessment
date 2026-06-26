import csv
from datetime import date
from decimal import Decimal

from ledger.generate_data import generate
from ledger.rates import build_rate_map, read_rates


def _read_txns(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_generate_invariants(tmp_path):
    tx_path, rates_path = generate(seed=42, n_transactions=2000, out_dir=str(tmp_path))
    txns = _read_txns(tx_path)
    assert len(txns) == 2000

    ids = {int(r["id"]) for r in txns}
    assert all(100 <= i <= 999 for i in ids)
    dates = {r["date"] for r in txns}
    assert len(dates) <= 10

    rate_map = build_rate_map(read_rates(rates_path))
    # Every (currency, date) in transactions has a matching rate.
    for r in txns:
        assert (r["currency"].upper(), date.fromisoformat(r["date"])) in rate_map
    # USD always 1.0
    for (cur, _d), rate in rate_map.items():
        if cur == "USD":
            assert rate == Decimal("1.0")


def test_generate_is_deterministic(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    tx_a, _ = generate(seed=7, n_transactions=500, out_dir=str(a))
    tx_b, _ = generate(seed=7, n_transactions=500, out_dir=str(b))
    assert open(tx_a).read() == open(tx_b).read()
