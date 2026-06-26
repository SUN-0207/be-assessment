import csv
import subprocess
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal

from httpx import ASGITransport, AsyncClient

from ledger.db import create_pool
from ledger.generate_data import generate
from ledger.rates import build_rate_map, from_usd, read_rates, round_money
from ledger.server import app


def _reference(tx_path: str, rates_path: str):
    """Independent reference: inline arithmetic, no ingest code reused."""
    rate_map = build_rate_map(read_rates(rates_path))
    balances: dict[int, Decimal] = defaultdict(lambda: Decimal(0))
    names: dict[int, str] = {}
    with open(tx_path, newline="") as f:
        for r in csv.DictReader(f):
            cur = r["currency"].upper()
            d = date.fromisoformat(r["date"])
            delta = (Decimal(r["plus"]) - Decimal(r["minus"])) * rate_map[(cur, d)]
            balances[int(r["id"])] += delta
            names[int(r["id"])] = r["name"]
    max_date = max(d for (_c, d) in rate_map)
    return balances, names, rate_map, max_date


async def test_ingest_restart_and_serve(tmp_path):
    tx_path, rates_path = generate(seed=123, n_transactions=5000, out_dir=str(tmp_path))
    balances, names, rate_map, max_date = _reference(tx_path, rates_path)

    # Run ingestion as a separate process and let it fully exit (restart isolation).
    proc = subprocess.run(
        [sys.executable, "-m", "ledger.ingest", tx_path, rates_path],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

    eur_rate = rate_map[("EUR", max_date)]
    sample_ids = sorted(balances)[:20]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        for acc in sample_ids:
            r = await c.get(f"/balance/{acc}")
            assert r.json()["balance"] == str(round_money(balances[acc]))

            r_eur = await c.get(f"/balance/{acc}", params={"currency": "EUR"})
            expected_eur = str(round_money(from_usd(balances[acc], eur_rate)))
            assert r_eur.json()["balance"] == expected_eur

        total = sum(balances.values(), Decimal(0))
        r_total = await c.get("/total")
        assert r_total.json()["total"] == str(round_money(total))

        r_total_eur = await c.get("/total", params={"currency": "EUR"})
        assert r_total_eur.json()["total"] == str(round_money(from_usd(total, eur_rate)))

        missing = await c.get("/balance/99")
        assert missing.status_code == 404
