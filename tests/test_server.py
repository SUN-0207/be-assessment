from datetime import date
from decimal import Decimal

from httpx import ASGITransport, AsyncClient

from ledger.server import app


async def _seed(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO balances (id, name, balance_usd) VALUES (100,'acct100',200),(243,'acct243',-50)"
        )
        await conn.executemany(
            "INSERT INTO exchange_rates (rate_date, currency, rate) VALUES ($1,$2,$3)",
            [
                (date(2026, 6, 24), "USD", Decimal("1.0")),
                (date(2026, 6, 24), "EUR", Decimal("2.0")),  # rate at the most recent date
                (date(2026, 6, 15), "EUR", Decimal("5.0")),  # older rate — must NOT be used
                (date(2026, 6, 15), "USD", Decimal("1.0")),
            ],
        )


async def test_balance_usd_and_404(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/balance/100")
        assert r.status_code == 200
        assert r.json() == {"id": 100, "name": "acct100", "currency": "USD", "balance": "200.00"}

        r404 = await c.get("/balance/999")
        assert r404.status_code == 404


async def test_balance_non_usd_divides(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/balance/100", params={"currency": "eur"})
        # 200 USD ÷ 2.0 (the most-recent-date rate) = 100.00 EUR. A non-1.0 rate makes this fail
        # if the server multiplied instead of divided; the older 5.0 rate must be ignored (→ 40.00).
        assert r.json() == {"id": 100, "name": "acct100", "currency": "EUR", "balance": "100.00"}

        bad = await c.get("/balance/100", params={"currency": "ZZZ"})
        assert bad.status_code == 400


async def test_total(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/total")
        assert r.json() == {"currency": "USD", "total": "150.00"}  # 200 + (-50)

        r_eur = await c.get("/total", params={"currency": "eur"})
        assert r_eur.json() == {"currency": "EUR", "total": "75.00"}  # 150 ÷ 2.0 (max-date rate)


async def test_total_empty_db_is_zero(pool):
    # The pool fixture truncates balances; an empty ledger totals to 0 (spec: empty -> 0).
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/total")
        assert r.json() == {"currency": "USD", "total": "0.00"}
