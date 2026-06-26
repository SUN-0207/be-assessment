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
                (date(2026, 6, 24), "EUR", Decimal("1.0")),  # 1 EUR = 1 USD for easy math
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
        # 200 USD / 1.0 = 200.00 EUR; currency upper-cased
        assert r.json() == {"id": 100, "name": "acct100", "currency": "EUR", "balance": "200.00"}

        bad = await c.get("/balance/100", params={"currency": "ZZZ"})
        assert bad.status_code == 400


async def test_total(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/total")
        assert r.json() == {"currency": "USD", "total": "150.00"}  # 200 + (-50)
