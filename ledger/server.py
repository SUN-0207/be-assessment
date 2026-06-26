import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Query

from ledger.db import create_pool
from ledger.rates import from_usd, round_money

MAX_DATE_RATE_SQL = (
    "SELECT rate FROM exchange_rates "
    "WHERE currency = $1 AND rate_date = (SELECT max(rate_date) FROM exchange_rates)"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state._pool_loop = asyncio.get_running_loop()
    try:
        yield
    finally:
        await app.state.pool.close()
        app.state.pool = None


app = FastAPI(lifespan=lifespan)


async def _get_pool():
    """Return app.state.pool, initialising lazily if lifespan did not fire (e.g. tests)."""
    current_loop = asyncio.get_running_loop()
    pool = getattr(app.state, "pool", None)
    pool_loop = getattr(app.state, "_pool_loop", None)
    if pool is None or pool_loop is not current_loop:
        pool = await create_pool()
        app.state.pool = pool
        app.state._pool_loop = current_loop
    return pool


async def _conversion_rate(conn, currency: str) -> Decimal:
    currency = currency.upper()
    if currency == "USD":
        return Decimal("1")
    row = await conn.fetchrow(MAX_DATE_RATE_SQL, currency)
    if row is None:
        raise HTTPException(status_code=400, detail=f"No rate available for currency {currency}")
    return row["rate"]


@app.get("/balance/{account_id}")
async def get_balance(account_id: int, currency: str = Query("USD")):
    pool = await _get_pool()
    async with pool.acquire() as conn:
        # Read the balance and the rate from one consistent snapshot, so a
        # concurrent ingest publish can never be observed half-applied within a
        # single request.
        async with conn.transaction(isolation="repeatable_read"):
            row = await conn.fetchrow(
                "SELECT name, balance_usd FROM balances WHERE id = $1", account_id
            )
            rate = None if row is None else await _conversion_rate(conn, currency)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
    value = round_money(from_usd(row["balance_usd"], rate))
    return {
        "id": account_id,
        "name": row["name"],
        "currency": currency.upper(),
        "balance": str(value),
    }


@app.get("/total")
async def get_total(currency: str = Query("USD")):
    pool = await _get_pool()
    async with pool.acquire() as conn:
        # Sum and rate from one consistent snapshot (see get_balance).
        async with conn.transaction(isolation="repeatable_read"):
            total_usd = await conn.fetchval("SELECT COALESCE(SUM(balance_usd), 0) FROM balances")
            rate = await _conversion_rate(conn, currency)
    value = round_money(from_usd(total_usd, rate))
    return {"currency": currency.upper(), "total": str(value)}
