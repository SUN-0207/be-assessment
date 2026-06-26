from ledger.db import create_pool


async def test_schema_creates_tables(pool):
    async with pool.acquire() as conn:
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
    names = {r["table_name"] for r in tables}
    assert {"balances", "exchange_rates"} <= names


async def test_truncate_all_clears_rows(pool):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO balances (id, name, balance_usd) VALUES (100, 'x', 5)")
        from ledger.db import truncate_all

        await truncate_all(conn)
        count = await conn.fetchval("SELECT count(*) FROM balances")
    assert count == 0
