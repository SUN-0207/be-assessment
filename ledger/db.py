from pathlib import Path

import asyncpg

from ledger.config import load_config

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


async def create_pool(min_size: int = 2, max_size: int | None = None) -> asyncpg.Pool:
    cfg = load_config()
    return await asyncpg.create_pool(
        dsn=cfg.database_url,
        min_size=min_size,
        max_size=max_size if max_size is not None else cfg.pool_size,
    )


async def apply_schema(conn) -> None:
    await conn.execute(SCHEMA_PATH.read_text())


async def truncate_all(conn) -> None:
    await conn.execute("TRUNCATE balances, exchange_rates;")
