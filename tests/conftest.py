import pytest_asyncio

from ledger.db import apply_schema, create_pool, truncate_all


@pytest_asyncio.fixture
async def pool():
    p = await create_pool()
    async with p.acquire() as conn:
        await apply_schema(conn)
        await truncate_all(conn)
    yield p
    await p.close()
