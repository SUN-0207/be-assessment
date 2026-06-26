import asyncio
from decimal import Decimal

from ledger.ingest import Batch, TxnRow, batch_and_aggregate, upsert_batch


def test_aggregate_sums_duplicate_ids_within_batch():
    rows = [
        TxnRow(100, "acct100", Decimal("10")),
        TxnRow(100, "acct100", Decimal("5")),
        TxnRow(243, "acct243", Decimal("7")),
    ]
    batches = list(batch_and_aggregate(rows, batch_size=10))
    assert len(batches) == 1
    b = batches[0]
    by_id = dict(zip(b.ids, b.deltas))
    assert by_id[100] == Decimal("15")
    assert by_id[243] == Decimal("7")
    assert len(b.ids) == len(set(b.ids))  # unique ids within a batch


def test_batch_size_boundary():
    rows = [TxnRow(i, f"acct{i}", Decimal("1")) for i in range(100, 105)]
    batches = list(batch_and_aggregate(rows, batch_size=2))
    assert [len(b.ids) for b in batches] == [2, 2, 1]


async def test_concurrent_upserts_no_lost_update(pool):
    # Same id appears in many batches applied concurrently -> contention on one row.
    n_batches = 50
    delta = Decimal("1.25")
    batches = [Batch([100], ["acct100"], [delta]) for _ in range(n_batches)]
    await asyncio.gather(*(upsert_batch(pool, b) for b in batches))
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT balance_usd FROM balances WHERE id=100")
    assert total == delta * n_batches  # 62.50, no updates lost
