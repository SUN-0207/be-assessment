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


def test_aggregate_emits_sorted_ids():
    # Ids arrive jumbled; each batch must present them ascending so concurrent
    # upserts lock rows in a consistent order (deadlock prevention).
    rows = [TxnRow(i, f"acct{i}", Decimal("1")) for i in (300, 100, 250, 100, 175)]
    (batch,) = list(batch_and_aggregate(rows, batch_size=10))
    assert batch.ids == sorted(batch.ids)
    assert batch.ids == [100, 175, 250, 300]
    # names/deltas stay aligned with ids after the sort
    assert dict(zip(batch.ids, batch.names))[100] == "acct100"
    assert dict(zip(batch.ids, batch.deltas))[100] == Decimal("2")  # id 100 appeared twice


async def test_run_ingest_many_concurrent_batches_no_deadlock(pool, tmp_path, monkeypatch):
    # Real-flow scenario: many concurrent batches sharing overlapping id sets.
    # Before sorting ids per batch this deadlocked; now it must complete and match
    # an independent inline reference.
    import csv
    from collections import defaultdict
    from datetime import date

    from ledger.generate_data import generate
    from ledger.ingest import run_ingest
    from ledger.rates import build_rate_map, read_rates

    monkeypatch.setenv("BATCH_SIZE", "300")  # ~20 concurrent multi-id batches
    tx, rates = generate(seed=7, n_transactions=6000, out_dir=str(tmp_path))

    await run_ingest(tx, rates)  # raised DeadlockDetectedError before the fix

    rmap = build_rate_map(read_rates(rates))
    ref: dict[int, Decimal] = defaultdict(lambda: Decimal(0))
    with open(tx, newline="") as f:
        for r in csv.DictReader(f):
            ref[int(r["id"])] += (Decimal(r["plus"]) - Decimal(r["minus"])) * rmap[
                (r["currency"].upper(), date.fromisoformat(r["date"]))
            ]
    async with pool.acquire() as conn:
        got = {
            r["id"]: r["balance_usd"]
            for r in await conn.fetch("SELECT id, balance_usd FROM balances")
        }

    assert set(got) == set(ref)
    for acc, expected in ref.items():
        assert got[acc] == expected, f"id {acc}: {got[acc]} != {expected}"
