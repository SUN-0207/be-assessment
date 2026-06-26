import asyncio
from decimal import Decimal

import pytest

from ledger.ingest import (
    Batch,
    TxnRow,
    batch_and_aggregate,
    publish,
    run_ingest,
    upsert_batch,
)


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
    # upsert_batch writes to the staging table; this checks the accumulation there.
    n_batches = 50
    delta = Decimal("1.25")
    batches = [Batch([100], ["acct100"], [delta]) for _ in range(n_batches)]
    await asyncio.gather(*(upsert_batch(pool, b) for b in batches))
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT balance_usd FROM balances_staging WHERE id=100")
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


async def test_writes_target_staging_then_publish_swaps(pool):
    # A live snapshot that must stay visible until the swap commits.
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO balances (id, name, balance_usd) VALUES (100,'old',999)")
        await conn.execute(
            "INSERT INTO exchange_rates (rate_date, currency, rate) VALUES ('2026-06-24','USD',1.0)"
        )

    # Stage a different dataset (upsert_batch targets balances_staging).
    await upsert_batch(
        pool, Batch([100, 200], ["acct100", "acct200"], [Decimal("5"), Decimal("7")])
    )

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO exchange_rates_staging (rate_date, currency, rate) VALUES ('2026-06-25','USD',1.0)"
        )
        # Live data is untouched while staging is populated.
        assert await conn.fetchval("SELECT balance_usd FROM balances WHERE id=100") == Decimal("999")

        # Publish swaps staged -> live atomically.
        await publish(conn)
        live = {
            r["id"]: r["balance_usd"]
            for r in await conn.fetch("SELECT id, balance_usd FROM balances")
        }
        max_rate_date = await conn.fetchval("SELECT max(rate_date) FROM exchange_rates")

    assert live == {100: Decimal("5"), 200: Decimal("7")}
    assert str(max_rate_date) == "2026-06-25"


async def test_failed_ingest_leaves_live_untouched(pool, tmp_path):
    # Seed a previous good state in the LIVE table.
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO balances (id, name, balance_usd) VALUES (100,'good',999)")

    # Input that fails mid-ingest: a transaction references a currency with no rate.
    tx = tmp_path / "t.csv"
    tx.write_text("id,name,plus,minus,currency,date\n100,acct100,10,0,XXX,2026-06-15\n")
    rates = tmp_path / "r.csv"
    rates.write_text("date,currency,rate\n2026-06-15,USD,1.0\n")

    with pytest.raises(ValueError):
        await run_ingest(str(tx), str(rates))

    # The live table still holds the previous good state — publish never ran.
    async with pool.acquire() as conn:
        live = {
            r["id"]: r["balance_usd"]
            for r in await conn.fetch("SELECT id, balance_usd FROM balances")
        }
    assert live == {100: Decimal("999")}


async def test_publish_is_mvcc_safe_for_inflight_readers(pool):
    # An in-flight read snapshot opened BEFORE a publish must keep seeing the old
    # data in full while the swap commits on another connection — never empty,
    # never the new rows. (This is why publish uses DELETE, not TRUNCATE, which
    # is not MVCC-safe and would make the reader observe an empty table.)
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO balances (id, name, balance_usd) VALUES (100,'old',999)")
    await upsert_batch(pool, Batch([200], ["acct200"], [Decimal("7")]))  # stage a new dataset

    reader = await pool.acquire()
    try:
        tx = reader.transaction(isolation="repeatable_read")
        await tx.start()
        before = {r["id"] for r in await reader.fetch("SELECT id FROM balances")}
        assert before == {100}

        async with pool.acquire() as writer:  # publish the swap, commit
            await publish(writer)

        during = {r["id"] for r in await reader.fetch("SELECT id FROM balances")}
        assert during == {100}, f"in-flight reader saw a partial swap: {during}"
        await tx.rollback()
    finally:
        await pool.release(reader)

    async with pool.acquire() as conn:  # a fresh read sees the new data
        after = {r["id"] for r in await conn.fetch("SELECT id FROM balances")}
    assert after == {200}
