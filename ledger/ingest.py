import asyncio
import csv
import sys
from collections import OrderedDict
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Iterable, Iterator, NamedTuple

from ledger.config import load_config
from ledger.db import apply_schema, create_pool, truncate_staging
from ledger.rates import RateRow, build_rate_map, read_rates, to_usd

# Ingestion writes into staging tables, then publishes into the live tables in a
# single transaction (the swap below). The live tables are never touched until
# that swap commits, so a read server can keep serving a consistent snapshot
# throughout an ingest, and a crash before the swap leaves the live data intact.
UPSERT_SQL = """
INSERT INTO balances_staging (id, name, balance_usd)
SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
ON CONFLICT (id) DO UPDATE
    SET balance_usd = balances_staging.balance_usd + EXCLUDED.balance_usd;
"""

# Swap staged data into the live tables. Run inside one transaction so readers
# see either the old snapshot or the new one in full — never a partial state.
# DELETE (not TRUNCATE) on purpose: TRUNCATE is NOT MVCC-safe — a concurrent
# reader on an older snapshot would see the table as empty. DELETE keeps the old
# rows visible to in-flight snapshots and only takes ROW EXCLUSIVE, so reads are
# never blocked and never observe a partial swap.
PUBLISH_SQL = """
DELETE FROM balances;
INSERT INTO balances SELECT * FROM balances_staging;
DELETE FROM exchange_rates;
INSERT INTO exchange_rates SELECT * FROM exchange_rates_staging;
"""


class TxnRow(NamedTuple):
    id: int
    name: str
    usd_delta: Decimal


class Batch(NamedTuple):
    ids: list[int]
    names: list[str]
    deltas: list[Decimal]


def _aggregate(buffer: list[TxnRow]) -> Batch:
    acc: "OrderedDict[int, list]" = OrderedDict()
    for r in buffer:
        if r.id in acc:
            acc[r.id][1] += r.usd_delta
        else:
            acc[r.id] = [r.name, r.usd_delta]
    # Sort ids ascending so every concurrent batch acquires row locks in the
    # same order. Without this, two batches updating an overlapping set of ids
    # in different orders can deadlock (Postgres locks ON CONFLICT rows in the
    # order they are presented).
    ids = sorted(acc)
    names = [acc[i][0] for i in ids]
    deltas = [acc[i][1] for i in ids]
    return Batch(ids, names, deltas)


def batch_and_aggregate(rows: Iterable[TxnRow], batch_size: int) -> Iterator[Batch]:
    buffer: list[TxnRow] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield _aggregate(buffer)
            buffer = []
    if buffer:
        yield _aggregate(buffer)


def read_transactions(path: str, rate_map: dict) -> Iterator[TxnRow]:
    with open(path, newline="") as f:
        for lineno, r in enumerate(csv.DictReader(f), start=2):
            try:
                acc_id = int(r["id"])
                name = r["name"]
                plus = Decimal(r["plus"])
                minus = Decimal(r["minus"])
                currency = r["currency"].upper()
                d = date.fromisoformat(r["date"])
            except (KeyError, ValueError, InvalidOperation) as e:
                raise ValueError(
                    f"{path} line {lineno}: malformed transaction row {dict(r)!r} ({e})"
                ) from e
            try:
                rate = rate_map[(currency, d)]
            except KeyError:
                raise ValueError(
                    f"{path} line {lineno}: no exchange rate for ({currency}, {d.isoformat()})"
                ) from None
            yield TxnRow(acc_id, name, to_usd(plus, minus, rate))


async def upsert_batch(pool, batch: Batch) -> None:
    async with pool.acquire() as conn:
        await conn.execute(UPSERT_SQL, batch.ids, batch.names, batch.deltas)


async def bulk_load_rates(conn, rates: list[RateRow]) -> None:
    await conn.copy_records_to_table(
        "exchange_rates_staging",
        records=[(r.rate_date, r.currency, r.rate) for r in rates],
        columns=["rate_date", "currency", "rate"],
    )


async def publish(conn) -> None:
    """Atomically replace the live tables with the staged data, in one transaction."""
    async with conn.transaction():
        await conn.execute(PUBLISH_SQL)


async def run_ingest(transactions_path: str, rates_path: str) -> None:
    cfg = load_config()
    rates = read_rates(rates_path)
    rate_map = build_rate_map(rates)

    pool = await create_pool()
    try:
        # 1. Prepare staging (live tables are untouched).
        async with pool.acquire() as conn:
            await apply_schema(conn)
            await truncate_staging(conn)
            await bulk_load_rates(conn, rates)

        # 2. Concurrently accumulate balances into staging.
        sem = asyncio.Semaphore(cfg.pool_size)

        async def worker(batch: Batch) -> None:
            async with sem:
                await upsert_batch(pool, batch)

        txns = read_transactions(transactions_path, rate_map)
        tasks = [
            asyncio.create_task(worker(batch))
            for batch in batch_and_aggregate(txns, cfg.batch_size)
        ]
        await asyncio.gather(*tasks)

        # 3. Publish: atomic swap into the live tables.
        async with pool.acquire() as conn:
            await publish(conn)
    finally:
        await pool.close()


def main() -> None:
    tx = sys.argv[1] if len(sys.argv) > 1 else "transactions.csv"
    rates = sys.argv[2] if len(sys.argv) > 2 else "rates.csv"
    try:
        asyncio.run(run_ingest(tx, rates))
    except (FileNotFoundError, ValueError) as e:
        print(f"ingest failed: {e}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
