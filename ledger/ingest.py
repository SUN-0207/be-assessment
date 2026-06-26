import asyncio
import csv
import sys
from collections import OrderedDict
from datetime import date
from decimal import Decimal
from typing import Iterable, Iterator, NamedTuple

from ledger.config import load_config
from ledger.db import apply_schema, create_pool, truncate_all
from ledger.rates import RateRow, build_rate_map, read_rates, to_usd

UPSERT_SQL = """
INSERT INTO balances (id, name, balance_usd)
SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
ON CONFLICT (id) DO UPDATE
    SET balance_usd = balances.balance_usd + EXCLUDED.balance_usd;
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
        for r in csv.DictReader(f):
            currency = r["currency"].upper()
            d = date.fromisoformat(r["date"])
            rate = rate_map[(currency, d)]
            delta = to_usd(Decimal(r["plus"]), Decimal(r["minus"]), rate)
            yield TxnRow(int(r["id"]), r["name"], delta)


async def upsert_batch(pool, batch: Batch) -> None:
    async with pool.acquire() as conn:
        await conn.execute(UPSERT_SQL, batch.ids, batch.names, batch.deltas)


async def bulk_load_rates(conn, rates: list[RateRow]) -> None:
    await conn.copy_records_to_table(
        "exchange_rates",
        records=[(r.rate_date, r.currency, r.rate) for r in rates],
        columns=["rate_date", "currency", "rate"],
    )


async def run_ingest(transactions_path: str, rates_path: str) -> None:
    cfg = load_config()
    rates = read_rates(rates_path)
    rate_map = build_rate_map(rates)

    pool = await create_pool()
    try:
        async with pool.acquire() as conn:
            await apply_schema(conn)
            await truncate_all(conn)
            await bulk_load_rates(conn, rates)

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
    finally:
        await pool.close()


def main() -> None:
    tx = sys.argv[1] if len(sys.argv) > 1 else "transactions.csv"
    rates = sys.argv[2] if len(sys.argv) > 2 else "rates.csv"
    asyncio.run(run_ingest(tx, rates))


if __name__ == "__main__":
    main()
