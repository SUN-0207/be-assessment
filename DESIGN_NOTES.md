# Design Notes — Concurrency & Performance

## Concurrency safety (no lost updates)
Balance accumulation happens **inside PostgreSQL**, never as an app-side read-modify-write.
Ingestion accumulates into a staging table (see *Atomic publish* below):

    INSERT INTO balances_staging (id, name, balance_usd)
    SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
    ON CONFLICT (id) DO UPDATE
        SET balance_usd = balances_staging.balance_usd + EXCLUDED.balance_usd;

Many batches run concurrently (asyncpg pool), and the same account id appears in many of them.
For each conflicting row, `ON CONFLICT DO UPDATE` takes a row-level lock and performs the
`balance + delta` atomically, so the result is correct regardless of ordering or interleaving —
no update is lost. `tests/test_ingest.py::test_concurrent_upserts_no_lost_update` proves this by
applying 50 concurrent upserts to one row and asserting the exact sum.

**Intra-batch aggregation** is both a performance win and a correctness necessity: a single
`INSERT ... ON CONFLICT DO UPDATE` cannot affect the same target row twice, so duplicate ids
within one statement are pre-summed before the upsert.

**Consistent lock ordering (deadlock avoidance).** Concurrent batches update overlapping sets
of ids, and Postgres takes the `ON CONFLICT` row locks in the order rows are presented. If two
batches presented their shared ids in different orders they could deadlock (A holds id X waiting
for Y while B holds Y waiting for X). Each batch therefore sorts its ids ascending, so every
concurrent statement acquires locks in the same order and no cycle can form.
`tests/test_ingest.py::test_run_ingest_many_concurrent_batches_no_deadlock` exercises many
concurrent multi-id batches (it deadlocked before this fix).

## Atomic publish & online reload
Ingestion writes **only** to staging tables. The live `balances` / `exchange_rates` are replaced
in a single transaction at the very end:

    BEGIN;
    DELETE FROM balances;        INSERT INTO balances        SELECT * FROM balances_staging;
    DELETE FROM exchange_rates;  INSERT INTO exchange_rates  SELECT * FROM exchange_rates_staging;
    COMMIT;

So the read server can keep serving **while an ingest runs**: a request sees either the old
snapshot or the new one in full — never empty, partial, or a balance/rate mismatch. Two details
make it airtight:

- **`DELETE`, not `TRUNCATE`.** `TRUNCATE` is not MVCC-safe — a concurrent reader on an older
  snapshot would see the table as *empty*. `DELETE` keeps the old rows visible to in-flight
  snapshots and only takes `ROW EXCLUSIVE`, so reads are never blocked and never observe a partial
  swap. `tests/test_ingest.py::test_publish_is_mvcc_safe_for_inflight_readers` pins this (it was
  caught by a live read-during-reload test that saw a transient `0.00` with `TRUNCATE`).
- **Snapshot reads.** Each endpoint reads the balance and the rate inside one `REPEATABLE READ`
  transaction, so its two queries can't straddle a publish commit.

Crash safety falls out for free: if ingestion dies before the publish commit, the live tables are
untouched (`tests/test_ingest.py::test_failed_ingest_leaves_live_untouched`). Re-running is
idempotent — staging is rebuilt each run.

## Performance
- 50,000 single-row inserts collapse into a few hundred multi-row statements (batch size 5000,
  each aggregated to <=900 unique ids).
- A connection pool (default 16) runs batches concurrently, overlapping IO latency.
- `unnest(...)` ships each batch as arrays in a single round trip.
- All arithmetic runs in `NUMERIC`; the app only ships deltas. Rates are bulk-loaded via `COPY`.
- Transactions are streamed from CSV; memory stays bounded.
- Result: 50k rows ingest in well under a few seconds on a local PostgreSQL.

## Correctness & currency
- `NUMERIC(38,10)` + Python `Decimal` end to end; no floats on the money path.
- Ingest converts each transaction with `rate(currency, date)` (multiply); reads convert
  USD -> target with `rate(currency, max_date)` (divide). Output rounded to 2dp, HALF_UP.

## ORM consideration
An ORM was considered and deliberately **not** used. The core operation is an *additive* upsert
(`balance + EXCLUDED`); most ORM upsert helpers only overwrite, so the additive form would drop
to explicit column arithmetic anyway. Raw SQL keeps the atomic-accumulation and row-lock
behavior fully visible (which the assessment asks to be explained), avoids losing the `unnest`
optimization, and keeps dependencies minimal.

## Restart isolation
The read server creates its own pool on startup and reads only from PostgreSQL — it holds no
in-memory state from ingestion. `tests/test_end_to_end.py` runs ingestion as a subprocess, waits
for it to exit, then serves and asserts — proving balances survive process termination. Thanks to
the atomic publish above, the server also does **not** have to be stopped for an ingest: it can
stay up and serve a consistent snapshot throughout (the assessment's lifecycle of ingest-then-serve
still works exactly as before; online reload is a superset).
