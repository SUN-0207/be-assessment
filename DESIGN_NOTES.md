# Design Notes — Concurrency & Performance

## Concurrency safety (no lost updates)
Balance accumulation happens **inside PostgreSQL**, never as an app-side read-modify-write:

    INSERT INTO balances (id, name, balance_usd)
    SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
    ON CONFLICT (id) DO UPDATE
        SET balance_usd = balances.balance_usd + EXCLUDED.balance_usd;

Many batches run concurrently (asyncpg pool), and the same account id appears in many of them.
For each conflicting row, `ON CONFLICT DO UPDATE` takes a row-level lock and performs the
`balance + delta` atomically, so the result is correct regardless of ordering or interleaving —
no update is lost. `tests/test_ingest.py::test_concurrent_upserts_no_lost_update` proves this by
applying 50 concurrent upserts to one row and asserting the exact sum.

**Intra-batch aggregation** is both a performance win and a correctness necessity: a single
`INSERT ... ON CONFLICT DO UPDATE` cannot affect the same target row twice, so duplicate ids
within one statement are pre-summed before the upsert.

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
The read server creates its own pool on startup and reads only from PostgreSQL. The ingest
process exits before it starts. `tests/test_end_to_end.py` runs ingestion as a subprocess, waits
for it to exit, then serves and asserts — proving balances survive process termination.
