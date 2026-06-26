# Design Notes — Concurrency & Performance

## Concurrency: no lost updates
Balances are accumulated **inside PostgreSQL**, never via an app-side read-modify-write. Each
transaction is converted to USD, rows are grouped into batches, and every batch runs one statement:

    INSERT INTO balances_staging (id, name, balance_usd)
    SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
    ON CONFLICT (id) DO UPDATE
        SET balance_usd = balances_staging.balance_usd + EXCLUDED.balance_usd;

Batches run concurrently over an asyncpg pool. The row-level lock on `ON CONFLICT DO UPDATE` makes
each `+= delta` atomic, so the result is correct under any interleaving — no update is lost. Two
supporting rules:

- **Intra-batch aggregation** — duplicate ids in a batch are pre-summed first (one statement can't
  touch a row twice), which also shrinks the work.
- **Sorted ids** — each batch sorts ids ascending so concurrent statements take row locks in the
  same order; otherwise overlapping batches deadlock.

*Tests: `test_concurrent_upserts_no_lost_update`, `test_run_ingest_many_concurrent_batches_no_deadlock`.*

## Atomic publish & online reload
Ingestion writes only to `*_staging` tables; the live tables are swapped in **one transaction** at
the end:

    BEGIN;
    DELETE FROM balances;        INSERT INTO balances        SELECT * FROM balances_staging;
    DELETE FROM exchange_rates;  INSERT INTO exchange_rates  SELECT * FROM exchange_rates_staging;
    COMMIT;

So the read server can keep serving **while an ingest runs** — a request sees the old snapshot or
the new one in full, never empty/partial. Two details make it airtight:

- **`DELETE`, not `TRUNCATE`** — `TRUNCATE` is not MVCC-safe (an in-flight reader would see the
  table empty); `DELETE` keeps old rows visible to older snapshots and doesn't block reads.
- **Snapshot reads** — each endpoint reads balance + rate in one `REPEATABLE READ` transaction, so
  its queries can't straddle the publish commit.

If ingestion dies before the commit, the live tables are untouched; re-running rebuilds staging
(idempotent).

*Tests: `test_publish_is_mvcc_safe_for_inflight_readers`, `test_failed_ingest_leaves_live_untouched`.*

## Performance
- ~10 multi-row upserts (50k rows ÷ batch 5000, each ≤900 unique ids) instead of 50k single inserts.
- Connection pool (default 16) runs batches concurrently; `unnest(...)` ships each batch in one round
  trip; rates load via `COPY`.
- All arithmetic in `NUMERIC`; the CSV is streamed, so memory stays bounded.
- 50k rows ingest in ~1s on a local PostgreSQL.

## Correctness & currency
- `NUMERIC(38,10)` + Python `Decimal` end to end; no floats on the money path.
- Ingest **multiplies** by `rate(currency, date)`; reads **divide** by `rate(currency, max_date)`;
  output rounded to 2dp, HALF_UP. `test_end_to_end` checks served values against an independent
  reference.

## Restart isolation
The read server is a separate process with its own pool, reading **only** from PostgreSQL — it
keeps no in-memory state from ingestion, so balances survive the ingest process exiting (and, via
the atomic publish above, the server need not be stopped for an ingest at all).

## Raw SQL, not an ORM
The core operation is an *additive* upsert (`balance + EXCLUDED`); ORM upsert helpers typically only
overwrite, so the additive form drops to explicit SQL anyway. Raw SQL keeps the row-lock and
atomic-swap behavior visible and dependencies minimal.
