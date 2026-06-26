# Design — Ledger Balance Aggregation Service

**Date:** 2026-06-26
**Source requirement:** [backend_assessment.md](../../../backend_assessment.md)
**Stack:** Python 3.11+ · PostgreSQL · asyncpg · FastAPI · Docker Compose

---

## 1. Summary

A small service with two distinct, separately-run phases:

1. **Ingest** (batch process, runs once then exits): reads a transactions file and an
   exchange-rate file, converts every transaction to USD using the rate for its own
   `(currency, date)`, accumulates the final balance per account, and persists one row per
   account into PostgreSQL. Exchange rates are also persisted.
2. **Read server** (separate process, started after ingest exits): a minimal FastAPI HTTP
   interface exposing two read endpoints that return balances in a requested currency,
   reading **exclusively** from PostgreSQL.

The defining constraint: the read server shares **no in-memory state** with ingestion.
Everything required to serve reads — balances *and* the exchange rates needed for read-side
conversion — lives in PostgreSQL.

Synthetic data only; a seeded generator produces both input files.

---

## 2. Goals / Non-goals

**Goals**
- Correct per-account USD balances: `Σ credits − Σ debits`, each transaction converted at its
  own `(currency, date)` rate.
- Correct read-side currency conversion using the most recent date in the rate input.
- Concurrency safety: no lost updates when balance writes are applied concurrently.
- Restart isolation: reads served from PostgreSQL after the ingest process has fully exited.
- Reasonable ingest performance for 50,000 rows on a local PostgreSQL.
- Runnable with minimal setup; a meaningful end-to-end test.

**Non-goals**
- Authentication / authorization.
- Incremental / streaming re-ingestion (each ingest is a fresh full load).
- Multi-currency *storage* (we store USD canonically; conversion is computed on read).
- Overdraft / non-negative rules — negative balances are valid.
- High availability, horizontal scaling, production deployment concerns.

---

## 3. Architecture

```
generate_data.py  ──► transactions.csv + rates.csv      (synthetic input, seeded)

ingest.py         ──► read both files
                      convert each txn to USD (Decimal)
                      batch + aggregate per id within batch
                      N concurrent workers → atomic UPSERT into `balances`
                      bulk-load all rates into `exchange_rates`
                      close pool → PROCESS EXITS

server.py (FastAPI) ──► GET /balance/{id}?currency=XXX
                        GET /total?currency=XXX
                        reads ONLY from PostgreSQL
```

Two independent OS processes. The only channel between them is PostgreSQL.

---

## 4. Data model (deliverable #2 — DDL)

```sql
-- Final balance per account. One row per id.
CREATE TABLE IF NOT EXISTS balances (
    id          INTEGER         PRIMARY KEY,        -- 3-digit account id, 100..999
    name        TEXT            NOT NULL,
    balance_usd NUMERIC(38, 10) NOT NULL DEFAULT 0  -- canonical balance in USD
);

-- Persisted so the read server (no in-memory state) can do read-side conversion.
CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_date DATE            NOT NULL,
    currency  TEXT            NOT NULL,
    rate      NUMERIC(38, 10) NOT NULL,             -- multiplier: 1 unit currency -> USD
    PRIMARY KEY (rate_date, currency)
);
```

**Why `NUMERIC`, not floating point:** money and rates must be exact. `NUMERIC` maps to
Python `Decimal` losslessly via asyncpg. No `float` appears anywhere on the money path.

**Why store `name`:** the per-id read returns the account holder name alongside the balance.
For a given id the name is constant across rows; on conflict we keep the existing name.

**Why store all rates (not just the latest date):** simpler and lets us derive the valuation
date in SQL (`SELECT max(rate_date) FROM exchange_rates`). The extra rows are negligible.

---

## 5. Conversion rules (explicit, to avoid direction errors)

`rate(currency, date)` is the multiplier converting **1 unit of `currency` → USD**.

- **Ingest (currency → USD):**
  `usd_delta = (Decimal(plus) − Decimal(minus)) × rate(currency, date)`
  using the transaction's *own* `(currency, date)`.

- **Read, non-USD target `C` (USD → C):**
  `amount_C = usd_value / rate(C, max_rate_date)`
  where `max_rate_date = max(rate_date)` over the entire rate input.
  (Divide, because rate goes *currency → USD*.)

- **Read, USD or no currency param:** return the stored USD value unchanged (rate = 1.0).

The same `max_rate_date` and the same per-currency rate are used for **both** the per-id
balance and the total, so the two are valued consistently.

---

## 6. Decimal / rounding policy

- Internal arithmetic: full `decimal.Decimal` precision; storage `NUMERIC(38, 10)`.
- Per-transaction conversion and accumulation: **no intermediate rounding**.
- Read output: quantize to **2 decimal places, `ROUND_HALF_UP`**.
- Money is serialized in JSON as a **string** (not a JSON number) to avoid float
  round-tripping on the client side.
- The independent reference in the test applies the identical rounding rule → exact match.

---

## 7. Ingestion algorithm

1. **Load rates.** Parse `rates.csv` into `rate_map[(currency, date)] = Decimal(rate)` (used
   for per-transaction conversion) and bulk-load **all** rows into `exchange_rates`
   (via `COPY` / `copy_records_to_table`).
2. **Fresh load.** `TRUNCATE balances` and `TRUNCATE exchange_rates` at the start so a re-run
   does not double-count (idempotent full reload). Schema created with `IF NOT EXISTS`.
3. **Stream & convert.** Read `transactions.csv` row by row; compute
   `usd_delta = (plus − minus) × rate_map[(currency, date)]` as `Decimal`.
4. **Batch + intra-batch aggregation.** Group rows into batches (default 5,000 rows). **Within
   each batch, sum `usd_delta` per id** (and keep the name). This is required: a single
   `INSERT ... ON CONFLICT DO UPDATE` cannot affect the same target row twice, so duplicate
   ids inside one statement must be pre-summed. It also shrinks each batch to ≤ ~900 rows.
5. **Concurrent atomic upsert.** A pool of N workers (default `min(16, batches)`, pool size
   8–16) runs batches **concurrently**, each issuing one atomic statement:

   ```sql
   INSERT INTO balances (id, name, balance_usd)
   SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
   ON CONFLICT (id) DO UPDATE
       SET balance_usd = balances.balance_usd + EXCLUDED.balance_usd;
   ```
6. **Exit.** When all batches complete, close the pool and let the process terminate. This
   satisfies "the ingesting process must be shut down" before the read server starts.

---

## 8. Concurrency & correctness reasoning (deliverable #5)

- The naive "read balance in app → add delta → write back" loses updates under concurrency:
  two workers read the same value, both add, the second write clobbers the first.
- We avoid this entirely by performing the accumulation **inside the database**:
  `balance_usd = balances.balance_usd + EXCLUDED.balance_usd`. PostgreSQL takes a row-level
  lock on the conflicting row for the duration of the `ON CONFLICT DO UPDATE`, so the
  read-modify-write is atomic per row.
- Because the same id appears in many batches that run **concurrently**, this is a genuine
  concurrent-contention scenario — exactly what the requirement targets. Correctness holds
  regardless of ordering or interleaving; no update is lost.
- Intra-batch aggregation is the one subtlety: it is both a performance win and a correctness
  necessity (PostgreSQL errors with "ON CONFLICT DO UPDATE command cannot affect row a second
  time" if the same id appears twice in one statement).

---

## 9. Performance approach (deliverable #5)

- **Batching:** 50,000 single-row inserts collapse to a few hundred multi-row statements;
  intra-batch aggregation caps each statement at ≤ ~900 rows.
- **Concurrency:** a pool of 8–16 connections overlaps network/IO latency across workers.
- **One round trip per batch:** `unnest(...)` sends a batch as arrays in a single statement.
- **DB-side arithmetic:** accumulation runs in PostgreSQL `NUMERIC`; the app only ships deltas.
- **Rates via `COPY`:** bulk-loaded rather than row-by-row.
- **Bounded memory:** transactions are streamed; only one batch is held at a time.
- **Target:** 50,000 rows ingested in well under a few seconds on a local PostgreSQL.

---

## 10. Read API contract

Money fields are returned as JSON **strings**. `currency` is normalized to upper-case.

### `GET /balance/{account_id}?currency=XXX`
- `currency` optional; defaults to `USD`.
- **200**: `{"id": 100, "name": "acct100", "currency": "USD", "balance": "123.45"}`
- **404**: account id not present in `balances`.
- **400**: requested currency has no rate at `max_rate_date`.

### `GET /total?currency=XXX`
- `currency` optional; defaults to `USD`.
- Total = sum of all account balances, then converted.
- **200**: `{"currency": "EUR", "total": "918273.64"}`
- **400**: requested currency has no rate at `max_rate_date`.

The test waits for read-server readiness by retrying its first request (no dedicated health
endpoint).

SQL used:
```sql
-- valuation date (shared by both endpoints)
SELECT max(rate_date) FROM exchange_rates;
-- rate for a target currency at the valuation date
SELECT rate FROM exchange_rates WHERE currency = $1 AND rate_date = $2;
-- per-id
SELECT name, balance_usd FROM balances WHERE id = $1;
-- total
SELECT COALESCE(sum(balance_usd), 0) FROM balances;
```

### `GET /health` (optional, minimal)
- **200**: `{"status": "ok"}` after a trivial `SELECT 1`. Useful for the test to wait on
  server readiness. No business logic.

---

## 11. Data generator (deliverable #3)

`generate_data.py` — seeded for reproducibility.

- **transactions.csv:** 50,000 rows. `id` drawn from 100–999 (≤ ~900 distinct, each appearing
  many times). `name = "acct{id}"`. `plus`/`minus` random non-negative `Decimal` (2 dp).
  `currency` drawn from a small set (e.g. USD, EUR, GBP, JPY, SGD). `date` drawn from ≤ 10
  distinct dates.
- **rates.csv:** one rate per `(currency, date)` for **every** pair that appears in the
  transactions (guaranteeing the requirement "every `(currency, date)` has a matching rate"),
  plus a deterministic rate value per currency. USD rate is always `1.0`.
- A fixed seed makes the test's independent reference deterministic.

---

## 12. Test plan (deliverable #4)

`tests/test_end_to_end.py` — ingest, restart, assert.

1. **Generate** a deterministic dataset with a fixed seed (a smaller row count is acceptable
   for the test; the same code path).
2. **Independent reference (pure Python):** re-parse both files, group by id, compute
   `Σ (plus − minus) × rate(currency, date)` per id → reference USD balances; sum → reference
   total; convert one non-USD currency at `max_rate_date` → reference converted values. This
   reference shares no code with the ingest accumulation path.
3. **Ingest as a subprocess** and wait for it to **exit** (proves balances survive process
   termination — restart isolation).
4. **Start the read server** with a fresh connection pool (no shared memory with ingest) and
   query over HTTP:
   - per-id balance in USD matches the reference,
   - per-id balance in a non-USD currency matches the reference,
   - total in USD and in a non-USD currency match the reference,
   - a missing id returns 404.
5. Assertions compare on the 2-dp rounded values.

---

## 13. Project layout (root = current folder)

```
be-assessment/
  backend_assessment.md         # the requirement (already present)
  docker-compose.yml            # local PostgreSQL
  requirements.txt
  .env.example                  # DATABASE_URL template
  README.md                     # setup / run instructions (deliverable #1)
  db/
    schema.sql                  # DDL (deliverable #2)
  ledger/
    __init__.py
    config.py                   # DATABASE_URL, batch size, pool size from env
    rates.py                    # rate loading + conversion helpers
    generate_data.py            # synthetic data generator (deliverable #3)
    ingest.py                   # ingestion entrypoint
    server.py                   # FastAPI read server
  tests/
    test_end_to_end.py          # ingest → restart → assert (deliverable #4)
  DESIGN_NOTES.md               # concurrency & performance note (deliverable #5)
  docs/superpowers/specs/
    2026-06-26-ledger-balance-aggregation-design.md   # this document
```

---

## 14. Configuration & setup

- `DATABASE_URL` from environment (`.env`), e.g.
  `postgresql://postgres:postgres@localhost:5432/ledger`.
- Tunables via env with sensible defaults: `BATCH_SIZE` (5000), `POOL_SIZE` (16).

Run sequence (documented in README):
```bash
docker compose up -d                 # start PostgreSQL
pip install -r requirements.txt
psql "$DATABASE_URL" -f db/schema.sql # or auto-create on ingest
python -m ledger.generate_data        # writes transactions.csv + rates.csv
python -m ledger.ingest               # ingests, then exits
uvicorn ledger.server:app --port 8000 # start read server (separate run)
pytest                                # end-to-end test
```

---

## 15. Edge cases & assumptions

- **Negative balances** are valid and expected; no clamping.
- **Unknown currency on read** (no rate at `max_rate_date`) → `400`.
- **Unknown id on read** → `404`.
- **Currency case** normalized to upper-case on input.
- **Empty balances** → `/total` returns `0`.
- **Re-running ingest** is safe (truncate-then-load); not designed for incremental merge.
- **Assumption:** the rate input is well-formed and covers every `(currency, date)` present in
  the transactions, as the requirement guarantees.

---

## 16. Dependencies (kept minimal)

- `asyncpg` — PostgreSQL driver (binary protocol, native `NUMERIC ↔ Decimal`).
- `fastapi` + `uvicorn` — read server.
- `httpx` — test HTTP client.
- `pytest` + `pytest-asyncio` — test runner.
- `python-dotenv` (optional) — load `.env`.
- `decimal` — standard library.

---

## 17. Deliverables mapping

| # | Deliverable | Where |
|---|---|---|
| 1 | Source code + run instructions | `ledger/`, `README.md` |
| 2 | Schema / DDL | `db/schema.sql` (§4) |
| 3 | Data generator | `ledger/generate_data.py` (§11) |
| 4 | Ingest→restart→assert test | `tests/test_end_to_end.py` (§12) |
| 5 | Concurrency & performance note | `DESIGN_NOTES.md` (§8, §9) |
