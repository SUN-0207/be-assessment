# Ledger Balance Aggregation

A small backend service that **ingests a file of financial transactions, computes the final
balance per account in USD, persists it in PostgreSQL, and serves those balances in any
requested currency.**

Ingestion and reads run as **two separate processes** whose only shared channel is PostgreSQL:

```
 transactions.csv ┐
 rates.csv ────────┤
                   ▼
        ┌──────────────────────┐     writes      ┌──────────────┐     reads      ┌────────────────────┐
        │  ingest  (batch run)  │ ──────────────▶ │  PostgreSQL   │ ◀───────────── │  read server (API)  │
        │  converts → upserts   │   balances +    │  balances     │   balances +   │  FastAPI, own pool  │
        │  then the process     │   exchange_rates│  exchange_rates│  rates         │  GET /balance /total│
        │  EXITS                 │                 └──────────────┘                └────────────────────┘
        └──────────────────────┘
        no in-memory state is shared between the two processes
```

A visual walk-through of the architecture and full flow is in **[docs/architecture.html](docs/architecture.html)**.

---

## Contents
- [Requirements](#requirements)
- [Project layout](#project-layout)
- [Setup](#setup)
- [Configuration](#configuration)
- [Run the full flow](#run-the-full-flow)
- [Using your own input files](#using-your-own-input-files)
- [API reference](#api-reference)
- [How it works](#how-it-works)
- [Database schema](#database-schema)
- [Testing](#testing)
- [Deliverables](#deliverables)

---

## Requirements

- **Python 3.11+** (on Windows, use the `py` launcher if `python` is not on `PATH`)
- **Docker** (for a local PostgreSQL instance)

Python dependencies are intentionally minimal — `asyncpg`, `fastapi`, `uvicorn`, plus
`httpx`/`pytest` for tests. See [requirements.txt](requirements.txt).

---

## Project layout

```
be-assessment/
├── docker-compose.yml        # local PostgreSQL 16
├── requirements.txt
├── .env.example              # DATABASE_URL / BATCH_SIZE / POOL_SIZE (+ optional DB_HOST_PORT)
├── db/
│   └── schema.sql            # DDL — deliverable #2
├── ledger/
│   ├── config.py             # env-driven config
│   ├── rates.py              # currency conversion + rounding + rate parsing (pure)
│   ├── db.py                 # connection pool, schema apply, truncate
│   ├── generate_data.py      # seeded synthetic data generator — deliverable #3
│   ├── ingest.py             # batched concurrent ingestion (exits when done)
│   └── server.py             # FastAPI read server
├── tests/                    # unit + integration + end-to-end — deliverable #4
├── docs/
│   └── architecture.html     # visual architecture & flow
├── DESIGN_NOTES.md           # concurrency & performance notes — deliverable #5
└── README.md
```

---

## Setup

```bash
# 1. Start PostgreSQL
cp .env.example .env
docker compose up -d                 # PostgreSQL on localhost:5432

# 2. Create a virtual environment
python -m venv .venv                 # Windows, if 'python' is missing:  py -m venv .venv

# 3. Activate it
source .venv/bin/activate            # Linux / macOS
# .venv\Scripts\Activate.ps1         # Windows (PowerShell)
# source .venv/Scripts/activate      # Windows (Git Bash)

# 4. Install dependencies
pip install -r requirements.txt
```

> **Windows / PowerShell note.** If activation is blocked by the execution policy
> (`running scripts is disabled on this system`), run
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first, then activate.
>
> All `python` / `pytest` / `uvicorn` commands below assume the virtual environment is
> **activated**. If you prefer not to activate it, call the venv binaries directly instead:
> `.venv/bin/python -m ledger.ingest ...` (Linux/macOS) or
> `.venv\Scripts\python.exe -m ledger.ingest ...` (Windows).

---

## Configuration

All configuration comes from environment variables (loaded from `.env` if present).

| Variable        | Default                                                    | Purpose                                  |
|-----------------|------------------------------------------------------------|------------------------------------------|
| `DATABASE_URL`  | `postgresql://postgres:postgres@localhost:5432/ledger`     | Postgres connection string (app)         |
| `BATCH_SIZE`    | `5000`                                                      | Rows per ingest batch                    |
| `POOL_SIZE`     | `16`                                                        | Max concurrent DB connections at ingest  |
| `DB_HOST_PORT`  | `5432`                                                      | Host port Docker maps Postgres to        |

### Port conflicts

The DB host port defaults to **5432**. If 5432 is already in use on your machine, run Postgres
on another host port **without editing any tracked files** — add two lines to your `.env`:

```
DB_HOST_PORT=5433
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/ledger
```

`docker-compose.yml` reads `DB_HOST_PORT` and the app reads `DATABASE_URL`, so nothing else
changes. (Then re-run `docker compose up -d`.)

---

## Run the full flow

```bash
# 1. Generate synthetic input → transactions.csv + rates.csv
#    args: <seed> <n_transactions> <output_dir>
python -m ledger.generate_data 42 50000 .

# 2. Ingest: create schema, load balances + rates, then the process EXITS
python -m ledger.ingest transactions.csv rates.csv

# 3. Start the read server (a SEPARATE process, reads only from PostgreSQL)
uvicorn ledger.server:app --port 8000
```

Then query it (see [API reference](#api-reference)):

```bash
curl "http://localhost:8000/balance/100"
curl "http://localhost:8000/balance/100?currency=EUR"
curl "http://localhost:8000/total?currency=JPY"
```

On a local Postgres, ingesting 50,000 rows completes in roughly **one second**.

---

## Using your own input files

You don't have to use the generator. Step 1 above only exists to produce synthetic data;
**if you already have the two input files, skip it and point `ingest` straight at them:**

```bash
python -m ledger.ingest /path/to/your_transactions.csv /path/to/your_rates.csv
```

The expected formats (CSV with a header row) are:

**Transactions** — `id,name,plus,minus,currency,date`
```
id,name,plus,minus,currency,date
100,acct100,250.00,120.50,USD,2026-06-15
243,acct243,80.00,0.00,EUR,2026-06-16
```

**Exchange rates** — `date,currency,rate` where `rate` is the multiplier `1 unit currency → USD`
```
date,currency,rate
2026-06-15,USD,1.0
2026-06-15,EUR,1.0832
```

Every `(currency, date)` that appears in the transactions must have a matching rate row.
Re-running ingest is safe — it truncates and reloads (a fresh full load).

---

## API reference

Money is always returned as a **string** (to avoid float precision loss on the client).
`currency` is optional and case-insensitive; it defaults to `USD`.

### `GET /balance/{id}`

The account's final balance, converted to the requested currency.

| Param      | In    | Default | Notes                          |
|------------|-------|---------|--------------------------------|
| `id`       | path  | —       | account id (3-digit, 100–999)  |
| `currency` | query | `USD`   | target currency                |

```bash
curl "http://localhost:8000/balance/100?currency=EUR"
```
```json
{ "id": 100, "name": "acct100", "currency": "EUR", "balance": "12177.51" }
```

- **404** — id not found.
- **400** — requested currency has no rate on the valuation date.

### `GET /total`

The sum of all account balances, converted to the requested currency.

```bash
curl "http://localhost:8000/total"
```
```json
{ "currency": "USD", "total": "-226328.98" }
```

- **400** — requested currency has no rate on the valuation date.

> **PowerShell:** the bare `curl` alias maps to `Invoke-WebRequest` and formats differently.
> Use `curl.exe "http://localhost:8000/balance/100"` or
> `Invoke-RestMethod "http://localhost:8000/balance/100?currency=EUR"`.

### Finding valid ids

There is no "list accounts" endpoint (the spec asks only for per-id and total). Ids are the
3-digit values `100–999`. To list the ones actually present:

```bash
docker exec be-assessment-db-1 psql -U postgres -d ledger -c "select id from balances order by id limit 20;"
```

---

## How it works

**Per-transaction conversion (ingest).** Each row is converted to USD with the rate for its
*own* `(currency, date)`: `usd = (plus − minus) × rate(currency, date)`. All money uses Python
`Decimal` ↔ Postgres `NUMERIC(38,10)` — never floats.

**Read-side conversion.** For a non-USD request, the stored USD value is converted using the
rate of the **most recent date present in the rate input**: `value = usd ÷ rate(currency,
max_date)`. The same valuation date is used for both per-id and total. A USD (or omitted)
request returns the stored USD value. Output is rounded to 2 decimals, half-up.

**Concurrency safety.** Balance accumulation happens inside one SQL statement, never as an
app-side read-modify-write (ingestion accumulates into a staging table):

```sql
INSERT INTO balances_staging (id, name, balance_usd)
SELECT * FROM unnest($1::int[], $2::text[], $3::numeric[])
ON CONFLICT (id) DO UPDATE
    SET balance_usd = balances_staging.balance_usd + EXCLUDED.balance_usd;
```

Batches run concurrently over a connection pool; the row-level lock on `ON CONFLICT DO UPDATE`
makes each `+=` atomic, so no update is lost under any interleaving. Ids within a batch are
pre-summed (a statement can't touch a row twice) and sorted ascending so concurrent batches
acquire row locks in a consistent order (deadlock avoidance).

**Atomic publish & online reload.** Ingestion writes only to staging tables, then replaces the
live `balances` / `exchange_rates` in a **single transaction** (`DELETE` + `INSERT … SELECT FROM
*_staging`). So the read server can keep serving **while an ingest runs** — every request sees
either the old snapshot or the new one in full, never empty/partial. `DELETE` is used rather than
`TRUNCATE` (which is not MVCC-safe), and each endpoint reads balance + rate inside one
`REPEATABLE READ` snapshot. If ingestion crashes before the swap, the live tables are untouched.

**Restart isolation.** The read server opens its own pool and reads only from PostgreSQL; it
shares no memory with ingestion. The end-to-end test runs ingestion as a subprocess, waits for it
to exit, then starts the server and asserts the served values. The server does not need to be
stopped for an ingest (see Atomic publish above).

Full rationale (including the ORM-vs-raw-SQL decision and the deadlock fix) is in
**[DESIGN_NOTES.md](DESIGN_NOTES.md)**.

---

## Database schema

See [db/schema.sql](db/schema.sql). Two live tables — final balances (one row per id) and the
exchange rates (persisted so the read server can convert without ingestion's memory). The schema
also defines matching `*_staging` tables that ingestion writes to before the atomic publish swap:

```sql
CREATE TABLE IF NOT EXISTS balances (
    id          INTEGER         PRIMARY KEY,        -- 3-digit account id, 100..999
    name        TEXT            NOT NULL,
    balance_usd NUMERIC(38, 10) NOT NULL DEFAULT 0  -- canonical balance in USD
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_date DATE            NOT NULL,
    currency  TEXT            NOT NULL,
    rate      NUMERIC(38, 10) NOT NULL,             -- multiplier: 1 unit currency -> USD
    PRIMARY KEY (rate_date, currency)
);
```

---

## Testing

PostgreSQL must be running (`docker compose up -d`), then:

```bash
pytest -v
```

The suite covers:

| Area | File |
|------|------|
| Config defaults/overrides | `tests/test_config.py` |
| Conversion & rounding (multiply/divide, half-up) | `tests/test_rates.py` |
| Generator invariants (full rate grid, determinism, ranges) | `tests/test_generate_data.py` |
| Schema apply + truncate | `tests/test_db.py` |
| Concurrency: no lost updates, no deadlock, atomic publish, MVCC-safe swap, crash-safety | `tests/test_ingest.py` |
| Endpoints: USD/non-USD, 404, 400, empty-DB total | `tests/test_server.py` |
| **End-to-end: ingest → restart → serve, vs an independent reference** | `tests/test_end_to_end.py` |

The end-to-end test computes its expected values with a separate inline implementation (it does
not reuse the ingest code path), so a passing run means the served balances match an
independently computed reference.

---

## Deliverables

| # | Deliverable | Where |
|---|-------------|-------|
| 1 | Source code + setup/run instructions | [`ledger/`](ledger/), this README |
| 2 | Schema / DDL | [`db/schema.sql`](db/schema.sql) |
| 3 | Data generator | [`ledger/generate_data.py`](ledger/generate_data.py) |
| 4 | Ingest → restart → assert test (per-id and total) | [`tests/test_end_to_end.py`](tests/test_end_to_end.py) |
| 5 | Concurrency & performance design note | [`DESIGN_NOTES.md`](DESIGN_NOTES.md) |
