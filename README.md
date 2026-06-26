# Ledger Balance Aggregation

Ingests financial transactions, computes per-account balances in USD, and serves them in any
requested currency. Ingestion and reads run as **separate processes** sharing only PostgreSQL.

## Requirements
- Python 3.11+
- Docker (for local PostgreSQL)

## Setup
```bash
cp .env.example .env
docker compose up -d            # start PostgreSQL on localhost:5432
pip install -r requirements.txt
```

### Port conflicts

The DB host port defaults to 5432. If 5432 is already in use on your machine, you can run
Postgres on a different host port without editing any tracked files — add these two lines to
your `.env` file:

```
DB_HOST_PORT=5433
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/ledger
```

`docker-compose.yml` reads `DB_HOST_PORT` and the app reads `DATABASE_URL` from `.env`
automatically, so no other changes are needed.

## Run
```bash
# 1. Generate synthetic input (transactions.csv + rates.csv)
python -m ledger.generate_data 42 50000 .

# 2. Ingest (creates schema, loads balances + rates, then EXITS)
python -m ledger.ingest transactions.csv rates.csv

# 3. Start the read server (separate process)
uvicorn ledger.server:app --port 8000
```

## Endpoints
- `GET /balance/{id}?currency=XXX` — account balance in `currency` (default USD).
- `GET /total?currency=XXX` — sum of all balances in `currency` (default USD).

Money is returned as a string. Non-USD values use the rate of the most recent date in the
rate input. Examples:
```bash
curl "http://localhost:8000/balance/100"
curl "http://localhost:8000/balance/100?currency=EUR"
curl "http://localhost:8000/total?currency=JPY"
```

## Test
```bash
docker compose up -d
pytest -v
```
