# Ledger Balance Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a service that ingests 50k financial transactions, computes per-account USD balances with concurrency-safe writes into PostgreSQL, then serves per-id and total balances in any requested currency from a separately-run read server.

**Architecture:** Two independent OS processes that communicate only through PostgreSQL. An ingest process converts each transaction to USD at its own `(currency, date)` rate, aggregates per id within batches, and applies batched concurrent atomic upserts (`balance = balance + EXCLUDED`) — correctness under concurrency comes from PostgreSQL row locks. A FastAPI read server then serves balances, converting USD → target currency at the most recent rate date.

**Tech Stack:** Python 3.11+, asyncpg (raw SQL), FastAPI + uvicorn, PostgreSQL 16 (Docker Compose), pytest + pytest-asyncio + httpx. Money handled with `decimal.Decimal` ↔ `NUMERIC` only; no floats on the money path.

## Global Constraints

- **Language/stack:** Python 3.11+ and PostgreSQL only. Raw SQL via asyncpg — no ORM.
- **Money type:** `decimal.Decimal` in Python, `NUMERIC(38,10)` in Postgres. No `float` anywhere on the money path. JSON money fields serialized as **strings**.
- **Rate semantics:** `rate(currency, date)` is the multiplier `1 unit currency → USD`. Ingest **multiplies**; read **divides**.
- **Rounding:** output quantized to 2 decimals, `ROUND_HALF_UP`. No intermediate rounding during accumulation.
- **Read valuation date:** the most recent `rate_date` present in the rate input; the same date and rate apply to both per-id and total.
- **Process separation:** read server shares no in-memory state with ingest; it reads only from PostgreSQL. Ingest process exits before the read server starts.
- **Concurrency:** balance writes applied concurrently; no lost updates regardless of order/interleaving.
- **Minimal dependencies; synthetic data only.**
- **Currency normalization:** upper-case on input.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `requirements.txt` | Pinned minimal dependencies |
| `docker-compose.yml` | Local PostgreSQL 16 with healthcheck |
| `.env.example` | `DATABASE_URL`, `BATCH_SIZE`, `POOL_SIZE` template |
| `pytest.ini` | `asyncio_mode = auto` |
| `db/schema.sql` | DDL for `balances` and `exchange_rates` |
| `ledger/__init__.py` | Package marker |
| `ledger/config.py` | Load config from env (+ `.env`) |
| `ledger/rates.py` | Pure conversion + rounding + rate CSV parsing |
| `ledger/db.py` | Pool creation, schema apply, truncate (shared) |
| `ledger/generate_data.py` | Seeded synthetic data generator |
| `ledger/ingest.py` | Batch/aggregate + concurrent atomic upsert orchestration |
| `ledger/server.py` | FastAPI read endpoints |
| `tests/conftest.py` | `pool` fixture (schema + truncate per test) |
| `tests/test_config.py` | Config defaults/overrides |
| `tests/test_rates.py` | Conversion + rounding + parsing |
| `tests/test_generate_data.py` | Generator invariants |
| `tests/test_ingest.py` | Aggregation (pure) + concurrency safety (DB) |
| `tests/test_server.py` | Endpoint behavior (DB) |
| `tests/test_end_to_end.py` | Ingest→exit→serve→assert vs reference |
| `README.md` | Setup / run instructions (deliverable #1) |
| `DESIGN_NOTES.md` | Concurrency & performance note (deliverable #5) |

> **Note on additions vs spec §13:** `ledger/db.py` and `tests/conftest.py` are added for DRY (both ingest and server need a pool; tests need a schema-ready DB). Everything else matches the spec.

---

## Task 1: Project scaffolding + config

**Files:**
- Create: `requirements.txt`, `docker-compose.yml`, `.env.example`, `pytest.ini`
- Create: `ledger/__init__.py`, `ledger/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ledger.config.load_config() -> Config` where `Config` is a frozen dataclass with fields `database_url: str`, `batch_size: int`, `pool_size: int`. Defaults: `DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ledger`, `BATCH_SIZE=5000`, `POOL_SIZE=16`.

- [ ] **Step 1: Create infrastructure files**

`requirements.txt`:
```text
asyncpg>=0.29
fastapi>=0.110
uvicorn[standard]>=0.29
httpx>=0.27
pytest>=8.0
pytest-asyncio>=0.23
python-dotenv>=1.0
```

`docker-compose.yml`:
```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: ledger
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d ledger"]
      interval: 2s
      timeout: 3s
      retries: 20
```

`.env.example`:
```text
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ledger
BATCH_SIZE=5000
POOL_SIZE=16
```

`pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
```

`ledger/__init__.py`:
```python
```

- [ ] **Step 2: Write the failing test**

`tests/test_config.py`:
```python
import importlib

import ledger.config as config_module


def test_defaults(monkeypatch):
    for var in ("DATABASE_URL", "BATCH_SIZE", "POOL_SIZE"):
        monkeypatch.delenv(var, raising=False)
    importlib.reload(config_module)
    cfg = config_module.load_config()
    assert cfg.database_url == "postgresql://postgres:postgres@localhost:5432/ledger"
    assert cfg.batch_size == 5000
    assert cfg.pool_size == 16


def test_overrides(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x@y/z")
    monkeypatch.setenv("BATCH_SIZE", "10")
    monkeypatch.setenv("POOL_SIZE", "4")
    importlib.reload(config_module)
    cfg = config_module.load_config()
    assert cfg.database_url == "postgresql://x@y/z"
    assert cfg.batch_size == 10
    assert cfg.pool_size == 4
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.config'` (or `AttributeError: load_config`).

- [ ] **Step 4: Write minimal implementation**

`ledger/config.py`:
```python
import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional
    pass

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ledger"


@dataclass(frozen=True)
class Config:
    database_url: str
    batch_size: int
    pool_size: int


def load_config() -> Config:
    return Config(
        database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        batch_size=int(os.environ.get("BATCH_SIZE", "5000")),
        pool_size=int(os.environ.get("POOL_SIZE", "16")),
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add requirements.txt docker-compose.yml .env.example pytest.ini ledger/__init__.py ledger/config.py tests/test_config.py
git commit -m "chore: project scaffolding and config module"
```

---

## Task 2: Conversion helpers (rates.py)

**Files:**
- Create: `ledger/rates.py`
- Test: `tests/test_rates.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `RateRow(rate_date: datetime.date, currency: str, rate: Decimal)` — a `NamedTuple`.
  - `to_usd(plus: Decimal, minus: Decimal, rate: Decimal) -> Decimal` → `(plus - minus) * rate`.
  - `from_usd(usd: Decimal, rate: Decimal) -> Decimal` → `usd / rate`.
  - `round_money(value: Decimal) -> Decimal` → quantize 2dp `ROUND_HALF_UP`.
  - `read_rates(path: str) -> list[RateRow]` (currency upper-cased).
  - `build_rate_map(rates: list[RateRow]) -> dict[tuple[str, date], Decimal]`.

- [ ] **Step 1: Write the failing test**

`tests/test_rates.py`:
```python
from datetime import date
from decimal import Decimal

from ledger.rates import (
    RateRow,
    build_rate_map,
    from_usd,
    read_rates,
    round_money,
    to_usd,
)


def test_to_usd_multiplies():
    assert to_usd(Decimal("250.00"), Decimal("120.50"), Decimal("1.0")) == Decimal("129.50")
    assert to_usd(Decimal("80.00"), Decimal("0.00"), Decimal("1.0832")) == Decimal("86.656000")


def test_from_usd_divides():
    assert from_usd(Decimal("129.50"), Decimal("1.0")) == Decimal("129.50")
    # 100 USD at rate 0.00642 (JPY) -> 100 / 0.00642
    assert round_money(from_usd(Decimal("100"), Decimal("0.00642"))) == Decimal("15576.32")


def test_round_money_half_up():
    assert round_money(Decimal("1.005")) == Decimal("1.01")
    assert round_money(Decimal("2.675")) == Decimal("2.68")
    assert round_money(Decimal("-1.005")) == Decimal("-1.00")  # HALF_UP rounds toward +inf at .5


def test_read_rates_and_map(tmp_path):
    p = tmp_path / "rates.csv"
    p.write_text("date,currency,rate\n2026-06-15,usd,1.0\n2026-06-15,EUR,1.0832\n")
    rows = read_rates(str(p))
    assert RateRow(date(2026, 6, 15), "USD", Decimal("1.0")) in rows
    rate_map = build_rate_map(rows)
    assert rate_map[("EUR", date(2026, 6, 15))] == Decimal("1.0832")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_rates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.rates'`.

- [ ] **Step 3: Write minimal implementation**

`ledger/rates.py`:
```python
import csv
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import NamedTuple

TWO_PLACES = Decimal("0.01")


class RateRow(NamedTuple):
    rate_date: date
    currency: str
    rate: Decimal


def to_usd(plus: Decimal, minus: Decimal, rate: Decimal) -> Decimal:
    return (plus - minus) * rate


def from_usd(usd: Decimal, rate: Decimal) -> Decimal:
    return usd / rate


def round_money(value: Decimal) -> Decimal:
    return value.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


def read_rates(path: str) -> list[RateRow]:
    rows: list[RateRow] = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(
                RateRow(
                    date.fromisoformat(r["date"]),
                    r["currency"].upper(),
                    Decimal(r["rate"]),
                )
            )
    return rows


def build_rate_map(rates: list[RateRow]) -> dict[tuple[str, date], Decimal]:
    return {(r.currency, r.rate_date): r.rate for r in rates}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_rates.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add ledger/rates.py tests/test_rates.py
git commit -m "feat: currency conversion and rounding helpers"
```

---

## Task 3: Data generator (generate_data.py)

**Files:**
- Create: `ledger/generate_data.py`
- Test: `tests/test_generate_data.py`

**Interfaces:**
- Consumes: `ledger.rates.read_rates`, `build_rate_map` (test only).
- Produces:
  - `CURRENCIES: list[str]` and `BASE_RATES: dict[str, Decimal]` (module constants).
  - `generate(seed: int, n_transactions: int, out_dir: str, n_ids: int = 900, n_dates: int = 10) -> tuple[str, str]` returns `(transactions_path, rates_path)`. Writes `transactions.csv` and `rates.csv` into `out_dir`. Emits a **full grid** of rates (every currency × every date), guaranteeing coverage. USD rate is always `1.0`.

- [ ] **Step 1: Write the failing test**

`tests/test_generate_data.py`:
```python
import csv
from datetime import date
from decimal import Decimal

from ledger.generate_data import generate
from ledger.rates import build_rate_map, read_rates


def _read_txns(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_generate_invariants(tmp_path):
    tx_path, rates_path = generate(seed=42, n_transactions=2000, out_dir=str(tmp_path))
    txns = _read_txns(tx_path)
    assert len(txns) == 2000

    ids = {int(r["id"]) for r in txns}
    assert all(100 <= i <= 999 for i in ids)
    dates = {r["date"] for r in txns}
    assert len(dates) <= 10

    rate_map = build_rate_map(read_rates(rates_path))
    # Every (currency, date) in transactions has a matching rate.
    for r in txns:
        assert (r["currency"].upper(), date.fromisoformat(r["date"])) in rate_map
    # USD always 1.0
    for (cur, _d), rate in rate_map.items():
        if cur == "USD":
            assert rate == Decimal("1.0")


def test_generate_is_deterministic(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    tx_a, _ = generate(seed=7, n_transactions=500, out_dir=str(a))
    tx_b, _ = generate(seed=7, n_transactions=500, out_dir=str(b))
    assert open(tx_a).read() == open(tx_b).read()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_generate_data.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.generate_data'`.

- [ ] **Step 3: Write minimal implementation**

`ledger/generate_data.py`:
```python
import csv
import random
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "SGD"]
BASE_RATES = {
    "USD": Decimal("1.0"),
    "EUR": Decimal("1.0832"),
    "GBP": Decimal("1.2710"),
    "JPY": Decimal("0.00642"),
    "SGD": Decimal("0.7395"),
}
START_DATE = date(2026, 6, 15)


def _rate_for(currency: str, day_index: int) -> Decimal:
    if currency == "USD":
        return Decimal("1.0")
    # Deterministic per-day variation so per-(currency,date) conversion matters.
    factor = Decimal("1") + Decimal(day_index) / Decimal("1000")
    return (BASE_RATES[currency] * factor).quantize(Decimal("0.000001"))


def generate(
    seed: int,
    n_transactions: int,
    out_dir: str,
    n_ids: int = 900,
    n_dates: int = 10,
) -> tuple[str, str]:
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    dates = [START_DATE + timedelta(days=i) for i in range(n_dates)]
    ids = list(range(100, 100 + n_ids))  # 100..999 for n_ids=900

    tx_path = out / "transactions.csv"
    with open(tx_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "plus", "minus", "currency", "date"])
        for _ in range(n_transactions):
            acc = rng.choice(ids)
            cur = rng.choice(CURRENCIES)
            d = rng.choice(dates)
            plus = Decimal(rng.randint(0, 200000)) / 100
            minus = Decimal(rng.randint(0, 200000)) / 100
            w.writerow([acc, f"acct{acc}", f"{plus:.2f}", f"{minus:.2f}", cur, d.isoformat()])

    rates_path = out / "rates.csv"
    with open(rates_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "currency", "rate"])
        for day_index, d in enumerate(dates):
            for cur in CURRENCIES:
                w.writerow([d.isoformat(), cur, str(_rate_for(cur, day_index))])

    return str(tx_path), str(rates_path)


def main() -> None:
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    tx, rates = generate(seed=seed, n_transactions=n, out_dir=out_dir)
    print(f"wrote {tx} and {rates}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_generate_data.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add ledger/generate_data.py tests/test_generate_data.py
git commit -m "feat: seeded synthetic data generator"
```

---

## Task 4: Database layer + schema (db.py, schema.sql, conftest)

**Files:**
- Create: `db/schema.sql`, `ledger/db.py`, `tests/conftest.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: `ledger.config.load_config`.
- Produces:
  - `ledger.db.create_pool(min_size: int = 2, max_size: int | None = None) -> asyncpg.Pool` (uses `config.pool_size` when `max_size` is `None`).
  - `ledger.db.apply_schema(conn) -> None` (executes `db/schema.sql`; idempotent).
  - `ledger.db.truncate_all(conn) -> None` (`TRUNCATE balances, exchange_rates`).
  - `tests/conftest.py` exposes a `pool` async fixture that yields a ready pool with schema applied and both tables truncated.

> **Requires a running database** (`docker compose up -d`). These are integration tests.

- [ ] **Step 1: Create the schema file**

`db/schema.sql`:
```sql
CREATE TABLE IF NOT EXISTS balances (
    id          INTEGER         PRIMARY KEY,
    name        TEXT            NOT NULL,
    balance_usd NUMERIC(38, 10) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_date DATE            NOT NULL,
    currency  TEXT            NOT NULL,
    rate      NUMERIC(38, 10) NOT NULL,
    PRIMARY KEY (rate_date, currency)
);
```

- [ ] **Step 2: Write the failing test**

`tests/test_db.py`:
```python
from ledger.db import create_pool


async def test_schema_creates_tables(pool):
    async with pool.acquire() as conn:
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
    names = {r["table_name"] for r in tables}
    assert {"balances", "exchange_rates"} <= names


async def test_truncate_all_clears_rows(pool):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO balances (id, name, balance_usd) VALUES (100, 'x', 5)")
        from ledger.db import truncate_all

        await truncate_all(conn)
        count = await conn.fetchval("SELECT count(*) FROM balances")
    assert count == 0
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.db'` (and `conftest` import error).

- [ ] **Step 4: Write minimal implementation**

`ledger/db.py`:
```python
from pathlib import Path

import asyncpg

from ledger.config import load_config

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


async def create_pool(min_size: int = 2, max_size: int | None = None) -> asyncpg.Pool:
    cfg = load_config()
    return await asyncpg.create_pool(
        dsn=cfg.database_url,
        min_size=min_size,
        max_size=max_size if max_size is not None else cfg.pool_size,
    )


async def apply_schema(conn) -> None:
    await conn.execute(SCHEMA_PATH.read_text())


async def truncate_all(conn) -> None:
    await conn.execute("TRUNCATE balances, exchange_rates;")
```

`tests/conftest.py`:
```python
import pytest_asyncio

from ledger.db import apply_schema, create_pool, truncate_all


@pytest_asyncio.fixture
async def pool():
    p = await create_pool()
    async with p.acquire() as conn:
        await apply_schema(conn)
        await truncate_all(conn)
    yield p
    await p.close()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `docker compose up -d && pytest tests/test_db.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add db/schema.sql ledger/db.py tests/conftest.py tests/test_db.py
git commit -m "feat: database layer, schema, and test pool fixture"
```

---

## Task 5: Ingestion — batch/aggregate + concurrent atomic upsert (ingest.py)

**Files:**
- Create: `ledger/ingest.py`
- Test: `tests/test_ingest.py`

**Interfaces:**
- Consumes: `ledger.config.load_config`, `ledger.db.create_pool/apply_schema/truncate_all`, `ledger.rates.read_rates/build_rate_map/to_usd`.
- Produces:
  - `TxnRow(id: int, name: str, usd_delta: Decimal)` — `NamedTuple`.
  - `Batch(ids: list[int], names: list[str], deltas: list[Decimal])` — `NamedTuple`.
  - `batch_and_aggregate(rows: Iterable[TxnRow], batch_size: int) -> Iterator[Batch]` — groups raw rows into buffers of `batch_size`, then **sums `usd_delta` per id within each buffer** (so each `Batch` has unique ids).
  - `read_transactions(path: str, rate_map: dict) -> Iterator[TxnRow]`.
  - `upsert_batch(pool, batch: Batch) -> None`.
  - `bulk_load_rates(conn, rates: list[RateRow]) -> None`.
  - `run_ingest(transactions_path: str, rates_path: str) -> None` — full orchestration; closes the pool at the end.
  - `main() -> None` — CLI entry (`python -m ledger.ingest <tx.csv> <rates.csv>`).

- [ ] **Step 1: Write the failing tests (pure aggregation + DB concurrency)**

`tests/test_ingest.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_ingest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.ingest'`.

- [ ] **Step 3: Write minimal implementation**

`ledger/ingest.py`:
```python
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
    ids = list(acc.keys())
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `docker compose up -d && pytest tests/test_ingest.py -v`
Expected: PASS (3 passed) — including `test_concurrent_upserts_no_lost_update`.

- [ ] **Step 5: Commit**

```bash
git add ledger/ingest.py tests/test_ingest.py
git commit -m "feat: concurrency-safe batched ingestion"
```

---

## Task 6: Read server (server.py)

**Files:**
- Create: `ledger/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `ledger.db.create_pool`, `ledger.rates.from_usd/round_money`.
- Produces a FastAPI `app` with:
  - `GET /balance/{account_id}?currency=USD` → `{"id", "name", "currency", "balance"}` (200); 404 unknown id; 400 unknown currency.
  - `GET /total?currency=USD` → `{"currency", "total"}` (200); 400 unknown currency.
  - Money fields are strings; `currency` upper-cased; valuation date = `max(rate_date)`.
  - Internal helper `async def _conversion_rate(conn, currency: str) -> Decimal` (returns `Decimal("1")` for USD; raises `HTTPException(400)` if no rate at the max date).

- [ ] **Step 1: Write the failing test**

`tests/test_server.py`:
```python
from datetime import date
from decimal import Decimal

from httpx import ASGITransport, AsyncClient

from ledger.server import app


async def _seed(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO balances (id, name, balance_usd) VALUES (100,'acct100',200),(243,'acct243',-50)"
        )
        await conn.executemany(
            "INSERT INTO exchange_rates (rate_date, currency, rate) VALUES ($1,$2,$3)",
            [
                (date(2026, 6, 24), "USD", Decimal("1.0")),
                (date(2026, 6, 24), "EUR", Decimal("1.0")),  # 1 EUR = 1 USD for easy math
                (date(2026, 6, 15), "USD", Decimal("1.0")),
            ],
        )


async def test_balance_usd_and_404(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/balance/100")
        assert r.status_code == 200
        assert r.json() == {"id": 100, "name": "acct100", "currency": "USD", "balance": "200.00"}

        r404 = await c.get("/balance/999")
        assert r404.status_code == 404


async def test_balance_non_usd_divides(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/balance/100", params={"currency": "eur"})
        # 200 USD / 1.0 = 200.00 EUR; currency upper-cased
        assert r.json() == {"id": 100, "name": "acct100", "currency": "EUR", "balance": "200.00"}

        bad = await c.get("/balance/100", params={"currency": "ZZZ"})
        assert bad.status_code == 400


async def test_total(pool):
    await _seed(pool)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/total")
        assert r.json() == {"currency": "USD", "total": "150.00"}  # 200 + (-50)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_server.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ledger.server'`.

- [ ] **Step 3: Write minimal implementation**

`ledger/server.py`:
```python
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Query

from ledger.db import create_pool
from ledger.rates import from_usd, round_money

MAX_DATE_RATE_SQL = (
    "SELECT rate FROM exchange_rates "
    "WHERE currency = $1 AND rate_date = (SELECT max(rate_date) FROM exchange_rates)"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    try:
        yield
    finally:
        await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


async def _conversion_rate(conn, currency: str) -> Decimal:
    currency = currency.upper()
    if currency == "USD":
        return Decimal("1")
    row = await conn.fetchrow(MAX_DATE_RATE_SQL, currency)
    if row is None:
        raise HTTPException(status_code=400, detail=f"No rate available for currency {currency}")
    return row["rate"]


@app.get("/balance/{account_id}")
async def get_balance(account_id: int, currency: str = Query("USD")):
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT name, balance_usd FROM balances WHERE id = $1", account_id
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
        rate = await _conversion_rate(conn, currency)
        value = round_money(from_usd(row["balance_usd"], rate))
    return {
        "id": account_id,
        "name": row["name"],
        "currency": currency.upper(),
        "balance": str(value),
    }


@app.get("/total")
async def get_total(currency: str = Query("USD")):
    async with app.state.pool.acquire() as conn:
        total_usd = await conn.fetchval("SELECT COALESCE(SUM(balance_usd), 0) FROM balances")
        rate = await _conversion_rate(conn, currency)
        value = round_money(from_usd(total_usd, rate))
    return {"currency": currency.upper(), "total": str(value)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker compose up -d && pytest tests/test_server.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add ledger/server.py tests/test_server.py
git commit -m "feat: FastAPI read server with currency conversion"
```

---

## Task 7: End-to-end test (ingest → exit → serve → assert)

**Files:**
- Test: `tests/test_end_to_end.py`

**Interfaces:**
- Consumes: `ledger.generate_data.generate`, `ledger.rates.read_rates/build_rate_map/from_usd/round_money`, `ledger.server.app`, `ledger.db.create_pool`. Runs `python -m ledger.ingest` as a **subprocess**.
- Produces: no new modules; asserts served values match an independent pure-Python reference.

- [ ] **Step 1: Write the failing test**

`tests/test_end_to_end.py`:
```python
import csv
import subprocess
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal

from httpx import ASGITransport, AsyncClient

from ledger.db import create_pool
from ledger.generate_data import generate
from ledger.rates import build_rate_map, from_usd, read_rates, round_money
from ledger.server import app


def _reference(tx_path: str, rates_path: str):
    """Independent reference: inline arithmetic, no ingest code reused."""
    rate_map = build_rate_map(read_rates(rates_path))
    balances: dict[int, Decimal] = defaultdict(lambda: Decimal(0))
    names: dict[int, str] = {}
    with open(tx_path, newline="") as f:
        for r in csv.DictReader(f):
            cur = r["currency"].upper()
            d = date.fromisoformat(r["date"])
            delta = (Decimal(r["plus"]) - Decimal(r["minus"])) * rate_map[(cur, d)]
            balances[int(r["id"])] += delta
            names[int(r["id"])] = r["name"]
    max_date = max(d for (_c, d) in rate_map)
    return balances, names, rate_map, max_date


async def test_ingest_restart_and_serve(tmp_path):
    tx_path, rates_path = generate(seed=123, n_transactions=5000, out_dir=str(tmp_path))
    balances, names, rate_map, max_date = _reference(tx_path, rates_path)

    # Run ingestion as a separate process and let it fully exit (restart isolation).
    proc = subprocess.run(
        [sys.executable, "-m", "ledger.ingest", tx_path, rates_path],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

    eur_rate = rate_map[("EUR", max_date)]
    sample_ids = sorted(balances)[:20]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        for acc in sample_ids:
            r = await c.get(f"/balance/{acc}")
            assert r.json()["balance"] == str(round_money(balances[acc]))

            r_eur = await c.get(f"/balance/{acc}", params={"currency": "EUR"})
            expected_eur = str(round_money(from_usd(balances[acc], eur_rate)))
            assert r_eur.json()["balance"] == expected_eur

        total = sum(balances.values(), Decimal(0))
        r_total = await c.get("/total")
        assert r_total.json()["total"] == str(round_money(total))

        r_total_eur = await c.get("/total", params={"currency": "EUR"})
        assert r_total_eur.json()["total"] == str(round_money(from_usd(total, eur_rate)))

        missing = await c.get("/balance/99")
        assert missing.status_code == 404
```

- [ ] **Step 2: Run test to verify it fails (or errors) before ingest works end-to-end**

Run: `docker compose up -d && pytest tests/test_end_to_end.py -v`
Expected: FAIL if any prior task incomplete; otherwise this is the integration gate. (If all of Tasks 1–6 are done, it should pass — run it to confirm.)

- [ ] **Step 3: Run the full suite to verify everything passes**

Run: `pytest -v`
Expected: PASS (all tests across all files).

- [ ] **Step 4: Commit**

```bash
git add tests/test_end_to_end.py
git commit -m "test: end-to-end ingest, restart, and serve assertions"
```

---

## Task 8: Documentation (README + DESIGN_NOTES)

**Files:**
- Create: `README.md`, `DESIGN_NOTES.md`

**Interfaces:**
- Consumes: nothing (documentation).
- Produces: deliverable #1 (run instructions) and #5 (design note).

- [ ] **Step 1: Write `README.md`**

`README.md`:
````markdown
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
````

- [ ] **Step 2: Write `DESIGN_NOTES.md`**

`DESIGN_NOTES.md`:
```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add README.md DESIGN_NOTES.md
git commit -m "docs: README and concurrency/performance design notes"
```

---

## Self-Review

**1. Spec coverage**

| Spec section | Task |
|---|---|
| §3 two-process architecture | Tasks 5 (ingest exits), 6 (server), 7 (subprocess proof) |
| §4 schema (DDL) | Task 4 |
| §5 conversion rules (multiply/divide) | Tasks 2, 6 |
| §6 decimal/rounding (HALF_UP, string JSON) | Tasks 2, 6 |
| §7 ingestion algorithm (truncate, batch+aggregate, concurrent upsert, exit) | Task 5 |
| §8 concurrency reasoning + test | Task 5 test, Task 8 notes |
| §9 performance approach | Task 5, Task 8 notes |
| §10 read API contract (200/404/400, no /health) | Task 6 |
| §11 data generator (seeded, full grid, USD=1.0) | Task 3 |
| §12 test plan (ingest→restart→assert + reference) | Task 7 |
| §13 project layout | All tasks (+ documented db.py/conftest.py additions) |
| §14 setup/run | Task 8 (README) |
| §15 edge cases (negative ok, 404, 400, empty total=0) | Tasks 6, 7 |
| §16 dependencies | Task 1 |
| §17 deliverables | Tasks 1–8 |

No gaps found.

**2. Placeholder scan:** No TBD/TODO; every code step contains complete code; commands include expected output. Clean.

**3. Type consistency:** `Config(database_url, batch_size, pool_size)`, `RateRow(rate_date, currency, rate)`, `TxnRow(id, name, usd_delta)`, `Batch(ids, names, deltas)`, `to_usd`/`from_usd`/`round_money`, `create_pool`/`apply_schema`/`truncate_all`, `batch_and_aggregate`/`upsert_batch`/`run_ingest` — names and signatures used identically across producing and consuming tasks. Consistent.
