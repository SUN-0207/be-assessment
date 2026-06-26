# Backend Engineering Assessment — Ledger Balance Aggregation

**Estimated time:** 1–2 hours
**Stack:** Python or Node.js (your choice) + PostgreSQL

---

## Summary

Build a small service that ingests a file of financial transactions, computes the final balance per account in USD, stores the result in PostgreSQL, and exposes read endpoints that return balances in a requested currency.

Use synthetic data only.

---

## Input

You will be given **two files**.

### 1. Transactions file — 50,000 rows

| Field      | Type    | Description                                   |
|------------|---------|-----------------------------------------------|
| `id`       | integer | 3-digit account id (100–999)                  |
| `name`     | string  | Account holder name                           |
| `plus`     | number  | Amount credited, in `currency`                |
| `minus`    | number  | Amount debited, in `currency`                 |
| `currency` | string  | Currency code of this row (e.g. `USD`, `EUR`) |
| `date`     | date    | Date the transaction applies to               |

- At most ~900 distinct ids; each id appears many times.
- At most 10 distinct dates appear across the file.

First rows (header + sample):

```
id,name,plus,minus,currency,date
100,acct100,250.00,120.50,USD,2026-06-15
243,acct243,80.00,0.00,EUR,2026-06-16
587,acct587,0.00,45.75,GBP,2026-06-15
912,acct912,1200.00,300.00,JPY,2026-06-17
100,acct100,15.25,60.00,SGD,2026-06-18
```

### 2. Exchange-rate file

One rate per currency per day, provided as input.

| Field      | Type   | Description                                       |
|------------|--------|---------------------------------------------------|
| `date`     | date   | The day the rate applies to                       |
| `currency` | string | Currency code                                     |
| `rate`     | number | Multiplier to convert 1 unit of `currency` to USD |

Every `(currency, date)` pair present in the transactions file has a matching rate.

First rows (header + sample):

```
date,currency,rate
2026-06-15,USD,1.0
2026-06-15,EUR,1.0832
2026-06-15,GBP,1.2710
2026-06-15,JPY,0.00642
2026-06-15,SGD,0.7395
```

---

## What to build

Ingest the transactions file and persist, in PostgreSQL, the final balance for each account. After ingestion finishes, the ingesting process must be shut down and a **separate server started** to serve the read endpoints below from PostgreSQL.

### Read endpoints

A minimal HTTP interface with two reads, both accepting a target `currency` parameter:

- **Balance for a given id** — the account's final balance, in the requested currency.
- **Total** — the sum of all account balances, in the requested currency.

---

## Definition of correctness

- The balance of an account is the sum of its credits minus its debits, with **each transaction converted to USD using the rate for its own `(currency, date)`**.
- Negative balances are valid. This is pure arithmetic — no overdraft or non-negative rules.
- When a read requests a non-USD currency, convert the stored USD value using the rate for the **most recent date present in the exchange-rate input**. The same valuation date applies to the per-id balance and the total. A request for USD (or no currency) returns the stored USD value.

---

## Requirements

- **Concurrency.** Transactions are applied concurrently: a write for one row may be issued without waiting for the previous one to finish. The persisted balances must be correct regardless of order or interleaving; no updates may be lost.
- **Restart isolation.** Balances must survive the ingesting process exiting. The read server is a separate run and may not rely on any in-memory state from ingestion.
- **Performance.** Ingestion and persistence must complete in a reasonable time on a local PostgreSQL instance.
- **Persistence.** Final balances live in PostgreSQL, one row per id.

---

## Deliverables

1. Source code (Python or Node.js) with clear setup/run instructions.
2. Schema / DDL for your Postgres table(s).
3. The data generator, if you generate the input files yourself.
4. A test that ingests, restarts, and asserts the served balances (per-id and total) are correct.
5. A short note on the design decisions behind your concurrency and performance approach.

---

## Evaluation criteria

- **Correctness** — served per-id balances and total match an independently computed reference.
- **Currency handling** — per-transaction USD conversion and read-side conversion are both correct.
- **Concurrency safety** — no lost updates under concurrent application.
- **Performance** — ingestion completes in a reasonable time.
- **Restart isolation** — reads are served from PostgreSQL after a clean restart.
- **Code quality** — clear structure, sensible schema, runnable setup, a meaningful test.

---

## Constraints

- Synthetic data only.
- Libraries and ORMs are allowed; be prepared to explain what happens at the SQL/transaction level.
- Keep external dependencies minimal so the submission is easy to run.
