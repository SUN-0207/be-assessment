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

-- Staging tables. Ingestion writes here concurrently; the live tables above are
-- only touched by the final publish, which swaps the staged data in inside one
-- transaction. This lets the read server keep serving a consistent snapshot
-- throughout an ingest, and leaves the live data intact if ingestion crashes.
-- UNLOGGED because the contents are transient scratch, rebuilt on every run.
CREATE UNLOGGED TABLE IF NOT EXISTS balances_staging (
    id          INTEGER         PRIMARY KEY,
    name        TEXT            NOT NULL,
    balance_usd NUMERIC(38, 10) NOT NULL DEFAULT 0
);

CREATE UNLOGGED TABLE IF NOT EXISTS exchange_rates_staging (
    rate_date DATE            NOT NULL,
    currency  TEXT            NOT NULL,
    rate      NUMERIC(38, 10) NOT NULL,
    PRIMARY KEY (rate_date, currency)
);
