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
