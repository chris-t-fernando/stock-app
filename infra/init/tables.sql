CREATE TABLE stock_ohlcv (
    ticker       TEXT        NOT NULL,
    interval     TEXT        NOT NULL,  -- e.g. '1m', '5m', '1d'
    ts           TIMESTAMPTZ NOT NULL,
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    close        DOUBLE PRECISION,
    volume       BIGINT,
    PRIMARY KEY (ticker, interval, ts)
);


CREATE TABLE stock_ta_macd (
    ticker            TEXT        NOT NULL,
    interval          TEXT        NOT NULL,          -- e.g. '1m', '5m', '1d'
    ts                TIMESTAMPTZ NOT NULL,          -- timestamp aligned with stock_ohlcv.ts
    macd              DOUBLE PRECISION NULL,
    macd_signal       DOUBLE PRECISION NULL,
    macd_hist         DOUBLE PRECISION NULL,
    macd_diff         DOUBLE PRECISION NULL,
    macd_crossover    BOOLEAN         NULL,
    macd_crossover_type TEXT           NULL,         -- values like 'bullish' or 'bearish'
    PRIMARY KEY (ticker, interval, ts)
);

