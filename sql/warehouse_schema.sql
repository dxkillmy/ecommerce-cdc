CREATE SCHEMA staging;

CREATE TABLE staging.customers (
    customer_id   INT PRIMARY KEY,
    country       TEXT,
    created_at    TIMESTAMPTZ,
    _deleted      BOOLEAN NOT NULL DEFAULT false,
    _source_lsn   BIGINT NOT NULL,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE staging.products (
    stock_code    TEXT PRIMARY KEY,
    description   TEXT,
    unit_price    NUMERIC(10,2),
    updated_at    TIMESTAMPTZ,
    _deleted      BOOLEAN NOT NULL DEFAULT false,
    _source_lsn   BIGINT NOT NULL,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE staging.orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   INT,
    status        TEXT,
    order_ts      TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _deleted      BOOLEAN NOT NULL DEFAULT false,
    _source_lsn   BIGINT NOT NULL,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE staging.order_items (
    order_id      TEXT,
    line_no       INT,
    stock_code    TEXT,
    quantity      INT,
    unit_price    NUMERIC(10,2),
    _deleted      BOOLEAN NOT NULL DEFAULT false,
    _source_lsn   BIGINT NOT NULL,
    _ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_no)
);

SELECT order_id, status, _source_lsn FROM staging.orders 
WHERE status = 'cancelled';