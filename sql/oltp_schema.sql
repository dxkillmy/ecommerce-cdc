CREATE TABLE customers (
    customer_id  INT PRIMARY KEY,
    country      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    stock_code   TEXT PRIMARY KEY,
    description  TEXT,
    unit_price   NUMERIC(10,2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  INT REFERENCES customers(customer_id),
    status       TEXT NOT NULL DEFAULT 'placed'
                 CHECK (status IN ('placed', 'cancelled')),
    order_ts     TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id     TEXT REFERENCES orders(order_id),
    line_no      INT,
    stock_code   TEXT REFERENCES products(stock_code),
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

ALTER TABLE orders REPLICA IDENTITY FULL;

UPDATE orders SET status = 'cancelled', updated_at= now()
WHERE order_id = (SELECT order_id FROM orders LIMIT 1);

DELETE FROM order_items WHERE order_id = (SELECT max(order_id) FROM orders);
DELETE FROM orders      WHERE order_id = (SELECT max(order_id) FROM orders);