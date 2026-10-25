import json

import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer

WAREHOUSE_DSN = "host=localhost port=5434 dbname=warehouse user=wh password=wh"

TOPICS = {
    "shop.public.customers":   ("customers",   ["customer_id"]),
    "shop.public.products":    ("products",    ["stock_code"]),
    "shop.public.orders":      ("orders",      ["order_id"]),
    "shop.public.order_items": ("order_items", ["order_id", "line_no"]),
}

def apply_event(cur, table, pk_cols, event):
    p = event["payload"]
    lsn = p["source"]["lsn"]
    tbl = sql.Identifier("staging", table)

    if p["op"] == "d":
        row = p["before"]
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in pk_cols
        )
        cur.execute(
            sql.SQL("""UPDATE {} SET _deleted = true, _source_lsn = %s,
                       _ingested_at = now()
                       WHERE {} AND _source_lsn < %s""").format(tbl, where),
            [lsn, *[row[c] for c in pk_cols], lsn],
        )
        return

    row = p["after"]
    cols = list(row)
    query = sql.SQL("""
        INSERT INTO {tbl} ({cols}, _source_lsn) VALUES ({vals}, %s)
        ON CONFLICT ({pk}) DO UPDATE
        SET {updates}, _deleted = false,
            _source_lsn = EXCLUDED._source_lsn, _ingested_at = now()
        WHERE {tbl}._source_lsn < EXCLUDED._source_lsn
    """).format(
        tbl=tbl,
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        pk=sql.SQL(", ").join(map(sql.Identifier, pk_cols)),
        updates=sql.SQL(", ").join(
            sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in cols
        ),
    )
    cur.execute(query, [row[c] for c in cols] + [lsn])

def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:29092",
        "group.id": "warehouse-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPICS))
    conn = psycopg2.connect(WAREHOUSE_DSN)
    processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("kafka error:", msg.error())
                continue

            if msg.value() is not None:
                event = json.loads(msg.value())
                table, pk_cols = TOPICS[msg.topic()]
                with conn, conn.cursor() as cur:
                    apply_event(cur, table, pk_cols, event)

            consumer.commit(message=msg, asynchronous=False)
            processed += 1
            if processed % 500 == 0:
                print(f"processed={processed}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()
        print(f"stopped, processed={processed}")

if __name__ == "__main__":
    main()