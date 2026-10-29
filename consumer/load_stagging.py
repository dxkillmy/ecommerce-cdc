import json
import time
import os
import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer, KafkaError, KafkaException

WAREHOUSE_DSN = os.environ.get("WH_DSN", "host=localhost port=5434 dbname=warehouse user=wh password=wh")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

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

RETRIABLE = {
    KafkaError.NOT_COORDINATOR,
    KafkaError.COORDINATOR_NOT_AVAILABLE,
    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    KafkaError.REQUEST_TIMED_OUT,
}

def commit_with_retry(consumer, attempts=5):
    for i in range(attempts):
        try:
            consumer.commit(asynchronous=False)
            return
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError._ASSIGNMENT_LOST:
                print("assignment lost; batch will be re-consumed after rejoin")
                return
            retriable = err.retriable() or err.code() in RETRIABLE
            if not retriable:
                raise
            wait = 2**i
            print(f"commit failed ({err.str()}), retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to commit Kafka offset after {attempts} attempts")

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "warehouse-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPICS))
    conn = psycopg2.connect(WAREHOUSE_DSN)
    processed = 0

    try:
        while True:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            if not msgs:
                continue

            with conn, conn.cursor() as cur:
                for msg in msgs:
                    if msg.error():
                        print("kafka error:", msg.error())
                        continue
                    if msg.value() is None:
                        continue
                    event = json.loads(msg.value())
                    table, pk_cols = TOPICS[msg.topic()]
                    apply_event(cur, table, pk_cols, event)

            commit_with_retry(consumer)
            processed += len(msgs)
            print(f"processed={processed} (batch={len(msgs)})")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()
        print(f"stopped, processed={processed}")

if __name__ == "__main__":
    main()