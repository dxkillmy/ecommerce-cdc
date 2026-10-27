import argparse
import time
import pandas as pd
import psycopg2

DSN = "host=localhost port=5433 dbname=shop user=shop password=shop"

COLUMNS = ["invoice", "stock_code", "description", "quantity",
           "invoice_date", "price", "customer_id", "country"]

def load_orders(paths: list[str]):
    frames = [
        pd.read_csv(p, dtype={"Invoice": str, "StockCode": str},
                    parse_dates=["InvoiceDate"])
        for p in paths
    ]
    df = pd.concat(frames, ignore_index=True)
    df.columns = COLUMNS

    df = df.drop_duplicates()
    df = df[-df["invoice"].str.startswith("C")]
    df = df[(df["quantity"] > 0) & (df["price"] > 0)]
    df = df.dropna(subset=["customer_id"])
    df["customer_id"] = df["customer_id"].astype(int)

    df = df.sort_values(["invoice_date", "invoice"])
    return df.groupby("invoice", sort=False)

def write_order(conn, invoice: str, items: pd.DataFrame) -> bool:
    first = items.iloc[0]
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO customers (customer_id, country)
                   VALUES (%s, %s)
                   ON CONFLICT (customer_id) DO NOTHING""",
                (int(first.customer_id), first.country),
            )

            cur.execute(
                """INSERT INTO orders (order_id, customer_id, order_ts)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (order_id) DO NOTHING""",
                (invoice, int(first.customer_id), first.invoice_date),
            )
            if cur.rowcount == 0:
                return False

            for line_no, row in enumerate(items.itertuples(index=False), start=1):
                cur.execute(
                    """INSERT INTO products (stock_code, description, unit_price)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (stock_code) DO UPDATE
                       SET description = EXCLUDED.description,
                           unit_price  = EXCLUDED.unit_price,
                           updated_at  = now()
                       WHERE products.unit_price IS DISTINCT FROM EXCLUDED.unit_price""",
                    (row.stock_code, row.description, float(row.price)),
                )
                cur.execute(
                    """INSERT INTO order_items
                           (order_id, line_no, stock_code, quantity, unit_price)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (invoice, line_no, row.stock_code,
                     int(row.quantity), float(row.price)),
                )
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--files", default=["data/online_retail_09_10.csv",
                                       "data/online_retail_10_11.csv"])
    p.add_argument("--rate", type=float, default=5, help="orders per second")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()

    conn = psycopg2.connect(DSN)
    sent = skipped = 0
    for invoice, items in load_orders(args.files):
        if args.limit and sent + skipped >= args.limit:
            break
        if write_order(conn, invoice, items):
            sent += 1
            print(f"sent={sent} order={invoice}")
            time.sleep(1 / args.rate)
        else:
            skipped += 1

    conn.close()
    print(f"sent={sent} skipped={skipped}")


if __name__ == "__main__":
    main()