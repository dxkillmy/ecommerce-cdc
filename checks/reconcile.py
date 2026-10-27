import sys
import pandas as pd
import psycopg2
import argparse
import os
import sys

OLTP_DSN = os.environ.get("OLTP_DSN", "host=localhost port=5433 dbname=shop user=shop password=shop")
WH_DSN = os.environ.get("WH_DSN", "host=localhost port=5434 dbname=warehouse user=wh password=wh")
TOLERANCE = 0.01

SOURCE_SQL = """
    SELECT o.order_id, o.status,
           count(*) AS lines,
           sum(i.quantity * i.unit_price) AS revenue
    FROM orders o
    JOIN order_items i USING (order_id)
    WHERE o.updated_at < now() - make_interval(mins => %s)
    GROUP BY o.order_id, o.status
"""

TARGET_SQL = """
    SELECT order_id, status,
           count(*) AS lines,
           sum(revenue) AS revenue
    FROM analytics.fact_sales
    WHERE order_id = ANY(%s)
    GROUP BY order_id, status
"""

def fetch(dsn, query, params=None):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    df["revenue"] = df["revenue"].astype(float)
    return df

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lag-minutes", type=int, default=0)
    args = p.parse_args()

    src = fetch(OLTP_DSN, SOURCE_SQL, (args.lag_minutes,))
    tgt = fetch(WH_DSN, TARGET_SQL, (src["order_id"].tolist(),))

    m = src.merge(tgt, on="order_id", how="outer",
                  suffixes=("_src", "_tgt"), indicator=True)

    missing = m[m["_merge"] == "left_only"]
    extra = m[m["_merge"] == "right_only"]
    both = m[m["_merge"] == "both"]
    mismatched = both[
        (both.lines_src != both.lines_tgt)
        | (both.status_src != both.status_tgt)
        | ((both.revenue_src - both.revenue_tgt).abs() > TOLERANCE)
    ]

    print(f"source : {len(src):>6} orders  revenue={src.revenue.sum():,.2f}")
    print(f"target : {len(tgt):>6} orders  revenue={tgt.revenue.sum():,.2f}")
    print(f"missing_in_target={len(missing)}  extra_in_target={len(extra)}  "
          f"mismatched={len(mismatched)}")

    problems = pd.concat([missing, extra, mismatched])
    if len(problems):
        print(problems[["order_id", "_merge", "lines_src", "lines_tgt",
                        "revenue_src", "revenue_tgt"]].head(20).to_string())
        sys.exit(1)

    print("RECONCILIATION PASSED")

if __name__ == "__main__":
    main()