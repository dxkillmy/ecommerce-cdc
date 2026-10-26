import sys
import pandas as pd
import psycopg2

CDC_DSN = "host=localhost port=5434 dbname=warehouse user=wh password=wh"
RETAIL_DSN = "host=localhost port=5432 dbname=retail user=retail password=retail"
TOLERANCE = 0.01

CDC_SQL = """
    SELECT order_id, count(*) AS lines, sum(revenue) AS revenue
    FROM analytics.fact_sales
    GROUP BY order_id
"""

RETAIL_SQL = """
    SELECT invoice AS order_id, count(*) AS lines, sum(line_amount) AS revenue
    FROM analytics.fact_sales
    WHERE invoice = ANY(%s)
      AND NOT is_return
      AND has_customer
    GROUP BY invoice
"""


def fetch(dsn, query, params=None):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    df["revenue"] = df["revenue"].astype(float)
    return df


def main():
    df_cdc = fetch(CDC_DSN, CDC_SQL)
    df_retail = fetch(RETAIL_DSN, RETAIL_SQL, (df_cdc["order_id"].tolist(),))

    m = df_cdc.merge(df_retail, on="order_id", how="outer",
                     suffixes=("_cdc", "_retail"), indicator=True)

    missing_in_retail = m[m["_merge"] == "left_only"]
    both = m[m["_merge"] == "both"]
    mismatched = both[
        (both.lines_cdc != both.lines_retail)
        | ((both.revenue_cdc - both.revenue_retail).abs() > TOLERANCE)
    ]

    print(f"CDC Warehouse : {len(df_cdc):>6} orders  revenue={df_cdc.revenue.sum():,.2f}")
    print(f"Source Retail : {len(df_retail):>6} orders  revenue={df_retail.revenue.sum():,.2f}")
    print(f"missing_in_retail={len(missing_in_retail)}  mismatched={len(mismatched)}")

    problems = pd.concat([missing_in_retail, mismatched])
    if len(problems):
        print(problems[["order_id", "_merge", "lines_cdc", "lines_retail",
                        "revenue_cdc", "revenue_retail"]].head(20).to_string())
        sys.exit(1)

    print("CROSS-PROJECT RECONCILIATION PASSED")


if __name__ == "__main__":
    main()