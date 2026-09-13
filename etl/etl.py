"""
Sales Data Engineering Platform - ETL pipeline.

Extracts the raw Superstore sales extract, cleans it (dedup, null handling,
validation), transforms it into a star schema, and loads it into the
Postgres data warehouse. Designed to be run either directly:

    python etl/etl.py

or as an Airflow task (see dags/sales_etl_dag.py), which imports and calls
run().

Connection settings come from environment variables so the same script works
unmodified on a laptop (host machine) and inside the Airflow containers:
    DW_HOST      default "localhost"
    DW_PORT      default "5433"
    DW_NAME      default "sales_dw"
    DW_USER      default "dw_user"
    DW_PASSWORD  default "dw_password"
"""

import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs, register_adapter

# pandas/numpy scalar dtypes (int64, float64, bool_) aren't adaptable by
# psycopg2 out of the box; register them so execute_values() can bind them.
register_adapter(np.int64, lambda v: AsIs(int(v)))
register_adapter(np.float64, lambda v: AsIs("NULL" if np.isnan(v) else float(v)))
register_adapter(np.bool_, lambda v: AsIs(bool(v)))

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_CSV = os.path.join(PROJECT_ROOT, "data", "raw", "superstore_raw.csv")
SCHEMA_SQL = os.path.join(PROJECT_ROOT, "sql", "schema.sql")
SUMMARY_PATH = os.path.join(PROJECT_ROOT, "logs", "etl_run_summary.json")


def get_raw_conn():
    host = os.environ.get("DW_HOST", "localhost")
    port = os.environ.get("DW_PORT", "5433")
    dbname = os.environ.get("DW_NAME", "sales_dw")
    user = os.environ.get("DW_USER", "dw_user")
    password = os.environ.get("DW_PASSWORD", "dw_password")
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )


def apply_schema():
    with open(SCHEMA_SQL, "r") as f:
        ddl = f.read()
    conn = get_raw_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def extract(stats):
    df = pd.read_csv(RAW_CSV, encoding="utf-8")
    stats["rows_extracted"] = len(df)
    return df


def clean(df, stats):
    """Clean the raw extract. Every issue found is counted into `stats`
    so the pipeline can report honest, real numbers about the run."""

    start_n = len(df)

    # 1. Exact duplicate rows
    dup_mask = df.duplicated(keep="first")
    stats["duplicate_rows_removed"] = int(dup_mask.sum())
    df = df[~dup_mask].copy()

    # 2. Drop rows missing values in fields that are required for a valid fact row
    required = ["Customer Name", "Sales", "Order Date", "Customer ID", "Product ID"]
    null_mask = df[required].isnull().any(axis=1)
    stats["rows_dropped_missing_required_fields"] = int(null_mask.sum())
    df = df[~null_mask].copy()

    # 3. Fill non-critical missing values instead of dropping
    postal_nulls = int(df["Postal Code"].isnull().sum())
    stats["postal_code_nulls_filled"] = postal_nulls
    df["Postal Code"] = df["Postal Code"].astype("Int64").astype(str)
    df.loc[df["Postal Code"] == "<NA>", "Postal Code"] = "UNKNOWN"

    # 4. Parse dates; drop rows with unparseable Order Date or Ship Date
    order_dt = pd.to_datetime(df["Order Date"], errors="coerce", format="%m/%d/%Y")
    ship_dt = pd.to_datetime(df["Ship Date"], errors="coerce", format="%m/%d/%Y")
    bad_date_mask = order_dt.isnull() | ship_dt.isnull()
    stats["rows_dropped_invalid_dates"] = int(bad_date_mask.sum())
    df = df[~bad_date_mask].copy()
    order_dt = order_dt[~bad_date_mask]
    ship_dt = ship_dt[~bad_date_mask]
    df["Order Date"] = order_dt
    df["Ship Date"] = ship_dt

    # 5. Ship date must not be before order date
    bad_order_mask = df["Ship Date"] < df["Order Date"]
    stats["rows_dropped_ship_before_order"] = int(bad_order_mask.sum())
    df = df[~bad_order_mask].copy()

    # 6. Sales must be positive
    bad_sales_mask = df["Sales"] <= 0
    stats["rows_dropped_invalid_sales"] = int(bad_sales_mask.sum())
    df = df[~bad_sales_mask].copy()

    # 7. Quantity must be positive
    bad_qty_mask = df["Quantity"] <= 0
    stats["rows_dropped_invalid_quantity"] = int(bad_qty_mask.sum())
    df = df[~bad_qty_mask].copy()

    stats["rows_after_cleaning"] = len(df)
    stats["total_bad_records_removed"] = start_n - len(df)
    return df.reset_index(drop=True)


def build_dim_date(dates):
    dates = pd.to_datetime(pd.Series(dates).unique())
    dim = pd.DataFrame({"full_date": dates})
    dim["date_id"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["day"] = dim["full_date"].dt.day
    dim["day_of_week"] = dim["full_date"].dt.day_name()
    dim["is_weekend"] = dim["full_date"].dt.dayofweek >= 5
    return dim.drop_duplicates(subset="date_id").reset_index(drop=True)


def transform(df, stats):
    dim_customer = (
        df[["Customer ID", "Customer Name", "Segment", "Country", "City", "State",
            "Postal Code", "Region"]]
        .drop_duplicates(subset="Customer ID")
        .rename(columns={
            "Customer ID": "customer_id", "Customer Name": "customer_name",
            "Segment": "segment", "Country": "country", "City": "city",
            "State": "state", "Postal Code": "postal_code", "Region": "region",
        })
        .reset_index(drop=True)
    )

    dim_product = (
        df[["Product ID", "Category", "Sub-Category", "Product Name"]]
        .drop_duplicates(subset="Product ID")
        .rename(columns={
            "Product ID": "product_id", "Category": "category",
            "Sub-Category": "sub_category", "Product Name": "product_name",
        })
        .reset_index(drop=True)
    )

    all_dates = pd.concat([df["Order Date"], df["Ship Date"]])
    dim_date = build_dim_date(all_dates)

    fact_sales = pd.DataFrame({
        "row_id": range(1, len(df) + 1),
        "order_id": df["Order ID"],
        "order_date_id": df["Order Date"].dt.strftime("%Y%m%d").astype(int),
        "ship_date_id": df["Ship Date"].dt.strftime("%Y%m%d").astype(int),
        "ship_mode": df["Ship Mode"],
        "customer_id": df["Customer ID"],
        "product_id": df["Product ID"],
        "sales": df["Sales"].round(4),
        "quantity": df["Quantity"],
        "discount": df["Discount"].round(4),
        "profit": df["Profit"].round(4),
    })

    stats["dim_customer_rows"] = len(dim_customer)
    stats["dim_product_rows"] = len(dim_product)
    stats["dim_date_rows"] = len(dim_date)
    stats["fact_sales_rows"] = len(fact_sales)

    return dim_customer, dim_product, dim_date, fact_sales


def _bulk_insert(cur, table, df):
    from psycopg2.extras import execute_values

    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    col_list = ", ".join(cols)
    query = f"INSERT INTO {table} ({col_list}) VALUES %s"
    execute_values(cur, query, values, page_size=1000)


def load(dim_customer, dim_product, dim_date, fact_sales):
    conn = get_raw_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE fact_sales, dim_customer, dim_product, dim_date CASCADE;")
            _bulk_insert(cur, "dim_customer", dim_customer)
            _bulk_insert(cur, "dim_product", dim_product)
            _bulk_insert(cur, "dim_date", dim_date)
            _bulk_insert(cur, "fact_sales", fact_sales)
        conn.commit()
    finally:
        conn.close()


def run():
    stats = {"run_timestamp": datetime.now().isoformat()}

    print("=== EXTRACT ===")
    df = extract(stats)
    print(f"Extracted {stats['rows_extracted']} rows from {RAW_CSV}")

    print("=== CLEAN ===")
    df_clean = clean(df, stats)
    print(f"Duplicates removed: {stats['duplicate_rows_removed']}")
    print(f"Dropped (missing required fields): {stats['rows_dropped_missing_required_fields']}")
    print(f"Postal code nulls filled with 'UNKNOWN': {stats['postal_code_nulls_filled']}")
    print(f"Dropped (invalid/unparseable dates): {stats['rows_dropped_invalid_dates']}")
    print(f"Dropped (ship date before order date): {stats['rows_dropped_ship_before_order']}")
    print(f"Dropped (invalid sales <= 0): {stats['rows_dropped_invalid_sales']}")
    print(f"Dropped (invalid quantity <= 0): {stats['rows_dropped_invalid_quantity']}")
    print(f"Total bad records removed: {stats['total_bad_records_removed']}")
    print(f"Rows remaining after cleaning: {stats['rows_after_cleaning']}")

    print("=== SCHEMA ===")
    apply_schema()
    print("Star schema ensured (CREATE TABLE IF NOT EXISTS).")

    print("=== TRANSFORM ===")
    dim_customer, dim_product, dim_date, fact_sales = transform(df_clean, stats)
    print(f"dim_customer: {stats['dim_customer_rows']} rows")
    print(f"dim_product:  {stats['dim_product_rows']} rows")
    print(f"dim_date:     {stats['dim_date_rows']} rows")
    print(f"fact_sales:   {stats['fact_sales_rows']} rows")

    print("=== LOAD ===")
    load(dim_customer, dim_product, dim_date, fact_sales)
    print("Loaded into Postgres warehouse (sales_dw).")

    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    with open(SUMMARY_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nRun summary written to {SUMMARY_PATH}")
    print(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"ETL FAILED: {e}", file=sys.stderr)
        raise
