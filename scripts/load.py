# scripts/load.py
import os, pandas as pd, sqlalchemy as sa, calendar, datetime
from pathlib import Path
from sqlalchemy import text

DBURL = os.getenv("DATABASE_URL", "postgresql+psycopg2:///olist")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))  # override: DATA_DIR=~/Downloads/olist make load
eng = sa.create_engine(DBURL, future=True)

def read_any(logical, candidates):
    for name in candidates:
        p = DATA_DIR / name
        if p.exists():
            print(f"[OK] Using {p} for {logical}")
            return pd.read_csv(p)
    tried = ", ".join(str(DATA_DIR / n) for n in candidates)
    raise FileNotFoundError(f"Missing {logical}. Looked for: {tried}")

# Try common names and Olist originals (and .csv.gz)
orders  = read_any("orders",      ["orders.csv", "orders.csv.gz", "olist_orders_dataset.csv", "olist_orders_dataset.csv.gz"])
items   = read_any("order_items", ["order_items.csv", "order_items.csv.gz", "olist_order_items_dataset.csv", "olist_order_items_dataset.csv.gz"])
prods   = read_any("products",    ["products.csv", "products.csv.gz", "olist_products_dataset.csv", "olist_products_dataset.csv.gz"])
sellers = read_any("sellers",     ["sellers.csv", "sellers.csv.gz", "olist_sellers_dataset.csv", "olist_sellers_dataset.csv.gz"])
custs   = read_any("customers",   ["customers.csv", "customers.csv.gz", "olist_customers_dataset.csv", "olist_customers_dataset.csv.gz"])

# Ensure schemas exist (safe if already created by 01_schema.sql)
with eng.begin() as con:
    con.execute(text("CREATE SCHEMA IF NOT EXISTS stg;"))
    con.execute(text("CREATE SCHEMA IF NOT EXISTS dw;"))

# ---- build dim_date from orders ----
orders["purchase_ts"]   = pd.to_datetime(orders["order_purchase_timestamp"])
orders["purchase_date"] = orders["purchase_ts"].dt.date

dim_date = pd.DataFrame({"date_actual": pd.to_datetime(sorted(set(orders["purchase_date"])))})
dim_date["date_key"]   = dim_date["date_actual"].dt.strftime("%Y%m%d").astype(int)
dim_date["year"]       = dim_date["date_actual"].dt.year
dim_date["quarter"]    = dim_date["date_actual"].dt.quarter
dim_date["month"]      = dim_date["date_actual"].dt.month
dim_date["day"]        = dim_date["date_actual"].dt.day
dim_date["dow"]        = dim_date["date_actual"].dt.dayofweek
dim_date["month_name"] = dim_date["date_actual"].dt.month_name()
dim_date = dim_date[["date_key","date_actual","year","quarter","month","day","dow","month_name"]]

with eng.begin() as con:
    # upsert helper via staging
    def upsert(df, table, conflict_cols):
        tmp = f"{table}_stg"
        df.to_sql(tmp, con, schema="stg", if_exists="replace", index=False)
        cols = list(df.columns)
        sets = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])
        con.execute(text(f"""
            INSERT INTO dw.{table} ({", ".join(cols)})
            SELECT {", ".join(cols)} FROM stg.{tmp}
            ON CONFLICT ({", ".join(conflict_cols)}) DO UPDATE SET {sets};
        """))

    upsert(dim_date, "dim_date", ["date_key"])
    upsert(custs[["customer_id","customer_city","customer_state"]], "dim_customer", ["customer_id"])
    upsert(sellers[["seller_id","seller_city","seller_state"]], "dim_seller", ["seller_id"])
    prods2 = prods[["product_id","product_category_name","product_weight_g","product_length_cm","product_height_cm","product_width_cm"]]
    upsert(prods2, "dim_product", ["product_id"])

    # lookup maps
    dc = dict(con.execute(text("SELECT customer_id, customer_sk FROM dw.dim_customer")).all())
    ds = dict(con.execute(text("SELECT seller_id, seller_sk FROM dw.dim_seller")).all())
    dp = dict(con.execute(text("SELECT product_id, product_sk FROM dw.dim_product")).all())
    dd = dict(con.execute(text("SELECT date_actual, date_key FROM dw.dim_date")).all())

orders["date_key_purchase"] = pd.to_datetime(orders["purchase_date"]).map(dd)

# build fact
fact = (items
        .merge(orders[["order_id","customer_id","date_key_purchase"]], on="order_id", how="left")
        .merge(custs[["customer_id"]], on="customer_id", how="left")
        .merge(sellers[["seller_id"]], on="seller_id", how="left")
        .merge(prods[["product_id"]], on="product_id", how="left"))

fact["customer_sk"] = fact["customer_id"].map(dc)
fact["seller_sk"]   = fact["seller_id"].map(ds)
fact["product_sk"]  = fact["product_id"].map(dp)

fact = fact[["order_id","order_item_id","date_key_purchase","customer_sk","seller_sk","product_sk","price","freight_value"]]
fact = fact.dropna()
fact["order_item_id"] = fact["order_item_id"].astype(int)

def ensure_month_partition(con, y, m):
    first = int(datetime.date(y,m,1).strftime("%Y%m%d"))
    last_day  = calendar.monthrange(y,m)[1]
    next_first = int((datetime.date(y,m,last_day)+datetime.timedelta(days=1)).strftime("%Y%m%d"))
    name = f"fact_order_item_{y:04d}_{m:02d}"
    con.execute(text(f"""
        CREATE TABLE IF NOT EXISTS dw.{name}
        PARTITION OF dw.fact_order_item
        FOR VALUES FROM ({first}) TO ({next_first});
    """))

with eng.begin() as con:
    ym = fact["date_key_purchase"].astype(str).str[:6].unique()
    for ym_ in ym:
        y, m = int(ym_[:4]), int(ym_[4:6])
        ensure_month_partition(con, y, m)

    fact.to_sql("fact_order_item_stg", con, schema="stg", if_exists="replace", index=False)
    con.execute(text("""
        INSERT INTO dw.fact_order_item
        SELECT * FROM stg.fact_order_item_stg
        ON CONFLICT (order_id, order_item_id, date_key_purchase) DO NOTHING;
    """))

print("Load completed.")

