CREATE SCHEMA IF NOT EXISTS olist;

-- Dimensions
CREATE TABLE IF NOT EXISTS olist.dim_customer (
  customer_key SERIAL PRIMARY KEY,
  customer_id TEXT UNIQUE,
  customer_unique_id TEXT,
  city TEXT,
  state TEXT
);

CREATE TABLE IF NOT EXISTS olist.dim_seller (
  seller_key SERIAL PRIMARY KEY,
  seller_id TEXT UNIQUE,
  city TEXT,
  state TEXT
);

CREATE TABLE IF NOT EXISTS olist.dim_product (
  product_key SERIAL PRIMARY KEY,
  product_id TEXT UNIQUE,
  category_name TEXT,
  category_name_english TEXT,
  weight_g INT,
  length_cm INT,
  height_cm INT,
  width_cm INT
);

CREATE TABLE IF NOT EXISTS olist.dim_date (
  date_key DATE PRIMARY KEY,
  year INT, quarter INT, month INT, day INT, dow INT
);

-- Facts
CREATE TABLE IF NOT EXISTS olist.fact_order_item (
  order_id TEXT,
  order_item_id INT,
  customer_key INT REFERENCES olist.dim_customer(customer_key),
  seller_key INT REFERENCES olist.dim_seller(seller_key),
  product_key INT REFERENCES olist.dim_product(product_key),
  order_purchase_date DATE REFERENCES olist.dim_date(date_key),
  order_approved_at DATE REFERENCES olist.dim_date(date_key),
  order_delivered_carrier_date DATE REFERENCES olist.dim_date(date_key),
  order_delivered_customer_date DATE REFERENCES olist.dim_date(date_key),
  order_estimated_delivery_date DATE REFERENCES olist.dim_date(date_key),
  shipping_limit_date DATE REFERENCES olist.dim_date(date_key),
  price NUMERIC(12,2),
  freight_value NUMERIC(12,2),
  revenue NUMERIC(12,2),
  PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS olist.fact_payment (
  order_id TEXT,
  payment_sequential INT,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC(12,2),
  PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS olist.fact_review (
  review_id TEXT PRIMARY KEY,
  order_id TEXT,
  review_score INT,
  review_creation_date DATE REFERENCES olist.dim_date(date_key),
  review_answer_timestamp DATE REFERENCES olist.dim_date(date_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_item_purchase ON olist.fact_order_item(order_purchase_date);
CREATE INDEX IF NOT EXISTS idx_fact_item_cust ON olist.fact_order_item(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_item_prod ON olist.fact_order_item(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_item_sell ON olist.fact_order_item(seller_key);

