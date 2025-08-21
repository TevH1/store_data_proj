-- data warehouse
CREATE SCHEMA IF NOT EXISTS dw;
--raw data
CREATE SCHEMA IF NOT EXISTS stg;


--calendar lookup 
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key      integer PRIMARY KEY,          -- YYYYMMDD
  date_actual   date NOT NULL UNIQUE,
  year          smallint NOT NULL,
  quarter       smallint NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  month         smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
  day           smallint NOT NULL CHECK (day BETWEEN 1 AND 31),
  dow           smallint NOT NULL CHECK (dow BETWEEN 0 AND 6), -- 0=Sun
  month_name    text NOT NULL
);

--customer dimensions
CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk   bigserial PRIMARY KEY,
  customer_id   text NOT NULL UNIQUE,  -- business key from source
  customer_city text,
  customer_state text
);


--seller dimensions
CREATE TABLE IF NOT EXISTS dw.dim_seller (
  seller_sk     bigserial PRIMARY KEY,
  seller_id     text NOT NULL UNIQUE,
  seller_city   text,
  seller_state  text
);

--product dimensions
CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_sk      bigserial PRIMARY KEY,
  product_id      text NOT NULL UNIQUE,
  product_category_name text,
  product_weight_g integer,
  product_length_cm integer,
  product_height_cm integer,
  product_width_cm integer
);

--fact table
CREATE TABLE IF NOT EXISTS dw.fact_order_item (
  order_id          text        NOT NULL,
  order_item_id     integer     NOT NULL,
  date_key_purchase integer     NOT NULL REFERENCES dw.dim_date(date_key),
  customer_sk       bigint      NOT NULL REFERENCES dw.dim_customer(customer_sk),
  seller_sk         bigint      NOT NULL REFERENCES dw.dim_seller(seller_sk),
  product_sk        bigint      NOT NULL REFERENCES dw.dim_product(product_sk),
  price             numeric(12,2) NOT NULL,
  freight_value     numeric(12,2) NOT NULL,
  -- PK must include the partition key (date_key_purchase)
  PRIMARY KEY (order_id, order_item_id, date_key_purchase)
) PARTITION BY RANGE (date_key_purchase);
 



CREATE INDEX IF NOT EXISTS fact_order_item_datekey_idx
  ON dw.fact_order_item USING BRIN (date_key_purchase);

CREATE INDEX IF NOT EXISTS fact_order_item_customer_idx
  ON dw.fact_order_item (customer_sk);

CREATE INDEX IF NOT EXISTS fact_order_item_seller_idx
  ON dw.fact_order_item (seller_sk);

CREATE INDEX IF NOT EXISTS fact_order_item_product_idx
  ON dw.fact_order_item (product_sk);
