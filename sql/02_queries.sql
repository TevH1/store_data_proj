-- A. Revenue by month (star schema simplicity)
SELECT d.year, d.month, SUM(price + freight_value) AS revenue
FROM dw.fact_order_item f
JOIN dw.dim_date d ON d.date_key = f.date_key_purchase
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- B. Top 20 customers by LTV (window + group)
WITH cust_rev AS (
  SELECT customer_sk, SUM(price + freight_value) AS revenue
  FROM dw.fact_order_item
  GROUP BY customer_sk
)
SELECT customer_sk, revenue,
       RANK() OVER (ORDER BY revenue DESC) AS rnk
FROM cust_rev
ORDER BY revenue DESC
LIMIT 20;

-- C. 7-day repeat rate (CTE + date math)
WITH first_orders AS (
  SELECT customer_sk, MIN(date_key_purchase) AS first_date
  FROM dw.fact_order_item
  GROUP BY customer_sk
)
SELECT
  COUNT(*) FILTER (
    WHERE f.date_key_purchase BETWEEN fo.first_date AND fo.first_date + 7
  )::float / COUNT(*) AS repeat_within_7d
FROM dw.fact_order_item f
JOIN first_orders fo USING (customer_sk);

-- D. Category performance (rollup)
SELECT d.year, d.month, p.product_category_name,
       SUM(f.price + f.freight_value) AS revenue
FROM dw.fact_order_item f
JOIN dw.dim_date d ON d.date_key = f.date_key_purchase
JOIN dw.dim_product p ON p.product_sk = f.product_sk
GROUP BY ROLLUP (d.year, d.month, p.product_category_name)
ORDER BY d.year NULLS LAST, d.month NULLS LAST, revenue DESC;

