-- Row counts
SELECT 'dim_customer' AS table, COUNT(*) FROM olist.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM olist.dim_product
UNION ALL SELECT 'dim_seller', COUNT(*) FROM olist.dim_seller
UNION ALL SELECT 'dim_date', COUNT(*) FROM olist.dim_date
UNION ALL SELECT 'fact_order_item', COUNT(*) FROM olist.fact_order_item
UNION ALL SELECT 'fact_payment', COUNT(*) FROM olist.fact_payment
UNION ALL SELECT 'fact_review', COUNT(*) FROM olist.fact_review;

-- Monthly revenue & orders
SELECT date_trunc('month', order_purchase_date) AS month,
       SUM(revenue) AS revenue,
       COUNT(DISTINCT order_id) AS orders
FROM olist.fact_order_item
GROUP BY 1 ORDER BY 1;

