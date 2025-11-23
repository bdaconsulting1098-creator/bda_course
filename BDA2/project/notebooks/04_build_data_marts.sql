-- Data Marts on top of Warehouse

-- Sales Mart: daily metrics
CREATE OR REPLACE TABLE mart_sales_daily AS
SELECT
  d.date,
  SUM(f.extended_net) AS revenue,
  SUM(f.margin_amount) AS margin,
  COUNT(DISTINCT f.order_id) AS orders
FROM fact_order_items f
JOIN dim_date d
  ON f.order_date_key = d.date_key
GROUP BY d.date
ORDER BY d.date;

-- Marketing Mart: performance by channel
CREATE OR REPLACE TABLE mart_marketing_channel AS
SELECT
  c.marketing_channel,
  SUM(f.extended_net) AS revenue,
  SUM(f.margin_amount) AS margin,
  COUNT(DISTINCT f.customer_id) AS customers
FROM fact_order_items f
JOIN dim_customer c
  ON f.customer_id = c.customer_id
GROUP BY c.marketing_channel
ORDER BY revenue DESC;

-- Product Mart: performance by category
CREATE OR REPLACE TABLE mart_product_category AS
SELECT
  p.category,
  SUM(f.quantity) AS units,
  SUM(f.extended_net) AS revenue,
  SUM(f.margin_amount) AS margin
FROM fact_order_items f
JOIN dim_product p
  ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;
