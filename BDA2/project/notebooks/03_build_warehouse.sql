-- Build Warehouse (Gold) Layer: dim_date, dim_customer, dim_product, fact_order_items

-- DIM DATE
CREATE OR REPLACE TABLE dim_date AS
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d AS date,
  year(d) AS year,
  month(d) AS month,
  day(d) AS day,
  weekofyear(d) AS week_of_year,
  date_format(d, 'E') AS day_name
FROM (
  SELECT explode(sequence(to_date('2020-01-01'), to_date('2025-12-31'), interval 1 day)) AS d
);

-- DIM CUSTOMER
CREATE OR REPLACE TABLE dim_customer AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  first_name,
  last_name,
  email,
  country,
  marketing_channel,
  created_ts,
  DATE(created_ts) AS signup_date
FROM stg_customers;

-- DIM PRODUCT
CREATE OR REPLACE TABLE dim_product AS
SELECT
  CAST(product_id AS STRING) AS product_id,
  product_name,
  category,
  brand,
  CAST(current_price AS DECIMAL(18, 2)) AS current_price,
  CAST(cost AS DECIMAL(18, 2)) AS cost
FROM stg_products;

-- FACT ORDER ITEMS
CREATE OR REPLACE TABLE fact_order_items AS
SELECT
  CAST(oi.order_item_id AS STRING) AS order_item_id,
  CAST(oi.order_id AS STRING) AS order_id,
  CAST(o.customer_id AS STRING) AS customer_id,
  CAST(oi.product_id AS STRING) AS product_id,
  d.date_key AS order_date_key,
  DATE(o.order_ts) AS order_date,
  o.order_status,
  o.currency,
  CAST(oi.quantity AS INT) AS quantity,
  CAST(oi.unit_price AS DECIMAL(18, 2)) AS unit_price,
  CAST(oi.discount_amount AS DECIMAL(18, 2)) AS discount_amount,
  (oi.quantity * oi.unit_price) AS extended_gross,
  oi.discount_amount AS extended_discount,
  (oi.quantity * oi.unit_price - oi.discount_amount) AS extended_net,
  (oi.quantity * p.cost) AS cost_amount,
  ((oi.quantity * oi.unit_price - oi.discount_amount) - (oi.quantity * p.cost)) AS margin_amount
FROM stg_order_items oi
JOIN stg_orders o
  ON oi.order_id = o.order_id
LEFT JOIN dim_product p
  ON CAST(oi.product_id AS STRING) = p.product_id
LEFT JOIN dim_date d
  ON DATE(o.order_ts) = d.date;
