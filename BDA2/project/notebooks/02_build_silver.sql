-- Create Silver (staging) tables from raw CSVs in FileStore

CREATE OR REPLACE TABLE stg_customers
USING csv
OPTIONS (
  path "/FileStore/tables/raw_customers.csv",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TABLE stg_products
USING csv
OPTIONS (
  path "/FileStore/tables/raw_products.csv",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TABLE stg_orders
USING csv
OPTIONS (
  path "/FileStore/tables/raw_orders.csv",
  header "true",
  inferSchema "true"
);

CREATE OR REPLACE TABLE stg_order_items
USING csv
OPTIONS (
  path "/FileStore/tables/raw_order_items.csv",
  header "true",
  inferSchema "true"
);
