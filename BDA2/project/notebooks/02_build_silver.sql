%sql
-- Create Silver (staging) tables from raw CSVs in Unity Catalog Volume using COPY INTO

CREATE TABLE IF NOT EXISTS stg_customers;
COPY INTO stg_customers
FROM '/Volumes/workspace/default/course_data/project/raw_customers.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true');

CREATE TABLE IF NOT EXISTS stg_products;
COPY INTO stg_products
FROM '/Volumes/workspace/default/course_data/project/raw_products.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true');

CREATE TABLE IF NOT EXISTS stg_orders;
COPY INTO stg_orders
FROM '/Volumes/workspace/default/course_data/project/raw_orders.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true');

CREATE TABLE IF NOT EXISTS stg_order_items;
COPY INTO stg_order_items
FROM '/Volumes/workspace/default/course_data/project/raw_order_items.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'true');