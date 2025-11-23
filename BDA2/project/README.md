# E-Commerce Analytics Warehouse on Databricks

This project is a small but realistic data warehouse built on Databricks Community Edition for a fictional e-commerce business.

It includes:
- Synthetic data generation (customers, products, orders, order items)
- Staging (silver) tables
- Star-schema warehouse (dim_date, dim_customer, dim_product, fact_order_items)
- Data marts (sales, marketing, product)

## Notebooks

1. 01_generate_raw_data.py  – generate CSVs into /dbfs/FileStore/tables
2. 02_build_silver.sql      – create stg_* tables from CSVs
3. 03_build_warehouse.sql   – build dim_* and fact_order_items
4. 04_build_data_marts.sql  – build mart_* tables

Run them in that order in Databricks.
