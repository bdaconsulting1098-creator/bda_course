# Architecture

Bronze → Silver → Gold → Marts on Databricks.

- Bronze: raw CSV data written to /dbfs/FileStore/tables by 01_generate_raw_data.py
- Silver: stg_* tables created from raw CSVs (02_build_silver.sql)
- Gold: dim_* and fact_order_items tables (03_build_warehouse.sql)
- Marts: mart_* tables (04_build_data_marts.sql)
