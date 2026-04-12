# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Bronze Layer
# MAGIC **Verizon Business Credit Risk — 2009–2010**
# MAGIC
# MAGIC Ingest both RAW CSV files into Delta Lake exactly as-is.
# MAGIC No cleaning. Audit metadata only.
# MAGIC
# MAGIC ```
# MAGIC RAW_Credit_Applications.csv  →  bronze_credit_applications
# MAGIC RAW_Credit_Risk.csv          →  bronze_credit_risk
# MAGIC ```
# MAGIC
# MAGIC > **Grain**: 1 row = 1 BAN (Billing Account Number = 1 unique credit application).
# MAGIC > Both tables share BAN as primary key — they join 1:1.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Keep RAW_PATH unchanged
bucket_PATH =f"/Workspace/Users/{current_user}/bda_course/Business_Credit"
RAW_PATH = f"/Volumes/workspace/default/course_data/Business_Credit"
dbutils.fs.cp(
    bucket_PATH,
    RAW_PATH,
    recurse=True
)
CATALOG = "workspace"
SCHEMA = "Business_Credit_sql"

# Create schema under catalog workspace
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Catalog     : {CATALOG}")
print(f"Schema      : {SCHEMA}")
print(f"Raw path    : {RAW_PATH}")


# COMMAND ----------

# MAGIC %md ## 1. Helpers

# COMMAND ----------

def read_raw(filename):
    return (spark.read
            .option("header",      "true")
            .option("inferSchema", "false")   # Bronze: everything stays as STRING
            .option("multiLine",   "true")
            .option("escape",      '"')
            .csv(f"{RAW_PATH}/{filename}"))

def write_bronze(df, table_name):
    df_out = (df
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_date",  F.lit(datetime.now().strftime("%Y-%m-%d")))
    )
    (df_out.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table_name}"))
    n = df_out.count()
    print(f"  bronze_{table_name:30s}  {n:>8,} rows")

# COMMAND ----------

# MAGIC %md ## 2. Ingest Tables

# COMMAND ----------

# MAGIC %md ### 2.1 Credit Applications

# COMMAND ----------

df = read_raw("RAW_Credit_Applications.csv")
print("Columns:", df.columns)
df.show(5, truncate=False)
# Rename column with space to underscore for Delta compatibility
df = df.withColumnRenamed("LINES REQUESTED", "LINES_REQUESTED")
write_bronze(df, "credit_applications")

# COMMAND ----------

# MAGIC %md ### 2.2 Credit Risk

# COMMAND ----------

df = read_raw("RAW_Credit_Risk.csv")
print("Columns:", df.columns)
df.show(5, truncate=False)
write_bronze(df, "credit_risk")

# COMMAND ----------

# MAGIC %md ## 3. Row count check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'credit_applications' AS table_name, COUNT(*) AS total_rows
# MAGIC FROM bronze_credit_applications
# MAGIC UNION ALL
# MAGIC SELECT 'credit_risk', COUNT(*)
# MAGIC FROM bronze_credit_risk

# COMMAND ----------

# MAGIC %md ## 4. Spot the data problems — do NOT fix here, that is Silver's job

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHANNEL_TYPE: mixed casing (Dealers / DEALERS / dealers)
# MAGIC -- AGE_GROUP: mixed casing and trailing spaces
# MAGIC -- ADR_PROVINCE: mixed casing (ON / on / ON)
# MAGIC -- LINES_REQUESTED: some NULLs
# MAGIC -- CREATE_WEEK/START_WEEK: 3 mixed date formats (YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD)
# MAGIC -- Duplicate BANs (30 injected duplicates)
# MAGIC SELECT BAN, CREATE_MONTH, CREATE_WEEK, CHANNEL_TYPE, AGE_GROUP, ADR_PROVINCE, `LINES_REQUESTED`
# MAGIC FROM bronze_credit_applications
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FIRST_CREDIT_CLASS: mixed casing (B / b / ' B ')
# MAGIC -- SCORE_CARD: mixed casing (EQFX / eqfx)
# MAGIC -- CREDIT_SCORE: some NULLs, some -1 (invalid)
# MAGIC -- FIRST_CREDIT_DATE: 3 mixed date formats
# MAGIC -- Duplicate BANs (25 injected duplicates)
# MAGIC SELECT BAN, FIRST_CREDIT_DATE, FIRST_CREDIT_CLASS, SCORE_CARD, CREDIT_SCORE
# MAGIC FROM bronze_credit_risk
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many distinct dirty formats exist per column?
# MAGIC SELECT CHANNEL_TYPE, COUNT(*) AS rows
# MAGIC FROM bronze_credit_applications
# MAGIC GROUP BY CHANNEL_TYPE ORDER BY rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT FIRST_CREDIT_CLASS, COUNT(*) AS rows
# MAGIC FROM bronze_credit_risk
# MAGIC GROUP BY FIRST_CREDIT_CLASS ORDER BY rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many applications have no START_MONTH (never activated)?
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_applications,
# MAGIC     SUM(CASE WHEN START_MONTH IS NULL THEN 1 ELSE 0 END) AS not_activated,
# MAGIC     SUM(CASE WHEN START_MONTH IS NOT NULL THEN 1 ELSE 0 END) AS activated
# MAGIC FROM bronze_credit_applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze Complete
# MAGIC Both tables landed unchanged. Proceed to **Notebook 2 — Silver**.
