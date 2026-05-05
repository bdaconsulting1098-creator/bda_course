# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Bronze Layer
# MAGIC **City Fleet Services Division — Vehicle Fleet Analysis**
# MAGIC
# MAGIC Ingest both RAW CSV files into Delta Lake exactly as-is.
# MAGIC No cleaning. Audit metadata only.
# MAGIC
# MAGIC ```
# MAGIC RAW_Fleet_Units.csv   →  bronze_fleet_units   (vehicle master — 5,640 rows incl. dups)
# MAGIC RAW_Fleet_Usage.csv   →  bronze_fleet_usage   (usage metrics  — 5,635 rows incl. dups)
# MAGIC ```
# MAGIC
# MAGIC > **Primary key**: `UNIT_NO` — unique vehicle identifier.
# MAGIC > Both tables join 1:1 on `UNIT_NO`.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------



from pyspark.sql import functions as F
from datetime import datetime

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Keep RAW_PATH unchanged
bucket_PATH =f"/Workspace/Users/{current_user}/bda_course/fleet_service"
RAW_PATH = f"/Volumes/workspace/default/course_data/fleet_service"
dbutils.fs.cp(
    bucket_PATH,
    RAW_PATH,
    recurse=True
)
CATALOG = "workspace"
SCHEMA = "fleet_service_sql"

# Create schema under catalog workspace
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Catalog     : {CATALOG}")
print(f"Schema      : {SCHEMA}")
print(f"Raw path    : {RAW_PATH}")

# COMMAND ----------

# MAGIC %md ## 1. Helpers

# COMMAND ----------

# DBTITLE 1,Cell 5
def read_raw(filename):
    return (spark.read
            .option("header",      "true")
            .option("inferSchema", "false")
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
    print(f"  bronze_{table_name:20s}  {n:>8,} rows")

# COMMAND ----------

# MAGIC %md ## 2. Ingest Tables

# COMMAND ----------

# MAGIC %md ### 2.1 Fleet Units (vehicle master)

# COMMAND ----------

df = read_raw("RAW_Fleet_Units.csv")
print("Columns:", df.columns)
df.show(5, truncate=False)

# Rename columns with invalid Delta characters
df = df.withColumnRenamed("EXPECTED_LIFE(YR)", "EXPECTED_LIFE_YR") \
       .withColumnRenamed("MAINTENANCE CLASSIFICATION CODE", "MAINTENANCE_CLASSIFICATION_CODE")

write_bronze(df, "fleet_units")

# COMMAND ----------

# MAGIC %md ### 2.2 Fleet Usage (usage metrics)

# COMMAND ----------

df = read_raw("RAW_Fleet_Usage.csv")
print("Columns:", df.columns)
df.show(5, truncate=False)

# Rename columns with invalid Delta characters
df = df.withColumnRenamed("M5_LIFE_KM_USAGE(KM)", "M5_LIFE_KM_USAGE_KM") \
       .withColumnRenamed("M5_LIFE_HRS_USAGE(Hrs)", "M5_LIFE_HRS_USAGE_HRS") \
       .withColumnRenamed("M5_YTD_KM_USAGE(KM)", "M5_YTD_KM_USAGE_KM") \
       .withColumnRenamed("M5_YTD_HRS_USAGE(Hrs)", "M5_YTD_HRS_USAGE_HRS") \
       .withColumnRenamed("AVAILABLE HOURS (Hrs)", "AVAILABLE_HOURS_HRS") \
       .withColumnRenamed("DOWNTIME(Hrs)", "DOWNTIME_HRS") \
       .withColumnRenamed("EXPECT_USAGE(KM)", "EXPECT_USAGE_KM") \
       .withColumnRenamed("EXPECT_USAGE(Hrs)", "EXPECT_USAGE_HRS")

write_bronze(df, "fleet_usage")

# COMMAND ----------

# MAGIC %md ## 3. Row count check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fleet_units' AS table_name, COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT UNIT_NO) AS distinct_units
# MAGIC FROM bronze_fleet_units
# MAGIC UNION ALL
# MAGIC SELECT 'fleet_usage', COUNT(*), COUNT(DISTINCT UNIT_NO)
# MAGIC FROM bronze_fleet_usage

# COMMAND ----------

# MAGIC %md ## 4. Spot the data problems — do NOT fix here

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC -- DIVISION: mixed casing ('PARKS, FORESTRY & RECREATION' vs 'parks, forestry & recreation')
# MAGIC -- CURRENT_STATUS_DESCRIPTION: mixed casing
# MAGIC -- UNIT_TYPE: mixed casing + some rows have fuel values (column shift)
# MAGIC -- FUEL_PRODUCT: mixed casing + some rows have date serials
# MAGIC -- AGE: some NULL, some negative
# MAGIC -- EXPECTED_LIFE_YR: some rows contain date serials instead of years
# MAGIC -- Duplicate UNIT_NO rows (30 injected duplicates)
# MAGIC SELECT UNIT_NO, DIVISION, YEAR, MAKE, UNIT_TYPE, FUEL_PRODUCT,
# MAGIC        AGE, EXPECTED_LIFE_YR, CURRENT_STATUS_DESCRIPTION
# MAGIC FROM bronze_fleet_units
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many distinct dirty STATUS values?
# MAGIC SELECT CURRENT_STATUS_DESCRIPTION, COUNT(*) AS rows
# MAGIC FROM bronze_fleet_units
# MAGIC GROUP BY CURRENT_STATUS_DESCRIPTION
# MAGIC ORDER BY rows DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC -- DOWNTIME: some NULL, some -1 (invalid)
# MAGIC -- AVAILABLE HOURS: some NULL
# MAGIC -- VEU: some NULL
# MAGIC -- Date fields: mixed formats
# MAGIC SELECT UNIT_NO, AVAILABLE_HOURS_HRS, DOWNTIME_HRS,
# MAGIC        M5_YTD_KM_USAGE_KM, VEU, LAST_FUEL_DATE
# MAGIC FROM bronze_fleet_usage
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC -- How many units have negative or invalid downtime?
# MAGIC SELECT COUNT(*) AS invalid_downtime_rows
# MAGIC FROM bronze_fleet_usage
# MAGIC WHERE TRY_CAST(DOWNTIME_HRS AS DOUBLE) < 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze Complete
# MAGIC Both tables landed unchanged. Proceed to **Notebook 2 — Silver**.
