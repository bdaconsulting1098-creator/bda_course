# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — Pipeline Orchestrator
# MAGIC **Verizon Business Credit Risk Pipeline**
# MAGIC Runs Bronze → Silver → Gold in sequence.
# MAGIC
# MAGIC | Mode | When to use |
# MAGIC |------|-------------|
# MAGIC | `full_refresh` | First run, or full rebuild |
# MAGIC | `incremental`  | Monthly run — only new CREATE_MONTH rows |
# MAGIC
# MAGIC ### Databricks Job setup
# MAGIC ```
# MAGIC Workflows → Jobs → Create Job
# MAGIC   Task       : Notebook  →  /path/to/04_Pipeline_Orchestrator
# MAGIC   Schedule   : 0 6 1 * *   (1st of every month at 6 AM)
# MAGIC   Parameters : {"mode": "incremental"}
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 0. Configuration + Mode

# COMMAND ----------

try:
    MODE = dbutils.widgets.get("mode")
except Exception:
    MODE = "full_refresh"

CATALOG = "workspace"
SCHEMA = "Business_Credit_sql"
RAW_PATH = "/Volumes/workspace/default/course_data/Business_Credit"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Mode    : {MODE}")
print(f"Run ID  : {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## 1. Watermark + Run Log

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.pipeline_watermarks (
        table_name     STRING,
        last_month     STRING,
        rows_processed LONG,
        run_status     STRING,
        updated_at     TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.pipeline_run_log (
        run_id      STRING,
        run_mode    STRING,
        stage       STRING,
        rows_out    LONG,
        run_status  STRING,
        error_msg   STRING,
        started_at  TIMESTAMP,
        finished_at TIMESTAMP
    ) USING DELTA
""")

def get_watermark():
    rows = spark.sql(f"""
        SELECT last_month FROM {CATALOG}.{SCHEMA}.pipeline_watermarks
        WHERE table_name = 'bronze_credit_applications'
          AND run_status = 'SUCCESS'
        ORDER BY updated_at DESC LIMIT 1
    """).collect()
    return rows[0]["last_month"] if rows else "1900-01"

def set_watermark(last_month, n):
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.pipeline_watermarks VALUES
        ('bronze_credit_applications', '{last_month}', {n}, 'SUCCESS', CURRENT_TIMESTAMP())
    """)
    print(f"  Watermark → last_month={last_month} ({n:,} rows)")

def log_run(stage, rows=0, status="SUCCESS", error=""):
    safe_error = error[:200].replace("'", "")
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.pipeline_run_log VALUES
        ('{RUN_ID}', '{MODE}', '{stage}', {rows}, '{status}',
         '{safe_error}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """)

# COMMAND ----------

# MAGIC %md ## 2. Shared helpers

# COMMAND ----------

def read_raw(filename):
    return (spark.read
            .option("header",      "true")
            .option("inferSchema", "false")
            .option("multiLine",   "true")
            .option("escape",      '"')
            .csv(f"{RAW_PATH}/{filename}"))

def parse_mixed_date(col_name):
    """Handles 3 date formats: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY"""
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
    )

def write_table(df, table_name, mode="overwrite"):
    (df.write
       .format("delta")
       .mode(mode)
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))

# COMMAND ----------

# MAGIC %md ## 3. Bronze Stage

# COMMAND ----------

def run_bronze():
    print("\n▓▓▓  BRONZE  ▓▓▓")
    t = datetime.now()

    def ingest(filename, table_name, mode="overwrite"):
        df = (read_raw(filename)
              .withColumn("_ingest_time", F.current_timestamp())
              .withColumn("_source_file", F.lit(filename))
              .withColumn("_batch_date",  F.lit(datetime.now().strftime("%Y-%m-%d"))))
        write_table(df, f"{CATALOG}.{SCHEMA}.bronze_{table_name}", mode=mode)
        n = df.count()
        print(f"  bronze_{table_name:30s}  {n:>8,} rows")
        return n

    # Credit Risk table: always full overwrite (it is a lookup enrichment)
    ingest("RAW_Credit_Risk.csv", "credit_risk", mode="overwrite")

    # Credit Applications: incremental on CREATE_MONTH
    df_raw = (read_raw("RAW_Credit_Applications.csv")
              .withColumnRenamed("LINES REQUESTED", "LINES_REQUESTED")  # Fix: rename column with space
              .withColumn("_month", F.trim(F.col("CREATE_MONTH"))))

    if MODE == "incremental":
        wm_month = get_watermark()
        print(f"  Watermark: last_month={wm_month}")
        df_new = df_raw.filter(F.col("_month") > wm_month)
        n = df_new.count()
        if n == 0:
            print("  bronze_credit_applications      0 new rows — nothing to do")
            log_run("bronze", rows=0)
            return
        write_mode = "append"
    else:
        df_new     = df_raw
        n          = df_new.count()
        write_mode = "overwrite"

    df_out = (df_new.drop("_month")
              .withColumn("_ingest_time", F.current_timestamp())
              .withColumn("_source_file", F.lit("RAW_Credit_Applications.csv"))
              .withColumn("_batch_date",  F.lit(datetime.now().strftime("%Y-%m-%d"))))
    write_table(df_out, f"{CATALOG}.{SCHEMA}.bronze_credit_applications", mode=write_mode)

    max_month = df_new.agg(F.max("_month")).first()[0]
    set_watermark(max_month, n)
    print(f"  bronze_credit_applications      {n:>8,} rows")

    log_run("bronze", rows=n)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 4. Silver Stage

# COMMAND ----------

def run_silver():
    print("\n▓▓▓  SILVER  ▓▓▓")
    t = datetime.now()

    # ── dim_date ────────────────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_date
    USING DELTA AS
    WITH all_months AS (
        SELECT DISTINCT TRIM(CREATE_MONTH) AS YearMonth
        FROM {CATALOG}.{SCHEMA}.bronze_credit_applications
        WHERE CREATE_MONTH IS NOT NULL
        UNION
        SELECT DISTINCT TRIM(START_MONTH) AS YearMonth
        FROM {CATALOG}.{SCHEMA}.bronze_credit_applications
        WHERE START_MONTH IS NOT NULL
    ),
    with_month_number AS (
        SELECT 
            YearMonth,
            CAST(SUBSTRING(REPLACE(YearMonth, '-', ''), 1, 6) AS INT) AS DateKey,
            CAST(SUBSTRING(YearMonth, 1, 4) AS INT) AS Year,
            CAST(SUBSTRING(YearMonth, 6, 2) AS INT) AS Month
        FROM all_months
    )
    SELECT
        DateKey,
        YearMonth,
        Year,
        Month,
        DATE_FORMAT(TO_DATE(CONCAT(YearMonth, '-01'), 'yyyy-MM-dd'), 'MMMM') AS MonthName,
        CASE 
            WHEN Month IN (1, 2, 3) THEN 'Q1'
            WHEN Month IN (4, 5, 6) THEN 'Q2'
            WHEN Month IN (7, 8, 9) THEN 'Q3'
            ELSE 'Q4'
        END AS Quarter,
        CONCAT(CAST(Year AS STRING), '-',
               CASE 
                   WHEN Month IN (1, 2, 3) THEN 'Q1'
                   WHEN Month IN (4, 5, 6) THEN 'Q2'
                   WHEN Month IN (7, 8, 9) THEN 'Q3'
                   ELSE 'Q4'
               END) AS YearQuarter
    FROM with_month_number
    ORDER BY YearMonth
    """)
    n1 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_date").collect()[0][0]
    print(f"  silver_dim_date                     {n1:>4,} rows")

    # ── dim_province ──────────────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_province
    USING DELTA AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ProvinceCode) AS ProvinceKey,
        ProvinceCode,
        ProvinceName,
        Region
    FROM VALUES
        ('AB', 'Alberta', 'Prairie'),
        ('BC', 'British Columbia', 'West Coast'),
        ('ON', 'Ontario', 'Central'),
        ('QC', 'Quebec', 'Central')
    AS province_meta(ProvinceCode, ProvinceName, Region)
    """)
    n2 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_province").collect()[0][0]
    print(f"  silver_dim_province                 {n2:>4,} rows")

    # ── dim_credit_class ───────────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_credit_class
    USING DELTA AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY CreditClass) AS CreditClassKey,
        CreditClass,
        RiskLevel,
        RiskDescription,
        Offer,
        CAST(BadDebtRate1Yr AS DOUBLE) AS BadDebtRate1Yr,
        EligibleForActivation
    FROM VALUES
        ('B', 'Low Risk', 'Good credit score', '$0 phone on 2yr contract', 0.10, true),
        ('K', 'Extreme Risk', 'Very low credit score', 'Declined — do not activate', NULL, false),
        ('L', 'Medium Risk', 'Medium/no credit score', '$0 phone on 2yr contract', 0.35, true),
        ('X', 'High Risk', 'Low credit score', 'Client pays for device', 0.55, true)
    AS credit_meta(CreditClass, RiskLevel, RiskDescription, Offer, BadDebtRate1Yr, EligibleForActivation)
    """)
    n3 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_credit_class").collect()[0][0]
    print(f"  silver_dim_credit_class             {n3:>4,} rows")

    # ── dim_channel ───────────────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_channel
    USING DELTA AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ChannelType) AS ChannelKey,
        ChannelType,
        ChannelDescription
    FROM VALUES
        ('Corporate Stores', 'Verizon-owned stores'),
        ('Dealers', 'Verizon franchise stores')
    AS channel_meta(ChannelType, ChannelDescription)
    """)
    n4 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_channel").collect()[0][0]
    print(f"  silver_dim_channel                  {n4:>4,} rows")

    # ── dim_age_group ────────────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_age_group
    USING DELTA AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY AgeMin) AS AgeGroupKey,
        AgeGroup,
        AgeMin,
        AgeMax
    FROM VALUES
        ('18-30', 18, 30),
        ('31-40', 31, 40),
        ('41-50', 41, 50),
        ('50+', 51, 99)
    AS age_meta(AgeGroup, AgeMin, AgeMax)
    """)
    n5 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_age_group").collect()[0][0]
    print(f"  silver_dim_age_group                {n5:>4,} rows")

    # ── fact_applications ─────────────────────────────────────────────
    if MODE == "incremental":
        # For incremental mode, use MERGE instead of CREATE OR REPLACE
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.silver_fact_applications (
            BAN STRING,
            CreateDateKey INT,
            StartDateKey INT,
            ProvinceKey INT,
            CreditClassKey INT,
            ChannelKey INT,
            AgeGroupKey INT,
            CreateMonth STRING,
            StartMonth STRING,
            CreateWeek DATE,
            StartWeek DATE,
            LinesRequested INT,
            LinesActivated INT,
            CreditScore INT,
            CreditBureauDate DATE,
            ScoreCard STRING,
            IsActivated INT,
            EligibleForActivation BOOLEAN,
            ExpectedBadDebtAccounts DOUBLE,
            AnyFraudFlag INT,
            DuplicateFlag INT,
            MismatchFlag INT,
            SkipFlag INT,
            DaysToActivation INT
        ) USING DELTA
        """)
        
        spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.silver_fact_applications AS target
        USING (
            WITH cleaned_applications AS (
                SELECT
                    TRIM(BAN) AS BAN,
                    TRIM(CREATE_MONTH) AS CREATE_MONTH,
                    TRIM(START_MONTH) AS START_MONTH,
                    COALESCE(
                        TRY_TO_DATE(CREATE_WEEK, 'yyyy-MM-dd'),
                        TRY_TO_DATE(CREATE_WEEK, 'dd/MM/yyyy'),
                        TRY_TO_DATE(CREATE_WEEK, 'MM/dd/yyyy')
                    ) AS CREATE_WEEK,
                    COALESCE(
                        TRY_TO_DATE(START_WEEK, 'yyyy-MM-dd'),
                        TRY_TO_DATE(START_WEEK, 'dd/MM/yyyy'),
                        TRY_TO_DATE(START_WEEK, 'MM/dd/yyyy')
                    ) AS START_WEEK,
                    INITCAP(TRIM(CHANNEL_TYPE)) AS CHANNEL_TYPE,
                    UPPER(TRIM(AGE_GROUP)) AS AGE_GROUP,
                    UPPER(TRIM(ADR_PROVINCE)) AS ADR_PROVINCE,
                    INITCAP(TRIM(ACCOUNT_DESC)) AS ACCOUNT_DESC,
                    CASE 
                        WHEN LINES_REQUESTED IS NULL THEN 4
                        ELSE CAST(LINES_REQUESTED AS INT)
                    END AS LINES_REQUESTED,
                    CASE WHEN HCDP_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDP_IND,
                    CASE WHEN HCDM_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDM_IND,
                    CASE WHEN HCDS_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDS_IND
                FROM {CATALOG}.{SCHEMA}.bronze_credit_applications
                WHERE BAN IS NOT NULL
            ),
            deduped_applications AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY BAN ORDER BY CREATE_MONTH) AS rn
                FROM cleaned_applications
            ),
            cleaned_risk AS (
                SELECT
                    TRIM(BAN) AS BAN,
                    COALESCE(
                        TRY_TO_DATE(FIRST_CREDIT_DATE, 'yyyy-MM-dd'),
                        TRY_TO_DATE(FIRST_CREDIT_DATE, 'dd/MM/yyyy'),
                        TRY_TO_DATE(FIRST_CREDIT_DATE, 'MM/dd/yyyy')
                    ) AS FIRST_CREDIT_DATE,
                    UPPER(TRIM(FIRST_CREDIT_CLASS)) AS FIRST_CREDIT_CLASS,
                    UPPER(TRIM(SCORE_CARD)) AS SCORE_CARD,
                    CASE 
                        WHEN CAST(CREDIT_SCORE AS INT) < 0 OR CREDIT_SCORE IS NULL THEN NULL
                        ELSE CAST(CREDIT_SCORE AS INT)
                    END AS CREDIT_SCORE
                FROM {CATALOG}.{CATALOG}.bronze_credit_risk
                WHERE BAN IS NOT NULL
            ),
            deduped_risk AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY BAN ORDER BY FIRST_CREDIT_DATE) AS rn
                FROM cleaned_risk
            ),
            joined_data AS (
                SELECT
                    a.BAN, a.CREATE_MONTH, a.START_MONTH, a.CREATE_WEEK, a.START_WEEK,
                    a.CHANNEL_TYPE, a.AGE_GROUP, a.ADR_PROVINCE, a.ACCOUNT_DESC,
                    a.LINES_REQUESTED, a.HCDP_IND, a.HCDM_IND, a.HCDS_IND,
                    r.FIRST_CREDIT_DATE, r.FIRST_CREDIT_CLASS, r.SCORE_CARD, r.CREDIT_SCORE
                FROM deduped_applications a
                INNER JOIN deduped_risk r ON a.BAN = r.BAN
                WHERE a.rn = 1 AND r.rn = 1
            ),
            with_dimension_keys AS (
                SELECT
                    j.*,
                    d1.DateKey AS CreateDateKey,
                    d2.DateKey AS StartDateKey,
                    p.ProvinceKey,
                    cc.CreditClassKey,
                    cc.BadDebtRate1Yr,
                    cc.EligibleForActivation,
                    ch.ChannelKey,
                    ag.AgeGroupKey
                FROM joined_data j
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d1 ON j.CREATE_MONTH = d1.YearMonth
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d2 ON j.START_MONTH = d2.YearMonth
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_province p ON j.ADR_PROVINCE = p.ProvinceCode
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class cc ON j.FIRST_CREDIT_CLASS = cc.CreditClass
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_channel ch ON j.CHANNEL_TYPE = ch.ChannelType
                LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_age_group ag ON j.AGE_GROUP = ag.AgeGroup
            )
            SELECT
                BAN, CreateDateKey, StartDateKey, ProvinceKey, CreditClassKey, ChannelKey, AgeGroupKey,
                CREATE_MONTH AS CreateMonth,
                START_MONTH AS StartMonth,
                CREATE_WEEK AS CreateWeek,
                START_WEEK AS StartWeek,
                LINES_REQUESTED AS LinesRequested,
                CASE WHEN START_MONTH IS NOT NULL THEN LINES_REQUESTED ELSE 0 END AS LinesActivated,
                CREDIT_SCORE AS CreditScore,
                FIRST_CREDIT_DATE AS CreditBureauDate,
                SCORE_CARD AS ScoreCard,
                CASE WHEN START_MONTH IS NOT NULL THEN 1 ELSE 0 END AS IsActivated,
                EligibleForActivation,
                CASE WHEN START_MONTH IS NOT NULL THEN ROUND(BadDebtRate1Yr, 4) ELSE 0.0 END AS ExpectedBadDebtAccounts,
                CASE WHEN HCDP_IND = 1 OR HCDM_IND = 1 OR HCDS_IND = 1 THEN 1 ELSE 0 END AS AnyFraudFlag,
                HCDP_IND AS DuplicateFlag,
                HCDM_IND AS MismatchFlag,
                HCDS_IND AS SkipFlag,
                CASE WHEN START_MONTH IS NOT NULL THEN DATEDIFF(START_WEEK, CREATE_WEEK) ELSE NULL END AS DaysToActivation
            FROM with_dimension_keys
        ) AS source
        ON target.BAN = source.BAN
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        # Full refresh mode - use CREATE OR REPLACE
        spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_fact_applications
        USING DELTA AS
        WITH cleaned_applications AS (
            SELECT
                TRIM(BAN) AS BAN,
                TRIM(CREATE_MONTH) AS CREATE_MONTH,
                TRIM(START_MONTH) AS START_MONTH,
                COALESCE(
                    TRY_TO_DATE(CREATE_WEEK, 'yyyy-MM-dd'),
                    TRY_TO_DATE(CREATE_WEEK, 'dd/MM/yyyy'),
                    TRY_TO_DATE(CREATE_WEEK, 'MM/dd/yyyy')
                ) AS CREATE_WEEK,
                COALESCE(
                    TRY_TO_DATE(START_WEEK, 'yyyy-MM-dd'),
                    TRY_TO_DATE(START_WEEK, 'dd/MM/yyyy'),
                    TRY_TO_DATE(START_WEEK, 'MM/dd/yyyy')
                ) AS START_WEEK,
                INITCAP(TRIM(CHANNEL_TYPE)) AS CHANNEL_TYPE,
                UPPER(TRIM(AGE_GROUP)) AS AGE_GROUP,
                UPPER(TRIM(ADR_PROVINCE)) AS ADR_PROVINCE,
                INITCAP(TRIM(ACCOUNT_DESC)) AS ACCOUNT_DESC,
                CASE 
                    WHEN LINES_REQUESTED IS NULL THEN 4
                    ELSE CAST(LINES_REQUESTED AS INT)
                END AS LINES_REQUESTED,
                CASE WHEN HCDP_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDP_IND,
                CASE WHEN HCDM_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDM_IND,
                CASE WHEN HCDS_IND IS NOT NULL THEN 1 ELSE 0 END AS HCDS_IND
            FROM {CATALOG}.{SCHEMA}.bronze_credit_applications
            WHERE BAN IS NOT NULL
        ),
        deduped_applications AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY BAN ORDER BY CREATE_MONTH) AS rn
            FROM cleaned_applications
        ),
        cleaned_risk AS (
            SELECT
                TRIM(BAN) AS BAN,
                COALESCE(
                    TRY_TO_DATE(FIRST_CREDIT_DATE, 'yyyy-MM-dd'),
                    TRY_TO_DATE(FIRST_CREDIT_DATE, 'dd/MM/yyyy'),
                    TRY_TO_DATE(FIRST_CREDIT_DATE, 'MM/dd/yyyy')
                ) AS FIRST_CREDIT_DATE,
                UPPER(TRIM(FIRST_CREDIT_CLASS)) AS FIRST_CREDIT_CLASS,
                UPPER(TRIM(SCORE_CARD)) AS SCORE_CARD,
                CASE 
                    WHEN CAST(CREDIT_SCORE AS INT) < 0 OR CREDIT_SCORE IS NULL THEN NULL
                    ELSE CAST(CREDIT_SCORE AS INT)
                END AS CREDIT_SCORE
            FROM {CATALOG}.{SCHEMA}.bronze_credit_risk
            WHERE BAN IS NOT NULL
        ),
        deduped_risk AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY BAN ORDER BY FIRST_CREDIT_DATE) AS rn
            FROM cleaned_risk
        ),
        joined_data AS (
            SELECT
                a.BAN, a.CREATE_MONTH, a.START_MONTH, a.CREATE_WEEK, a.START_WEEK,
                a.CHANNEL_TYPE, a.AGE_GROUP, a.ADR_PROVINCE, a.ACCOUNT_DESC,
                a.LINES_REQUESTED, a.HCDP_IND, a.HCDM_IND, a.HCDS_IND,
                r.FIRST_CREDIT_DATE, r.FIRST_CREDIT_CLASS, r.SCORE_CARD, r.CREDIT_SCORE
            FROM deduped_applications a
            INNER JOIN deduped_risk r ON a.BAN = r.BAN
            WHERE a.rn = 1 AND r.rn = 1
        ),
        with_dimension_keys AS (
            SELECT
                j.*,
                d1.DateKey AS CreateDateKey,
                d2.DateKey AS StartDateKey,
                p.ProvinceKey,
                cc.CreditClassKey,
                cc.BadDebtRate1Yr,
                cc.EligibleForActivation,
                ch.ChannelKey,
                ag.AgeGroupKey
            FROM joined_data j
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d1 ON j.CREATE_MONTH = d1.YearMonth
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d2 ON j.START_MONTH = d2.YearMonth
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_province p ON j.ADR_PROVINCE = p.ProvinceCode
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class cc ON j.FIRST_CREDIT_CLASS = cc.CreditClass
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_channel ch ON j.CHANNEL_TYPE = ch.ChannelType
            LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_age_group ag ON j.AGE_GROUP = ag.AgeGroup
        )
        SELECT
            BAN, CreateDateKey, StartDateKey, ProvinceKey, CreditClassKey, ChannelKey, AgeGroupKey,
            CREATE_MONTH AS CreateMonth,
            START_MONTH AS StartMonth,
            CREATE_WEEK AS CreateWeek,
            START_WEEK AS StartWeek,
            LINES_REQUESTED AS LinesRequested,
            CASE WHEN START_MONTH IS NOT NULL THEN LINES_REQUESTED ELSE 0 END AS LinesActivated,
            CREDIT_SCORE AS CreditScore,
            FIRST_CREDIT_DATE AS CreditBureauDate,
            SCORE_CARD AS ScoreCard,
            CASE WHEN START_MONTH IS NOT NULL THEN 1 ELSE 0 END AS IsActivated,
            EligibleForActivation,
            CASE WHEN START_MONTH IS NOT NULL THEN ROUND(BadDebtRate1Yr, 4) ELSE 0.0 END AS ExpectedBadDebtAccounts,
            CASE WHEN HCDP_IND = 1 OR HCDM_IND = 1 OR HCDS_IND = 1 THEN 1 ELSE 0 END AS AnyFraudFlag,
            HCDP_IND AS DuplicateFlag,
            HCDM_IND AS MismatchFlag,
            HCDS_IND AS SkipFlag,
            CASE WHEN START_MONTH IS NOT NULL THEN DATEDIFF(START_WEEK, CREATE_WEEK) ELSE NULL END AS DaysToActivation
        FROM with_dimension_keys
        """)
    
    n6 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications").collect()[0][0]
    print(f"  silver_fact_applications        {n6:>8,} rows")

    log_run("silver", rows=n6)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 5. Gold Stage

# COMMAND ----------

def run_gold():
    print("\n▓▓▓  GOLD  ▓▓▓")
    t = datetime.now()

    # ── gold_monthly_activation_trend ─────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_monthly_activation_trend
    USING DELTA AS
    WITH base_agg AS (
        SELECT
            d.YearMonth, d.Year, d.Month, d.MonthName, d.Quarter, d.YearQuarter,
            dc.CreditClass, dc.RiskLevel,
            COUNT(f.BAN) AS Applications,
            SUM(f.IsActivated) AS Activations,
            SUM(f.LinesRequested) AS LinesRequested,
            SUM(f.LinesActivated) AS LinesActivated,
            SUM(f.AnyFraudFlag) AS FraudFlaggedApps
        FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
        INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_date d ON f.CreateDateKey = d.DateKey
        INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
        GROUP BY d.YearMonth, d.Year, d.Month, d.MonthName, d.Quarter, d.YearQuarter,
                 dc.CreditClass, dc.RiskLevel
    ),
    with_rates AS (
        SELECT *,
            ROUND(Activations / Applications * 100, 2) AS ActivationRatePct,
            Applications - Activations AS NotActivated,
            LAG(ROUND(Activations / Applications * 100, 2), 1) OVER (
                PARTITION BY CreditClass ORDER BY YearMonth
            ) AS PrevMonthRate
        FROM base_agg
    )
    SELECT
        YearMonth, Year, Month, MonthName, Quarter, YearQuarter,
        CreditClass, RiskLevel, Applications, Activations,
        LinesRequested, LinesActivated, FraudFlaggedApps,
        ActivationRatePct, NotActivated, PrevMonthRate,
        ROUND(ActivationRatePct - PrevMonthRate, 2) AS MoMChangePoints,
        ROUND(
            AVG(ActivationRatePct) OVER (
                PARTITION BY CreditClass ORDER BY YearMonth
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 2
        ) AS RollingAvg3MonthRate,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM with_rates
    ORDER BY YearMonth, CreditClass
    """)
    n1 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_monthly_activation_trend").collect()[0][0]
    print(f"  gold_monthly_activation_trend       {n1:>6,} rows")

    # ── gold_credit_class_performance ─────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_credit_class_performance
    USING DELTA AS
    SELECT
        dc.CreditClass, dc.RiskLevel, dc.RiskDescription, dc.Offer,
        dc.BadDebtRate1Yr, dc.EligibleForActivation,
        COUNT(f.BAN) AS TotalApplications,
        SUM(f.IsActivated) AS TotalActivations,
        ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
        ROUND(STDDEV(f.CreditScore), 1) AS StdCreditScore,
        MIN(f.CreditScore) AS MinCreditScore,
        MAX(f.CreditScore) AS MaxCreditScore,
        SUM(f.LinesRequested) AS TotalLinesRequested,
        SUM(f.LinesActivated) AS TotalLinesActivated,
        ROUND(AVG(f.LinesRequested), 2) AS AvgLinesRequested,
        SUM(f.AnyFraudFlag) AS FraudFlaggedApps,
        SUM(f.DuplicateFlag) AS DuplicateFlags,
        SUM(f.MismatchFlag) AS MismatchFlags,
        SUM(f.SkipFlag) AS SkipFlags,
        ROUND(AVG(f.DaysToActivation), 1) AS AvgDaysToActivation,
        ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
        COUNT(f.BAN) - SUM(f.IsActivated) AS NotActivated,
        ROUND(SUM(f.AnyFraudFlag) / COUNT(f.BAN) * 100, 2) AS FraudFlagRatePct,
        ROUND(SUM(f.IsActivated) * dc.BadDebtRate1Yr, 0) AS ExpectedBadDebtAccounts,
        SUM(f.LinesRequested) - SUM(f.LinesActivated) AS LinesNotActivated,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
    GROUP BY dc.CreditClass, dc.RiskLevel, dc.RiskDescription, dc.Offer,
             dc.BadDebtRate1Yr, dc.EligibleForActivation
    ORDER BY dc.CreditClass
    """)
    n2 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_credit_class_performance").collect()[0][0]
    print(f"  gold_credit_class_performance       {n2:>6,} rows")

    # ── gold_province_risk ────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_province_risk
    USING DELTA AS
    SELECT
        dp.ProvinceCode, dp.ProvinceName, dp.Region,
        dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr,
        COUNT(f.BAN) AS Applications,
        SUM(f.IsActivated) AS Activations,
        SUM(f.LinesRequested) AS LinesRequested,
        SUM(f.LinesActivated) AS LinesActivated,
        ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
        SUM(f.AnyFraudFlag) AS FraudFlaggedApps,
        ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
        ROUND(SUM(f.AnyFraudFlag) / COUNT(f.BAN) * 100, 2) AS FraudFlagRatePct,
        ROUND(SUM(f.IsActivated) * dc.BadDebtRate1Yr, 0) AS ExpectedBadDebtAccounts,
        CASE WHEN dc.CreditClass IN ('X', 'K') THEN COUNT(f.BAN) ELSE 0 END AS IsHighOrExtremeRisk,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp ON f.ProvinceKey = dp.ProvinceKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
    GROUP BY dp.ProvinceCode, dp.ProvinceName, dp.Region,
             dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr
    ORDER BY dp.ProvinceCode, dc.CreditClass
    """)
    n3 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_province_risk").collect()[0][0]
    print(f"  gold_province_risk                  {n3:>6,} rows")

    # ── gold_bad_debt_exposure ────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_bad_debt_exposure
    USING DELTA AS
    SELECT
        d.YearMonth, d.Year, d.Quarter, d.YearQuarter,
        dp.ProvinceCode, dp.ProvinceName,
        dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr,
        dch.ChannelType,
        COUNT(f.BAN) AS ActivatedAccounts,
        SUM(f.LinesActivated) AS LinesActivated,
        ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
        ROUND(COUNT(f.BAN) * dc.BadDebtRate1Yr, 1) AS ExpectedBadDebtAccounts,
        COUNT(f.BAN) - ROUND(COUNT(f.BAN) * dc.BadDebtRate1Yr, 1) AS GoodDebtAccounts,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_date d ON f.CreateDateKey = d.DateKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp ON f.ProvinceKey = dp.ProvinceKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch ON f.ChannelKey = dch.ChannelKey
    WHERE f.IsActivated = 1
    GROUP BY d.YearMonth, d.Year, d.Quarter, d.YearQuarter,
             dp.ProvinceCode, dp.ProvinceName,
             dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr,
             dch.ChannelType
    ORDER BY d.YearMonth, dp.ProvinceCode, dc.CreditClass
    """)
    n4 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_bad_debt_exposure").collect()[0][0]
    print(f"  gold_bad_debt_exposure              {n4:>6,} rows")

    # ── gold_channel_age_analysis ─────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_channel_age_analysis
    USING DELTA AS
    SELECT
        dch.ChannelType, da.AgeGroup,
        dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr,
        dp.ProvinceCode,
        COUNT(f.BAN) AS Applications,
        SUM(f.IsActivated) AS Activations,
        SUM(f.LinesRequested) AS LinesRequested,
        SUM(f.LinesActivated) AS LinesActivated,
        ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
        SUM(f.AnyFraudFlag) AS FraudFlaggedApps,
        ROUND(AVG(f.DaysToActivation), 1) AS AvgDaysToActivation,
        ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
        ROUND(SUM(f.AnyFraudFlag) / COUNT(f.BAN) * 100, 2) AS FraudFlagRatePct,
        ROUND(SUM(f.IsActivated) * dc.BadDebtRate1Yr, 1) AS ExpectedBadDebtAccounts,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch ON f.ChannelKey = dch.ChannelKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_age_group da ON f.AgeGroupKey = da.AgeGroupKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp ON f.ProvinceKey = dp.ProvinceKey
    GROUP BY dch.ChannelType, da.AgeGroup, dc.CreditClass, dc.RiskLevel,
             dc.BadDebtRate1Yr, dp.ProvinceCode
    ORDER BY dch.ChannelType, da.AgeGroup, dc.CreditClass
    """)
    n5 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_channel_age_analysis").collect()[0][0]
    print(f"  gold_channel_age_analysis           {n5:>6,} rows")

    # ── gold_fraud_flags ──────────────────────────────────────────
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_fraud_flags
    USING DELTA AS
    SELECT
        dc.CreditClass, dc.RiskLevel,
        dp.ProvinceCode, dch.ChannelType,
        f.DuplicateFlag, f.MismatchFlag, f.SkipFlag, f.AnyFraudFlag,
        COUNT(f.BAN) AS Applications,
        SUM(f.IsActivated) AS Activations,
        ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
        SUM(f.LinesRequested) AS LinesRequested,
        SUM(f.LinesActivated) AS LinesActivated,
        ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
        CURRENT_TIMESTAMP() AS _gold_ts
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc ON f.CreditClassKey = dc.CreditClassKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp ON f.ProvinceKey = dp.ProvinceKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch ON f.ChannelKey = dch.ChannelKey
    GROUP BY dc.CreditClass, dc.RiskLevel, dp.ProvinceCode, dch.ChannelType,
             f.DuplicateFlag, f.MismatchFlag, f.SkipFlag, f.AnyFraudFlag
    ORDER BY dc.CreditClass, dp.ProvinceCode, f.AnyFraudFlag
    """)
    n6 = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_fraud_flags").collect()[0][0]
    print(f"  gold_fraud_flags                    {n6:>6,} rows")

    log_run("gold", rows=n1+n2+n3+n4+n5+n6)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 6. Run the pipeline

# COMMAND ----------

t0 = datetime.now()
print(f"""
╔══════════════════════════════════════════════╗
║  Credit Risk Pipeline                        ║
║  Run ID : {RUN_ID}             ║
║  Mode   : {MODE:<12}                    ║
║  Started: {t0.strftime('%Y-%m-%d %H:%M:%S')}               ║
╚══════════════════════════════════════════════╝""")

try:
    run_bronze()
except Exception as e:
    log_run("bronze", status="FAILED", error=str(e))
    raise

try:
    run_silver()
except Exception as e:
    log_run("silver", status="FAILED", error=str(e))
    raise

try:
    run_gold()
except Exception as e:
    log_run("gold", status="FAILED", error=str(e))
    raise

print(f"\n✅ Pipeline complete in {(datetime.now()-t0).seconds}s")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, run_mode, stage, rows_out, run_status,
# MAGIC        CAST(started_at AS STRING) AS started_at
# MAGIC FROM workspace.Business_Credit_sql.pipeline_run_log
# MAGIC ORDER BY started_at DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, last_month, rows_processed,
# MAGIC        run_status, CAST(updated_at AS STRING) AS updated_at
# MAGIC FROM workspace.Business_Credit_sql.pipeline_watermarks
# MAGIC ORDER BY updated_at DESC
