# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Silver Layer: Cleaning + Data Modeling
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Clean both Bronze tables
# MAGIC 2. Generate Surrogate Keys
# MAGIC 3. Join CREDIT_APPLICATIONS → CREDIT_RISK on BAN
# MAGIC 4. Build star schema dimensions + fact table
# MAGIC
# MAGIC **Star Schema:**
# MAGIC ```
# MAGIC dim_date          (DateKey)         ─┐
# MAGIC dim_province      (ProvinceKey)      ─┤
# MAGIC dim_credit_class  (CreditClassKey)   ─┼──► fact_applications
# MAGIC dim_channel       (ChannelKey)       ─┤
# MAGIC dim_age_group     (AgeGroupKey)      ─┘
# MAGIC ```
# MAGIC
# MAGIC **Bad Debt Rates (from project brief):**
# MAGIC ```
# MAGIC B (Low Risk)     → 10% bad debt rate
# MAGIC L (Medium Risk)  → 35% bad debt rate
# MAGIC X (High Risk)    → 55% bad debt rate
# MAGIC K (Extreme Risk) → Declined — not activated
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "Business_Credit_sql"

# Create schema under catalog workspace
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Catalog     : {CATALOG}")
print(f"Schema      : {SCHEMA}")


from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Bad debt rates from project brief — encoded as a lookup
BAD_DEBT_RATES = {"B": 0.10, "L": 0.35, "X": 0.55, "K": None}

def write_silver(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_{table_name}"))
    n = df.count()
    print(f"  silver_{table_name:30s}  {n:>8,} rows")

# ── Shared date parser: handles 3 formats ────────────────────────
# "2009-01-03"  →  standard
# "03/01/2009"  →  DD/MM/YYYY
# "2009/01/03"  →  YYYY/MM/DD
def parse_mixed_date(col_name):
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "yyyy/MM/dd"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. dim_date
# MAGIC **Natural key**: `YYYY-MM` month string (e.g. "2009-01")
# MAGIC **Key**: `DateKey` = YYYYMM integer → 200901
# MAGIC
# MAGIC Covers all 24 months in the dataset (Jan 2009 – Dec 2010).

# COMMAND ----------

# Build date dimension from all distinct CREATE_MONTHs in source using SQL
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
        CAST(SUBSTRING(YearMonth, 6, 2) AS INT) AS Month
    FROM all_months
)
SELECT 
    CAST(REPLACE(YearMonth, '-', '') AS INT) AS DateKey,
    YearMonth,
    CAST(SUBSTRING(YearMonth, 1, 4) AS INT) AS Year,
    Month,
    DATE_FORMAT(TO_DATE(CONCAT(YearMonth, '-01'), 'yyyy-MM-dd'), 'MMMM') AS MonthName,
    CASE 
        WHEN Month IN (1,2,3) THEN 'Q1'
        WHEN Month IN (4,5,6) THEN 'Q2'
        WHEN Month IN (7,8,9) THEN 'Q3'
        ELSE 'Q4'
    END AS Quarter,
    CONCAT(
        CAST(SUBSTRING(YearMonth, 1, 4) AS STRING), 
        '-', 
        CASE 
            WHEN Month IN (1,2,3) THEN 'Q1'
            WHEN Month IN (4,5,6) THEN 'Q2'
            WHEN Month IN (7,8,9) THEN 'Q3'
            ELSE 'Q4'
        END
    ) AS YearQuarter
FROM with_month_number
ORDER BY DateKey
""")

print("dim_date:")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_date").show(truncate=False)

# Print row count
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_date").collect()[0][0]
print(f"  silver_dim_date                          {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. dim_province
# MAGIC **Natural key**: `ADR_PROVINCE` (AB / BC / ON / QC)
# MAGIC **Key**: `ProvinceKey` = row_number ordered by Province

# COMMAND ----------

# Build dim_province using SQL
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

print("dim_province:")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_province").show(truncate=False)

# Print row count
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_province").collect()[0][0]
print(f"  silver_dim_province                      {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. dim_credit_class
# MAGIC **Natural key**: `FIRST_CREDIT_CLASS` (B / L / X / K)
# MAGIC **Key**: `CreditClassKey` = row_number ordered by CreditClass
# MAGIC
# MAGIC Includes bad debt rates and risk metadata from project brief.

# COMMAND ----------

# Build dim_credit_class using SQL
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

print("dim_credit_class:")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_credit_class").show(truncate=False)

# Print row count
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_credit_class").collect()[0][0]
print(f"  silver_dim_credit_class                  {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. dim_channel
# MAGIC **Natural key**: `CHANNEL_TYPE` (Dealers / Corporate Stores)
# MAGIC **Key**: `ChannelKey` = row_number ordered by ChannelType

# COMMAND ----------

# Build dim_channel using SQL
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

print("dim_channel:")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_channel").show(truncate=False)

# Print row count
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_channel").collect()[0][0]
print(f"  silver_dim_channel                       {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. dim_age_group
# MAGIC **Natural key**: `AGE_GROUP` (18-30 / 31-40 / 41-50 / 50+)
# MAGIC **Key**: `AgeGroupKey` = row_number ordered by AgeGroup

# COMMAND ----------

# Build dim_age_group using SQL
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

print("dim_age_group:")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_age_group").show(truncate=False)

# Print row count
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_dim_age_group").collect()[0][0]
print(f"  silver_dim_age_group                     {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. fact_applications
# MAGIC
# MAGIC Core modeling step:
# MAGIC 1. Clean bronze_credit_applications (casing, dates, nulls, dedup)
# MAGIC 2. Clean bronze_credit_risk (casing, dates, invalid scores, dedup)
# MAGIC 3. Inner join on BAN
# MAGIC 4. Join all 5 dimensions → surrogate keys
# MAGIC 5. Derive calculated fields: IsActivated, BadDebtExpected, LinesActivated, etc.
# MAGIC
# MAGIC **Join chain:**
# MAGIC ```
# MAGIC cleaned_applications
# MAGIC   INNER JOIN  cleaned_risk        on  BAN
# MAGIC   LEFT JOIN   dim_date            on  CREATE_MONTH  → CreateDateKey
# MAGIC   LEFT JOIN   dim_date            on  START_MONTH   → StartDateKey
# MAGIC   LEFT JOIN   dim_province        on  ADR_PROVINCE  → ProvinceKey
# MAGIC   LEFT JOIN   dim_credit_class    on  FIRST_CREDIT_CLASS → CreditClassKey
# MAGIC   LEFT JOIN   dim_channel         on  CHANNEL_TYPE  → ChannelKey
# MAGIC   LEFT JOIN   dim_age_group       on  AGE_GROUP     → AgeGroupKey
# MAGIC ```

# COMMAND ----------

# Build fact_applications using SQL with all cleaning, joins, and derived metrics
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_fact_applications
USING DELTA AS
WITH cleaned_applications AS (
    SELECT
        TRIM(BAN) AS BAN,
        TRIM(CREATE_MONTH) AS CREATE_MONTH,
        TRIM(START_MONTH) AS START_MONTH,
        -- Parse 3-format week dates using COALESCE with TRY_TO_DATE
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
        -- Cast numeric - fill NULL LINES_REQUESTED with median=4
        CASE 
            WHEN `LINES_REQUESTED` IS NULL THEN 4
            ELSE CAST(`LINES_REQUESTED` AS INT)
        END AS LINES_REQUESTED,
        -- Standardise HCD indicators (flag or null)
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
        -- Parse mixed date format with TRY_TO_DATE
        COALESCE(
            TRY_TO_DATE(FIRST_CREDIT_DATE, 'yyyy-MM-dd'),
            TRY_TO_DATE(FIRST_CREDIT_DATE, 'dd/MM/yyyy'),
            TRY_TO_DATE(FIRST_CREDIT_DATE, 'MM/dd/yyyy')
        ) AS FIRST_CREDIT_DATE,
        -- Standardise class: trim + upper
        UPPER(TRIM(FIRST_CREDIT_CLASS)) AS FIRST_CREDIT_CLASS,
        UPPER(TRIM(SCORE_CARD)) AS SCORE_CARD,
        -- Cast score - treat -1 and NULL as invalid
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
        a.BAN,
        a.CREATE_MONTH,
        a.START_MONTH,
        a.CREATE_WEEK,
        a.START_WEEK,
        a.CHANNEL_TYPE,
        a.AGE_GROUP,
        a.ADR_PROVINCE,
        a.ACCOUNT_DESC,
        a.LINES_REQUESTED,
        a.HCDP_IND,
        a.HCDM_IND,
        a.HCDS_IND,
        r.FIRST_CREDIT_DATE,
        r.FIRST_CREDIT_CLASS,
        r.SCORE_CARD,
        r.CREDIT_SCORE
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
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d1 
        ON j.CREATE_MONTH = d1.YearMonth
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_date d2 
        ON j.START_MONTH = d2.YearMonth
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_province p 
        ON j.ADR_PROVINCE = p.ProvinceCode
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class cc 
        ON j.FIRST_CREDIT_CLASS = cc.CreditClass
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_channel ch 
        ON j.CHANNEL_TYPE = ch.ChannelType
    LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_age_group ag 
        ON j.AGE_GROUP = ag.AgeGroup
)
SELECT
    BAN,
    CreateDateKey,
    StartDateKey,
    ProvinceKey,
    CreditClassKey,
    ChannelKey,
    AgeGroupKey,
    CREATE_MONTH AS CreateMonth,
    START_MONTH AS StartMonth,
    CREATE_WEEK AS CreateWeek,
    START_WEEK AS StartWeek,
    LINES_REQUESTED AS LinesRequested,
    -- Lines activated (only if activated, else 0)
    CASE WHEN START_MONTH IS NOT NULL THEN LINES_REQUESTED ELSE 0 END AS LinesActivated,
    CREDIT_SCORE AS CreditScore,
    FIRST_CREDIT_DATE AS CreditBureauDate,
    SCORE_CARD AS ScoreCard,
    -- IsActivated: START_MONTH populated = phone was activated
    CASE WHEN START_MONTH IS NOT NULL THEN 1 ELSE 0 END AS IsActivated,
    EligibleForActivation,
    -- Expected bad debt accounts in year 1
    CASE 
        WHEN START_MONTH IS NOT NULL THEN ROUND(BadDebtRate1Yr, 4)
        ELSE 0.0
    END AS ExpectedBadDebtAccounts,
    -- Fraud risk flag: any HCD indicator triggered
    CASE 
        WHEN HCDP_IND = 1 OR HCDM_IND = 1 OR HCDS_IND = 1 THEN 1
        ELSE 0
    END AS AnyFraudFlag,
    HCDP_IND AS DuplicateFlag,
    HCDM_IND AS MismatchFlag,
    HCDS_IND AS SkipFlag,
    -- Days from create to activation (NULL if not activated)
    CASE 
        WHEN START_MONTH IS NOT NULL THEN DATEDIFF(START_WEEK, CREATE_WEEK)
        ELSE NULL
    END AS DaysToActivation
FROM with_dimension_keys
""")

# Join quality check
print("\n=== Join Quality Check ===")
total = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications").collect()[0][0]
null_create = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE CreateDateKey IS NULL").collect()[0][0]
null_province = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE ProvinceKey IS NULL").collect()[0][0]
null_credit = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE CreditClassKey IS NULL").collect()[0][0]
null_channel = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE ChannelKey IS NULL").collect()[0][0]
null_age = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE AgeGroupKey IS NULL").collect()[0][0]
activated = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE IsActivated = 1").collect()[0][0]
not_activated = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE IsActivated = 0").collect()[0][0]
fraud_flag = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.silver_fact_applications WHERE AnyFraudFlag = 1").collect()[0][0]

print(f"Total rows              : {total:,}")
print(f"NULL CreateDateKey      : {null_create:,}")
print(f"NULL ProvinceKey        : {null_province:,}")
print(f"NULL CreditClassKey     : {null_credit:,}")
print(f"NULL ChannelKey         : {null_channel:,}")
print(f"NULL AgeGroupKey        : {null_age:,}")
print(f"Activated               : {activated:,}")
print(f"Not activated           : {not_activated:,}")
print(f"Any fraud flag          : {fraud_flag:,}")

# Print row count
print(f"\n  silver_fact_applications             {total:>8,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Star Schema row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_date'         AS table_name, COUNT(*) AS rows FROM silver_dim_date
# MAGIC UNION ALL SELECT 'dim_province',         COUNT(*) FROM silver_dim_province
# MAGIC UNION ALL SELECT 'dim_credit_class',     COUNT(*) FROM silver_dim_credit_class
# MAGIC UNION ALL SELECT 'dim_channel',          COUNT(*) FROM silver_dim_channel
# MAGIC UNION ALL SELECT 'dim_age_group',        COUNT(*) FROM silver_dim_age_group
# MAGIC UNION ALL SELECT 'fact_applications',    COUNT(*) FROM silver_fact_applications

# COMMAND ----------

# MAGIC %md ## 8. Smoke test — star schema join

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.YearMonth,
# MAGIC     cc.CreditClass,
# MAGIC     cc.RiskLevel,
# MAGIC     p.ProvinceCode,
# MAGIC     COUNT(f.BAN)                              AS Applications,
# MAGIC     SUM(f.IsActivated)                        AS Activations,
# MAGIC     ROUND(SUM(f.IsActivated)/COUNT(f.BAN)*100,1) AS ActivationRatePct,
# MAGIC     SUM(f.LinesActivated)                     AS LinesActivated,
# MAGIC     ROUND(AVG(f.CreditScore),0)               AS AvgCreditScore
# MAGIC FROM silver_fact_applications   f
# MAGIC JOIN silver_dim_date            d  ON f.CreateDateKey  = d.DateKey
# MAGIC JOIN silver_dim_credit_class    cc ON f.CreditClassKey = cc.CreditClassKey
# MAGIC JOIN silver_dim_province        p  ON f.ProvinceKey    = p.ProvinceKey
# MAGIC GROUP BY d.YearMonth, cc.CreditClass, cc.RiskLevel, p.ProvinceCode
# MAGIC ORDER BY d.YearMonth, cc.CreditClass, p.ProvinceCode
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Complete
# MAGIC ```
# MAGIC dim_date          CreateDateKey  ─┐
# MAGIC dim_province      ProvinceKey    ─┤
# MAGIC dim_credit_class  CreditClassKey ─┼──► fact_applications  (73,661 rows)
# MAGIC dim_channel       ChannelKey     ─┤
# MAGIC dim_age_group     AgeGroupKey    ─┘
# MAGIC ```
# MAGIC Proceed to **Notebook 3 — Gold**.
