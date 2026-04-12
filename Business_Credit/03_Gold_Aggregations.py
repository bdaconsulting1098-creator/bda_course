# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Gold Layer: Credit Risk Analysis
# MAGIC
# MAGIC Each Gold table directly answers one or more project questions:
# MAGIC
# MAGIC | Gold Table                    | Project Question |
# MAGIC |-------------------------------|-----------------|
# MAGIC | `gold_monthly_activation_trend` | Q1: Monthly activation rate trend |
# MAGIC | `gold_credit_class_performance` | Q2: Which credit class has lower activation rate? |
# MAGIC | `gold_province_risk`            | Q3: Which province is most risk-prone? |
# MAGIC | `gold_bad_debt_exposure`        | Q4/Q5: Bad debt expected, revenue opportunity |
# MAGIC | `gold_channel_age_analysis`     | Q4/Q5: Channel & age group insights |
# MAGIC | `gold_fraud_flags`              | Q4: Fraud indicator analysis |

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "Business_Credit_sql"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Load Silver tables — reused across all Gold builds
fact    = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_fact_applications")
d_date  = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_dim_date")
d_prov  = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_dim_province")
d_class = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_dim_credit_class")
d_chan  = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_dim_channel")
d_age   = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_dim_age_group")

def write_gold(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_{table_name}"))
    n = df.count()
    print(f"  gold_{table_name:35s}  {n:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. gold_monthly_activation_trend
# MAGIC **Answers Q1**: What is the overall monthly activation rate trend?
# MAGIC
# MAGIC Activation Rate = Total Activations / Total Applications
# MAGIC Includes month-over-month change and rolling 3-month average.

# COMMAND ----------

# Build gold_monthly_activation_trend using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_monthly_activation_trend
USING DELTA AS
WITH base_agg AS (
    SELECT
        d.YearMonth,
        d.Year,
        d.Month,
        d.MonthName,
        d.Quarter,
        d.YearQuarter,
        dc.CreditClass,
        dc.RiskLevel,
        COUNT(f.BAN) AS Applications,
        SUM(f.IsActivated) AS Activations,
        SUM(f.LinesRequested) AS LinesRequested,
        SUM(f.LinesActivated) AS LinesActivated,
        SUM(f.AnyFraudFlag) AS FraudFlaggedApps
    FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_date d
        ON f.CreateDateKey = d.DateKey
    INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
        ON f.CreditClassKey = dc.CreditClassKey
    GROUP BY d.YearMonth, d.Year, d.Month, d.MonthName, d.Quarter, d.YearQuarter,
             dc.CreditClass, dc.RiskLevel
),
with_rates AS (
    SELECT
        *,
        ROUND(Activations / Applications * 100, 2) AS ActivationRatePct,
        Applications - Activations AS NotActivated,
        LAG(ROUND(Activations / Applications * 100, 2), 1) OVER (
            PARTITION BY CreditClass ORDER BY YearMonth
        ) AS PrevMonthRate
    FROM base_agg
)
SELECT
    YearMonth,
    Year,
    Month,
    MonthName,
    Quarter,
    YearQuarter,
    CreditClass,
    RiskLevel,
    Applications,
    Activations,
    LinesRequested,
    LinesActivated,
    FraudFlaggedApps,
    ActivationRatePct,
    NotActivated,
    PrevMonthRate,
    ROUND(ActivationRatePct - PrevMonthRate, 2) AS MoMChangePoints,
    ROUND(
        AVG(ActivationRatePct) OVER (
            PARTITION BY CreditClass 
            ORDER BY YearMonth 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS RollingAvg3MonthRate,
    CURRENT_TIMESTAMP() AS _gold_ts
FROM with_rates
ORDER BY YearMonth, CreditClass
""")

print("Monthly activation trend (sample):")
spark.sql(f"""
    SELECT YearMonth, CreditClass, Applications, Activations, ActivationRatePct, MoMChangePoints
    FROM {CATALOG}.{SCHEMA}.gold_monthly_activation_trend
    ORDER BY YearMonth, CreditClass
    LIMIT 16
""").show(truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_monthly_activation_trend").collect()[0][0]
print(f"  gold_monthly_activation_trend        {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. gold_credit_class_performance
# MAGIC **Answers Q2**: Which credit class has lower activation rate and why?
# MAGIC
# MAGIC Compares all 4 credit classes across activation, score distribution,
# MAGIC fraud flags, lines requested, and bad debt exposure.

# COMMAND ----------

# Build gold_credit_class_performance using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_credit_class_performance
USING DELTA AS
SELECT
    dc.CreditClass,
    dc.RiskLevel,
    dc.RiskDescription,
    dc.Offer,
    dc.BadDebtRate1Yr,
    dc.EligibleForActivation,
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
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
    ON f.CreditClassKey = dc.CreditClassKey
GROUP BY dc.CreditClass, dc.RiskLevel, dc.RiskDescription, dc.Offer, 
         dc.BadDebtRate1Yr, dc.EligibleForActivation
ORDER BY dc.CreditClass
""")

print("Credit class performance:")
spark.sql(f"""
    SELECT CreditClass, RiskLevel, TotalApplications, TotalActivations,
           ActivationRatePct, AvgCreditScore, FraudFlagRatePct, ExpectedBadDebtAccounts
    FROM {CATALOG}.{SCHEMA}.gold_credit_class_performance
    ORDER BY CreditClass
""").show(truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_credit_class_performance").collect()[0][0]
print(f"  gold_credit_class_performance        {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. gold_province_risk
# MAGIC **Answers Q3**: Which province is most risk-prone?
# MAGIC
# MAGIC Risk measured by: credit class distribution, activation rate,
# MAGIC bad debt exposure, and fraud flag rate per province.

# COMMAND ----------

# Build gold_province_risk using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_province_risk
USING DELTA AS
SELECT
    dp.ProvinceCode,
    dp.ProvinceName,
    dp.Region,
    dc.CreditClass,
    dc.RiskLevel,
    dc.BadDebtRate1Yr,
    COUNT(f.BAN) AS Applications,
    SUM(f.IsActivated) AS Activations,
    SUM(f.LinesRequested) AS LinesRequested,
    SUM(f.LinesActivated) AS LinesActivated,
    ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
    SUM(f.AnyFraudFlag) AS FraudFlaggedApps,
    ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
    ROUND(SUM(f.AnyFraudFlag) / COUNT(f.BAN) * 100, 2) AS FraudFlagRatePct,
    ROUND(SUM(f.IsActivated) * dc.BadDebtRate1Yr, 0) AS ExpectedBadDebtAccounts,
    CASE 
        WHEN dc.CreditClass IN ('X', 'K') THEN COUNT(f.BAN)
        ELSE 0
    END AS IsHighOrExtremeRisk,
    CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp
    ON f.ProvinceKey = dp.ProvinceKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
    ON f.CreditClassKey = dc.CreditClassKey
GROUP BY dp.ProvinceCode, dp.ProvinceName, dp.Region,
         dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr
ORDER BY dp.ProvinceCode, dc.CreditClass
""")

print("Province risk (sample):")
spark.sql(f"""
    SELECT ProvinceCode, CreditClass, Applications, ActivationRatePct,
           AvgCreditScore, FraudFlagRatePct, ExpectedBadDebtAccounts
    FROM {CATALOG}.{SCHEMA}.gold_province_risk
    ORDER BY ProvinceCode, CreditClass
    LIMIT 16
""").show(truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_province_risk").collect()[0][0]
print(f"  gold_province_risk                   {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. gold_bad_debt_exposure
# MAGIC **Answers Q4/Q5**: Expected bad debt by month, class, and province.
# MAGIC Shows where delinquency risk is highest and incremental revenue opportunity.

# COMMAND ----------

# Build gold_bad_debt_exposure using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_bad_debt_exposure
USING DELTA AS
SELECT
    d.YearMonth,
    d.Year,
    d.Quarter,
    d.YearQuarter,
    dp.ProvinceCode,
    dp.ProvinceName,
    dc.CreditClass,
    dc.RiskLevel,
    dc.BadDebtRate1Yr,
    dch.ChannelType,
    COUNT(f.BAN) AS ActivatedAccounts,
    SUM(f.LinesActivated) AS LinesActivated,
    ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
    ROUND(COUNT(f.BAN) * dc.BadDebtRate1Yr, 1) AS ExpectedBadDebtAccounts,
    COUNT(f.BAN) - ROUND(COUNT(f.BAN) * dc.BadDebtRate1Yr, 1) AS GoodDebtAccounts,
    CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_date d
    ON f.CreateDateKey = d.DateKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp
    ON f.ProvinceKey = dp.ProvinceKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
    ON f.CreditClassKey = dc.CreditClassKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch
    ON f.ChannelKey = dch.ChannelKey
WHERE f.IsActivated = 1
GROUP BY d.YearMonth, d.Year, d.Quarter, d.YearQuarter,
         dp.ProvinceCode, dp.ProvinceName,
         dc.CreditClass, dc.RiskLevel, dc.BadDebtRate1Yr,
         dch.ChannelType
ORDER BY d.YearMonth, dp.ProvinceCode, dc.CreditClass
""")

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_bad_debt_exposure").collect()[0][0]
print(f"  gold_bad_debt_exposure               {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. gold_channel_age_analysis
# MAGIC **Answers Q4/Q5**: Channel and age group insights — where are risk patterns different?

# COMMAND ----------

# Build gold_channel_age_analysis using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_channel_age_analysis
USING DELTA AS
SELECT
    dch.ChannelType,
    da.AgeGroup,
    dc.CreditClass,
    dc.RiskLevel,
    dc.BadDebtRate1Yr,
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
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
    ON f.CreditClassKey = dc.CreditClassKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch
    ON f.ChannelKey = dch.ChannelKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_age_group da
    ON f.AgeGroupKey = da.AgeGroupKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp
    ON f.ProvinceKey = dp.ProvinceKey
GROUP BY dch.ChannelType, da.AgeGroup, dc.CreditClass, dc.RiskLevel,
         dc.BadDebtRate1Yr, dp.ProvinceCode
ORDER BY dch.ChannelType, da.AgeGroup, dc.CreditClass
""")

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_channel_age_analysis").collect()[0][0]
print(f"  gold_channel_age_analysis            {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. gold_fraud_flags
# MAGIC **Answers Q4**: Fraud indicator (HCDP / HCDM / HCDS) impact on risk.

# COMMAND ----------

# Build gold_fraud_flags using SQL
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_fraud_flags
USING DELTA AS
SELECT
    dc.CreditClass,
    dc.RiskLevel,
    dp.ProvinceCode,
    dch.ChannelType,
    f.DuplicateFlag,
    f.MismatchFlag,
    f.SkipFlag,
    f.AnyFraudFlag,
    COUNT(f.BAN) AS Applications,
    SUM(f.IsActivated) AS Activations,
    ROUND(AVG(f.CreditScore), 1) AS AvgCreditScore,
    SUM(f.LinesRequested) AS LinesRequested,
    SUM(f.LinesActivated) AS LinesActivated,
    ROUND(SUM(f.IsActivated) / COUNT(f.BAN) * 100, 2) AS ActivationRatePct,
    CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_applications f
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_credit_class dc
    ON f.CreditClassKey = dc.CreditClassKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_province dp
    ON f.ProvinceKey = dp.ProvinceKey
INNER JOIN {CATALOG}.{SCHEMA}.silver_dim_channel dch
    ON f.ChannelKey = dch.ChannelKey
GROUP BY dc.CreditClass, dc.RiskLevel, dp.ProvinceCode, dch.ChannelType,
         f.DuplicateFlag, f.MismatchFlag, f.SkipFlag, f.AnyFraudFlag
ORDER BY dc.CreditClass, dp.ProvinceCode, f.AnyFraudFlag
""")

row_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.gold_fraud_flags").collect()[0][0]
print(f"  gold_fraud_flags                     {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Gold summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'monthly_activation_trend'  AS gold_table, COUNT(*) AS rows FROM gold_monthly_activation_trend
# MAGIC UNION ALL SELECT 'credit_class_performance', COUNT(*) FROM gold_credit_class_performance
# MAGIC UNION ALL SELECT 'province_risk',            COUNT(*) FROM gold_province_risk
# MAGIC UNION ALL SELECT 'bad_debt_exposure',        COUNT(*) FROM gold_bad_debt_exposure
# MAGIC UNION ALL SELECT 'channel_age_analysis',     COUNT(*) FROM gold_channel_age_analysis
# MAGIC UNION ALL SELECT 'fraud_flags',              COUNT(*) FROM gold_fraud_flags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: Monthly activation rate trend (all classes combined)
# MAGIC SELECT
# MAGIC     YearMonth,
# MAGIC     SUM(Applications)   AS Applications,
# MAGIC     SUM(Activations)    AS Activations,
# MAGIC     ROUND(SUM(Activations)/SUM(Applications)*100, 2) AS OverallActivationRatePct
# MAGIC FROM gold_monthly_activation_trend
# MAGIC GROUP BY YearMonth
# MAGIC ORDER BY YearMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: Credit class with lowest activation rate
# MAGIC SELECT
# MAGIC     CreditClass, RiskLevel, TotalApplications, TotalActivations,
# MAGIC     ActivationRatePct, AvgCreditScore, FraudFlagRatePct,
# MAGIC     ExpectedBadDebtAccounts, LinesNotActivated
# MAGIC FROM gold_credit_class_performance
# MAGIC ORDER BY ActivationRatePct ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3: Province risk ranking — % of High+Extreme Risk applications
# MAGIC SELECT
# MAGIC     ProvinceCode, ProvinceName,
# MAGIC     SUM(Applications)             AS TotalApplications,
# MAGIC     SUM(Activations)              AS TotalActivations,
# MAGIC     ROUND(SUM(Activations)/SUM(Applications)*100, 2) AS ActivationRatePct,
# MAGIC     ROUND(AVG(AvgCreditScore), 0) AS AvgCreditScore,
# MAGIC     ROUND(AVG(FraudFlagRatePct),2) AS AvgFraudFlagRatePct,
# MAGIC     SUM(ExpectedBadDebtAccounts)  AS TotalExpectedBadDebt,
# MAGIC     SUM(IsHighOrExtremeRisk)      AS HighOrExtremeRiskApps,
# MAGIC     ROUND(SUM(IsHighOrExtremeRisk)/SUM(Applications)*100, 1) AS HighRiskPct
# MAGIC FROM gold_province_risk
# MAGIC GROUP BY ProvinceCode, ProvinceName
# MAGIC ORDER BY HighRiskPct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4: Fraud flag impact — do flagged apps activate at lower rates?
# MAGIC SELECT
# MAGIC     AnyFraudFlag,
# MAGIC     SUM(Applications)  AS Applications,
# MAGIC     SUM(Activations)   AS Activations,
# MAGIC     ROUND(SUM(Activations)/SUM(Applications)*100,2) AS ActivationRatePct,
# MAGIC     ROUND(AVG(AvgCreditScore),0) AS AvgCreditScore
# MAGIC FROM gold_fraud_flags
# MAGIC GROUP BY AnyFraudFlag
# MAGIC ORDER BY AnyFraudFlag

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Complete
# MAGIC
# MAGIC | Table | Answers |
# MAGIC |-------|---------|
# MAGIC | `gold_monthly_activation_trend`  | Q1: Monthly activation rate + MoM trend |
# MAGIC | `gold_credit_class_performance`  | Q2: Credit class activation rates + bad debt |
# MAGIC | `gold_province_risk`             | Q3: Province risk ranking |
# MAGIC | `gold_bad_debt_exposure`         | Q4/Q5: Bad debt exposure + revenue opportunity |
# MAGIC | `gold_channel_age_analysis`      | Q4/Q5: Channel and age group risk patterns |
# MAGIC | `gold_fraud_flags`               | Q4: Fraud indicator impact |
