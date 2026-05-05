# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Silver Layer: Cleaning + Data Modeling
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Clean bronze_fleet_units — casing, invalid values, dedup
# MAGIC 2. Clean bronze_fleet_usage — invalid downtime, nulls, dedup
# MAGIC 3. Build 5 dimensions + 1 fact table
# MAGIC
# MAGIC **Star Schema:**
# MAGIC ```
# MAGIC dim_division      (DivisionKey)     ─┐
# MAGIC dim_vehicle_type  (VehicleTypeKey)  ─┤
# MAGIC dim_status        (StatusKey)       ─┼──► fact_fleet
# MAGIC dim_fuel          (FuelKey)         ─┤
# MAGIC dim_location      (LocationKey)     ─┘
# MAGIC ```
# MAGIC
# MAGIC **Key derived KPI fields added in fact:**
# MAGIC - `IsOverdue`        — vehicle age > expected life
# MAGIC - `RemainingLifeYrs` — expected life − current age
# MAGIC - `UtilizationPct`   — YTD KM / Expected KM × 100
# MAGIC - `AvailabilityPct`  — (Available − Downtime) / Available × 100
# MAGIC - `InReplacementProgram` — status = Replacement Program

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

# DBTITLE 1,Cell 3
CATALOG = "workspace"
SCHEMA = "fleet_service_sql"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

VALID_UNIT_TYPES = ["LIGHT DUTY","OFF-ROAD","OTHER","HEAVY DUTY","MEDIUM DUTY"]
VALID_FUELS      = ["UNLEADED","DIESEL","DIESEL / DYED DIESEL","NATURAL GAS",
                    "ELECTRIC","DYED DIESEL","PROPANE","SOLAR POWER",
                    "UNLEADED / DYED DIESEL","UNLEADED / DIESEL"]
VALID_STATUSES   = ["Active Unit","Replacement Program","Unit SOLD","Unit is totaled",
                    "REDEPLOYED Unit","Unit at auction to be sold",
                    "Unit Return for Disp/Sale/Rem"]

def write_silver(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_{table_name}"))
    n = df.count()
    print(f"  silver_{table_name:25s}  {n:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. dim_division
# MAGIC **Natural key**: `DIVISION`
# MAGIC **Key**: `DivisionKey` = row_number ordered by Division name

# COMMAND ----------

# Create table from CTE
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_division AS
WITH distinct_divisions AS (
  SELECT DISTINCT
    INITCAP(TRIM(DIVISION)) AS Division,
    TRIM(OWNING_COST_CENTER) AS OwningCostCenter,
    TRIM(USING_COST_CENTER) AS UsingCostCenter,
    ROW_NUMBER() OVER (PARTITION BY INITCAP(TRIM(DIVISION)) ORDER BY DIVISION) AS rn
  FROM {CATALOG}.{SCHEMA}.bronze_fleet_units
  WHERE DIVISION IS NOT NULL
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY Division) AS DivisionKey,
  Division,
  OwningCostCenter,
  UsingCostCenter
FROM distinct_divisions
WHERE rn = 1
""")

row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.silver_dim_division").collect()[0]['cnt']
print(f"dim_division: {row_count} divisions")
print(f"  silver_dim_division                  {row_count:>8,} rows")

spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_division LIMIT 10").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. dim_vehicle_type
# MAGIC **Natural key**: `CATEGORY` + `CATEGORY_GROUP`
# MAGIC **Key**: `VehicleTypeKey` = row_number

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_vehicle_type AS
WITH cleaned_types AS (
  SELECT DISTINCT
    CASE 
      WHEN UPPER(TRIM(UNIT_TYPE)) IN ('LIGHT DUTY','OFF-ROAD','OTHER','HEAVY DUTY','MEDIUM DUTY')
      THEN INITCAP(TRIM(UNIT_TYPE))
      ELSE NULL 
    END AS UnitType,
    TRIM(CATEGORY) AS Category,
    INITCAP(TRIM(CATEGORY_DESC)) AS CategoryDesc,
    UPPER(TRIM(CATEGORY_CLASS)) AS CategoryClass,
    UPPER(TRIM(CATEGORY_GROUP)) AS CategoryGroup,
    INITCAP(TRIM(CATEGORY_GROUP_DESC)) AS CategoryGroupDesc,
    TRIM(TECH_SPEC) AS TechSpec,
    INITCAP(TRIM(TECH_SPEC_DESC)) AS TechSpecDesc,
    TRIM(BILLING_CODE) AS BillingCode,
    TRIM(`MAINTENANCE_CLASSIFICATION_CODE`) AS MaintenanceClassCode,
    ROW_NUMBER() OVER (PARTITION BY TRIM(CATEGORY) ORDER BY CATEGORY) AS rn
  FROM {CATALOG}.{SCHEMA}.bronze_fleet_units
  WHERE CATEGORY IS NOT NULL
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY Category) AS VehicleTypeKey,
  UnitType,
  Category,
  CategoryDesc,
  CategoryClass,
  CategoryGroup,
  CategoryGroupDesc,
  TechSpec,
  TechSpecDesc,
  BillingCode,
  MaintenanceClassCode
FROM cleaned_types
WHERE rn = 1
""")

row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.silver_dim_vehicle_type").collect()[0]['cnt']
print(f"dim_vehicle_type: {row_count} categories")
print(f"  silver_dim_vehicle_type              {row_count:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. dim_status
# MAGIC **Natural key**: `CURRENT_STATUS_DESCRIPTION` (standardised)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_status AS
WITH status_data AS (
  SELECT * FROM VALUES
    ('Active Unit',                    'Active',      'In service and operational'),
    ('Replacement Program',            'Replacement', 'Flagged for replacement'),
    ('Unit SOLD',                      'Disposed',    'Sold — no longer in fleet'),
    ('Unit is totaled',                'Disposed',    'Written off — total loss'),
    ('REDEPLOYED Unit',                'Active',      'Reassigned to another division'),
    ('Unit at auction to be sold',     'Disposed',    'Pending auction/sale'),
    ('Unit Return for Disp/Sale/Rem',  'Disposed',    'Returned for disposal'),
    ('Unknown',                        'Unknown',     'Status could not be determined')
  AS t(StatusDescription, StatusCategory, StatusNotes)
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY StatusDescription) AS StatusKey,
  StatusDescription,
  StatusCategory,
  StatusNotes
FROM status_data
""")

print(f"  silver_dim_status                        {8:>8,} rows")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_status").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. dim_fuel
# MAGIC **Natural key**: `FUEL_PRODUCT`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_fuel AS
WITH fuel_data AS (
  SELECT * FROM VALUES
    ('UNLEADED',                      'Fossil',     false),
    ('DIESEL',                        'Fossil',     false),
    ('DIESEL / DYED DIESEL',          'Fossil',     false),
    ('DYED DIESEL',                   'Fossil',     false),
    ('UNLEADED / DYED DIESEL',        'Fossil',     false),
    ('UNLEADED / DIESEL',             'Fossil',     false),
    ('NATURAL GAS',                   'Alternative',true),
    ('PROPANE',                       'Alternative',true),
    ('ELECTRIC',                      'Green',      true),
    ('SOLAR POWER',                   'Green',      true),
    ('PRODUCT N/A',                   'Unknown',    false)
  AS t(FuelProduct, FuelCategory, IsLowEmission)
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY FuelProduct) AS FuelKey,
  FuelProduct,
  FuelCategory,
  IsLowEmission
FROM fuel_data
""")

print(f"  silver_dim_fuel                         {11:>8,} rows")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.silver_dim_fuel").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. dim_location
# MAGIC **Natural key**: `MAINTENENACE_LOCATION_NAME`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_dim_location AS
WITH distinct_locations AS (
  SELECT DISTINCT
    INITCAP(TRIM(MAINTENENACE_LOCATION_NAME)) AS MaintenanceLocation,
    TRIM(PARK_LOCATION) AS ParkLocation,
    INITCAP(TRIM(PARK_LOCATION_NAME)) AS ParkLocationName,
    ROW_NUMBER() OVER (PARTITION BY INITCAP(TRIM(MAINTENENACE_LOCATION_NAME)) 
                       ORDER BY MAINTENENACE_LOCATION_NAME) AS rn
  FROM {CATALOG}.{SCHEMA}.bronze_fleet_units
  WHERE MAINTENENACE_LOCATION_NAME IS NOT NULL
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY MaintenanceLocation) AS LocationKey,
  MaintenanceLocation,
  ParkLocation,
  ParkLocationName
FROM distinct_locations
WHERE rn = 1
""")

row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.silver_dim_location").collect()[0]['cnt']
print(f"dim_location: {row_count} locations")
print(f"  silver_dim_location                      {row_count:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. fact_fleet
# MAGIC
# MAGIC Core modeling step:
# MAGIC 1. Clean both Bronze tables
# MAGIC 2. Join units + usage on UNIT_NO
# MAGIC 3. Join all 5 dimensions → surrogate keys
# MAGIC 4. Derive KPI fields

# COMMAND ----------

# ── SQL-based fact table creation ────────────────────────────────
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_fact_fleet AS
-- Step 1: Clean fleet_units
WITH cleaned_units AS (
  SELECT 
    TRIM(UNIT_NO) AS UNIT_NO,
    INITCAP(TRIM(DIVISION)) AS DIVISION,
    TRIM(CATEGORY) AS CATEGORY,
    CASE 
      WHEN UPPER(TRIM(UNIT_TYPE)) IN ('LIGHT DUTY','OFF-ROAD','OTHER','HEAVY DUTY','MEDIUM DUTY')
      THEN INITCAP(TRIM(UNIT_TYPE))
      ELSE NULL 
    END AS UNIT_TYPE,
    CASE 
      WHEN UPPER(TRIM(FUEL_PRODUCT)) IN ('UNLEADED','DIESEL','DIESEL / DYED DIESEL','NATURAL GAS',
                                          'ELECTRIC','DYED DIESEL','PROPANE','SOLAR POWER',
                                          'UNLEADED / DYED DIESEL','UNLEADED / DIESEL')
      THEN UPPER(TRIM(FUEL_PRODUCT))
      ELSE NULL 
    END AS FUEL_PRODUCT,
    CASE 
      WHEN INITCAP(TRIM(CURRENT_STATUS_DESCRIPTION)) IN ('Active Unit','Replacement Program','Unit SOLD',
                                                          'Unit is totaled','REDEPLOYED Unit',
                                                          'Unit at auction to be sold',
                                                          'Unit Return for Disp/Sale/Rem')
      THEN INITCAP(TRIM(CURRENT_STATUS_DESCRIPTION))
      ELSE 'Unknown'
    END AS CURRENT_STATUS_DESCRIPTION,
    INITCAP(TRIM(MAINTENENACE_LOCATION_NAME)) AS MAINTENENACE_LOCATION_NAME,
    MAKE,
    MODEL,
    CAST(TRY_CAST(YEAR AS DOUBLE) AS INT) AS YEAR,
    CASE 
      WHEN TRY_CAST(AGE AS DOUBLE) < 0 THEN 0.0
      ELSE TRY_CAST(AGE AS DOUBLE)
    END AS AGE,
    CASE 
      WHEN TRY_CAST(EXPECTED_LIFE_YR AS DOUBLE) BETWEEN 1 AND 50
      THEN TRY_CAST(EXPECTED_LIFE_YR AS DOUBLE)
      ELSE NULL 
    END AS EXPECTED_LIFE,
    CASE WHEN HIGH_PRIORITY IS NOT NULL AND TRIM(HIGH_PRIORITY) != '' THEN 1 ELSE 0 END AS HIGH_PRIORITY,
    CASE
      WHEN TRY_CAST(IN_SERVICE_DATE AS DOUBLE) > 30000
      THEN CAST(DATE_ADD(DATE'1900-01-01',
                         CAST(TRY_CAST(IN_SERVICE_DATE AS DOUBLE) - 2 AS INT)) AS STRING)
      ELSE IN_SERVICE_DATE
    END AS IN_SERVICE_DATE,
    ROW_NUMBER() OVER (PARTITION BY TRIM(UNIT_NO) ORDER BY UNIT_NO) AS rn
  FROM {CATALOG}.{SCHEMA}.bronze_fleet_units
),

-- Step 2: Clean fleet_usage
cleaned_usage AS (
  SELECT 
    TRIM(UNIT_NO) AS UNIT_NO,
    TRY_CAST(M5_LIFE_KM_USAGE_KM AS DOUBLE) AS LIFE_KM,
    TRY_CAST(M5_LIFE_HRS_USAGE_HRS AS DOUBLE) AS LIFE_HRS,
    TRY_CAST(M5_YTD_KM_USAGE_KM AS DOUBLE) AS YTD_KM,
    TRY_CAST(M5_YTD_HRS_USAGE_HRS AS DOUBLE) AS YTD_HRS,
    TRY_CAST(AVAILABLE_HOURS_HRS AS DOUBLE) AS AVAIL_HRS,
    CASE 
      WHEN TRY_CAST(DOWNTIME_HRS AS DOUBLE) < 0 THEN NULL
      ELSE TRY_CAST(DOWNTIME_HRS AS DOUBLE)
    END AS DOWNTIME_HRS,
    TRY_CAST(EXPECT_USAGE_KM AS DOUBLE) AS EXPECT_KM,
    TRY_CAST(EXPECT_USAGE_HRS AS DOUBLE) AS EXPECT_HRS,
    TRY_CAST(VEU AS DOUBLE) AS VEU,
    -- Date serials (e.g. 45084) → real dates (e.g. 2023-06-07)
    CASE
      WHEN TRY_CAST(LAST_FUEL_DATE AS DOUBLE) > 30000
      THEN CAST(DATE_ADD(DATE'1900-01-01',
                         CAST(TRY_CAST(LAST_FUEL_DATE AS DOUBLE) - 2 AS INT)) AS STRING)
      ELSE LAST_FUEL_DATE
    END AS LAST_FUEL_DATE,
    CASE
      WHEN TRY_CAST(LAST_WORKORDER_OPEN_DATE AS DOUBLE) > 30000
      THEN CAST(DATE_ADD(DATE'1900-01-01',
                         CAST(TRY_CAST(LAST_WORKORDER_OPEN_DATE AS DOUBLE) - 2 AS INT)) AS STRING)
      ELSE LAST_WORKORDER_OPEN_DATE
    END AS LAST_WORKORDER_OPEN_DATE,
    ROW_NUMBER() OVER (PARTITION BY TRIM(UNIT_NO) ORDER BY UNIT_NO) AS rn
  FROM {CATALOG}.{SCHEMA}.bronze_fleet_usage
),

-- Step 3: Join units + usage (deduped)
joined_data AS (
  SELECT u.*, 
         v.LIFE_KM, v.LIFE_HRS, v.YTD_KM, v.YTD_HRS,
         v.AVAIL_HRS, v.DOWNTIME_HRS, v.EXPECT_KM, v.EXPECT_HRS, v.VEU,
         v.LAST_FUEL_DATE, v.LAST_WORKORDER_OPEN_DATE
  FROM cleaned_units u
  LEFT JOIN cleaned_usage v ON u.UNIT_NO = v.UNIT_NO AND v.rn = 1
  WHERE u.rn = 1
),

-- Step 4: Join dimensions
with_dimensions AS (
  SELECT 
    j.*,
    div.DivisionKey,
    vt.VehicleTypeKey,
    st.StatusKey,
    f.FuelKey,
    loc.LocationKey
  FROM joined_data j
  LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_division div 
    ON j.DIVISION = div.Division
  LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_vehicle_type vt 
    ON j.CATEGORY = vt.Category
  LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_status st 
    ON j.CURRENT_STATUS_DESCRIPTION = st.StatusDescription
  LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_fuel f 
    ON j.FUEL_PRODUCT = f.FuelProduct
  LEFT JOIN {CATALOG}.{SCHEMA}.silver_dim_location loc 
    ON j.MAINTENENACE_LOCATION_NAME = loc.MaintenanceLocation
),

-- Step 5: Derive KPI columns
final_fact AS (
  SELECT 
    UNIT_NO,
    DivisionKey,
    VehicleTypeKey,
    StatusKey,
    FuelKey,
    LocationKey,
    YEAR AS ManufactureYear,
    MAKE AS Make,
    MODEL AS Model,
    AGE AS AgeYrs,
    EXPECTED_LIFE AS ExpectedLifeYrs,
    CASE 
      WHEN EXPECTED_LIFE IS NOT NULL AND AGE IS NOT NULL 
      THEN ROUND(EXPECTED_LIFE - AGE, 1)
      ELSE NULL 
    END AS RemainingLifeYrs,
    CASE 
      WHEN AGE IS NOT NULL AND EXPECTED_LIFE IS NOT NULL AND AGE > EXPECTED_LIFE 
      THEN 1 ELSE 0 
    END AS IsOverdue,
    CASE WHEN CURRENT_STATUS_DESCRIPTION = 'Replacement Program' THEN 1 ELSE 0 END AS InReplacementProgram,
    CASE WHEN CURRENT_STATUS_DESCRIPTION = 'Active Unit' THEN 1 ELSE 0 END AS IsActive,
    CASE WHEN FUEL_PRODUCT IN ('ELECTRIC','NATURAL GAS','PROPANE','SOLAR POWER') THEN 1 ELSE 0 END AS IsLowEmission,
    HIGH_PRIORITY AS IsHighPriority,
    LIFE_KM,
    LIFE_HRS,
    YTD_KM,
    YTD_HRS,
    AVAIL_HRS,
    DOWNTIME_HRS,
    EXPECT_KM,
    EXPECT_HRS,
    VEU,
    CASE 
      WHEN EXPECT_KM > 0 AND YTD_KM IS NOT NULL 
      THEN ROUND(YTD_KM / EXPECT_KM * 100, 1)
      ELSE NULL 
    END AS UtilizationPct,
    CASE 
      WHEN AVAIL_HRS > 0 AND DOWNTIME_HRS IS NOT NULL 
      THEN ROUND((AVAIL_HRS - DOWNTIME_HRS) / AVAIL_HRS * 100, 1)
      ELSE NULL 
    END AS AvailabilityPct,
    LAST_FUEL_DATE,
    LAST_WORKORDER_OPEN_DATE,
    IN_SERVICE_DATE AS InServiceDate
  FROM with_dimensions
)

-- Step 6: Final SELECT
SELECT * FROM final_fact
""")

# Quality check
print("\n=== Join Quality Check ===")
df_check = spark.sql(f"""
SELECT 
  COUNT(*) as total_rows,
  SUM(CASE WHEN DivisionKey IS NULL THEN 1 ELSE 0 END) as null_division,
  SUM(CASE WHEN VehicleTypeKey IS NULL THEN 1 ELSE 0 END) as null_vtype,
  SUM(CASE WHEN StatusKey IS NULL THEN 1 ELSE 0 END) as null_status,
  SUM(CASE WHEN FuelKey IS NULL THEN 1 ELSE 0 END) as null_fuel,
  SUM(CASE WHEN ManufactureYear IS NULL THEN 1 ELSE 0 END) as null_year,
  SUM(CASE WHEN AgeYrs IS NULL THEN 1 ELSE 0 END) as null_age,
  SUM(CASE WHEN ExpectedLifeYrs IS NULL THEN 1 ELSE 0 END) as null_exp_life,
  SUM(CASE WHEN IsOverdue = 1 THEN 1 ELSE 0 END) as overdue_vehicles,
  SUM(CASE WHEN InReplacementProgram = 1 THEN 1 ELSE 0 END) as in_replacement,
  SUM(CASE WHEN IsLowEmission = 1 THEN 1 ELSE 0 END) as low_emission
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet
""")

row = df_check.collect()[0]
print(f"Total rows          : {row['total_rows']:,}")
print(f"NULL DivisionKey    : {row['null_division']:,}")
print(f"NULL VehicleTypeKey : {row['null_vtype']:,}")
print(f"NULL StatusKey      : {row['null_status']:,}")
print(f"NULL FuelKey        : {row['null_fuel']:,}")
print(f"NULL ManufactureYear: {row['null_year']:,}")
print(f"NULL AgeYrs         : {row['null_age']:,}")
print(f"NULL ExpectedLifeYrs: {row['null_exp_life']:,}")
print(f"Overdue vehicles    : {row['overdue_vehicles']:,}")
print(f"In replacement prog : {row['in_replacement']:,}")
print(f"Low emission        : {row['low_emission']:,}")
print(f"  silver_fact_fleet                    {row['total_rows']:>5,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Star Schema row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_division'     AS table_name, COUNT(*) AS rows FROM silver_dim_division
# MAGIC UNION ALL SELECT 'dim_vehicle_type',     COUNT(*) FROM silver_dim_vehicle_type
# MAGIC UNION ALL SELECT 'dim_status',           COUNT(*) FROM silver_dim_status
# MAGIC UNION ALL SELECT 'dim_fuel',             COUNT(*) FROM silver_dim_fuel
# MAGIC UNION ALL SELECT 'dim_location',         COUNT(*) FROM silver_dim_location
# MAGIC UNION ALL SELECT 'fact_fleet',           COUNT(*) FROM silver_fact_fleet

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Complete
# MAGIC ```
# MAGIC dim_division     DivisionKey    ─┐
# MAGIC dim_vehicle_type VehicleTypeKey ─┤
# MAGIC dim_status       StatusKey      ─┼──► fact_fleet  (5,610 vehicles)
# MAGIC dim_fuel         FuelKey        ─┤
# MAGIC dim_location     LocationKey    ─┘
# MAGIC ```
# MAGIC Proceed to **Notebook 3 — Gold**.
