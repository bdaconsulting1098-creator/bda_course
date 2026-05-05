# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — Pipeline Orchestrator
# MAGIC **City Fleet Services Division Pipeline**
# MAGIC Runs Bronze → Silver → Gold in sequence.
# MAGIC
# MAGIC | Mode | When to use |
# MAGIC |------|-------------|
# MAGIC | `full_refresh` | First run, or full rebuild |
# MAGIC | `incremental`  | Daily run — only new/changed UNIT_NO rows via MERGE |
# MAGIC
# MAGIC ### Databricks Job setup
# MAGIC ```
# MAGIC Workflows → Jobs → Create Job
# MAGIC   Task       : Notebook  →  /path/to/04_Pipeline_Orchestrator
# MAGIC   Schedule   : 0 5 * * *   (every day at 5 AM)
# MAGIC   Parameters : {"mode": "incremental"}
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 0. Configuration + Mode

# COMMAND ----------

try:
    MODE = dbutils.widgets.get("mode")
except Exception:
    MODE = "full_refresh"

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

current_user = spark.sql("SELECT current_user()").collect()[0][0]

CATALOG = "workspace"
SCHEMA = "fleet_service_sql"
RAW_PATH = f"/Volumes/workspace/default/course_data/fleet_service"
DATABASE = f"{CATALOG}.{SCHEMA}"  # Full qualified name for compatibility

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Mode    : {MODE}")
print(f"Run ID  : {RUN_ID}")

VALID_UNIT_TYPES = ["LIGHT DUTY","OFF-ROAD","OTHER","HEAVY DUTY","MEDIUM DUTY"]
VALID_FUELS      = ["UNLEADED","DIESEL","DIESEL / DYED DIESEL","NATURAL GAS",
                    "ELECTRIC","DYED DIESEL","PROPANE","SOLAR POWER",
                    "UNLEADED / DYED DIESEL","UNLEADED / DIESEL"]
VALID_STATUSES   = ["Active Unit","Replacement Program","Unit SOLD","Unit is totaled",
                    "REDEPLOYED Unit","Unit at auction to be sold",
                    "Unit Return for Disp/Sale/Rem"]

# COMMAND ----------

# MAGIC %md ## 1. Watermark + Run Log

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_watermarks (
        table_name     STRING,
        last_run_date  STRING,
        rows_processed LONG,
        run_status     STRING,
        updated_at     TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.pipeline_run_log (
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

def log_run(stage, rows=0, status="SUCCESS", error=""):
    safe_error = error[:200].replace("'","")
    spark.sql(f"""
        INSERT INTO {DATABASE}.pipeline_run_log VALUES
        ('{RUN_ID}','{MODE}','{stage}',{rows},'{status}',
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

def create_temp_view(df, view_name):
    """Create a temporary view from a DataFrame for SQL operations"""
    df.createOrReplaceTempView(view_name)

# COMMAND ----------

# MAGIC %md ## 3. Bronze Stage

# COMMAND ----------

def run_bronze():
    print("\n▓▓▓  BRONZE  ▓▓▓")
    t = datetime.now()
    batch_date = datetime.now().strftime("%Y-%m-%d")

    def ingest(filename, table_name, key_col="UNIT_NO"):
        # Read raw CSV and add metadata columns
        df = (read_raw(filename)
              .withColumn("_ingest_time", F.current_timestamp())
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_batch_date", F.lit(batch_date)))
        
        # Rename columns with invalid characters (faster with DataFrame API)
        if table_name == "fleet_units":
            df = (df.withColumnRenamed("EXPECTED_LIFE(YR)", "EXPECTED_LIFE_YR")
                    .withColumnRenamed("MAINTENANCE CLASSIFICATION CODE", "MAINTENANCE_CLASSIFICATION_CODE"))
        elif table_name == "fleet_usage":
            df = (df.withColumnRenamed("M5_LIFE_KM_USAGE(KM)", "M5_LIFE_KM_USAGE_KM")
                    .withColumnRenamed("M5_LIFE_HRS_USAGE(Hrs)", "M5_LIFE_HRS_USAGE_HRS")
                    .withColumnRenamed("M5_YTD_KM_USAGE(KM)", "M5_YTD_KM_USAGE_KM")
                    .withColumnRenamed("M5_YTD_HRS_USAGE(Hrs)", "M5_YTD_HRS_USAGE_HRS")
                    .withColumnRenamed("AVAILABLE HOURS (Hrs)", "AVAILABLE_HOURS_HRS")
                    .withColumnRenamed("DOWNTIME(Hrs)", "DOWNTIME_HRS")
                    .withColumnRenamed("EXPECT_USAGE(KM)", "EXPECT_USAGE_KM")
                    .withColumnRenamed("EXPECT_USAGE(Hrs)", "EXPECT_USAGE_HRS"))
        
        full_table_name = f"{DATABASE}.bronze_{table_name}"
        
        if MODE == "full_refresh":
            # Fast path: direct overwrite
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        else:
            # Incremental: use MERGE via temp view
            create_temp_view(df, "staging_clean")
            table_exists = spark.catalog.tableExists(full_table_name)
            if table_exists:
                spark.sql(f"""
                    MERGE INTO {full_table_name} AS e
                    USING staging_clean AS n
                    ON e.{key_col} = n.{key_col}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            else:
                df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        
        n = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']
        print(f"  bronze_{table_name:20s}  {n:>8,} rows")
        return n

    n1 = ingest("RAW_Fleet_Units.csv", "fleet_units")
    n2 = ingest("RAW_Fleet_Usage.csv", "fleet_usage")

    spark.sql(f"""
        INSERT INTO {DATABASE}.pipeline_watermarks VALUES
        ('fleet', '{batch_date}', {n1+n2}, 'SUCCESS', CURRENT_TIMESTAMP())
    """)

    log_run("bronze", rows=n1+n2)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 4. Silver Stage

# COMMAND ----------

def run_silver():
    print("\n▓▓▓  SILVER  ▓▓▓")
    t = datetime.now()
    
    valid_unit_types_sql = "('" + "','".join(VALID_UNIT_TYPES) + "')"
    valid_fuels_sql = "('" + "','".join(VALID_FUELS) + "')"
    valid_statuses_sql = "('" + "','".join(VALID_STATUSES) + "')"

    # ── dim_division (small, fast) ────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_dim_division AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY Division) AS DivisionKey,
            Division, OwningCostCenter, UsingCostCenter
        FROM (
            SELECT DISTINCT
                INITCAP(TRIM(DIVISION)) AS Division,
                TRIM(OWNING_COST_CENTER) AS OwningCostCenter,
                TRIM(USING_COST_CENTER) AS UsingCostCenter
            FROM {DATABASE}.bronze_fleet_units
        )
    """)
    n_div = spark.sql(f"SELECT COUNT(*) as cnt FROM {DATABASE}.silver_dim_division").collect()[0]['cnt']
    print(f"  silver_dim_division         {n_div:>4} rows")

    # ── dim_vehicle_type ──────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_dim_vehicle_type AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY Category) AS VehicleTypeKey,
            UnitType, Category, CategoryDesc, CategoryClass, CategoryGroup,
            CategoryGroupDesc, TechSpec, TechSpecDesc, BillingCode, MaintenanceClassCode
        FROM (
            SELECT DISTINCT
                CASE 
                    WHEN UPPER(TRIM(UNIT_TYPE)) IN {valid_unit_types_sql}
                    THEN INITCAP(TRIM(UNIT_TYPE)) ELSE NULL
                END AS UnitType,
                TRIM(CATEGORY) AS Category,
                INITCAP(TRIM(CATEGORY_DESC)) AS CategoryDesc,
                UPPER(TRIM(CATEGORY_CLASS)) AS CategoryClass,
                UPPER(TRIM(CATEGORY_GROUP)) AS CategoryGroup,
                INITCAP(TRIM(CATEGORY_GROUP_DESC)) AS CategoryGroupDesc,
                TRIM(TECH_SPEC) AS TechSpec,
                INITCAP(TRIM(TECH_SPEC_DESC)) AS TechSpecDesc,
                TRIM(BILLING_CODE) AS BillingCode,
                TRIM(MAINTENANCE_CLASSIFICATION_CODE) AS MaintenanceClassCode
            FROM {DATABASE}.bronze_fleet_units
        )
    """)
    n_vtype = spark.sql(f"SELECT COUNT(*) as cnt FROM {DATABASE}.silver_dim_vehicle_type").collect()[0]['cnt']
    print(f"  silver_dim_vehicle_type     {n_vtype:>4} rows")

    # ── dim_status (static) ───────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_dim_status AS
        SELECT ROW_NUMBER() OVER (ORDER BY StatusDescription) AS StatusKey,
               StatusDescription, StatusCategory, StatusNotes
        FROM VALUES
            ('Active Unit', 'Active', 'In service and operational'),
            ('REDEPLOYED Unit', 'Active', 'Reassigned to another division'),
            ('Replacement Program', 'Replacement', 'Flagged for replacement'),
            ('Unit SOLD', 'Disposed', 'Sold — no longer in fleet'),
            ('Unit is totaled', 'Disposed', 'Written off — total loss'),
            ('Unit at auction to be sold', 'Disposed', 'Pending auction/sale'),
            ('Unit Return for Disp/Sale/Rem', 'Disposed', 'Returned for disposal'),
            ('Unknown', 'Unknown', 'Status could not be determined')
        AS t(StatusDescription, StatusCategory, StatusNotes)
    """)
    print(f"  silver_dim_status           8 rows")

    # ── dim_fuel (static) ────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_dim_fuel AS
        SELECT ROW_NUMBER() OVER (ORDER BY FuelProduct) AS FuelKey,
               FuelProduct, FuelCategory, IsLowEmission
        FROM VALUES
            ('DIESEL', 'Fossil', FALSE), ('DIESEL / DYED DIESEL', 'Fossil', FALSE),
            ('DYED DIESEL', 'Fossil', FALSE), ('ELECTRIC', 'Green', TRUE),
            ('NATURAL GAS', 'Alternative', TRUE), ('PRODUCT N/A', 'Unknown', FALSE),
            ('PROPANE', 'Alternative', TRUE), ('SOLAR POWER', 'Green', TRUE),
            ('UNLEADED', 'Fossil', FALSE), ('UNLEADED / DIESEL', 'Fossil', FALSE),
            ('UNLEADED / DYED DIESEL', 'Fossil', FALSE)
        AS t(FuelProduct, FuelCategory, IsLowEmission)
    """)
    print(f"  silver_dim_fuel             11 rows")

    # ── dim_location ──────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_dim_location AS
        SELECT ROW_NUMBER() OVER (ORDER BY MaintenanceLocation) AS LocationKey,
               MaintenanceLocation, ParkLocation, ParkLocationName
        FROM (
            SELECT DISTINCT
                INITCAP(TRIM(MAINTENENACE_LOCATION_NAME)) AS MaintenanceLocation,
                TRIM(PARK_LOCATION) AS ParkLocation,
                INITCAP(TRIM(PARK_LOCATION_NAME)) AS ParkLocationName
            FROM {DATABASE}.bronze_fleet_units
        )
    """)
    n_loc = spark.sql(f"SELECT COUNT(*) as cnt FROM {DATABASE}.silver_dim_location").collect()[0]['cnt']
    print(f"  silver_dim_location         {n_loc:>4} rows")

    # ── fact_fleet (FIX: deduplicate properly by UNIT_NO) ────────────────────
    # Stage 1: Clean and deduplicate units data (keep most recent per UNIT_NO)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW units_clean AS
        SELECT UNIT_NO, DIVISION, CATEGORY, UNIT_TYPE, FUEL_PRODUCT, STATUS_DESC,
               LOCATION, YEAR, AGE, EXPECTED_LIFE, HIGH_PRIORITY, MAKE, MODEL
        FROM (
            SELECT 
                TRIM(UNIT_NO) AS UNIT_NO,
                INITCAP(TRIM(DIVISION)) AS DIVISION,
                TRIM(CATEGORY) AS CATEGORY,
                CASE WHEN UPPER(TRIM(UNIT_TYPE)) IN {valid_unit_types_sql}
                     THEN INITCAP(TRIM(UNIT_TYPE)) ELSE NULL END AS UNIT_TYPE,
                CASE WHEN UPPER(TRIM(FUEL_PRODUCT)) IN {valid_fuels_sql}
                     THEN UPPER(TRIM(FUEL_PRODUCT)) ELSE NULL END AS FUEL_PRODUCT,
                CASE WHEN INITCAP(TRIM(CURRENT_STATUS_DESCRIPTION)) IN {valid_statuses_sql}
                     THEN INITCAP(TRIM(CURRENT_STATUS_DESCRIPTION)) ELSE 'Unknown' END AS STATUS_DESC,
                INITCAP(TRIM(MAINTENENACE_LOCATION_NAME)) AS LOCATION,
                TRY_CAST(TRY_CAST(YEAR AS DOUBLE) AS INT) AS YEAR,
                CASE WHEN TRY_CAST(AGE AS DOUBLE) < 0 THEN 0.0
                     ELSE TRY_CAST(AGE AS DOUBLE) END AS AGE,
                CASE WHEN TRY_CAST(EXPECTED_LIFE_YR AS DOUBLE) BETWEEN 1 AND 50
                     THEN TRY_CAST(EXPECTED_LIFE_YR AS DOUBLE) ELSE NULL END AS EXPECTED_LIFE,
                CASE WHEN HIGH_PRIORITY IS NOT NULL THEN 1 ELSE 0 END AS HIGH_PRIORITY,
                MAKE, MODEL,
                _ingest_time,
                ROW_NUMBER() OVER (PARTITION BY TRIM(UNIT_NO) ORDER BY _ingest_time DESC) as rn
            FROM {DATABASE}.bronze_fleet_units
        )
        WHERE rn = 1
    """)
    
    # Stage 2: Clean and deduplicate usage data (keep most recent per UNIT_NO)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW usage_clean AS
        SELECT UNIT_NO, LIFE_KM, LIFE_HRS, YTD_KM, YTD_HRS, AVAIL_HRS, DOWNTIME_HRS,
               EXPECT_KM, EXPECT_HRS, VEU, LAST_FUEL_DATE, LAST_WORKORDER_OPEN_DATE
        FROM (
            SELECT 
                TRIM(UNIT_NO) AS UNIT_NO,
                TRY_CAST(M5_LIFE_KM_USAGE_KM AS DOUBLE) AS LIFE_KM,
                TRY_CAST(M5_LIFE_HRS_USAGE_HRS AS DOUBLE) AS LIFE_HRS,
                TRY_CAST(M5_YTD_KM_USAGE_KM AS DOUBLE) AS YTD_KM,
                TRY_CAST(M5_YTD_HRS_USAGE_HRS AS DOUBLE) AS YTD_HRS,
                TRY_CAST(AVAILABLE_HOURS_HRS AS DOUBLE) AS AVAIL_HRS,
                CASE WHEN TRY_CAST(DOWNTIME_HRS AS DOUBLE) < 0 THEN NULL
                     ELSE TRY_CAST(DOWNTIME_HRS AS DOUBLE) END AS DOWNTIME_HRS,
                TRY_CAST(EXPECT_USAGE_KM AS DOUBLE) AS EXPECT_KM,
                TRY_CAST(EXPECT_USAGE_HRS AS DOUBLE) AS EXPECT_HRS,
                TRY_CAST(VEU AS DOUBLE) AS VEU,
                LAST_FUEL_DATE, LAST_WORKORDER_OPEN_DATE,
                _ingest_time,
                ROW_NUMBER() OVER (PARTITION BY TRIM(UNIT_NO) ORDER BY _ingest_time DESC) as rn
            FROM {DATABASE}.bronze_fleet_usage
        )
        WHERE rn = 1
    """)
    
    # Stage 3: Join all together and create fact table
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.silver_fact_fleet AS
        SELECT 
            u.UNIT_NO,
            d.DivisionKey, v.VehicleTypeKey, s.StatusKey, f.FuelKey, l.LocationKey,
            u.YEAR AS ManufactureYear, u.MAKE AS Make, u.MODEL AS Model,
            u.AGE AS AgeYrs, u.EXPECTED_LIFE AS ExpectedLifeYrs,
            CASE WHEN u.EXPECTED_LIFE IS NOT NULL AND u.AGE IS NOT NULL
                 THEN ROUND(u.EXPECTED_LIFE - u.AGE, 1) ELSE NULL END AS RemainingLifeYrs,
            CASE WHEN u.AGE > u.EXPECTED_LIFE THEN 1 ELSE 0 END AS IsOverdue,
            CASE WHEN u.STATUS_DESC = 'Replacement Program' THEN 1 ELSE 0 END AS InReplacementProgram,
            CASE WHEN u.STATUS_DESC = 'Active Unit' THEN 1 ELSE 0 END AS IsActive,
            CASE WHEN u.FUEL_PRODUCT IN ('ELECTRIC','NATURAL GAS','PROPANE','SOLAR POWER') 
                 THEN 1 ELSE 0 END AS IsLowEmission,
            u.HIGH_PRIORITY AS IsHighPriority,
            ug.LIFE_KM, ug.LIFE_HRS, ug.YTD_KM, ug.YTD_HRS, ug.AVAIL_HRS, ug.DOWNTIME_HRS,
            ug.EXPECT_KM, ug.EXPECT_HRS, ug.VEU,
            CASE WHEN ug.EXPECT_KM > 0 AND ug.YTD_KM IS NOT NULL
                 THEN ROUND(ug.YTD_KM / ug.EXPECT_KM * 100, 1) ELSE NULL END AS UtilizationPct,
            CASE WHEN ug.AVAIL_HRS > 0 AND ug.DOWNTIME_HRS IS NOT NULL
                 THEN ROUND((ug.AVAIL_HRS - ug.DOWNTIME_HRS) / ug.AVAIL_HRS * 100, 1) ELSE NULL END AS AvailabilityPct,
            ug.LAST_FUEL_DATE, ug.LAST_WORKORDER_OPEN_DATE
        FROM units_clean u
        LEFT JOIN usage_clean ug ON u.UNIT_NO = ug.UNIT_NO
        LEFT JOIN {DATABASE}.silver_dim_division d ON u.DIVISION = d.Division
        LEFT JOIN {DATABASE}.silver_dim_vehicle_type v ON u.CATEGORY = v.Category
        LEFT JOIN {DATABASE}.silver_dim_status s ON u.STATUS_DESC = s.StatusDescription
        LEFT JOIN {DATABASE}.silver_dim_fuel f ON u.FUEL_PRODUCT = f.FuelProduct
        LEFT JOIN {DATABASE}.silver_dim_location l ON u.LOCATION = l.MaintenanceLocation
    """)

    n_fact = spark.sql(f"SELECT COUNT(*) as cnt FROM {DATABASE}.silver_fact_fleet").collect()[0]['cnt']
    print(f"  silver_fact_fleet           {n_fact:>8,} rows")
    log_run("silver", rows=n_fact)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 5. Gold Stage

# COMMAND ----------

def run_gold():
    print("\n▓▓▓  GOLD  ▓▓▓")
    t = datetime.now()

    # fleet_overview
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_fleet_overview AS
        SELECT d.Division, v.UnitType, v.CategoryGroupDesc, s.StatusCategory, f.FuelCategory,
            COUNT(fact.UNIT_NO) AS VehicleCount,
            ROUND(AVG(fact.AgeYrs), 1) AS AvgAgeYrs,
            ROUND(MAX(fact.AgeYrs), 1) AS MaxAgeYrs,
            ROUND(AVG(fact.ExpectedLifeYrs), 1) AS AvgExpectedLifeYrs,
            SUM(fact.IsOverdue) AS OverdueCount,
            SUM(fact.InReplacementProgram) AS InReplacementCount,
            SUM(fact.IsActive) AS ActiveCount,
            SUM(fact.IsHighPriority) AS HighPriorityCount,
            SUM(fact.IsLowEmission) AS LowEmissionCount,
            ROUND(SUM(fact.IsOverdue) / COUNT(fact.UNIT_NO) * 100, 1) AS OverduePct,
            ROUND(SUM(fact.IsLowEmission) / COUNT(fact.UNIT_NO) * 100, 1) AS LowEmissionPct,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_division d ON fact.DivisionKey = d.DivisionKey
        JOIN {DATABASE}.silver_dim_vehicle_type v ON fact.VehicleTypeKey = v.VehicleTypeKey
        JOIN {DATABASE}.silver_dim_status s ON fact.StatusKey = s.StatusKey
        JOIN {DATABASE}.silver_dim_fuel f ON fact.FuelKey = f.FuelKey
        GROUP BY d.Division, v.UnitType, v.CategoryGroupDesc, s.StatusCategory, f.FuelCategory
    """)
    print(f"  gold_fleet_overview")

    # replacement_pipeline
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_replacement_pipeline AS
        SELECT d.Division, v.UnitType, v.Category, fact.UNIT_NO, fact.Make, fact.Model,
            fact.ManufactureYear, fact.AgeYrs, fact.ExpectedLifeYrs, fact.RemainingLifeYrs,
            s.StatusDescription, fact.IsOverdue, fact.InReplacementProgram, fact.IsHighPriority,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_division d ON fact.DivisionKey = d.DivisionKey
        JOIN {DATABASE}.silver_dim_vehicle_type v ON fact.VehicleTypeKey = v.VehicleTypeKey
        JOIN {DATABASE}.silver_dim_status s ON fact.StatusKey = s.StatusKey
        WHERE fact.IsOverdue = 1 OR fact.InReplacementProgram = 1
        ORDER BY fact.IsHighPriority DESC, fact.AgeYrs DESC
    """)
    print(f"  gold_replacement_pipeline")

    # utilization_summary
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_utilization_summary AS
        SELECT d.Division, v.UnitType, v.CategoryGroupDesc,
            COUNT(fact.UNIT_NO) AS VehicleCount,
            ROUND(AVG(fact.UtilizationPct), 1) AS AvgUtilizationPct,
            ROUND(AVG(fact.AvailabilityPct), 1) AS AvgAvailabilityPct,
            SUM(CAST(fact.YTD_KM AS BIGINT)) AS TotalYTDKm,
            SUM(CAST(fact.YTD_HRS AS BIGINT)) AS TotalYTDHrs,
            ROUND(AVG(fact.DOWNTIME_HRS), 1) AS AvgDowntimeHrs,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_division d ON fact.DivisionKey = d.DivisionKey
        JOIN {DATABASE}.silver_dim_vehicle_type v ON fact.VehicleTypeKey = v.VehicleTypeKey
        WHERE fact.IsActive = 1
        GROUP BY d.Division, v.UnitType, v.CategoryGroupDesc
    """)
    print(f"  gold_utilization_summary")

    # fuel_analysis
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_fuel_analysis AS
        SELECT d.Division, f.FuelProduct, f.FuelCategory, f.IsLowEmission,
            COUNT(fact.UNIT_NO) AS VehicleCount,
            ROUND(AVG(fact.AgeYrs), 1) AS AvgAgeYrs,
            ROUND(COUNT(fact.UNIT_NO) * 100.0 / SUM(COUNT(fact.UNIT_NO)) OVER (PARTITION BY d.Division), 1) AS PctOfDivision,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_division d ON fact.DivisionKey = d.DivisionKey
        JOIN {DATABASE}.silver_dim_fuel f ON fact.FuelKey = f.FuelKey
        WHERE fact.IsActive = 1
        GROUP BY d.Division, f.FuelProduct, f.FuelCategory, f.IsLowEmission
    """)
    print(f"  gold_fuel_analysis")

    # location_summary
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_location_summary AS
        SELECT l.MaintenanceLocation, l.ParkLocation, l.ParkLocationName, d.Division,
            COUNT(fact.UNIT_NO) AS VehicleCount,
            SUM(fact.IsActive) AS ActiveCount,
            SUM(fact.IsOverdue) AS OverdueCount,
            ROUND(AVG(fact.AgeYrs), 1) AS AvgAgeYrs,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_location l ON fact.LocationKey = l.LocationKey
        JOIN {DATABASE}.silver_dim_division d ON fact.DivisionKey = d.DivisionKey
        GROUP BY l.MaintenanceLocation, l.ParkLocation, l.ParkLocationName, d.Division
    """)
    print(f"  gold_location_summary")

    # age_distribution
    spark.sql(f"""
        CREATE OR REPLACE TABLE {DATABASE}.gold_age_distribution AS
        SELECT v.UnitType, v.CategoryGroupDesc,
            CASE 
                WHEN fact.AgeYrs < 2 THEN '0-2 years'
                WHEN fact.AgeYrs < 5 THEN '2-5 years'
                WHEN fact.AgeYrs < 10 THEN '5-10 years'
                WHEN fact.AgeYrs < 15 THEN '10-15 years'
                ELSE '15+ years'
            END AS AgeRange,
            COUNT(fact.UNIT_NO) AS VehicleCount,
            SUM(fact.IsOverdue) AS OverdueCount,
            SUM(fact.IsActive) AS ActiveCount,
            CURRENT_TIMESTAMP() AS _gold_ts
        FROM {DATABASE}.silver_fact_fleet fact
        JOIN {DATABASE}.silver_dim_vehicle_type v ON fact.VehicleTypeKey = v.VehicleTypeKey
        WHERE fact.AgeYrs IS NOT NULL
        GROUP BY v.UnitType, v.CategoryGroupDesc, 
                 CASE 
                     WHEN fact.AgeYrs < 2 THEN '0-2 years'
                     WHEN fact.AgeYrs < 5 THEN '2-5 years'
                     WHEN fact.AgeYrs < 10 THEN '5-10 years'
                     WHEN fact.AgeYrs < 15 THEN '10-15 years'
                     ELSE '15+ years'
                 END
    """)
    print(f"  gold_age_distribution")

    log_run("gold", rows=0)
    print(f"  ✔ {(datetime.now()-t).seconds}s")

# COMMAND ----------

# MAGIC %md ## 6. Run

# COMMAND ----------

t0 = datetime.now()
print(f"""
╔══════════════════════════════════════════════╗
║  Fleet Services Pipeline                     ║
║  Run ID : {RUN_ID}             ║
║  Mode   : {MODE:<12}                    ║
║  Started: {t0.strftime('%Y-%m-%d %H:%M:%S')}               ║
╚══════════════════════════════════════════════╝""")

try:    run_bronze()
except Exception as e: log_run("bronze",status="FAILED",error=str(e)); raise

try:    run_silver()
except Exception as e: log_run("silver",status="FAILED",error=str(e)); raise

try:    run_gold()
except Exception as e: log_run("gold",  status="FAILED",error=str(e)); raise

print(f"\n✅ Pipeline complete in {(datetime.now()-t0).seconds}s")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, run_mode, stage, rows_out, run_status,
# MAGIC        CAST(started_at AS STRING) AS started_at
# MAGIC FROM pipeline_run_log ORDER BY started_at DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, last_run_date, rows_processed, run_status,
# MAGIC        CAST(updated_at AS STRING) AS updated_at
# MAGIC FROM pipeline_watermarks ORDER BY updated_at DESC
