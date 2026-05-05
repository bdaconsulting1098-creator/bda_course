# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Gold Layer: Fleet KPI Analysis
# MAGIC
# MAGIC Each Gold table maps to a key KPI / Auditor General finding:
# MAGIC
# MAGIC | Gold Table                  | KPI / Question |
# MAGIC |-----------------------------|----------------|
# MAGIC | `gold_fleet_overview`        | Fleet composition, age, status summary |
# MAGIC | `gold_replacement_pipeline`  | Overdue + replacement program vehicles |
# MAGIC | `gold_availability`          | Downtime and availability by division/type |
# MAGIC | `gold_utilization`           | YTD usage vs expected — under/over-utilised |
# MAGIC | `gold_fuel_mix`              | Fuel type breakdown — green transition progress |
# MAGIC | `gold_division_scorecard`    | Per-division KPI rollup for management dashboard |

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "fleet_service_sql"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. gold_fleet_overview
# MAGIC Fleet composition and age health by division and vehicle type.
# MAGIC **KPIs**: Total fleet size, avg age, % overdue, % in replacement program.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_fleet_overview AS
SELECT 
  div.Division,
  vt.UnitType,
  vt.CategoryGroupDesc,
  st.StatusCategory,
  fu.FuelCategory,
  COUNT(f.UNIT_NO) AS VehicleCount,
  ROUND(AVG(f.AgeYrs), 1) AS AvgAgeYrs,
  ROUND(MAX(f.AgeYrs), 1) AS MaxAgeYrs,
  ROUND(AVG(f.ExpectedLifeYrs), 1) AS AvgExpectedLifeYrs,
  SUM(f.IsOverdue) AS OverdueCount,
  SUM(f.InReplacementProgram) AS InReplacementCount,
  SUM(f.IsActive) AS ActiveCount,
  SUM(f.IsHighPriority) AS HighPriorityCount,
  SUM(f.IsLowEmission) AS LowEmissionCount,
  ROUND(AVG(f.ManufactureYear), 0) AS AvgManufactureYear,
  ROUND(SUM(f.IsOverdue) / COUNT(f.UNIT_NO) * 100, 1) AS OverduePct,
  ROUND(SUM(f.InReplacementProgram) / COUNT(f.UNIT_NO) * 100, 1) AS ReplacementPct,
  ROUND(SUM(f.IsLowEmission) / COUNT(f.UNIT_NO) * 100, 1) AS LowEmissionPct,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_vehicle_type vt ON f.VehicleTypeKey = vt.VehicleTypeKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_status st ON f.StatusKey = st.StatusKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_fuel fu ON f.FuelKey = fu.FuelKey
GROUP BY div.Division, vt.UnitType, vt.CategoryGroupDesc, st.StatusCategory, fu.FuelCategory
ORDER BY div.Division, vt.UnitType
""")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_fleet_overview").collect()[0]['cnt']
print(f"  gold_fleet_overview                     {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. gold_replacement_pipeline
# MAGIC All vehicles that are overdue OR in replacement program.
# MAGIC **KPI**: How many vehicles need replacing, by division, type, and age band.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_replacement_pipeline AS
SELECT 
  f.UNIT_NO,
  div.Division,
  vt.UnitType,
  vt.CategoryGroupDesc,
  st.StatusDescription,
  fu.FuelProduct,
  fu.FuelCategory,
  loc.MaintenanceLocation,
  f.ManufactureYear,
  f.Make,
  f.Model,
  f.AgeYrs,
  f.ExpectedLifeYrs,
  f.RemainingLifeYrs,
  f.IsOverdue,
  f.InReplacementProgram,
  f.IsHighPriority,
  f.VEU,
  f.AvailabilityPct,
  f.UtilizationPct,
  CASE 
    WHEN f.RemainingLifeYrs < -5 THEN '5+ yrs overdue'
    WHEN f.RemainingLifeYrs < 0 THEN '1-5 yrs overdue'
    WHEN f.RemainingLifeYrs <= 2 THEN 'Due within 2 yrs'
    ELSE 'Due 2+ yrs'
  END AS AgeBand,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_vehicle_type vt ON f.VehicleTypeKey = vt.VehicleTypeKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_status st ON f.StatusKey = st.StatusKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_fuel fu ON f.FuelKey = fu.FuelKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_location loc ON f.LocationKey = loc.LocationKey
WHERE f.IsOverdue = 1 OR f.InReplacementProgram = 1
ORDER BY f.RemainingLifeYrs ASC
""")

print("Replacement pipeline:")
spark.sql(f"""
SELECT AgeBand, COUNT(*) AS count
FROM {CATALOG}.{SCHEMA}.gold_replacement_pipeline
GROUP BY AgeBand
ORDER BY AgeBand
""").show(truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_replacement_pipeline").collect()[0]['cnt']
print(f"  gold_replacement_pipeline               {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. gold_availability
# MAGIC Downtime and availability analysis by division, location, and vehicle type.
# MAGIC **KPIs**: Avg availability %, total downtime hours, worst-performing vehicles.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_availability AS
SELECT 
  div.Division,
  vt.UnitType,
  vt.CategoryGroupDesc,
  loc.MaintenanceLocation,
  COUNT(f.UNIT_NO) AS VehicleCount,
  ROUND(SUM(f.AVAIL_HRS), 0) AS TotalAvailHrs,
  ROUND(SUM(f.DOWNTIME_HRS), 0) AS TotalDowntimeHrs,
  ROUND(AVG(f.AvailabilityPct), 1) AS AvgAvailabilityPct,
  ROUND(MIN(f.AvailabilityPct), 1) AS MinAvailabilityPct,
  SUM(CASE WHEN f.AvailabilityPct < 80 THEN 1 ELSE 0 END) AS LowAvailCount,
  SUM(CASE WHEN f.DOWNTIME_HRS > 200 THEN 1 ELSE 0 END) AS HighDowntimeCount,
  ROUND(SUM(f.DOWNTIME_HRS) / NULLIF(SUM(f.AVAIL_HRS), 0) * 100, 1) AS DowntimePct,
  ROUND(SUM(CASE WHEN f.AvailabilityPct < 80 THEN 1 ELSE 0 END) / COUNT(f.UNIT_NO) * 100, 1) AS LowAvailPct,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_vehicle_type vt ON f.VehicleTypeKey = vt.VehicleTypeKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_location loc ON f.LocationKey = loc.LocationKey
GROUP BY div.Division, vt.UnitType, vt.CategoryGroupDesc, loc.MaintenanceLocation
ORDER BY AvgAvailabilityPct ASC
""")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_availability").collect()[0]['cnt']
print(f"  gold_availability                       {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. gold_utilization
# MAGIC YTD KM vs expected KM — identifies underutilised and overutilised vehicles.
# MAGIC **KPIs**: Utilization %, idle fleet count, VEU score.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_utilization AS
SELECT 
  div.Division,
  vt.UnitType,
  vt.CategoryGroupDesc,
  COUNT(f.UNIT_NO) AS VehicleCount,
  ROUND(SUM(f.YTD_KM), 0) AS TotalYTDKm,
  ROUND(SUM(f.EXPECT_KM), 0) AS TotalExpectKm,
  ROUND(AVG(f.UtilizationPct), 1) AS AvgUtilizationPct,
  ROUND(AVG(f.VEU), 2) AS AvgVEU,
  SUM(CASE WHEN f.UtilizationPct < 50 THEN 1 ELSE 0 END) AS UnderutilizedCount,
  SUM(CASE WHEN f.UtilizationPct > 120 THEN 1 ELSE 0 END) AS OverutilizedCount,
  ROUND(SUM(f.LIFE_KM), 0) AS TotalLifetimeKm,
  ROUND(SUM(f.YTD_KM) / NULLIF(SUM(f.EXPECT_KM), 0) * 100, 1) AS FleetUtilizationPct,
  ROUND(SUM(CASE WHEN f.UtilizationPct < 50 THEN 1 ELSE 0 END) / COUNT(f.UNIT_NO) * 100, 1) AS UnderutilizedPct,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_vehicle_type vt ON f.VehicleTypeKey = vt.VehicleTypeKey
GROUP BY div.Division, vt.UnitType, vt.CategoryGroupDesc
ORDER BY AvgUtilizationPct ASC
""")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_utilization").collect()[0]['cnt']
print(f"  gold_utilization                        {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. gold_fuel_mix
# MAGIC Fuel type breakdown — tracks green fleet transition progress.
# MAGIC **KPIs**: % electric/low-emission, fuel category distribution by division.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_fuel_mix AS
SELECT 
  div.Division,
  fu.FuelProduct,
  fu.FuelCategory,
  fu.IsLowEmission,
  COUNT(f.UNIT_NO) AS VehicleCount,
  ROUND(AVG(f.AgeYrs), 1) AS AvgAgeYrs,
  ROUND(AVG(f.UtilizationPct), 1) AS AvgUtilizationPct,
  SUM(f.IsActive) AS ActiveCount,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
JOIN {CATALOG}.{SCHEMA}.silver_dim_fuel fu ON f.FuelKey = fu.FuelKey
GROUP BY div.Division, fu.FuelProduct, fu.FuelCategory, fu.IsLowEmission
ORDER BY div.Division, fu.FuelCategory, fu.FuelProduct
""")

print("Fleet fuel mix summary:")
spark.sql(f"""
SELECT fu.FuelCategory, fu.IsLowEmission, COUNT(f.UNIT_NO) AS Count
FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
JOIN {CATALOG}.{SCHEMA}.silver_dim_fuel fu ON f.FuelKey = fu.FuelKey
GROUP BY fu.FuelCategory, fu.IsLowEmission
ORDER BY fu.FuelCategory
""").show(truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_fuel_mix").collect()[0]['cnt']
print(f"  gold_fuel_mix                           {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. gold_division_scorecard
# MAGIC One row per division — consolidated KPI scorecard for management dashboard.
# MAGIC Answers the Auditor General's efficiency and effectiveness questions.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_division_scorecard AS
WITH division_metrics AS (
  SELECT 
    div.Division,
    COUNT(f.UNIT_NO) AS TotalVehicles,
    SUM(f.IsActive) AS ActiveVehicles,
    SUM(f.InReplacementProgram) AS InReplacementProgram,
    SUM(f.IsOverdue) AS OverdueVehicles,
    SUM(f.IsHighPriority) AS HighPriorityVehicles,
    SUM(f.IsLowEmission) AS LowEmissionVehicles,
    ROUND(AVG(f.AgeYrs), 1) AS AvgFleetAgeYrs,
    ROUND(AVG(f.RemainingLifeYrs), 1) AS AvgRemainingLifeYrs,
    ROUND(AVG(f.AvailabilityPct), 1) AS AvgAvailabilityPct,
    ROUND(AVG(f.UtilizationPct), 1) AS AvgUtilizationPct,
    ROUND(AVG(f.VEU), 2) AS AvgVEU,
    ROUND(SUM(f.DOWNTIME_HRS), 0) AS TotalDowntimeHrs,
    ROUND(SUM(f.YTD_KM), 0) AS TotalYTDKm,
    SUM(CASE WHEN f.AvailabilityPct < 80 THEN 1 ELSE 0 END) AS LowAvailVehicles,
    SUM(CASE WHEN f.UtilizationPct < 50 THEN 1 ELSE 0 END) AS UnderutilizedVehicles
  FROM {CATALOG}.{SCHEMA}.silver_fact_fleet f
  JOIN {CATALOG}.{SCHEMA}.silver_dim_division div ON f.DivisionKey = div.DivisionKey
  GROUP BY div.Division
)
SELECT 
  Division,
  TotalVehicles,
  ActiveVehicles,
  InReplacementProgram,
  OverdueVehicles,
  HighPriorityVehicles,
  LowEmissionVehicles,
  AvgFleetAgeYrs,
  AvgRemainingLifeYrs,
  AvgAvailabilityPct,
  AvgUtilizationPct,
  AvgVEU,
  TotalDowntimeHrs,
  TotalYTDKm,
  LowAvailVehicles,
  UnderutilizedVehicles,
  ROUND(ActiveVehicles / TotalVehicles * 100, 1) AS ActivePct,
  ROUND(OverdueVehicles / TotalVehicles * 100, 1) AS OverduePct,
  ROUND(LowEmissionVehicles / TotalVehicles * 100, 1) AS LowEmissionPct,
  ROUND(LowAvailVehicles / TotalVehicles * 100, 1) AS LowAvailPct,
  ROUND(
    (ROUND(OverdueVehicles / TotalVehicles * 100, 1) * 0.4) +
    (ROUND(LowAvailVehicles / TotalVehicles * 100, 1) * 0.35) +
    (ROUND(UnderutilizedVehicles / TotalVehicles * 100, 1) * 0.25),
  1) AS RiskScore,
  CURRENT_TIMESTAMP() AS _gold_ts
FROM division_metrics
ORDER BY RiskScore DESC
""")

print("Division scorecard (top 10 by risk):")
spark.sql(f"""
SELECT Division, TotalVehicles, OverduePct, AvgAvailabilityPct, AvgUtilizationPct, LowEmissionPct, RiskScore
FROM {CATALOG}.{SCHEMA}.gold_division_scorecard
ORDER BY RiskScore DESC
LIMIT 10
""").show(10, truncate=False)

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.gold_division_scorecard").collect()[0]['cnt']
print(f"  gold_division_scorecard                 {row_count:>6,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fleet_overview'       AS gold_table, COUNT(*) AS rows FROM gold_fleet_overview
# MAGIC UNION ALL SELECT 'replacement_pipeline', COUNT(*) FROM gold_replacement_pipeline
# MAGIC UNION ALL SELECT 'availability',         COUNT(*) FROM gold_availability
# MAGIC UNION ALL SELECT 'utilization',          COUNT(*) FROM gold_utilization
# MAGIC UNION ALL SELECT 'fuel_mix',             COUNT(*) FROM gold_fuel_mix
# MAGIC UNION ALL SELECT 'division_scorecard',   COUNT(*) FROM gold_division_scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top KPI: Divisions with most overdue vehicles
# MAGIC SELECT Division, TotalVehicles, OverdueVehicles, OverduePct,
# MAGIC        AvgFleetAgeYrs, AvgAvailabilityPct, RiskScore
# MAGIC FROM gold_division_scorecard
# MAGIC ORDER BY OverduePct DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fuel transition: how green is the fleet?
# MAGIC SELECT FuelCategory, IsLowEmission,
# MAGIC        SUM(VehicleCount) AS Vehicles,
# MAGIC        ROUND(SUM(VehicleCount) / SUM(SUM(VehicleCount)) OVER () * 100, 1) AS PctOfFleet
# MAGIC FROM gold_fuel_mix
# MAGIC GROUP BY FuelCategory, IsLowEmission
# MAGIC ORDER BY Vehicles DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replacement urgency: how many vehicles 5+ years overdue?
# MAGIC SELECT AgeBand, COUNT(*) AS Vehicles, COUNT(DISTINCT Division) AS DivisionsAffected
# MAGIC FROM gold_replacement_pipeline
# MAGIC GROUP BY AgeBand
# MAGIC ORDER BY AgeBand

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Complete
# MAGIC
# MAGIC | Table | KPI / Auditor Finding |
# MAGIC |-------|----------------------|
# MAGIC | `gold_fleet_overview`       | Fleet composition, age, status by division |
# MAGIC | `gold_replacement_pipeline` | Overdue vehicles prioritised by urgency band |
# MAGIC | `gold_availability`         | Downtime hours, availability % by location |
# MAGIC | `gold_utilization`          | Under/over-utilised vehicles, VEU score |
# MAGIC | `gold_fuel_mix`             | Green fleet progress — low emission % |
# MAGIC | `gold_division_scorecard`   | Composite risk score per division for management |
