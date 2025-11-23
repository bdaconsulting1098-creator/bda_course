#!/usr/bin/env python3
# PySpark Promotion Pipeline (Refactored to read secrets from config.ini)

import configparser  ### NEW ###
import logging
import os
import smtplib
import sys
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (abs as spark_abs, col, concat_ws, count,
                                   isnan, lit, to_date, trim, upper, when)
from pyspark.sql.types import NumericType

# ===================== LOAD CONFIGURATION FROM FILE ===================== ### NEW ###
config = configparser.ConfigParser()
# Assumes config.ini is in the same directory as the script
config.read('config.ini')

db_password = config.get('database', 'password')
email_user = config.get('email', 'user')
email_password = config.get('email', 'password')

# ===================== MAIN CONFIG DICTIONARY =====================
# Secrets are now loaded from the config.ini file, not hardcoded here.
CONFIG = {
    "app_name": "PromotionPipeline",
    "log_dir": "logs",
    "data_dir": os.path.join(os.environ["USERPROFILE"], "Documents", "BDA2", "data"),
    "old_data_xls": "Promotion_data.xlsx",
    "new_data_xls": "Promotion_new_data.xlsx",
    "jdbc": {
        "url": "jdbc:sqlserver://localhost:1433;databaseName=datahub;encrypt=true;trustServerCertificate=true",
        "user": "sa",
        "password": db_password,  ### MODIFIED ###
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "table": "dbo.PromotionTable",
        "mode": "overwrite"
    },
    "email": {
        "host": "smtp.gmail.com",
        "port": 587,
        "user": email_user,      ### MODIFIED ###
        "password": email_password, ### MODIFIED ###
        "to": email_user # You can also move this to the config file if you want
    }
}

# ===================== HELPER FUNCTIONS =====================

def setup_logging(app_name: str, log_dir: str) -> (logging.Logger, str):
    """Configures and returns a logger and the log file path."""
    os.makedirs(log_dir, exist_ok=True)
    log_filename = datetime.now().strftime(os.path.join(log_dir, f"{app_name.lower()}_%Y%m%d_%H%M%S.log"))

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.FileHandler(log_filename, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(app_name), log_filename

def convert_excel_to_csv(xls_path: str, out_dir: str) -> str:
    """Converts the first sheet of an Excel file to a CSV file."""
    logger = logging.getLogger(CONFIG["app_name"])
    os.makedirs(out_dir, exist_ok=True)
    
    base_name = os.path.splitext(os.path.basename(xls_path))[0]
    out_file = os.path.join(out_dir, f"{base_name}.csv")

    try:
        logger.info(f"Converting {xls_path} to CSV...")
        xls = pd.ExcelFile(xls_path)
        df = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
        df.to_csv(out_file, index=False)
        logger.info(f"Successfully saved CSV to {out_file}")
        return out_file
    except Exception as e:
        logger.error(f"Failed to convert Excel file {xls_path}: {e}", exc_info=True)
        raise

def create_spark_session(app_name: str) -> SparkSession:
    """Creates and returns a Spark session."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logging.getLogger(app_name).info("Spark session created successfully.")
    return spark

def load_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """Loads a CSV file into a Spark DataFrame."""
    logger = logging.getLogger(CONFIG["app_name"])
    logger.info(f"Loading CSV from: {csv_path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_path)
    )
    logger.info(f"Loaded {df.count()} rows and {len(df.columns)} columns.")
    return df

def validate_dataframe(df: DataFrame, df_name: str):
    """Performs all data validation checks from the original script."""
    logger = logging.getLogger(CONFIG["app_name"])
    logger.info(f"Starting data validation for '{df_name}'...")

    # 1. Null counts per column
    null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).first().asDict()
    for c, n in null_counts.items():
        if n > 0:
            logger.warning(f"[NULL CHECK] Column '{c}' has {n} nulls in '{df_name}'.")

    # 2. Negative values check for specific numeric columns
    numeric_cols_to_check = ["Price", "Discount", "Units", "Sales $", "Gross Margin $", "# Transactions that contained the product"]
    for colname in numeric_cols_to_check:
        if colname in df.columns:
            negatives = df.filter(col(colname) < 0).count()
            if negatives > 0:
                logger.error(f"[NEGATIVE CHECK] Column '{colname}' has {negatives} negative values in '{df_name}'.")

    # 3. Discount between 0 and 1
    invalid_discounts = df.filter((col("Discount") < 0) | (col("Discount") > 1)).count()
    if invalid_discounts > 0:
        logger.error(f"[DISCOUNT CHECK] Found {invalid_discounts} invalid discount values (not between 0 and 1) in '{df_name}'.")

    # 4. 'On Flyer?' column contains only Yes/No
    invalid_on_flyer = df.filter(~col("On Flyer?").isin("Yes", "No")).count()
    if invalid_on_flyer > 0:
        logger.error(f"[ON FLYER CHECK] Found {invalid_on_flyer} invalid values in 'On Flyer?' column in '{df_name}'.")

    # 5. Year range check (2000-2030)
    invalid_years = df.filter((col("Year") < 2000) | (col("Year") > 2030)).count()
    if invalid_years > 0:
        logger.error(f"[YEAR CHECK] Found {invalid_years} invalid year values in '{df_name}'.")

    # 6. Week number between 1 and 53
    invalid_weeks = df.filter((col("week number") < 1) | (col("week number") > 53)).count()
    if invalid_weeks > 0:
        logger.error(f"[WEEK CHECK] Found {invalid_weeks} invalid week numbers in '{df_name}'.")

    # 7. Sales consistency check: Sales $ ≈ Units * Price * (1 - Discount)
    expected_sales = (col("Units") * col("Price") * (lit(1) - col("Discount")))
    sales_mismatch = df.filter(spark_abs(col("Sales $") - expected_sales) > 1e-2).count()
    if sales_mismatch > 0:
        logger.warning(f"[SALES CHECK] Found {sales_mismatch} sales consistency mismatches in '{df_name}'.")

    logger.info(f"Data validation finished for '{df_name}'.")

def transform_data(spark: SparkSession, df: DataFrame, view_name: str) -> DataFrame:
    """Transforms raw promotion data using Spark SQL."""
    logger = logging.getLogger(CONFIG["app_name"])
    
    # Clean column names for SQL compatibility
    clean_cols_df = df.toDF(*[c.strip().replace(" ", "_").replace("?", "").replace("$", "dollars").replace("#", "num") for c in df.columns])
    clean_cols_df.createOrReplaceTempView(view_name)
    logger.info(f"Temporary SQL view '{view_name}' created.")

    query = f"""
    SELECT
        Year,
        week_number,
        UPPER(TRIM(Product)) AS Product,
        Price,
        Discount,
        ROUND(Discount * 100, 2) AS Discount_Percent,
        ROUND(Price * (1 - Discount), 2) AS Final_Unit_Price,
        Units,
        Sales_dollars,
        Gross_Margin_dollars,
        ROUND((Gross_Margin_dollars / NULLIF(Sales_dollars, 0)) * 100, 2) AS Gross_Margin_Percent,
        CASE WHEN On_Flyer = 'Yes' THEN 1 ELSE 0 END AS On_Flyer_Flag,
        CASE WHEN Discount > 0.5 THEN 1 ELSE 0 END AS High_Discount_Flag,
        CASE 
            WHEN Units >= 100 THEN 'High'
            WHEN Units >= 50 THEN 'Medium'
            ELSE 'Low'
        END AS Sales_Category,
        -- Corrected date format pattern for Spark 3.0+
        to_date(concat_ws('-', Year, week_number), 'YYYY-ww') AS Week_Start_Date
    FROM {view_name}
    """
    transformed_df = spark.sql(query)
    logger.info(f"Transformation query executed successfully for '{view_name}'.")
    return transformed_df

def write_to_jdbc(df: DataFrame, jdbc_config: dict):
    """Writes a DataFrame to a JDBC data source."""
    logger = logging.getLogger(CONFIG["app_name"])
    logger.info(f"Writing {df.count()} rows to JDBC table: {jdbc_config['table']}")
    
    props = {"user": jdbc_config["user"], "password": jdbc_config["password"], "driver": jdbc_config["driver"]}
    
    try:
        df.write.jdbc(
            url=jdbc_config["url"], 
            table=jdbc_config["table"], 
            mode=jdbc_config["mode"], 
            properties=props
        )
        logger.info("Successfully wrote data to database.")
    except Exception as e:
        logger.error(f"JDBC write failed: {e}", exc_info=True)
        raise

def send_status_email(log_file: str, email_config: dict, success: bool):
    """Sends a pipeline status email with the log file attached."""
    logger = logging.getLogger(CONFIG["app_name"])

    if not all([email_config["user"], email_config["password"], email_config["to"]]):
        logger.warning("Email configuration is incomplete. Skipping email notification.")
        return

    status = "SUCCESS" if success else "FAILURE"
    subject = f"Promotion Pipeline Status: {status}"
    body = f"The Promotion Pipeline completed with status: {status}.\n\nPlease see the attached log file for details."

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = email_config["user"]
    msg["To"] = email_config["to"]
    msg.attach(MIMEText(body, "plain", _charset="utf-8"))

    if os.path.exists(log_file):
        with open(log_file, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(log_file))
            part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(log_file)}"')
            msg.attach(part)
        logger.info(f"Attached log file: {log_file}")

    try:
        with smtplib.SMTP(email_config["host"], email_config["port"]) as server:
            server.starttls()
            server.login(email_config["user"], email_config["password"])
            server.send_message(msg)
        logger.info("Status email sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send status email: {e}", exc_info=True)


# ===================== MAIN EXECUTION =====================
def main():
    """Main ETL pipeline orchestration function."""
    logger, log_filename = setup_logging(CONFIG["app_name"], CONFIG["log_dir"])
    pipeline_success = False
    spark = None
    
    try:
        # Step 1: Convert Excel files to CSV
        csv_dir = os.path.join(CONFIG["data_dir"], "csv")
        old_xls_path = os.path.join(CONFIG["data_dir"], CONFIG["old_data_xls"])
        new_xls_path = os.path.join(CONFIG["data_dir"], CONFIG["new_data_xls"])
        
        old_csv_path = convert_excel_to_csv(old_xls_path, csv_dir)
        new_csv_path = convert_excel_to_csv(new_xls_path, csv_dir)

        # Step 2: Initialize Spark
        spark = create_spark_session(CONFIG["app_name"])
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Step 3: Load and Validate Data
        df_old = load_data(spark, old_csv_path)
        validate_dataframe(df_old, "Promotion_data.csv")

        df_new = load_data(spark, new_csv_path)
        validate_dataframe(df_new, "Promotion_new_data.csv")

        # Step 4: Transform Data
        df_transformed_old = transform_data(spark, df_old, "promotion_data_old")
        df_transformed_new = transform_data(spark, df_new, "promotion_data_new")

        # Step 5: Combine Data
        df_combined = df_transformed_old.unionByName(df_transformed_new)
        logger.info(f"Combined dataframes. Total rows: {df_combined.count()}")
        df_combined.show(10, truncate=False)

        # Step 6: Load to Database
        write_to_jdbc(df_combined, CONFIG["jdbc"])

        pipeline_success = True
        logger.info("Promotion pipeline finished successfully.")

    except Exception as e:
        logger.error(f"Promotion pipeline failed: {e}", exc_info=True)
        pipeline_success = False

    finally:
        # Step 7: Send Notification
        send_status_email(log_filename, CONFIG["email"], pipeline_success)
        
        # Stop Spark Session
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()