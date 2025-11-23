@echo off
REM =================================================================
REM BATCH FILE TO RUN PYSPARK - FINAL VERSION WITH CALL
REM =================================================================

REM --- SET YOUR VARIABLES HERE (USE FULL PATHS!) ---

REM 1. Set the path to your Java installation
SET JAVA_HOME=C:\Program Files\Java\jdk1.8.0_331

REM 2. Set the path to your Spark installation
SET SPARK_HOME=C:\spark

REM 3. Set the full, absolute path to your python.exe
SET PYTHON_EXECUTABLE=C:\ProgramData\Anaconda3\python.exe

REM 4. Set the path to the directory containing your script
SET SCRIPT_DIR=%USERPROFILE%\Downloads

REM 5. Set the name of your python script
SET PYTHON_SCRIPT=data_pipeline_prod.py

REM 6. Set the full path to your SQL Server JDBC driver JAR file
SET JDBC_DRIVER_PATH=C:\spark\jars\mssql-jdbc-12.10.0.jre8.jar

REM 7. Set the path to the directory where you want to save the batch script's logs
SET LOG_DIR=%USERPROFILE%\Downloads\pipeline_logs
REM ---------------------------------

REM --- SCRIPT EXECUTION ---

REM Automatically create the log directory if it does not exist
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Change the current directory to the script's directory
cd /d "%SCRIPT_DIR%"

echo =================================================================
echo Starting Promotion Pipeline via spark-submit at %date% %time%
echo.
echo Using Spark from: "%SPARK_HOME%"
echo Using Python executable: "%PYTHON_EXECUTABLE%"
echo.
echo =================================================================

REM ### FINAL FIX - Use CALL to ensure control returns to this script ###
CALL "%SPARK_HOME%\bin\spark-submit.cmd" ^
    --master local[*] ^
    --driver-memory 1g ^
    --conf spark.pyspark.python="%PYTHON_EXECUTABLE%" ^
    --driver-class-path "%JDBC_DRIVER_PATH%" ^
    "%PYTHON_SCRIPT%" >> "%LOG_DIR%\pipeline_run_log.txt" 2>&1

echo.
echo Pipeline execution finished at %date% %time%
echo See the log file in: %LOG_DIR%
echo =================================================================
echo.

REM This command will now be reached
PAUSE