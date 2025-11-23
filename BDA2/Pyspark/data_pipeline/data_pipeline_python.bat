@echo off
REM =================================================================
REM BATCH FILE TO RUN PYSPARK SCRIPT WITH python.exe
REM (Corrected and using %USERPROFILE%)
REM =================================================================

REM --- SET YOUR VARIABLES HERE ---

REM ### FIX 1: JAVA_HOME IS REQUIRED FOR PYSPARK TO WORK ###
REM 1. Set the path to your Java installation
REM SET JAVA_HOME=C:\Program Files\Java\jdk1.8.0_331

REM ### FIX 2: USE THE FULL, SPECIFIC PATH TO YOUR SPARK INSTALLATION ###
REM 2. Set the path to your Spark installation
REM SET SPARK_HOME=C:\spark\

REM ### FIX 3: USE THE FULL, ABSOLUTE PATH TO python.exe ###
REM 3. Set the path to your python.exe
REM SET PYTHON_EXECUTABLE=C:\ProgramData\Anaconda3\python.exe

REM ### YOUR REQUEST: Using %USERPROFILE% for the script directory ###
REM 4. Set the path to the directory containing your script
SET SCRIPT_DIR=%USERPROFILE%\Downloads

REM 5. Set the name of your python script
SET PYTHON_SCRIPT=data_pipeline_prod.py

REM 6. Define a log directory
SET LOG_DIR=%USERPROFILE%\Downloads\pipeline_logs
REM ---------------------------------

REM Automatically create the log directory if it doesn't exist
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Change the current directory to the script's directory
cd /d "%SCRIPT_DIR%"

echo =================================================================
echo Starting Promotion Pipeline at %date% %time%
echo Script Directory: %SCRIPT_DIR%
echo Python Executable: %PYTHON_EXECUTABLE%
echo =================================================================

REM Run the python script and redirect output to a log file
"%PYTHON_EXECUTABLE%" "%PYTHON_SCRIPT%" >> "%LOG_DIR%\pipeline_run_log.txt" 2>&1

echo.
echo Pipeline execution finished at %date% %time%
echo See log file in %LOG_DIR%
echo =================================================================
echo.

REM Keep the window open
PAUSE