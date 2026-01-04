@echo off
echo Starting Automatic Weather Data Collector...
echo This script will run the ETL pipeline every 24 hours.
echo KEEP THIS WINDOW OPEN to continue collecting data.
python scheduler.py
pause
