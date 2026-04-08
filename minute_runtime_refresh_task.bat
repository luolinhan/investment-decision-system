@echo off
cd /d "%~dp0"

if not exist logs mkdir logs

python minute_runtime_refresh.py >> logs\minute_runtime_refresh.log 2>&1

