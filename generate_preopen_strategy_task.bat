@echo off
cd /d "%~dp0"

if not exist logs mkdir logs

python generate_preopen_strategy.py >> logs\generate_preopen_strategy.log 2>&1

