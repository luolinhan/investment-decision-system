@echo off
cd /d "%~dp0"

if not exist logs mkdir logs

python generate_llm_daily_brief.py --force-refresh --label scheduled >> logs\generate_llm_daily_brief.log 2>&1
