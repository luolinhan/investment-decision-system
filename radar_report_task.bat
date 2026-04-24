@echo off
setlocal

cd /d %~dp0

if not exist logs mkdir logs

echo ============================================
echo Radar Report Task - %date% %time%
echo ============================================

python scripts\export_radar_reports.py --report due --force-refresh >> logs\radar_reports.log 2>&1
set EXIT_CODE=%ERRORLEVEL%

echo Exit code: %EXIT_CODE%
exit /b %EXIT_CODE%
