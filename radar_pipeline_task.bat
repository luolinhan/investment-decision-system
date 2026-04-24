@echo off
setlocal

cd /d %~dp0

if not exist logs mkdir logs

echo ============================================
echo Radar Pipeline Task - %date% %time%
echo ============================================

python scripts\sync_radar_pipeline.py >> logs\radar_pipeline.log 2>&1
set PIPELINE_EXIT=%ERRORLEVEL%

python scripts\export_radar_reports.py --report due --force-refresh >> logs\radar_reports.log 2>&1
set REPORT_EXIT=%ERRORLEVEL%

if %PIPELINE_EXIT% NEQ 0 (
    echo Pipeline exit code: %PIPELINE_EXIT% >> logs\radar_pipeline.log
)
if %REPORT_EXIT% NEQ 0 (
    echo Report exit code: %REPORT_EXIT% >> logs\radar_reports.log
)

if %REPORT_EXIT% NEQ 0 (
    set EXIT_CODE=%REPORT_EXIT%
) else (
    set EXIT_CODE=0
)

echo Pipeline exit code: %PIPELINE_EXIT%
echo Report exit code: %REPORT_EXIT%
echo Exit code: %EXIT_CODE%
exit /b %EXIT_CODE%
