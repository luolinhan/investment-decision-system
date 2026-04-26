@echo off
cd /d C:\Users\Administrator\research_report_system

if not exist logs mkdir logs
if not exist tmp mkdir tmp

set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set LOG_FILE=logs\bailian-intelligence-aliyun.log
set LOCK_DIR=tmp\bailian-intelligence.lock

powershell -NoProfile -Command "& { $busy = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -ieq 'python.exe' -and ($_.CommandLine -like '*scripts\\translate_intelligence.py*' -or $_.CommandLine -like '*scripts\\sync_intelligence.py*') }); if ($busy.Count -gt 0) { exit 9 } }"
if errorlevel 9 (
  echo ==== [%date% %time%] Bailian intelligence aliyun skipped: process active ====>> "%LOG_FILE%"
  exit /b 0
)

mkdir "%LOCK_DIR%" 2>nul
if errorlevel 1 (
  echo ==== [%date% %time%] Bailian intelligence aliyun skipped: lock active ====>> "%LOG_FILE%"
  exit /b 0
)

echo ==== [%date% %time%] Bailian intelligence aliyun start ====>> "%LOG_FILE%"
call intelligence_aliyun_probe_task.bat >> "%LOG_FILE%" 2>&1
set RC=%ERRORLEVEL%
echo ==== [%date% %time%] Bailian intelligence aliyun end rc=%RC% ====>> "%LOG_FILE%"
rmdir "%LOCK_DIR%" >nul 2>&1
exit /b %RC%
