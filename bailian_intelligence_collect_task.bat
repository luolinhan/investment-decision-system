@echo off
cd /d C:\Users\Administrator\research_report_system

if not exist logs mkdir logs
if not exist tmp mkdir tmp

set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHON_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe
if not exist "%PYTHON_EXE%" set PYTHON_EXE=python
set INTELLIGENCE_TRANSLATION_LIMIT=100
set LOG_FILE=logs\bailian-intelligence-collect.log
set LOCK_DIR=tmp\bailian-intelligence.lock

powershell -NoProfile -Command "& { $busy = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -ieq 'python.exe' -and ($_.CommandLine -like '*scripts\\translate_intelligence.py*' -or $_.CommandLine -like '*scripts\\sync_intelligence.py*') }); if ($busy.Count -gt 0) { exit 9 } }"
if errorlevel 9 (
  echo ==== [%date% %time%] Bailian intelligence collect skipped: process active ====>> "%LOG_FILE%"
  exit /b 0
)

mkdir "%LOCK_DIR%" 2>nul
if errorlevel 1 (
  echo ==== [%date% %time%] Bailian intelligence collect skipped: lock active ====>> "%LOG_FILE%"
  exit /b 0
)

echo ==== [%date% %time%] Bailian intelligence collect start ====>> "%LOG_FILE%"
"%PYTHON_EXE%" scripts\sync_intelligence.py >> "%LOG_FILE%" 2>&1
set SYNC_RC=%ERRORLEVEL%
"%PYTHON_EXE%" scripts\translate_intelligence.py >> "%LOG_FILE%" 2>&1
set TRANSLATE_RC=%ERRORLEVEL%
echo ==== [%date% %time%] Bailian intelligence collect end sync_rc=%SYNC_RC% translate_rc=%TRANSLATE_RC% ====>> "%LOG_FILE%"
rmdir "%LOCK_DIR%" >nul 2>&1

if not "%TRANSLATE_RC%"=="0" exit /b %TRANSLATE_RC%
exit /b %SYNC_RC%
