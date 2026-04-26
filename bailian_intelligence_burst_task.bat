@echo off
setlocal EnableDelayedExpansion
cd /d C:\Users\Administrator\research_report_system

if not exist logs mkdir logs
if not exist tmp mkdir tmp

set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHON_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe
if not exist "%PYTHON_EXE%" set PYTHON_EXE=python
set INTELLIGENCE_TRANSLATION_LIMIT=160
set BAILIAN_BURST_PASSES=3
set LOG_FILE=logs\bailian-intelligence-burst.log
set LOCK_DIR=tmp\bailian-intelligence.lock

powershell -NoProfile -Command "& { $busy = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -ieq 'python.exe' -and ($_.CommandLine -like '*scripts\\translate_intelligence.py*' -or $_.CommandLine -like '*scripts\\sync_intelligence.py*') }); if ($busy.Count -gt 0) { exit 9 } }"
if errorlevel 9 (
  echo ==== [%date% %time%] Bailian intelligence burst skipped: process active ====>> "%LOG_FILE%"
  exit /b 0
)

mkdir "%LOCK_DIR%" 2>nul
if errorlevel 1 (
  echo ==== [%date% %time%] Bailian intelligence burst skipped: lock active ====>> "%LOG_FILE%"
  exit /b 0
)

set RC=0
echo ==== [%date% %time%] Bailian intelligence burst start ====>> "%LOG_FILE%"
for /l %%I in (1,1,%BAILIAN_BURST_PASSES%) do (
  echo ==== [%date% %time%] Bailian intelligence burst pass %%I start ====>> "%LOG_FILE%"
  "%PYTHON_EXE%" scripts\translate_intelligence.py >> "%LOG_FILE%" 2>&1
  set PASS_RC=!ERRORLEVEL!
  echo ==== [%date% %time%] Bailian intelligence burst pass %%I end rc=!PASS_RC! ====>> "%LOG_FILE%"
  if not "!PASS_RC!"=="0" set RC=!PASS_RC!
  if %%I LSS %BAILIAN_BURST_PASSES% timeout /t 20 /nobreak >nul
)
echo ==== [%date% %time%] Bailian intelligence burst end rc=%RC% ====>> "%LOG_FILE%"
rmdir "%LOCK_DIR%" >nul 2>&1
exit /b %RC%
