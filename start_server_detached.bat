@echo off
setlocal

set "PROJECT_DIR=C:\Users\Administrator\research_report_system"
set "PYTHON_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\server.log"

cd /d "%PROJECT_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "tokens=5" %%p in ('netstat -ano ^| findstr LISTENING ^| findstr :8080') do (
  taskkill /f /pid %%p >nul 2>nul
)

start "investment-hub-uvicorn" /min cmd /c ""%PYTHON_EXE%" -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1 --no-access-log >> "%LOG_FILE%" 2>&1"
