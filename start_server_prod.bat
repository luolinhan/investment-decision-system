@echo off
setlocal

REM ===== Investment Hub Production Start Script =====
set "PROJECT_DIR=C:\Users\Administrator\research_report_system"
set "VENV_PYTHON=%PROJECT_DIR%\venv\Scripts\python.exe"
set "SYSTEM_PYTHON=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\server.log"
set "PID_FILE=%LOG_DIR%\uvicorn.pid"
set "HOST=0.0.0.0"
set "PORT=8080"

if not exist "%PROJECT_DIR%" (
  echo [ERROR] Project directory not found: %PROJECT_DIR%
  exit /b 1
)

cd /d "%PROJECT_DIR%"

if not exist "%LOG_DIR%" (
  mkdir "%LOG_DIR%"
)

set "PYTHON_EXE="
if exist "%VENV_PYTHON%" set "PYTHON_EXE=%VENV_PYTHON%"
if not defined PYTHON_EXE if exist "%SYSTEM_PYTHON%" set "PYTHON_EXE=%SYSTEM_PYTHON%"
if not defined PYTHON_EXE (
  where python >nul 2>nul
  if not errorlevel 1 set "PYTHON_EXE=python"
)

if not defined PYTHON_EXE (
  echo [ERROR] Python executable not found. Checked:
  echo         1) %VENV_PYTHON%
  echo         2) %SYSTEM_PYTHON%
  echo         3) PATH (python)
  exit /b 2
)

set "PYTHONUNBUFFERED=1"
set "PYTHONDONTWRITEBYTECODE=1"
set "TZ=Asia/Shanghai"
set "APP_ENV=production"
set "UVICORN_WORKERS=1"

echo [INFO] ==================================================
echo [INFO] Starting Investment Hub API
echo [INFO] Time: %date% %time%
echo [INFO] Project: %PROJECT_DIR%
echo [INFO] Python: %PYTHON_EXE%
echo [INFO] Listen: http://%HOST%:%PORT%
echo [INFO] Log: %LOG_FILE%
echo [INFO] ==================================================

for /f "tokens=5" %%p in ('netstat -ano ^| findstr LISTENING ^| findstr :%PORT%') do (
  taskkill /f /pid %%p >nul 2>nul
)

if exist "%PID_FILE%" del /f /q "%PID_FILE%" >nul 2>nul

start "investment-hub-uvicorn" /min cmd /c ""%PYTHON_EXE%" -m uvicorn app.main:app --host %HOST% --port %PORT% --workers %UVICORN_WORKERS% --no-access-log >> "%LOG_FILE%" 2>&1"

timeout /t 2 /nobreak >nul

for /f "tokens=5" %%p in ('netstat -ano ^| findstr LISTENING ^| findstr :%PORT%') do (
  > "%PID_FILE%" echo %%p
  goto :pid_done
)

:pid_done
if exist "%PID_FILE%" (
  echo [INFO] Started. PID recorded in %PID_FILE%.
) else (
  echo [WARN] Started, but PID was not captured. Check logs and netstat.
)

echo [INFO] Tail logs with: powershell -Command "Get-Content -Path '%LOG_FILE%' -Wait"
exit /b 0
