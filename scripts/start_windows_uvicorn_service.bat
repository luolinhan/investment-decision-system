@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set REPO_DIR=%%~fI
cd /d "%REPO_DIR%"
set PYTHONW=%LocalAppData%\Programs\Python\Python311\pythonw.exe
if not exist "%PYTHONW%" set PYTHONW=pythonw
"%PYTHONW%" "%SCRIPT_DIR%windows_run_uvicorn_service.py"
