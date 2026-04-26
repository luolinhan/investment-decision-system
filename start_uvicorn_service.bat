@echo off
setlocal

powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\Administrator\research_report_system\start_uvicorn_service.ps1
exit /b %ERRORLEVEL%
