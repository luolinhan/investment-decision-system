@echo off
setlocal

cd /d C:\Users\Administrator\research_report_system

powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\Administrator\research_report_system\start_investment_hub.ps1

exit /b %ERRORLEVEL%
