@echo off
setlocal

cd /d C:\Users\Administrator\research_report_system

if not exist logs mkdir logs

powershell -NoProfile -Command "$conn = @(Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)[0]; if ($null -ne $conn) { Stop-Process -Id $conn.OwningProcess -Force }; Start-Process -FilePath 'C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe' -WorkingDirectory 'C:\Users\Administrator\research_report_system' -ArgumentList '-m uvicorn app.main:app --host 0.0.0.0 --port 8080 --no-access-log' -RedirectStandardOutput 'C:\Users\Administrator\research_report_system\logs\investment_hub.out.log' -RedirectStandardError 'C:\Users\Administrator\research_report_system\logs\investment_hub.err.log' -WindowStyle Hidden"

exit /b %ERRORLEVEL%
