@echo off
chcp 65001 >nul
cd /d C:\Projects\research_report_system

echo 启动研报下载系统...

call venv\Scripts\activate.bat

echo 服务启动中...
echo 访问地址:
echo   本地: http://localhost:8080
echo   远程: http://100.64.93.19:8080
echo.

python -m uvicorn app.main:app --host 0.0.0.0 --port 8080

pause