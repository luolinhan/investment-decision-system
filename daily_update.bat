@echo off
chcp 65001 >nul
cd /d C:\Users\Administrator\research_report_system

echo ============================================
echo 每日数据更新 - %date% %time%
echo ============================================

python daily_update_all.py
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================
echo 更新完成
echo ============================================

echo [%date% %time%] 数据更新完成 >> logs\daily_update.log