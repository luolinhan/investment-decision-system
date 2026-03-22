@echo off
chcp 65001 >nul
cd /d C:\Users\Administrator\research_report_system

echo ============================================
echo 股票数据采集 - %date% %time%
echo ============================================

python fetch_stock_windows.py

echo.
echo ============================================
echo 采集完成
echo ============================================

REM 添加到日志
echo [%date% %time%] 数据采集完成 >> logs\stock_update.log