@echo off
chcp 65001 >nul
cd /d C:\Users\Administrator\research_report_system
echo ============================================
echo 股票数据采集 - %date% %time%
echo ============================================
python fetch_stock_data.py
echo.
echo 采集完成