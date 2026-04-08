@echo off
set QW_SCAN_ALL=1
set QW_SCAN_LIMIT=400
cd /d C:\Users\Administrator\research_report_system
C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe sync_quant_workbench.py >> C:\Users\Administrator\research_report_system\logs\quant_workbench-sync.log 2>&1
