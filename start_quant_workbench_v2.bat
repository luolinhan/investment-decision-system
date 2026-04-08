@echo off
cd /d C:\Users\Administrator\research_report_system
set QW_SCAN_ALL=1
set QW_SCAN_LIMIT=800
echo Starting Quant Workbench with HS300+ZZ500...
C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe -m uvicorn quant_workbench.app:app --host 0.0.0.0 --port 8010 >> logs\quant_workbench-console.log 2>&1