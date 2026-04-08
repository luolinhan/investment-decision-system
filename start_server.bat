@echo off  
cd /d C:\Users\Administrator\research_report_system  
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 
