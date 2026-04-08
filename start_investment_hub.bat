@echo off
cd /d C:\Users\Administrator\research_report_system
C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --no-access-log
