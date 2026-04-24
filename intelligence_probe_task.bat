@echo off
cd /d C:\Users\Administrator\research_report_system
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
python scripts\sync_intelligence.py
python scripts\translate_intelligence.py
