@echo off
cd /d C:\Users\Administrator\research_report_system
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHON_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe
if not exist "%PYTHON_EXE%" set PYTHON_EXE=python
"%PYTHON_EXE%" scripts\sync_intelligence.py
"%PYTHON_EXE%" scripts\translate_intelligence.py
