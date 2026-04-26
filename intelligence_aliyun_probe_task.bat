@echo off
cd /d C:\Users\Administrator\research_report_system
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHON_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python311\python.exe
if not exist "%PYTHON_EXE%" set PYTHON_EXE=python
set REMOTE_HOST=admin@47.88.90.29
set REMOTE_DIR=/home/admin/investment-intel-collector
set SSH_KEY=%USERPROFILE%\.ssh\openclaw.pem
if exist C:\Users\Administrator\.ssh\openclaw.pem set SSH_KEY=C:\Users\Administrator\.ssh\openclaw.pem
set LOCAL_BUNDLE=data\intelligence_bundle.aliyun.json

ssh -i "%SSH_KEY%" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 %REMOTE_HOST% "cd %REMOTE_DIR% && mkdir -p data && INTELLIGENCE_COLLECTOR_MODE=aliyun INTELLIGENCE_SEARCH_PROXY_URL=http://127.0.0.1:8899 python3 scripts/sync_intelligence.py --collect-only --output data/intelligence_bundle.json --no-bootstrap"
if errorlevel 1 goto local_fallback

scp -i "%SSH_KEY%" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 %REMOTE_HOST%:%REMOTE_DIR%/data/intelligence_bundle.json "%LOCAL_BUNDLE%"
if errorlevel 1 goto local_fallback

"%PYTHON_EXE%" scripts\sync_intelligence.py --import-json "%LOCAL_BUNDLE%"
if errorlevel 1 goto local_fallback

"%PYTHON_EXE%" scripts\translate_intelligence.py
exit /b %errorlevel%

:local_fallback
"%PYTHON_EXE%" scripts\sync_intelligence.py
"%PYTHON_EXE%" scripts\translate_intelligence.py
