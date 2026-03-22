# 使用Python后台运行服务
import subprocess
import os
import sys

os.chdir(r'C:\Projects\research_report_system')

# 使用pythonw无窗口运行
subprocess.Popen([
    sys.executable.replace('python.exe', 'pythonw.exe'),
    '-m', 'uvicorn', 'app.main:app',
    '--host', '0.0.0.0',
    '--port', '8080'
])