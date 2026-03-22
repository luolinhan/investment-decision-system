#!/usr/bin/env python3
"""通过SSH传输文件到Windows服务器"""
import subprocess
import os
import base64

# 本地项目目录
LOCAL_DIR = "/Users/lhluo/research_report_system"
# 远程目录
REMOTE_DIR = "C:/Projects/research_report_system"

# 要传输的文件列表
FILES = [
    "requirements.txt",
    "start.bat",
    "README.md",
    ".env.example",
    "app/__init__.py",
    "app/config.py",
    "app/database.py",
    "app/main.py",
    "app/models.py",
    "app/routers/__init__.py",
    "app/routers/pages.py",
    "app/routers/reports.py",
    "app/services/__init__.py",
    "app/services/collector.py",
    "app/services/eastmoney_source.py",
    "app/services/hibor_source.py",
    "app/utils/__init__.py",
    "templates/base.html",
    "templates/index.html",
    "templates/report_detail.html",
    "templates/reports.html",
    "templates/settings.html",
    "templates/stocks.html",
]

def transfer_file(local_path, remote_path):
    """传输单个文件"""
    with open(local_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 使用base64编码避免转义问题
    encoded = base64.b64encode(content.encode('utf-8')).decode('ascii')

    # 使用PowerShell解码并写入文件
    ps_cmd = f'''
$content = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String("{encoded}"))
Set-Content -Path "{remote_path}" -Value $content -Encoding UTF8
'''

    cmd = ['ssh', 'win-exec', 'powershell', '-Command', ps_cmd]
    result = subprocess.run(cmd, capture_output=True)

    if result.returncode != 0:
        print(f"Error transferring {local_path}: {result.stderr.decode('utf-8', errors='ignore')}")
        return False
    return True

def main():
    print("开始传输文件到Windows服务器...")

    success = 0
    failed = 0

    for file in FILES:
        local_path = os.path.join(LOCAL_DIR, file)
        remote_path = f"{REMOTE_DIR}/{file}".replace('/', '\\')

        if not os.path.exists(local_path):
            print(f"文件不存在: {local_path}")
            failed += 1
            continue

        if transfer_file(local_path, remote_path):
            print(f"✓ {file}")
            success += 1
        else:
            print(f"✗ {file}")
            failed += 1

    print(f"\n传输完成: 成功 {success}, 失败 {failed}")

if __name__ == '__main__':
    main()