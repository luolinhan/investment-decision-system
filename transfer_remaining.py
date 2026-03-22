#!/usr/bin/env python3
"""传输剩余文件"""
import paramiko
import os

HOST = "192.168.3.87"
USERNAME = "Administrator"
KEY_FILE = os.path.expanduser("~/.ssh/id_ed25519_win")
LOCAL_DIR = "/Users/lhluo/research_report_system"

# 根目录文件
FILES = [
    "requirements.txt",
    "start.bat",
    "README.md",
    ".env.example",
]

def main():
    key = paramiko.Ed25519Key.from_private_key_file(KEY_FILE)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USERNAME, pkey=key)

    sftp = ssh.open_sftp()

    for file in FILES:
        local_path = os.path.join(LOCAL_DIR, file)
        remote_path = f"C:\\Projects\\research_report_system\\{file}"
        try:
            sftp.put(local_path, remote_path)
            print(f"✓ {file}")
        except Exception as e:
            print(f"✗ {file}: {e}")

    # 传输app目录下的文件
    app_files = [
        "app/__init__.py",
        "app/config.py",
        "app/database.py",
        "app/main.py",
        "app/models.py",
    ]
    for file in app_files:
        local_path = os.path.join(LOCAL_DIR, file)
        remote_path = f"C:\\Projects\\research_report_system\\{file}".replace('/', '\\')
        try:
            sftp.put(local_path, remote_path)
            print(f"✓ {file}")
        except Exception as e:
            print(f"✗ {file}: {e}")

    # 传输routers目录下的__init__.py
    sftp.put(
        os.path.join(LOCAL_DIR, "app/routers/__init__.py"),
        "C:\\Projects\\research_report_system\\app\\routers\\__init__.py"
    )
    print("✓ app/routers/__init__.py")

    # 传输services目录下的__init__.py
    sftp.put(
        os.path.join(LOCAL_DIR, "app/services/__init__.py"),
        "C:\\Projects\\research_report_system\\app\\services\\__init__.py"
    )
    print("✓ app/services/__init__.py")

    # 传输utils目录
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\app\\utils"')
    sftp.put(
        os.path.join(LOCAL_DIR, "app/utils/__init__.py"),
        "C:\\Projects\\research_report_system\\app\\utils\\__init__.py"
    )
    print("✓ app/utils/__init__.py")

    # 创建其他必要目录
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\data"')
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\data\\pdfs"')
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\logs"')
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\static"')
    print("✓ 目录创建完成")

    sftp.close()
    ssh.close()
    print("传输完成")

if __name__ == '__main__':
    main()