#!/usr/bin/env python3
"""通过SFTP传输文件到Windows服务器"""
import paramiko
import os

# 连接配置
HOST = "192.168.3.87"  # 内网IP
USERNAME = "Administrator"
KEY_FILE = os.path.expanduser("~/.ssh/id_ed25519_win")

# 目录配置
LOCAL_DIR = "/Users/lhluo/research_report_system"
REMOTE_DIR = "C:/Projects/research_report_system"

# 要传输的文件
FILES = [
    "app/routers/reports.py",
    "app/services/collector.py",
    "app/services/eastmoney_source.py",
    "app/services/hibor_source.py",
    "templates/base.html",
    "templates/index.html",
    "templates/reports.html",
    "templates/settings.html",
    "templates/stocks.html",
]

def main():
    print("连接到Windows服务器...")

    # 加载私钥
    key = paramiko.Ed25519Key.from_private_key_file(KEY_FILE)

    # 连接
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USERNAME, pkey=key)

    sftp = ssh.open_sftp()

    print("创建目录...")
    # 使用SSH创建目录（更可靠）
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\app\\routers"')
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\app\\services"')
    ssh.exec_command('cmd /c "mkdir C:\\Projects\\research_report_system\\templates"')

    print("传输文件...")

    for file in FILES:
        local_path = os.path.join(LOCAL_DIR, file)
        # 使用Windows路径格式
        remote_path = f"C:/Projects/research_report_system/{file}".replace('/', '\\')

        try:
            sftp.put(local_path, remote_path)
            print(f"✓ {file}")
        except Exception as e:
            print(f"✗ {file}: {e}")

    sftp.close()
    ssh.close()
    print("传输完成")

if __name__ == '__main__':
    main()