#!/usr/bin/env python
"""
通过 Clash Verge 管道切换节点
"""

import subprocess
import json
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

def send_pipe_command(command):
    """通过命名管道发送命令"""
    pipe_name = r"\\.\pipe\verge-mihomo"

    # 使用 PowerShell 发送命令
    ps_script = f'''
    $pipe = New-Object System.IO.Pipes.NamedPipeClientStream(".", "verge-mihomo", [System.IO.Pipes.PipeDirection]::InOut)
    $pipe.Connect(5000)
    $reader = New-Object System.IO.StreamReader($pipe)
    $writer = New-Object System.IO.StreamWriter($pipe)
    $writer.AutoFlush = $true

    # 发送命令
    $writer.WriteLine('{command}')
    $writer.Flush()

    # 读取响应
    Start-Sleep -Milliseconds 100
    while ($pipe.IsConnected) {{
        $response = $reader.ReadLine()
        if ($response) {{
            Write-Output $response
        }}
    }}

    $pipe.Close()
    '''

    result = subprocess.run(
        ["powershell", "-Command", ps_script],
        capture_output=True,
        text=True,
        timeout=10
    )

    return result.stdout, result.stderr


def switch_node_via_api():
    """尝试通过REST API切换节点"""
    import requests

    api_url = "http://127.0.0.1:9097"

    # 获取代理组
    try:
        resp = requests.get(f"{api_url}/proxies", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print("代理组列表:")
            for name, info in data.get("proxies", {}).items():
                if info.get("type") == "Selector":
                    print(f"  - {name}")
                    # 尝试切换到美国-05
                    for proxy in info.get("all", []):
                        if "美国-05" in proxy:
                            switch_resp = requests.put(
                                f"{api_url}/proxies/{name}",
                                json={"name": proxy},
                                timeout=5
                            )
                            print(f"切换到 {proxy}: {switch_resp.status_code}")
                            return True
            return False
    except Exception as e:
        print(f"API错误: {e}")
        return False


def main():
    print("="*60)
    print("尝试切换到 美国-05 节点")
    print("="*60)

    # 方法1: 通过API
    print("\n[1] 尝试通过 REST API...")
    if switch_node_via_api():
        print("节点切换成功!")
        return

    # 方法2: 通过命名管道
    print("\n[2] 尝试通过命名管道...")
    try:
        stdout, stderr = send_pipe_command("GET /proxies HTTP/1.1")
        print(f"响应: {stdout}")
    except Exception as e:
        print(f"管道错误: {e}")

    print("\n未能自动切换节点，请手动操作:")
    print("1. 打开 Clash Verge 应用")
    print("2. 点击 '代理' 标签")
    print("3. 选择 '美国-05' 节点")


if __name__ == "__main__":
    main()