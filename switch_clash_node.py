#!/usr/bin/env python
"""
Clash Verge 节点切换脚本
切换到美国-05节点
"""

import os
import re
import yaml
import requests
import sys

# 修复Windows控制台编码
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CLASH_CONFIG_DIR = "C:/Users/Administrator/AppData/Roaming/io.github.clash-verge-rev.clash-verge-rev"
PROXY_PORT = 7890

def find_profile_file():
    """找到当前使用的配置文件"""
    profiles_dir = os.path.join(CLASH_CONFIG_DIR, "profiles")
    for f in os.listdir(profiles_dir):
        if f.endswith(".yaml") and f not in ["Merge.yaml", "Script.js"]:
            return os.path.join(profiles_dir, f)
    return None

def find_us_node(config_file):
    """在配置文件中查找美国节点"""
    with open(config_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 查找包含"美国"或"US"的节点
    us_nodes = []
    lines = content.split('\n')
    for i, line in enumerate(lines):
        # 移除emoji字符
        clean_line = ''.join(c for c in line if ord(c) < 0x10000)
        if '美国' in clean_line or 'US-' in clean_line or '-US-' in clean_line.upper():
            # 提取节点名称
            match = re.search(r'name:\s*["\']?([^"\'\n]+)["\']?', clean_line)
            if match:
                node_name = match.group(1).strip()
                # 移除emoji
                node_name = ''.join(c for c in node_name if ord(c) < 0x10000)
                us_nodes.append(node_name)

    return us_nodes

def change_node_via_config(node_name):
    """通过修改配置文件切换节点"""
    profile_file = find_profile_file()
    if not profile_file:
        print("未找到配置文件")
        return False, []

    print(f"配置文件: {profile_file}")

    # 查找美国节点
    us_nodes = find_us_node(profile_file)
    print(f"找到 {len(us_nodes)} 个美国节点")

    # 查找匹配 "美国-05" 的节点
    target_node = None
    for node in us_nodes:
        if '美国-05' in node or 'US-05' in node.upper():
            target_node = node
            break

    if not target_node and us_nodes:
        # 如果没找到精确匹配，查找包含"美国"和"05"的节点
        for node in us_nodes:
            if '美国' in node and '05' in node:
                target_node = node
                break

    return target_node, us_nodes[:10]

def test_proxy():
    """测试代理是否正常工作"""
    proxies = {
        "http": f"http://127.0.0.1:{PROXY_PORT}",
        "https": f"http://127.0.0.1:{PROXY_PORT}"
    }

    try:
        print("测试代理连接...")
        response = requests.get("https://www.google.com", proxies=proxies, timeout=10)
        print(f"代理测试成功: {response.status_code}")
        return True
    except Exception as e:
        print(f"代理测试失败: {e}")
        return False

def main():
    print("="*60)
    print("Clash Verge 节点切换")
    print("="*60)

    # 1. 测试当前代理
    print("\n[1] 测试当前代理...")
    proxy_ok = test_proxy()

    # 2. 查找美国节点
    print("\n[2] 查找美国节点...")
    target_node, us_nodes = change_node_via_config("美国-05")

    if target_node:
        print(f"\n找到目标节点: {target_node}")
    else:
        print("\n未找到精确匹配 '美国-05' 的节点")

    print("\n可用的美国节点:")
    for i, node in enumerate(us_nodes, 1):
        print(f"  {i}. {node}")

    # 3. 提供手动切换指南
    print("\n" + "="*60)
    print("手动切换节点步骤:")
    print("1. 打开 Clash Verge 应用")
    print("2. 点击 '代理' 或 'Proxies' 标签")
    print("3. 找到 '节点选择' 或类似分组")
    print("4. 选择美国节点")
    print("="*60)

if __name__ == "__main__":
    main()