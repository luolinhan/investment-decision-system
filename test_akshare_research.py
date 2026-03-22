"""
采集研报 - 使用AKShare按股票代码获取
"""
import asyncio
import sys
import os
sys.path.insert(0, '.')

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

import akshare as ak
import pandas as pd
from datetime import date

# 关注的股票代码 (AKShare格式)
STOCKS = {
    # A股光伏
    "002459": "晶澳科技",
    "600438": "通威股份",
    "601012": "隆基绿能",
    "300763": "锦浪科技",
    # A股医药
    "688235": "百济神州",
    "603259": "药明康德",
    "600196": "复星医药",
    # A股消费
    "601888": "中国中免",
}

def test_akshare_api():
    """测试AKShare研报接口"""
    print("=== 测试AKShare研报接口 ===\n")

    # 测试1: 默认获取
    print("1. 默认获取研报:")
    try:
        df = ak.stock_research_report_em()
        print(f"   获取到 {len(df)} 条")
        print(f"   列名: {list(df.columns)}")
        # 显示前几条
        print(df.head(3).to_string())
    except Exception as e:
        print(f"   失败: {e}")

    # 测试2: 按股票代码获取
    print("\n2. 按股票代码获取 (晶澳科技 002459):")
    try:
        df = ak.stock_research_report_em(symbol="002459")
        print(f"   获取到 {len(df)} 条")
        if len(df) > 0:
            print(df.head(3).to_string())
    except Exception as e:
        print(f"   失败: {e}")

    # 测试3: 按股票代码获取 (通威股份 600438)
    print("\n3. 按股票代码获取 (通威股份 600438):")
    try:
        df = ak.stock_research_report_em(symbol="600438")
        print(f"   获取到 {len(df)} 条")
        if len(df) > 0:
            print(df.head(3).to_string())
    except Exception as e:
        print(f"   失败: {e}")


if __name__ == "__main__":
    test_akshare_api()