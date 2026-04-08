#!/usr/bin/env python3
"""
阿里巴巴港股股价推送到 Apple Watch (via Bark)
运行时间：港股交易时段 09:30-12:00, 13:00-16:00
"""

import requests
import json
from datetime import datetime

# ============ 配置区 ============
BARK_KEY = "YOUR_BARK_KEY"  # 替换为你的 Bark Key
BARK_URL = f"https://api.day.app/{BARK_KEY}"

# 阿里巴巴港股代码
STOCK_SYMBOL = "9988.HK"
STOCK_NAME = "阿里巴巴"

# 股价 API（多个备选）
APIS = [
    f"https://query1.finance.yahoo.com/v8/finance/chart/{STOCK_SYMBOL}?interval=1d&range=1d",
    f"https://finnhub.io/api/v1/quote?symbol={STOCK_SYMBOL}&token=YOUR_FINNHUB_TOKEN",
]


def get_stock_price():
    """获取股价"""
    for api in APIS:
        try:
            resp = requests.get(api, timeout=10)
            data = resp.json()

            # Yahoo Finance 格式
            if "chart" in data:
                meta = data["chart"]["result"][0]["meta"]
                price = meta.get("regularMarketPrice", 0)
                prev_close = meta.get("previousClose", 0)
                currency = meta.get("currency", "HKD")
                change = price - prev_close
                change_pct = (change / prev_close) * 100 if prev_close else 0
                return price, change, change_pct, currency

            # Finnhub 格式
            if "c" in data:
                price = data["c"]
                prev_close = data["pc"]
                change = price - prev_close
                change_pct = (change / prev_close) * 100 if prev_close else 0
                return price, change, change_pct, "HKD"

        except Exception as e:
            print(f"API 错误: {e}")
            continue

    return None, None, None, None


def is_trading_time():
    """检查是否在交易时间（港股）"""
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    weekday = now.weekday()

    # 周末不交易
    if weekday >= 5:
        return False

    # 上午 9:30 - 12:00
    if (9, 30) <= (hour, minute) <= (12, 0):
        return True

    # 下午 13:00 - 16:00
    if 13 <= hour < 16:
        return True

    return False


def send_to_bark(price, change, change_pct, currency):
    """通过 Bark 推送到 Apple Watch"""
    # 涨跌符号
    symbol = "📈" if change >= 0 else "📉"
    sign = "+" if change >= 0 else ""

    # 标题和内容
    title = f"{symbol} {STOCK_NAME}"
    body = f"当前价: {price:.2f} {currency}\n涨跌: {sign}{change:.2f} ({sign}{change_pct:.2f}%)"

    # 推送请求
    params = {
        "title": title,
        "body": body,
        "group": "stock",  # 分组
        "icon": "https://img.alicdn.com/tfs/TB1_uT8a5ERMeJjSspiXXbZLpXa-280-280.png",  # 阿里图标
    }

    try:
        resp = requests.get(f"{BARK_URL}", params=params, timeout=10)
        print(f"推送结果: {resp.json()}")
    except Exception as e:
        print(f"推送失败: {e}")


def main():
    # 检查交易时间
    if not is_trading_time():
        print("当前非交易时间，跳过推送")
        return

    # 获取股价
    price, change, change_pct, currency = get_stock_price()

    if price:
        print(f"{STOCK_NAME}: {price} {currency} ({change:+.2f}, {change_pct:+.2f}%)")
        send_to_bark(price, change, change_pct, currency)
    else:
        print("获取股价失败")


if __name__ == "__main__":
    main()