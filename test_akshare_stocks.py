# -*- coding: utf-8 -*-
"""使用AKShare获取股票行情数据"""
import akshare as ak
import os
import json

# 设置代理
os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"

results = {}

# 1. A股实时行情 (使用AKShare内置接口)
print("=== A股实时行情 ===")
try:
    df = ak.stock_zh_a_spot_em()
    print(f"Total: {len(df)} stocks")
    results["a_stocks"] = []

    keywords = ["晶澳科技", "通威股份", "隆基绿能", "锦浪科技", "百济神州", "药明康德"]
    for kw in keywords:
        found = df[df['名称'].str.contains(kw, na=False)]
        if len(found) > 0:
            row = found.iloc[0]
            print(f"  {row['名称']}: 价格={row['最新价']}, PE={row.get('市盈率-动态', 'N/A')}, 市值={row.get('总市值', 'N/A')}")
            results["a_stocks"].append({
                "name": row['名称'],
                "code": row['代码'],
                "price": row['最新价'],
                "pe": row.get('市盈率-动态', None),
                "market_cap": row.get('总市值', None)
            })
except Exception as e:
    print(f"Failed: {e}")
    results["a_error"] = str(e)

# 2. 港股实时行情
print("\n=== 港股实时行情 ===")
try:
    df = ak.stock_hk_spot_em()
    print(f"Total: {len(df)} stocks")
    results["hk_stocks"] = []

    keywords = ["阿里巴巴", "腾讯", "美团", "小米", "快手", "百济神州", "药明生物"]
    for kw in keywords:
        found = df[df['名称'].str.contains(kw, na=False)]
        if len(found) > 0:
            row = found.iloc[0]
            print(f"  {row['名称']}: 价格={row['最新价']}, 涨跌={row.get('涨跌幅', 'N/A')}%")
            results["hk_stocks"].append({
                "name": row['名称'],
                "code": row['代码'],
                "price": row['最新价'],
                "change_pct": row.get('涨跌幅', None)
            })
except Exception as e:
    print(f"Failed: {e}")
    results["hk_error"] = str(e)

# 3. 恒生指数
print("\n=== 恒生指数 ===")
try:
    df = ak.stock_hk_index_daily_em(symbol="HSI")
    latest = df.tail(1).iloc[0]
    prev = df.tail(2).iloc[0]
    change_pct = (latest['close'] - prev['close']) / prev['close'] * 100
    print(f"  恒生指数: {latest['close']:.2f} ({change_pct:.2f}%)")
    results["hsi"] = {
        "name": "恒生指数",
        "code": "HSI",
        "close": float(latest['close']),
        "change_pct": round(float(change_pct), 2),
        "date": str(latest['date'])
    }
except Exception as e:
    print(f"Failed: {e}")
    results["hsi_error"] = str(e)

# 4. 美股指数
print("\n=== 美股指数 ===")
try:
    df = ak.index_us_stock_sina(symbol=".DJI")
    print(f"  道琼斯: {len(df)} 条")
    results["dji"] = "OK"
except Exception as e:
    print(f"  道琼斯失败: {e}")

try:
    df = ak.index_us_stock_sina(symbol=".IXIC")
    print(f"  纳斯达克: {len(df)} 条")
    results["nasdaq"] = "OK"
except Exception as e:
    print(f"  纳斯达克失败: {e}")

# 保存结果
with open("test_akshare_stocks.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("\nDone! Results saved to test_akshare_stocks.json")