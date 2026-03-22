# -*- coding: utf-8 -*-
"""使用新浪财经API获取股票行情数据"""
import requests
import json
import re

results = {}

# A股股票代码
a_stock_codes = {
    "晶澳科技": "sz002459",
    "通威股份": "sh600438",
    "隆基绿能": "sh601012",
    "锦浪科技": "sz300763",
    "百济神州": "sh688235",
    "药明康德": "sh603259",
}

# 港股股票代码 (新浪格式: hk + 5位数字)
hk_stock_codes = {
    "阿里巴巴": "hk09988",
    "腾讯": "hk00700",
    "美团": "hk03690",
    "小米": "hk01810",
    "快手": "hk01024",
    "百济神州": "hk06160",
    "药明生物": "hk02269",
}

def get_sina_quote(codes):
    """从新浪财经获取实时行情"""
    url = "http://hq.sinajs.cn/list=" + ",".join(codes)
    headers = {
        "Referer": "http://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "gbk"
        return resp.text
    except Exception as e:
        print(f"  请求失败: {e}")
        return None

def parse_a_stock_quote(text):
    """解析A股行情数据"""
    quotes = {}
    pattern = r'var hq_str_(\w+)="(.*)";'
    matches = re.findall(pattern, text)
    for code, data in matches:
        if data:
            parts = data.split(",")
            if len(parts) >= 6:
                try:
                    quotes[code] = {
                        "name": parts[0],
                        "open": float(parts[1]) if parts[1] else 0,
                        "prev_close": float(parts[2]) if parts[2] else 0,
                        "price": float(parts[3]) if parts[3] else 0,
                        "high": float(parts[4]) if parts[4] else 0,
                        "low": float(parts[5]) if parts[5] else 0,
                    }
                    if quotes[code]["prev_close"] > 0:
                        quotes[code]["change_pct"] = round(
                            (quotes[code]["price"] - quotes[code]["prev_close"]) / quotes[code]["prev_close"] * 100, 2
                        )
                except ValueError:
                    pass
    return quotes

def parse_hk_stock_quote(text):
    """解析港股行情数据 - 港股格式不同"""
    quotes = {}
    pattern = r'var hq_str_(\w+)="(.*)";'
    matches = re.findall(pattern, text)
    for code, data in matches:
        if data:
            parts = data.split(",")
            # 港股格式: 名称,今开,昨收,最高,最低,买入,卖出,最新,涨跌,涨幅...
            if len(parts) >= 8:
                try:
                    name = parts[0]
                    latest_price = parts[6]  # 最新价
                    change = parts[7] if len(parts) > 7 else "0"  # 涨跌额
                    change_pct = parts[8] if len(parts) > 8 else "0"  # 涨跌幅

                    quotes[code] = {
                        "name": name,
                        "price": float(latest_price) if latest_price else 0,
                        "change": float(change) if change else 0,
                        "change_pct": float(change_pct) if change_pct else 0,
                    }
                except (ValueError, IndexError) as e:
                    print(f"  解析港股 {code} 失败: {e}")
    return quotes

# 获取A股行情
print("=== A股实时行情 (新浪) ===")
a_codes = list(a_stock_codes.values())
a_text = get_sina_quote(a_codes)
if a_text:
    a_quotes = parse_a_stock_quote(a_text)
    results["a_stocks"] = []
    for name, code in a_stock_codes.items():
        if code in a_quotes:
            q = a_quotes[code]
            print(f"  {name}: 价格={q['price']}, 涨跌={q.get('change_pct', 0)}%")
            results["a_stocks"].append({
                "name": name,
                "code": code,
                "price": q["price"],
                "change_pct": q.get("change_pct", 0)
            })
else:
    print("  获取失败")

# 获取港股行情
print("\n=== 港股实时行情 (新浪) ===")
hk_codes = list(hk_stock_codes.values())
hk_text = get_sina_quote(hk_codes)
if hk_text:
    hk_quotes = parse_hk_stock_quote(hk_text)
    results["hk_stocks"] = []
    for name, code in hk_stock_codes.items():
        if code in hk_quotes:
            q = hk_quotes[code]
            print(f"  {name}: 价格={q['price']}, 涨跌={q.get('change_pct', 0)}%")
            results["hk_stocks"].append({
                "name": name,
                "code": code,
                "price": q["price"],
                "change_pct": q.get("change_pct", 0)
            })
else:
    print("  获取失败")

# 保存结果
with open("test_sina_stocks.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("\nDone! Results saved to test_sina_stocks.json")