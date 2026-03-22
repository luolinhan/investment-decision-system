# -*- coding: utf-8 -*-
"""使用腾讯财经API获取股票行情 - 完整版"""
import requests
import json
import re

def get_tencent_quote(codes):
    """从腾讯财经获取实时行情"""
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://gu.qq.com/"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "gbk"
        return resp.text
    except Exception as e:
        return ""

def parse_a_stock(data):
    """解析A股数据
    格式: 51~名称~代码~现价~昨收~今开~...~涨跌额~涨跌幅~最高~最低~...
    """
    parts = data.split("~")
    if len(parts) < 35:
        return None
    try:
        return {
            "name": parts[1],
            "code": parts[2],
            "price": float(parts[3]) if parts[3] else 0,
            "prev_close": float(parts[4]) if parts[4] else 0,
            "open": float(parts[5]) if parts[5] else 0,
            "change": float(parts[30]) if len(parts) > 30 and parts[30] else 0,
            "change_pct": float(parts[31]) if len(parts) > 31 and parts[31] else 0,
            "high": float(parts[32]) if len(parts) > 32 and parts[32] else 0,
            "low": float(parts[33]) if len(parts) > 33 and parts[33] else 0,
            "volume": int(float(parts[6])) if len(parts) > 6 and parts[6] else 0,
        }
    except (ValueError, IndexError):
        return None

def parse_hk_stock(data):
    """解析港股数据
    格式: 1~名称~代码~现价~昨收~今开~成交量~...~涨跌额~涨跌幅~最高~最低~时间~...
    港股字段位置与A股略有不同
    """
    parts = data.split("~")
    if len(parts) < 12:
        return None
    try:
        return {
            "name": parts[1],
            "code": parts[2],
            "price": float(parts[3]) if parts[3] else 0,
            "prev_close": float(parts[4]) if parts[4] else 0,
            "open": float(parts[5]) if parts[5] else 0,
            "change": float(parts[8]) if len(parts) > 8 and parts[8] else 0,
            "change_pct": float(parts[9]) if len(parts) > 9 and parts[9] else 0,
            "high": float(parts[10]) if len(parts) > 10 and parts[10] else 0,
            "low": float(parts[11]) if len(parts) > 11 and parts[11] else 0,
        }
    except (ValueError, IndexError):
        return None

def parse_tencent_quote(text, code):
    """解析腾讯行情数据"""
    # 找到对应的变量
    pattern = rf'v_{code}="(.*)";'
    match = re.search(pattern, text)
    if not match:
        return None

    data = match.group(1)
    if not data:
        return None

    # 根据代码前缀判断是A股还是港股
    if code.startswith('hk'):
        return parse_hk_stock(data)
    else:
        return parse_a_stock(data)

# 测试
a_stock_codes = {
    "晶澳科技": "sz002459",
    "通威股份": "sh600438",
    "隆基绿能": "sh601012",
    "锦浪科技": "sz300763",
    "百济神州": "sh688235",
    "药明康德": "sh603259",
    "复星医药": "sh600196",
    "中国中免": "sh601888",
}

hk_stock_codes = {
    "阿里巴巴": "hk09988",
    "腾讯": "hk00700",
    "美团": "hk03690",
    "小米": "hk01810",
    "快手": "hk01024",
    "百济神州": "hk06160",
    "药明生物": "hk02269",
    "中国海洋石油": "hk00883",
}

print("=== 腾讯财经API测试 ===")
all_codes = list(a_stock_codes.values()) + list(hk_stock_codes.values())
text = get_tencent_quote(all_codes)

results = {"a_stocks": [], "hk_stocks": []}

print("A股:")
for name, code in a_stock_codes.items():
    q = parse_tencent_quote(text, code)
    if q:
        print(f"  {name}: 价格={q['price']}, 涨跌={q.get('change_pct', 0)}%")
        q['name'] = name
        results['a_stocks'].append(q)

print("\n港股:")
for name, code in hk_stock_codes.items():
    q = parse_tencent_quote(text, code)
    if q:
        print(f"  {name}: 价格={q['price']}, 涨跌={q.get('change_pct', 0)}%")
        q['name'] = name
        results['hk_stocks'].append(q)

# 保存结果
with open("test_tencent_result.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print("\n结果已保存到 test_tencent_result.json")