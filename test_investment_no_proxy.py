# -*- coding: utf-8 -*-
"""测试投资数据服务 - 不使用代理"""
import requests
import json

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
})

results = {}

# 测试港股API - 不使用代理
print("=== 港股API (无代理) ===")
url = "https://72.push2.eastmoney.com/api/qt/clist/get"
params = {
    "pn": 1, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f3",
    "fs": "m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2",
    "fields": "f12,f14,f2,f3,f5,f20"
}

try:
    resp = session.get(url, params=params, timeout=30)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("data", {}).get("diff", [])
        print(f"Total: {len(items)} stocks")
        results["hk_stocks"] = []
        keywords = ["阿里巴巴", "腾讯", "美团", "小米", "快手"]
        for item in items:
            name = item.get("f14", "")
            if any(kw in name for kw in keywords):
                price = item.get("f2", 0)
                change = item.get("f3", 0)
                print(f"  {name}: {price/100 if price > 0 else None} ({change/100}%)")
                results["hk_stocks"].append({
                    "name": name,
                    "price": price/100 if price > 0 else None,
                    "change_pct": change/100
                })
except Exception as e:
    print(f"Failed: {e}")
    results["hk_error"] = str(e)

# 测试A股API - 不使用代理
print("\n=== A股API (无代理) ===")
url = "https://82.push2.eastmoney.com/api/qt/clist/get"
params = {
    "pn": 1, "pz": 100, "po": 1, "np": 1, "fltt": 2, "invt": 2, "fid": "f3",
    "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
    "fields": "f12,f14,f2,f3,f5,f20,f9,f23"
}

try:
    resp = session.get(url, params=params, timeout=30)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        items = data.get("data", {}).get("diff", [])
        print(f"Total: {len(items)} stocks")
        results["a_stocks"] = []
        keywords = ["晶澳科技", "通威股份", "隆基绿能", "锦浪科技"]
        for item in items:
            name = item.get("f14", "")
            if any(kw in name for kw in keywords):
                price = item.get("f2", 0)
                pe = item.get("f9", 0)
                print(f"  {name}: Price={price/100 if price > 0 else None}, PE={pe/100 if pe != 0 else None}")
                results["a_stocks"].append({
                    "name": name,
                    "price": price/100 if price > 0 else None,
                    "pe": pe/100 if pe != 0 else None
                })
except Exception as e:
    print(f"Failed: {e}")
    results["a_error"] = str(e)

# 保存结果
with open("test_investment_no_proxy.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("\nDone! Results saved to test_investment_no_proxy.json")