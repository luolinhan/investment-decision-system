"""测试API - 使用代理"""
import requests
import os

# 设置代理
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

url = "https://push2.eastmoney.com/api/qt/stock/get"
params = {
    "secid": "1.603259",
    "fields": "f43,f44,f45,f46,f47,f48,f50,f51,f52,f58,f162,f167,f173",
    "ut": "fa5fd1943c7b386f172d6893dbfba10b"
}

print("测试东方财富API (使用代理)...")
try:
    resp = session.get(url, params=params, timeout=15)
    print(f"状态码: {resp.status_code}")

    data = resp.json()
    if data.get("data"):
        d = data["data"]
        print(f"\n股票: {d.get('f58')}")
        print(f"最新价: {d.get('f43', 0) / 100}")
        print(f"涨跌幅: {d.get('f51', 0) / 100}%")
        print(f"昨收: {d.get('f50', 0) / 100}")
        print(f"PE(TTM): {d.get('f162', 0) / 100}")
        print(f"PB: {d.get('f167', 0) / 100}")
        print(f"ROE: {d.get('f173', 0) / 100}%")
    else:
        print("无数据")
        print(resp.text[:500])
except Exception as e:
    print(f"错误: {e}")