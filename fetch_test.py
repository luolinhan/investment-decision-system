import urllib.request
import json
import ssl
import os

# 清除代理设置
for k in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    os.environ.pop(k, None)

# SSL上下文
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 创建无代理的opener
proxy_handler = urllib.request.ProxyHandler({})
opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPSHandler(context=ctx))
urllib.request.install_opener(opener)

stocks = [
    ("1.603259", "sh603259", "药明康德"),
    ("1.600438", "sh600438", "通威股份"),
    ("1.601012", "sh601012", "隆基绿能"),
    ("0.002459", "sz002459", "晶澳科技"),
    ("0.300763", "sz300763", "锦浪科技"),
    ("1.688235", "sh688235", "百济神州"),
    ("1.600196", "sh600196", "复星医药"),
    ("1.601888", "sh601888", "中国中免"),
    ("116.02269", "hk02269", "药明生物"),
    ("116.06160", "hk06160", "百济神州"),
    ("116.01177", "hk01177", "中国生物制药"),
    ("116.01880", "hk01880", "中国中免"),
    ("116.00700", "hk00700", "腾讯控股"),
    ("116.03690", "hk03690", "美团-W"),
    ("116.01810", "hk01810", "小米集团-W"),
    ("116.01024", "hk01024", "快手-W"),
    ("116.09988", "hk09988", "阿里巴巴-W"),
    ("116.00883", "hk00883", "中国海洋石油"),
]

results = []
for secid, code, name in stocks:
    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43,f50,f51,f58,f162,f167,f173&ut=fa5fd1943c7b386f172d6893dbfba10b"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
            if data.get("data"):
                d = data["data"]
                price = (d.get("f43") or 0) / 100
                change = (d.get("f51") or 0) / 100
                pe = (d.get("f162") or 0) / 100
                pb = (d.get("f167") or 0) / 100
                print(f"{name}: Price={price:.2f} Change={change:+.2f}% PE={pe:.2f} PB={pb:.2f}")
                results.append({"code": code, "name": name, "pe": pe, "pb": pb})
    except Exception as e:
        print(f"{name}: Error - {e}")

print(f"\nFetched {len(results)} stocks")
print("\n---JSON---")
print(json.dumps(results, ensure_ascii=False))