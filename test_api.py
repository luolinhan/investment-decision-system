"""测试API访问"""
import urllib.request
import ssl
import json

# 使用代理
proxy_handler = urllib.request.ProxyHandler({
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
})
opener = urllib.request.build_opener(proxy_handler, urllib.request.HTTPSHandler())

url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=0.000001&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg=20240101&end=20500101'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

try:
    resp = opener.open(req, timeout=30)
    data = json.loads(resp.read().decode())
    klines = data.get('data', {}).get('klines', [])
    print(f'成功! 获取到 {len(klines)} 条K线数据')
    if klines:
        print(f'最新: {klines[-1]}')
except Exception as e:
    print(f'失败: {e}')