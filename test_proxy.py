# 在Windows上配置全局代理后测试
import requests
import os

# 设置环境变量
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

# 配置requests使用代理
proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

# 测试连接
test_urls = [
    ('Google', 'https://www.google.com'),
    ('Baidu', 'https://www.baidu.com'),
    ('Eastmoney', 'https://data.eastmoney.com'),
]

results = []
for name, url in test_urls:
    try:
        resp = requests.get(url, proxies=proxies, timeout=10)
        results.append(f'{name}: OK ({resp.status_code})')
    except Exception as e:
        results.append(f'{name}: FAIL ({str(e)[:40]})')

# 写入结果
with open('proxy_test_result.txt', 'w', encoding='utf-8') as f:
    for r in results:
        f.write(r + '\n')
    f.write('Done\n')

print('Test completed')