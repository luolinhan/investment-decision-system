# 检查A股原始数据格式
import requests

url = "https://qt.gtimg.cn/q=sz002459,sh600438"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://gu.qq.com/"
}

resp = requests.get(url, headers=headers, timeout=10)
resp.encoding = "gbk"
text = resp.text

print("原始响应:")
print(text)
print("\n\n字段分析:")
lines = text.strip().split('\n')
for line in lines:
    if '=' in line:
        data = line.split('=')[1].strip('" ;')
        parts = data.split('~')
        print(f"\n共 {len(parts)} 个字段:")
        for i, p in enumerate(parts[:40]):
            print(f"  [{i}]: {p}")