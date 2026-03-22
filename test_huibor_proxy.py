# 通过代理访问慧博投研
import httpx
import time

username = "luolinhan"
password = "LUOLINHAN666"
proxy = "http://127.0.0.1:7890"

print("=== 慧博投研登录调试（使用代理）===\n")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

client = httpx.Client(
    follow_redirects=True,
    timeout=30,
    headers=headers,
    proxy=proxy
)

# 1. 访问首页
print("1. 访问首页...")
resp = client.get("https://www.hibor.com.cn/")
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

time.sleep(3)

# 2. 提交登录
print("\n2. 提交登录...")
login_url = "https://www.hibor.com.cn/ajax/login.ashx"
login_data = {
    "username": username,
    "password": password,
    "RememberMe": "1",
}

resp = client.post(login_url, data=login_data)
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

# 检查是否是成功响应
if len(resp.text) < 500 and "DOCTYPE" not in resp.text:
    print(f"   响应内容: {resp.text}")
    # 可能是JSON响应
    try:
        import json
        data = json.loads(resp.text)
        print(f"   JSON: {data}")
    except:
        pass
else:
    print("   响应是HTML页面（可能被限制）")

time.sleep(3)

# 3. 尝试访问研报列表
print("\n3. 访问研报列表...")
resp = client.get("https://www.hibor.com.cn/")
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

# 保存页面
with open("hibor_home.html", "wb") as f:
    f.write(resp.content)
print("   已保存到 hibor_home.html")