# 详细调试慧博投研登录
import httpx
import time

username = "luolinhan"
password = "LUOLINHAN666"

print("=== 慧博投研登录调试 ===\n")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

client = httpx.Client(follow_redirects=True, timeout=30, headers=headers)

# 1. 访问首页
print("1. 访问首页...")
resp = client.get("https://www.hibor.com.cn/")
print(f"   状态: {resp.status_code}")

time.sleep(1)

# 2. 访问登录页面
print("\n2. 访问登录页面...")
resp = client.get("https://www.hibor.com.cn/login.html")
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

time.sleep(1)

# 3. 提交登录
print("\n3. 提交登录...")
login_url = "https://www.hibor.com.cn/ajax/login.ashx"
login_data = {
    "username": username,
    "password": password,
    "RememberMe": "1",
}

resp = client.post(login_url, data=login_data)
print(f"   状态: {resp.status_code}")

# 保存响应到文件
with open("hibor_login_response.txt", "wb") as f:
    f.write(resp.content)
print("   响应已保存到 hibor_login_response.txt")

time.sleep(1)

# 4. 验证登录状态
print("\n4. 验证登录状态...")
resp = client.get("https://www.hibor.com.cn/usercenter/")
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

# 保存用户中心页面
with open("hibor_usercenter.html", "wb") as f:
    f.write(resp.content)
print("   已保存到 hibor_usercenter.html")

# 5. 访问研报搜索
print("\n5. 访问研报搜索...")
resp = client.get("https://www.hibor.com.cn/reportlist.html")
print(f"   状态: {resp.status_code}, 长度: {len(resp.text)}")

with open("hibor_reportlist.html", "wb") as f:
    f.write(resp.content)
print("   已保存到 hibor_reportlist.html")