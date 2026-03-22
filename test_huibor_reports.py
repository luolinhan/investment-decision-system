# 通过代理访问慧博投研 - 尝试直接访问研报页面
import httpx
import time
from bs4 import BeautifulSoup
import re

proxy = "http://127.0.0.1:7890"

print("=== 慧博投研研报获取 ===\n")

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

# 尝试访问研报分类页面
pages_to_try = [
    ("最新研报", "https://www.hibor.com.cn/newreport.html"),
    ("港股研报", "https://www.hibor.com.cn/stocktype-2.html"),
    ("机构研报-摩根士丹利", "https://www.hibor.com.cn/org/96.html"),
    ("机构研报-高盛", "https://www.hibor.com.cn/org/95.html"),
    ("机构研报-瑞银", "https://www.hibor.com.cn/org/97.html"),
]

results = {}

for name, url in pages_to_try:
    print(f"\n访问: {name}")
    print(f"URL: {url}")

    try:
        resp = client.get(url)
        print(f"状态: {resp.status_code}, 长度: {len(resp.text)}")

        if resp.status_code == 200 and len(resp.text) > 1000:
            # 解析研报列表
            soup = BeautifulSoup(resp.text, "lxml")
            reports = []

            # 查找研报链接
            for link in soup.select("a[href*='report'], a[href*='.pdf']"):
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if title and len(title) > 5:
                    reports.append({"title": title, "url": href})

            # 也查找表格中的研报
            for tr in soup.select("tr"):
                tds = tr.select("td")
                if len(tds) >= 2:
                    link = tr.select_one("a")
                    if link:
                        title = link.get_text(strip=True)
                        href = link.get("href", "")
                        if title and len(title) > 5:
                            reports.append({"title": title, "url": href})

            results[name] = reports
            print(f"找到 {len(reports)} 条研报")
            for r in reports[:3]:
                print(f"  - {r['title'][:50]}")

            # 保存HTML
            safe_name = name.replace("/", "_")
            with open(f"hibor_{safe_name}.html", "wb") as f:
                f.write(resp.content)

        else:
            print("访问受限")

    except Exception as e:
        print(f"错误: {e}")

    time.sleep(2)

print("\n" + "="*50)
print("汇总:")
for name, reports in results.items():
    print(f"  {name}: {len(reports)} 条")