# 测试慧博投研 - 添加延迟和Session管理
import httpx
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import quote

class HuiborClient:
    def __init__(self):
        self.base_url = "https://www.hibor.com.cn"
        # 使用更完整的headers模拟浏览器
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.hibor.com.cn/",
        }
        self.session = httpx.Client(
            follow_redirects=True,
            timeout=30,
            headers=self.headers
        )

    def login(self, username: str, password: str) -> bool:
        """登录慧博投研"""
        print(f"登录慧博投研: {username}")

        # 先访问首页
        resp = self.session.get(self.base_url)
        print(f"首页: {resp.status_code}")
        time.sleep(2)

        # 登录
        login_url = "https://www.hibor.com.cn/ajax/login.ashx"
        resp = self.session.post(
            login_url,
            data={
                "username": username,
                "password": password,
                "RememberMe": "1",
            }
        )
        print(f"登录: {resp.status_code}")
        time.sleep(2)

        # 验证登录状态 - 访问个人中心
        resp = self.session.get("https://www.hibor.com.cn/usercenter/")
        logged_in = "用户中心" in resp.text or "退出" in resp.text or resp.status_code == 200
        print(f"登录验证: {'成功' if logged_in else '失败'}")

        return logged_in

    def get_institution_list(self):
        """获取机构列表页面"""
        print("\n获取机构列表...")

        # 访问机构列表页面
        url = "https://www.hibor.com.cn/institutions.html"
        resp = self.session.get(url)
        print(f"机构列表: {resp.status_code}, 长度: {len(resp.text)}")

        with open("hibor_institutions.html", "w", encoding="utf-8") as f:
            f.write(resp.text)

        # 解析机构
        soup = BeautifulSoup(resp.text, "lxml")
        institutions = []

        for link in soup.select("a[href*='institution']"):
            name = link.get_text(strip=True)
            href = link.get("href", "")
            if name and href:
                institutions.append({
                    "name": name,
                    "url": href,
                })

        return institutions

    def get_stock_reports(self, stock_code: str, stock_name: str):
        """获取个股研报"""
        print(f"\n获取 {stock_name} ({stock_code}) 研报...")

        # 尝试访问个股页面
        url = f"https://www.hibor.com.cn/stock-{stock_code}.html"
        resp = self.session.get(url)
        print(f"个股页面: {resp.status_code}, 长度: {len(resp.text)}")

        with open("hibor_stock.html", "w", encoding="utf-8") as f:
            f.write(resp.text)

        return resp.text

    def search_reports_direct(self, keyword: str):
        """直接搜索研报"""
        print(f"\n搜索: {keyword}")

        # 使用搜索页面
        url = f"https://www.hibor.com.cn/search/?keyword={quote(keyword)}"
        resp = self.session.get(url)
        print(f"搜索结果: {resp.status_code}, 长度: {len(resp.text)}")

        with open("hibor_search_result.html", "w", encoding="utf-8") as f:
            f.write(resp.text)

        # 解析结果
        soup = BeautifulSoup(resp.text, "lxml")
        reports = []

        # 尝试多种选择器
        selectors = [
            "table.table tr",
            ".report-list .item",
            ".list-item",
            "ul.list li",
        ]

        for sel in selectors:
            items = soup.select(sel)
            if items:
                print(f"  选择器 {sel}: 找到 {len(items)} 个")
                for item in items[:5]:
                    link = item.select_one("a")
                    if link:
                        title = link.get_text(strip=True)
                        href = link.get("href", "")
                        if title and len(title) > 5:
                            reports.append({"title": title, "url": href})

        return reports


# 测试
client = HuiborClient()

if client.login("luolinhan", "LUOLINHAN666"):
    print("\n" + "="*50)

    # 获取机构列表
    institutions = client.get_institution_list()
    print(f"找到 {len(institutions)} 个机构")
    for inst in institutions[:10]:
        print(f"  {inst['name']}")

    time.sleep(3)

    # 获取个股研报
    client.get_stock_reports("09988", "阿里巴巴")

    time.sleep(3)

    # 搜索
    reports = client.search_reports_direct("阿里巴巴")
    print(f"\n搜索结果: {len(reports)} 条")
    for r in reports[:5]:
        print(f"  - {r['title'][:40]}")