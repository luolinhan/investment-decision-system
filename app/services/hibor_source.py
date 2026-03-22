"""
研报下载系统 - 慧博投研数据源
研报最全的数据源，包含大行研报
"""
import httpx
from datetime import datetime, date
from typing import List, Optional, Dict, Any
import json
import re
from bs4 import BeautifulSoup
import os


class HuiborSource:
    """慧博投研数据源"""

    def __init__(self, username: str = None, password: str = None):
        self.base_url = "https://www.hibor.com.cn"
        self.login_url = "https://www.hibor.com.cn/login.html"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        self.username = username
        self.password = password
        self.session = None
        self.logged_in = False

        # 支持代理
        self.proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        if not self.proxy:
            try:
                import winreg
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                    r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
                proxy_enable = winreg.QueryValueEx(key, "ProxyEnable")[0]
                if proxy_enable:
                    proxy_server = winreg.QueryValueEx(key, "ProxyServer")[0]
                    self.proxy = f"http://{proxy_server}"
                winreg.CloseKey(key)
            except:
                pass

    async def login(self) -> bool:
        """登录慧博投研"""
        if not self.username or not self.password:
            return False

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                # 获取登录页面
                response = await client.get(self.login_url, headers=self.headers)
                cookies = response.cookies

                # 提交登录
                login_data = {
                    "username": self.username,
                    "password": self.password,
                    "remember": "1",
                }

                response = await client.post(
                    "https://www.hibor.com.cn/login.ajax",
                    data=login_data,
                    headers=self.headers,
                    cookies=cookies
                )

                result = response.json()
                if result.get("success") or result.get("code") == 0:
                    self.logged_in = True
                    self.session = client
                    return True

            except Exception as e:
                print(f"登录慧博投研失败: {e}")

        return False

    async def search_reports(
        self,
        keyword: str,
        page: int = 1,
        page_size: int = 20
    ) -> List[Dict[str, Any]]:
        """
        搜索研报

        Args:
            keyword: 搜索关键词
            page: 页码
            page_size: 每页数量

        Returns:
            研报列表
        """
        url = "https://www.hibor.com.cn/search.html"
        params = {
            "keyword": keyword,
            "page": page,
        }

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                response = await client.get(url, params=params, headers=self.headers)
                soup = BeautifulSoup(response.text, "lxml")

                reports = []
                items = soup.select(".report-list .report-item")

                for item in items:
                    try:
                        title_elem = item.select_one(".report-title a")
                        if not title_elem:
                            continue

                        title = title_elem.text.strip()
                        href = title_elem.get("href", "")
                        report_id = self._extract_id(href)

                        institution = item.select_one(".report-org")
                        institution = institution.text.strip() if institution else ""

                        date_elem = item.select_one(".report-date")
                        publish_date = self._parse_date(date_elem.text.strip()) if date_elem else None

                        report = {
                            "external_id": f"hibor_{report_id}",
                            "title": title,
                            "institution": institution,
                            "publish_date": publish_date,
                            "pdf_url": f"https://www.hibor.com.cn/download/{report_id}.html",
                            "source": "hibor",
                            "detail_url": href,
                        }
                        reports.append(report)

                    except Exception as e:
                        print(f"解析研报失败: {e}")
                        continue

                return reports

            except Exception as e:
                print(f"搜索慧博研报失败: {e}")
                return []

    async def get_report_detail(self, report_id: str) -> Optional[Dict[str, Any]]:
        """
        获取研报详情

        Args:
            report_id: 研报ID

        Returns:
            研报详情
        """
        url = f"https://www.hibor.com.cn/report/{report_id}.html"

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                response = await client.get(url, headers=self.headers)
                soup = BeautifulSoup(response.text, "lxml")

                detail = {
                    "external_id": f"hibor_{report_id}",
                    "title": "",
                    "institution": "",
                    "author": "",
                    "publish_date": None,
                    "pdf_url": "",
                    "raw_content": "",
                    "source": "hibor",
                }

                # 提取标题
                title_elem = soup.select_one(".report-title")
                if title_elem:
                    detail["title"] = title_elem.text.strip()

                # 提取机构
                org_elem = soup.select_one(".report-org")
                if org_elem:
                    detail["institution"] = org_elem.text.strip()

                # 提取分析师
                author_elem = soup.select_one(".report-author")
                if author_elem:
                    detail["author"] = author_elem.text.strip()

                # 提取日期
                date_elem = soup.select_one(".report-date")
                if date_elem:
                    detail["publish_date"] = self._parse_date(date_elem.text.strip())

                # 提取摘要
                abstract_elem = soup.select_one(".report-abstract")
                if abstract_elem:
                    detail["raw_content"] = abstract_elem.text.strip()

                # PDF下载链接
                download_elem = soup.select_one(".download-btn")
                if download_elem:
                    detail["pdf_url"] = download_elem.get("href", "")

                return detail

            except Exception as e:
                print(f"获取研报详情失败: {e}")
                return None

    async def download_pdf(
        self,
        report_id: str,
        save_path: str
    ) -> bool:
        """
        下载研报PDF

        Args:
            report_id: 研报ID
            save_path: 保存路径

        Returns:
            是否成功
        """
        # 需要登录才能下载
        if not self.logged_in:
            print("需要登录才能下载慧博研报")
            return False

        download_url = f"https://www.hibor.com.cn/download/{report_id}.html"

        async with httpx.AsyncClient(timeout=60, proxy=self.proxy) as client:
            try:
                response = await client.get(
                    download_url,
                    headers=self.headers,
                    follow_redirects=True
                )

                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")
                    if "pdf" in content_type or len(response.content) > 10000:
                        with open(save_path, "wb") as f:
                            f.write(response.content)
                        return True

            except Exception as e:
                print(f"下载慧博PDF失败: {e}")

        return False

    async def search_by_institution(
        self,
        institution: str,
        page: int = 1
    ) -> List[Dict[str, Any]]:
        """
        按机构搜索研报

        Args:
            institution: 机构名称（如：摩根士丹利、高盛、瑞银）
            page: 页码

        Returns:
            研报列表
        """
        return await self.search_reports(institution, page=page)

    def _extract_id(self, url: str) -> str:
        """从URL中提取研报ID"""
        match = re.search(r'/(\d+)\.html', url)
        if match:
            return match.group(1)
        return ""

    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        if not date_str:
            return None

        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y年%m月%d日",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str[:19], fmt).date()
            except:
                continue

        return None


# 知名投行机构列表
MAJOR_INSTITUTIONS = [
    "摩根士丹利", "Morgan Stanley", "大摩",
    "高盛", "Goldman Sachs",
    "瑞银", "UBS",
    "摩根大通", "J.P. Morgan", "小摩",
    "花旗", "Citigroup", "Citi",
    "美银美林", "Bank of America Merrill Lynch",
    "瑞信", "Credit Suisse",
    "巴克莱", "Barclays",
    "德银", "Deutsche Bank",
    "中金公司", "CICC",
    "中信证券",
    "华泰证券",
    "国泰君安",
    "海通证券",
    "招商证券",
    "广发证券",
    "申万宏源",
]