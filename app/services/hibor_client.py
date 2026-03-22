"""
研报下载系统 - 慧博投研数据源（优化版）
"""
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, date
from typing import List, Optional, Dict, Any
import time
import os
import re


class HuiborClient:
    """慧博投研客户端"""

    def __init__(self, username: str = None, password: str = None):
        self.base_url = "http://www.hibor.com.cn"
        self.username = username
        self.password = password
        self.logged_in = False

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        self.proxy = self._get_proxy()
        self.client = httpx.Client(
            headers=self.headers,
            follow_redirects=True,
            timeout=30,
            proxy=self.proxy
        )

    def _get_proxy(self) -> Optional[str]:
        try:
            import winreg
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
            proxy_enable = winreg.QueryValueEx(key, "ProxyEnable")[0]
            if proxy_enable:
                proxy_server = winreg.QueryValueEx(key, "ProxyServer")[0]
                return f"http://{proxy_server}"
        except:
            pass
        return None

    def login(self) -> bool:
        if not self.username or not self.password:
            print("使用游客模式")
            return False

        print(f"登录: {self.username}")
        try:
            self.client.get(self.base_url)
            time.sleep(1)

            resp = self.client.post(
                f"{self.base_url}/ajax/login.ashx",
                data={"username": self.username, "password": self.password, "RememberMe": "1"}
            )

            if resp.status_code == 200:
                self.logged_in = True
                print("登录成功")
                return True
        except Exception as e:
            print(f"登录失败: {e}")
        return False

    def get_report_list(self, page: int = 1) -> List[Dict[str, Any]]:
        """获取研报列表"""
        reports = []
        url = f"{self.base_url}/microns_1_{page}.html"

        print(f"获取: {url}")

        try:
            resp = self.client.get(url, headers=self.headers)

            if resp.status_code != 200:
                return reports

            # 解码
            try:
                text = resp.content.decode('gbk')
            except:
                text = resp.text

            soup = BeautifulSoup(text, "html5lib")

            # 查找研报链接
            for link in soup.select("a[href*='docdetail']"):
                try:
                    href = link.get("href", "")
                    title = link.get("title", "") or link.get_text(strip=True)

                    if not title or len(title) < 5:
                        continue

                    if href.startswith("/"):
                        href = f"{self.base_url}{href}"

                    # 提取ID
                    match = re.search(r'_(\d+)\.html', href)
                    report_id = match.group(1) if match else ""

                    # 获取父元素中的其他信息
                    parent = link.find_parent("tr") or link.find_parent("div")
                    institution = ""
                    date_str = ""

                    if parent:
                        parent_text = parent.get_text()
                        # 查找机构名
                        for inst in ["证券", "投资", "银行", "资本", "基金"]:
                            if inst in parent_text:
                                # 提取机构名
                                for text_part in parent_text.split():
                                    if inst in text_part and len(text_part) < 20:
                                        institution = text_part
                                        break
                                break
                        # 查找日期
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', parent_text)
                        if date_match:
                            date_str = date_match.group(1)

                    reports.append({
                        "external_id": f"hibor_{report_id}",
                        "title": title,
                        "institution": institution,
                        "publish_date": date_str,
                        "detail_url": href,
                        "source": "hibor",
                    })

                except Exception as e:
                    continue

            print(f"获取到 {len(reports)} 条研报")

        except Exception as e:
            print(f"获取失败: {e}")

        return reports

    def get_report_detail(self, detail_url: str) -> Dict[str, Any]:
        """获取研报详情和PDF链接"""
        detail = {
            "title": "",
            "institution": "",
            "abstract": "",
            "pdf_url": "",
        }

        try:
            resp = self.client.get(detail_url, headers=self.headers)

            try:
                text = resp.content.decode('gbk')
            except:
                text = resp.text

            soup = BeautifulSoup(text, "html5lib")

            # 获取标题
            title_elem = soup.select_one("h1")
            if title_elem:
                detail["title"] = title_elem.get_text(strip=True)

            # 获取摘要
            for div in soup.select("div.neir"):
                text = div.get_text(strip=True)
                if len(text) > 50:
                    detail["abstract"] = text[:500]
                    break

            # 获取PDF链接
            for link in soup.select("a[href*='.pdf'], a[href*='download']"):
                href = link.get("href", "")
                if href and ('.pdf' in href.lower() or 'download' in href.lower()):
                    if href.startswith("/"):
                        href = f"{self.base_url}{href}"
                    detail["pdf_url"] = href
                    break

        except Exception as e:
            print(f"获取详情失败: {e}")

        return detail

    def close(self):
        self.client.close()


if __name__ == "__main__":
    client = HuiborClient(username="luolinhan", password="LUOLINHAN666")
    client.login()

    print("\n获取研报列表...")
    reports = client.get_report_list(page=1)

    print(f"\n找到 {len(reports)} 条研报:")
    for r in reports[:10]:
        print(f"  - {r['title'][:50]}")
        print(f"    机构: {r['institution']}, 日期: {r['publish_date']}")

    client.close()