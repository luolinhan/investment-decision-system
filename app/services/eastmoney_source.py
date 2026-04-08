"""
研报下载系统 - 东方财富数据源
基于AKShare和东方财富接口
"""
import httpx
from datetime import datetime, date
from typing import List, Optional, Dict, Any
import json
import re
import os


class EastMoneySource:
    """东方财富数据源"""

    def __init__(self, proxy: str = None):
        self.base_url = "https://reportapi.eastmoney.com/report"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/",
        }
        # 支持代理
        self.proxy = proxy or os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        if not self.proxy:
            # 尝试读取Windows系统代理
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

    async def get_reports_by_stock(
        self,
        stock_code: str,
        page_size: int = 50,
        page_index: int = 1
    ) -> List[Dict[str, Any]]:
        """
        根据股票代码获取研报列表

        Args:
            stock_code: 股票代码，如 09988.HK
            page_size: 每页数量
            page_index: 页码

        Returns:
            研报列表
        """
        # 转换股票代码格式
        sec_code = self._convert_stock_code(stock_code)
        if not sec_code:
            return []

        url = "https://reportapi.eastmoney.com/report/list"
        params = {
            "cb": "datatable",  # JSONP回调
            "industryCode": "*",
            "pageSize": page_size,
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": "",
            "endTime": "",
            "pageNo": page_index,
            "fields": "",
            "qType": "0",
            "orgCode": "",
            "code": sec_code,
            "rcode": "10",
            "_": int(datetime.now().timestamp() * 1000)
        }

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                response = await client.get(url, params=params, headers=self.headers)
                text = response.text

                # 解析JSONP
                if text.startswith("datatable("):
                    json_str = text[9:-2]  # 去掉 datatable() 包装
                    data = json.loads(json_str)

                    reports = []
                    for item in data.get("data", []):
                        report = {
                            "external_id": f"eastmoney_{item.get('infoCode', '')}",
                            "title": item.get("title", ""),
                            "stock_code": stock_code,
                            "stock_name": item.get("stockName", ""),
                            "institution": item.get("orgSName", ""),
                            "author": item.get("researcher", ""),
                            "rating": item.get("emRatingName", ""),
                            "publish_date": self._parse_date(item.get("publishDate", "")),
                            "pdf_url": f"https://pdf.dfcfw.com/pdf/H3_{item.get('infoCode', '')}_1.pdf",
                            "source": "eastmoney",
                            "raw_content": item.get("abstract", ""),
                        }
                        reports.append(report)

                    return reports
            except Exception as e:
                print(f"获取研报失败: {e}")
                return []

        return []

    async def get_reports_by_industry(
        self,
        industry: str,
        page_size: int = 50,
        page_index: int = 1
    ) -> List[Dict[str, Any]]:
        """
        根据行业获取研报列表

        Args:
            industry: 行业名称
            page_size: 每页数量
            page_index: 页码

        Returns:
            研报列表
        """
        url = "https://reportapi.eastmoney.com/report/list"
        params = {
            "cb": "datatable",
            "industryCode": "*",
            "pageSize": page_size,
            "industry": industry,
            "rating": "*",
            "ratingChange": "*",
            "beginTime": "",
            "endTime": "",
            "pageNo": page_index,
            "fields": "",
            "qType": "1",  # 行业研报
            "orgCode": "",
            "code": "",
            "rcode": "10",
            "_": int(datetime.now().timestamp() * 1000)
        }

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                response = await client.get(url, params=params, headers=self.headers)
                text = response.text

                if text.startswith("datatable("):
                    json_str = text[9:-2]
                    data = json.loads(json_str)

                    reports = []
                    for item in data.get("data", []):
                        report = {
                            "external_id": f"eastmoney_{item.get('infoCode', '')}",
                            "title": item.get("title", ""),
                            "stock_code": item.get("stockCode", ""),
                            "stock_name": item.get("stockName", ""),
                            "institution": item.get("orgSName", ""),
                            "author": item.get("researcher", ""),
                            "rating": item.get("emRatingName", ""),
                            "publish_date": self._parse_date(item.get("publishDate", "")),
                            "pdf_url": f"https://pdf.dfcfw.com/pdf/H3_{item.get('infoCode', '')}_1.pdf",
                            "source": "eastmoney",
                            "raw_content": item.get("abstract", ""),
                        }
                        reports.append(report)

                    return reports
            except Exception as e:
                print(f"获取行业研报失败: {e}")
                return []

        return []

    async def search_reports(
        self,
        keyword: str,
        page_size: int = 50,
        page_index: int = 1
    ) -> List[Dict[str, Any]]:
        """
        搜索研报

        Args:
            keyword: 搜索关键词
            page_size: 每页数量
            page_index: 页码

        Returns:
            研报列表
        """
        url = "https://reportapi.eastmoney.com/report/list"
        params = {
            "cb": "datatable",
            "industryCode": "*",
            "pageSize": page_size,
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": "",
            "endTime": "",
            "pageNo": page_index,
            "fields": "",
            "qType": "0",
            "orgCode": "",
            "code": "",
            "rcode": "10",
            "keywords": keyword,
            "_": int(datetime.now().timestamp() * 1000)
        }

        async with httpx.AsyncClient(timeout=30, proxy=self.proxy) as client:
            try:
                response = await client.get(url, params=params, headers=self.headers)
                text = response.text

                if text.startswith("datatable("):
                    json_str = text[9:-2]
                    data = json.loads(json_str)

                    reports = []
                    for item in data.get("data", []):
                        report = {
                            "external_id": f"eastmoney_{item.get('infoCode', '')}",
                            "title": item.get("title", ""),
                            "stock_code": item.get("stockCode", ""),
                            "stock_name": item.get("stockName", ""),
                            "institution": item.get("orgSName", ""),
                            "author": item.get("researcher", ""),
                            "rating": item.get("emRatingName", ""),
                            "publish_date": self._parse_date(item.get("publishDate", "")),
                            "pdf_url": f"https://pdf.dfcfw.com/pdf/H3_{item.get('infoCode', '')}_1.pdf",
                            "source": "eastmoney",
                            "raw_content": item.get("abstract", ""),
                        }
                        reports.append(report)

                    return reports
            except Exception as e:
                print(f"搜索研报失败: {e}")
                return []

        return []

    async def download_pdf(
        self,
        pdf_url: str,
        save_path: str
    ) -> bool:
        """
        下载PDF文件

        Args:
            pdf_url: PDF URL
            save_path: 保存路径

        Returns:
            是否成功
        """
        async with httpx.AsyncClient(timeout=60, proxy=self.proxy) as client:
            try:
                response = await client.get(pdf_url, headers=self.headers)
                if response.status_code == 200:
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    return True
            except Exception as e:
                print(f"下载PDF失败: {e}")
        return False

    def _convert_stock_code(self, stock_code: str) -> str:
        """
        转换股票代码格式
        09988.HK -> 09988.HK 或 09988 (港股)
        600438.SH -> 600438 (A股)
        """
        if not stock_code:
            return ""

        # 已经是正确格式
        if "." in stock_code:
            code, market = stock_code.split(".")
            if market == "HK":
                # 港股，保持原样
                return code
            elif market in ["SH", "SZ"]:
                # A股
                market_code = "1" if market == "SH" else "0"
                return f"{code}{market_code}"

        return stock_code

    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        if not date_str:
            return None

        # 尝试多种格式
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str[:19], fmt).date()
            except:
                continue

        return None