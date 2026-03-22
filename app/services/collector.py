"""
修复采集器 - 使用列索引避免编码问题
"""
import os
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.models import Report


class ReportCollector:
    """研报采集服务"""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.proxy = self._get_proxy()

    def _get_proxy(self) -> Optional[str]:
        proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        if proxy:
            return proxy
        try:
            import winreg
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
            proxy_enable = winreg.QueryValueEx(key, "ProxyEnable")[0]
            if proxy_enable:
                proxy_server = winreg.QueryValueEx(key, "ProxyServer")[0]
                proxy = f"http://{proxy_server}"
            winreg.CloseKey(key)
            return proxy
        except:
            pass
        return None

    async def collect_by_stock(self, stock_code: str, days: int = 90) -> int:
        """采集指定股票的研报"""
        import akshare as ak

        code = stock_code.split('.')[0] if '.' in stock_code else stock_code

        loop = asyncio.get_event_loop()
        try:
            df = await loop.run_in_executor(None, lambda: ak.stock_research_report_em(symbol=code))
        except Exception as e:
            print(f"获取研报失败: {e}")
            return 0

        if df is None or len(df) == 0:
            return 0

        print(f"AKShare获取到 {len(df)} 条研报")
        print(f"列名: {list(df.columns)}")

        # 使用列索引（避免编码问题）
        # 列顺序：序号, 股票代码, 股票简称, 研报标题, 研报类型, 机构, ...
        # PDF链接在最后一列
        cols = list(df.columns)

        cutoff_date = date.today() - timedelta(days=days)
        new_count = 0

        for idx, row in df.iterrows():
            try:
                # 使用iloc按位置获取
                values = row.values

                # 打印第一行的列信息帮助调试
                if idx == 0:
                    for i, (col, val) in enumerate(zip(cols, values)):
                        print(f"  [{i}] {col}: {str(val)[:30]}")

                # 获取各字段（根据实际列位置）
                title = ''
                pdf_url = ''
                stock_code_val = ''
                stock_name_val = ''
                institution = ''
                rating = ''
                publish_date = None

                # 遍历查找关键字段
                for i, col in enumerate(cols):
                    col_lower = str(col).lower()
                    val = str(values[i]) if i < len(values) else ''

                    if '标题' in col or 'title' in col_lower or '报告名称' in col:
                        title = val
                    elif 'pdf' in col_lower or '链接' in col:
                        pdf_url = val
                    elif '股票代码' in col or 'code' in col_lower:
                        stock_code_val = val
                    elif '股票简称' in col or '名称' in col:
                        stock_name_val = val
                    elif '机构' in col:
                        institution = val
                    elif '类型' in col or '评级' in col:
                        rating = val
                    elif '日期' in col or 'date' in col_lower:
                        publish_date = self._parse_date(val)

                if not title:
                    continue

                if publish_date and publish_date < cutoff_date:
                    continue

                # 转换股票代码格式
                if stock_code_val:
                    if stock_code_val.startswith('6'):
                        stock_code_val = f"{stock_code_val}.SH"
                    elif stock_code_val.startswith(('0', '3')):
                        stock_code_val = f"{stock_code_val}.SZ"

                report_data = {
                    "external_id": f"akshare_{code}_{idx}",
                    "title": title,
                    "stock_code": stock_code_val,
                    "stock_name": stock_name_val,
                    "institution": institution,
                    "author": "",
                    "rating": rating,
                    "publish_date": publish_date,
                    "pdf_url": pdf_url if pdf_url.startswith('http') else '',
                    "source": "eastmoney",
                }

                if await self._save_report(report_data):
                    new_count += 1

            except Exception as e:
                print(f"处理研报失败: {e}")
                continue

        await self.db.commit()
        print(f"新增 {new_count} 条研报")
        return new_count

    async def _save_report(self, report_data: Dict[str, Any]) -> bool:
        external_id = report_data.get("external_id")
        if not external_id:
            return False

        result = await self.db.execute(
            select(Report).where(Report.external_id == external_id)
        )
        if result.scalar_one_or_none():
            return False

        report = Report(
            external_id=external_id,
            title=report_data.get("title", ""),
            stock_code=report_data.get("stock_code", ""),
            stock_name=report_data.get("stock_name", ""),
            institution=report_data.get("institution", ""),
            author=report_data.get("author", ""),
            rating=report_data.get("rating", ""),
            publish_date=report_data.get("publish_date"),
            pdf_url=report_data.get("pdf_url", ""),
            source=report_data.get("source", "eastmoney"),
        )

        self.db.add(report)
        return True

    def _parse_date(self, date_str: str) -> Optional[date]:
        if not date_str:
            return None
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y年%m月%d日"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:19], fmt).date()
            except:
                continue
        return None