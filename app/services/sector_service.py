"""
板块数据服务

提供行业板块涨跌幅、资金流向、估值水平、轮动信号等数据。
数据源: akshare (东方财富)
"""
import os
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import akshare as ak
import pandas as pd

from app.db import get_sqlite_connection

# 清除代理环境变量（东方财富直连不需要代理）
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_k, None)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"


class SectorService:
    """板块数据服务"""

    def __init__(self):
        self._board_cache: Optional[Dict[str, Any]] = None
        self._board_cache_at: float = 0.0
        self._cache_ttl = 300  # 5分钟缓存
        self._refreshing = False
        self._lock = threading.Lock()

    def _get_db(self) -> sqlite3.Connection:
        return get_sqlite_connection(str(DB_PATH))

    def _ensure_sector_tables(self):
        conn = self._get_db()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS sector_performance (
                trade_date TEXT NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                change_pct REAL,
                turnover REAL,
                volume REAL,
                rise_count INTEGER,
                fall_count INTEGER,
                lead_stock TEXT,
                lead_stock_pct REAL,
                PRIMARY KEY (trade_date, sector_code)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sector_fund_flow (
                trade_date TEXT NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                main_net_inflow REAL,
                super_large_net_inflow REAL,
                large_net_inflow REAL,
                medium_net_inflow REAL,
                small_net_inflow REAL,
                PRIMARY KEY (trade_date, sector_code)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS sector_stock_map (
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def get_sector_list(self) -> List[Dict[str, Any]]:
        """
        获取行业板块列表（东方财富行业板块）
        返回: [{sector_code, sector_name, change_pct, turnover, ...}]
        """
        # 先检查缓存
        now = time.time()
        if self._board_cache and (now - self._board_cache_at) < self._cache_ttl:
            return self._board_cache

        if self._refreshing:
            return self._get_cached_sector_list()

        with self._lock:
            if self._refreshing:
                return self._get_cached_sector_list()
            self._refreshing = True

        try:
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                result = self._get_cached_sector_list()
                self._board_cache = result
                self._board_cache_at = now
                return result
            result = []
            for _, row in df.iterrows():
                result.append({
                    "sector_code": str(row.get("板块代码", "")),
                    "sector_name": str(row.get("板块名称", "")),
                    "change_pct": self._safe_float(row.get("涨跌幅")),
                    "turnover": self._safe_float(row.get("成交额")),
                    "volume": self._safe_float(row.get("成交量")),
                    "rise_count": self._safe_int(row.get("上涨家数")),
                    "fall_count": self._safe_int(row.get("下跌家数")),
                    "lead_stock": str(row.get("领涨股票", "")),
                    "lead_stock_pct": self._safe_float(row.get("领涨股票-涨跌幅")),
                    "updated_at": datetime.now().isoformat(),
                })
            self._board_cache = result
            self._board_cache_at = time.time()
            return result
        except Exception as exc:
            print(f"获取行业板块列表失败: {exc}")
            return self._get_cached_sector_list()
        finally:
            self._refreshing = False

    def get_sector_fund_flow(self, indicator: str = "今日") -> List[Dict[str, Any]]:
        """
        获取行业板块资金流向
        indicator: "今日", "3日排行", "5日排行", "10日排行", "20日排行"
        返回: [{sector_name, change_pct, main_net_inflow, ...}]
        """
        try:
            df = ak.stock_sector_fund_flow_rank(
                indicator=indicator,
                sector_type="行业资金流"
            )
            if df is None or df.empty:
                return self._get_cached_fund_flow()
            result = []
            for _, row in df.iterrows():
                result.append({
                    "sector_name": str(row.get("名称", "")),
                    "change_pct": self._safe_float(row.get("涨跌幅")),
                    "main_net_inflow": self._safe_float(row.get("今日主力净流入-净流入")),
                    "main_outflow": self._safe_float(row.get("今日主力净流出-净流出")),
                    "net_inflow": self._safe_float(row.get("今日主力净流入-净流入")),
                    "company_count": self._safe_int(row.get("公司家数")),
                    "top_stock": str(row.get("领涨股票", "")),
                    "top_stock_pct": self._safe_float(row.get("领涨股票-涨跌幅")),
                    "updated_at": datetime.now().isoformat(),
                })
            # 保存到DB
            self._save_fund_flow_cache(result)
            return result
        except Exception as exc:
            print(f"获取板块资金流失败: {exc}")
            return self._get_cached_fund_flow()

    def get_sector_stocks(self, sector_name: str) -> List[Dict[str, Any]]:
        """
        获取某行业板块下的成分股
        返回: [{stock_code, stock_name, price, change_pct, ...}]
        """
        try:
            df = ak.stock_board_industry_cons_em(symbol=sector_name)
            if df is None or df.empty:
                return []
            result = []
            for _, row in df.iterrows():
                code = str(row.get("代码", ""))
                result.append({
                    "stock_code": f"sh{code}" if code.startswith("6") else f"sz{code}",
                    "stock_name": str(row.get("名称", "")),
                    "price": self._safe_float(row.get("最新价")),
                    "change_pct": self._safe_float(row.get("涨跌幅")),
                    "volume": self._safe_float(row.get("成交量")),
                    "turnover": self._safe_float(row.get("成交额")),
                    "amplitude": self._safe_float(row.get("振幅")),
                })
            return result
        except Exception as exc:
            print(f"获取 {sector_name} 成分股失败: {exc}")
            return []

    def get_sector_leader(self, sector_name: str) -> Dict[str, Any]:
        """
        识别板块龙头股
        龙头 = 涨幅最大 + 成交活跃 + 市值较大的综合评分最高
        """
        stocks = self.get_sector_stocks(sector_name)
        if not stocks:
            return {}

        # 按涨跌幅和成交额综合排序
        for s in stocks:
            s["leader_score"] = (
                (s.get("change_pct") or 0) * 0.6
                + (s.get("turnover") or 0) * 0.4
            )
        stocks.sort(key=lambda x: x.get("leader_score", 0), reverse=True)
        return stocks[0] if stocks else {}

    def get_sector_rotation_signal(self, days: int = 20) -> Dict[str, Any]:
        """
        板块轮动信号
        计算各板块近期动量，输出轮动方向
        """
        sector_list = self.get_sector_list()
        if not sector_list:
            return {"status": "error", "message": "无法获取板块数据"}

        # 按涨跌幅排序
        by_change = sorted(
            sector_list,
            key=lambda x: x.get("change_pct") or 0,
            reverse=True
        )

        # 按成交额排序（资金关注度）
        by_turnover = sorted(
            sector_list,
            key=lambda x: x.get("turnover") or 0,
            reverse=True
        )

        # 资金流入排名
        fund_flow = self.get_sector_fund_flow()
        by_flow = sorted(
            fund_flow,
            key=lambda x: x.get("main_net_inflow") or 0,
            reverse=True
        ) if fund_flow else []

        # 综合排名
        top_gainers = by_change[:15]
        top_active = by_turnover[:15]
        top_inflow = by_flow[:15] if by_flow else []

        # 识别轮动方向
        rotation_leaders = []
        seen = set()
        for sector in top_gainers + top_inflow:
            name = sector.get("sector_name", "")
            if name and name not in seen:
                seen.add(name)
                rotation_leaders.append(sector)

        return {
            "as_of": datetime.now().isoformat(),
            "total_sectors": len(sector_list),
            "top_gainers": top_gainers[:10],
            "top_active": top_active[:10],
            "top_inflow": top_inflow[:10],
            "rotation_leaders": rotation_leaders[:10],
            "market_breadth": {
                "rising": sum(1 for s in sector_list if (s.get("change_pct") or 0) > 0),
                "falling": sum(1 for s in sector_list if (s.get("change_pct") or 0) <= 0),
            },
        }

    def get_sector_heatmap(self) -> List[Dict[str, Any]]:
        """
        板块热力图数据
        返回所有板块的涨跌幅，用于前端着色
        """
        sector_list = self.get_sector_list()
        return sector_list

    def get_latest_sector_date(self) -> str:
        """获取板块数据最新日期"""
        try:
            sector_list = self.get_sector_list()
            return datetime.now().strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")

    def save_sector_data(self):
        """保存板块数据到本地数据库"""
        self._ensure_sector_tables()
        today = datetime.now().strftime("%Y-%m-%d")
        conn = self._get_db()
        c = conn.cursor()

        try:
            # 保存板块表现
            sector_list = self.get_sector_list()
            for s in sector_list:
                c.execute("""
                    INSERT OR REPLACE INTO sector_performance
                    (trade_date, sector_code, sector_name, change_pct, turnover, volume,
                     rise_count, fall_count, lead_stock, lead_stock_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    s.get("sector_code"),
                    s.get("sector_name"),
                    s.get("change_pct"),
                    s.get("turnover"),
                    s.get("volume"),
                    s.get("rise_count"),
                    s.get("fall_count"),
                    s.get("lead_stock"),
                    s.get("lead_stock_pct"),
                ))

            # 保存资金流
            fund_flow = self.get_sector_fund_flow()
            for f in fund_flow:
                c.execute("""
                    INSERT OR REPLACE INTO sector_fund_flow
                    (trade_date, sector_code, sector_name, main_net_inflow,
                     super_large_net_inflow, large_net_inflow, medium_net_inflow,
                     small_net_inflow)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    f.get("sector_code", ""),
                    f.get("sector_name"),
                    f.get("main_net_inflow"),
                    f.get("super_large_net_inflow"),
                    f.get("large_net_inflow"),
                    f.get("medium_net_inflow"),
                    f.get("small_net_inflow"),
                ))

            conn.commit()
        except Exception as exc:
            print(f"保存板块数据失败: {exc}")
            conn.rollback()
        finally:
            conn.close()

    def _get_cached_sector_list(self) -> List[Dict[str, Any]]:
        """从数据库读取缓存的板块数据"""
        if not DB_PATH.exists():
            return []
        try:
            conn = self._get_db()
            c = conn.cursor()
            c.execute("""
                SELECT sector_code, sector_name, change_pct, turnover, volume,
                       rise_count, fall_count, lead_stock, lead_stock_pct, trade_date
                FROM sector_performance
                WHERE trade_date = (SELECT MAX(trade_date) FROM sector_performance)
                ORDER BY change_pct DESC
            """)
            rows = c.fetchall()
            conn.close()
            return [
                {
                    "sector_code": r[0],
                    "sector_name": r[1],
                    "change_pct": r[2],
                    "turnover": r[3],
                    "volume": r[4],
                    "rise_count": r[5],
                    "fall_count": r[6],
                    "lead_stock": r[7],
                    "lead_stock_pct": r[8],
                    "updated_at": r[9],
                }
                for r in rows
            ]
        except Exception:
            return []

    def _get_cached_fund_flow(self) -> List[Dict[str, Any]]:
        """从数据库读取缓存的资金流数据"""
        if not DB_PATH.exists():
            return []
        try:
            conn = self._get_db()
            c = conn.cursor()
            c.execute("""
                SELECT sector_name, main_net_inflow
                FROM sector_fund_flow
                WHERE trade_date = (SELECT MAX(trade_date) FROM sector_fund_flow)
                ORDER BY main_net_inflow DESC
            """)
            rows = c.fetchall()
            conn.close()
            return [
                {
                    "sector_name": r[0],
                    "change_pct": None,
                    "main_net_inflow": r[1],
                    "main_outflow": None,
                    "net_inflow": r[1],
                    "company_count": None,
                    "top_stock": "",
                    "top_stock_pct": None,
                }
                for r in rows
            ]
        except Exception:
            return []

    def _save_fund_flow_cache(self, flow_list: List[Dict[str, Any]]):
        """保存资金流数据到DB"""
        if not flow_list:
            return
        try:
            self._ensure_sector_tables()
            today = datetime.now().strftime("%Y-%m-%d")
            conn = self._get_db()
            c = conn.cursor()
            c.execute("DELETE FROM sector_fund_flow WHERE trade_date = ?", (today,))
            for i, f in enumerate(flow_list):
                c.execute("""
                    INSERT OR REPLACE INTO sector_fund_flow
                    (trade_date, sector_code, sector_name, main_net_inflow,
                     super_large_net_inflow, large_net_inflow, medium_net_inflow,
                     small_net_inflow)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    f"sec_{i:04d}",
                    f.get("sector_name"),
                    f.get("main_net_inflow"),
                    None,
                    None,
                    None,
                    None,
                ))
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"保存资金流缓存失败: {exc}")

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value in (None, "", "-", "--"):
            return None
        try:
            return float(str(value).replace(",", "").strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        parsed = SectorService._safe_float(value)
        return None if parsed is None else int(parsed)


# 单例
_sector_service = None
_sector_service_lock = threading.Lock()


def get_sector_service() -> SectorService:
    global _sector_service
    if _sector_service is None:
        with _sector_service_lock:
            if _sector_service is None:
                _sector_service = SectorService()
    return _sector_service
