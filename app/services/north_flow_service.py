"""
北向资金数据服务

提供北向资金流向、板块流向、个股持仓等数据。
数据源: akshare (东方财富)
"""
import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import akshare as ak

from app.db import get_sqlite_connection

# 清除代理环境变量（东方财富直连不需要代理）
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_k, None)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"


class NorthFlowService:
    """北向资金数据服务"""

    def __init__(self):
        self._lock = threading.Lock()

    def _get_db(self) -> sqlite3.Connection:
        return get_sqlite_connection(str(DB_PATH))

    def _ensure_tables(self):
        conn = self._get_db()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS north_flow_daily (
                trade_date TEXT PRIMARY KEY,
                sh_net REAL,
                sz_net REAL,
                total_net REAL,
                sh_buy REAL,
                sh_sell REAL,
                sz_buy REAL,
                sz_sell REAL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS north_stock_hold (
                trade_date TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                hold_shares REAL,
                hold_ratio REAL,
                hold_value REAL,
                change_pct REAL,
                PRIMARY KEY (trade_date, stock_code)
            )
        """)
        conn.commit()
        conn.close()

    def get_north_daily(self, days: int = 180) -> List[Dict[str, Any]]:
        """
        北向日度净流入
        返回: [{trade_date, sh_net, sz_net, total_net, ...}]
        """
        try:
            df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            if df is None or df.empty:
                return self._get_cached_north_daily(days)
            df = df.tail(days)
            result = []
            for _, row in df.iterrows():
                result.append({
                    "trade_date": str(row.get("日期", ""))[:10],
                    "sh_net": self._safe_float(row.get("沪股通-净成交")),
                    "sz_net": self._safe_float(row.get("深股通-净成交")),
                    "total_net": self._safe_float(row.get("北向资金-净成交")),
                })
            return result
        except Exception as exc:
            print(f"获取北向日度流向失败: {exc}")
            return self._get_cached_north_daily(days)

    def get_north_stock_hold(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        北向个股持仓排行
        返回: [{stock_code, stock_name, hold_shares, hold_ratio, hold_value, ...}]
        """
        try:
            # 东方财富 - 北向持股排行
            df = ak.stock_hsgt_hold_stock_em(
                market="北向",
                indicator="今日排行"
            )
            if df is None or df.empty:
                return self._get_cached_north_stock_hold()
            result = []
            for _, row in df.head(100).iterrows():
                code = str(row.get("代码", ""))
                result.append({
                    "stock_code": f"sh{code}" if code.startswith("6") else f"sz{code}",
                    "stock_name": str(row.get("名称", "")),
                    "price": self._safe_float(row.get("今日收盘价")),
                    "change_pct": self._safe_float(row.get("今日涨跌幅")),
                    "hold_shares": self._safe_float(row.get("今日持股-股数")),
                    "hold_ratio": self._safe_float(row.get("今日持股-占流通股比")),
                    "hold_value": self._safe_float(row.get("今日持股-市值")),
                    "change_pct_1d": self._safe_float(row.get("今日增持估计-市值增幅")),
                    "sector": str(row.get("所属板块", "")),
                })
            # 保存到DB缓存
            self._save_north_stock_hold_cache(result)
            return result
        except Exception as exc:
            print(f"获取北向个股持仓失败: {exc}")
            return self._get_cached_north_stock_hold()

    def get_north_sector_flow(self, days: int = 5) -> List[Dict[str, Any]]:
        """
        北向按行业/板块净流入（估算）
        通过北向持仓股变动推算板块资金流向
        """
        try:
            stocks = self.get_north_stock_hold()
            if not stocks:
                return []

            # 按个股持仓变动聚合到行业（简单按股票代码前缀映射到行业）
            sector_flow = {}
            for s in stocks:
                change = s.get("change_pct_1d") or 0
                code = s.get("stock_code", "")
                name = s.get("stock_name", "")
                # 简单行业映射（可根据实际数据源优化）
                sector = self._infer_sector(code, name)
                if sector not in sector_flow:
                    sector_flow[sector] = {"sector": sector, "total_change": 0, "stock_count": 0}
                sector_flow[sector]["total_change"] += change
                sector_flow[sector]["stock_count"] += 1

            result = sorted(sector_flow.values(), key=lambda x: x["total_change"], reverse=True)
            return result
        except Exception as exc:
            print(f"获取北向板块流向失败: {exc}")
            return []

    def get_north_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        北向资金总结
        返回: 近期净流入趋势、持仓排行、板块分布
        """
        daily = self.get_north_daily(days)
        top_hold = self.get_north_stock_hold()[:20]
        sector = self.get_north_sector_flow()

        total_net_5d = sum(d.get("total_net") or 0 for d in daily[-5:])
        total_net_20d = sum(d.get("total_net") or 0 for d in daily[-20:])
        inflow_days = sum(1 for d in daily if (d.get("total_net") or 0) > 0)

        return {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_net_5d": round(total_net_5d, 2),
                "total_net_20d": round(total_net_20d, 2),
                "inflow_days": inflow_days,
                "total_days": len(daily),
                "inflow_ratio_pct": round(inflow_days / max(len(daily), 1) * 100, 1),
            },
            "daily_trend": daily[-30:],
            "top_hold": top_hold,
            "sector_flow": sector,
        }

    def _get_cached_north_daily(self, days: int) -> List[Dict[str, Any]]:
        """从数据库缓存读取"""
        if not DB_PATH.exists():
            return []
        try:
            conn = self._get_db()
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("""
                SELECT * FROM north_flow_daily
                ORDER BY trade_date DESC LIMIT ?
            """, (days,))
            rows = c.fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception:
            return []

    def _get_cached_north_stock_hold(self) -> List[Dict[str, Any]]:
        """从数据库缓存读取北向持股"""
        if not DB_PATH.exists():
            return []
        try:
            conn = self._get_db()
            c = conn.cursor()
            c.execute("""
                SELECT stock_code, stock_name, hold_shares, hold_ratio, hold_value, change_pct
                FROM north_stock_hold
                WHERE trade_date = (SELECT MAX(trade_date) FROM north_stock_hold)
                ORDER BY hold_value DESC
                LIMIT 100
            """)
            rows = c.fetchall()
            conn.close()
            return [
                {
                    "stock_code": r[0],
                    "stock_name": r[1],
                    "hold_shares": r[2],
                    "hold_ratio": r[3],
                    "hold_value": r[4],
                    "change_pct": r[5],
                    "price": None,
                    "change_pct_1d": None,
                    "sector": "",
                }
                for r in rows
            ]
        except Exception:
            return []

    def _save_north_stock_hold_cache(self, holdings: List[Dict[str, Any]]):
        """保存北向持股到DB缓存"""
        if not holdings:
            return
        try:
            self._ensure_tables()
            today = datetime.now().strftime("%Y-%m-%d")
            conn = self._get_db()
            c = conn.cursor()
            c.execute("DELETE FROM north_stock_hold WHERE trade_date = ?", (today,))
            for h in holdings:
                c.execute("""
                    INSERT INTO north_stock_hold
                    (trade_date, stock_code, stock_name, hold_shares, hold_ratio, hold_value, change_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    today,
                    h.get("stock_code", ""),
                    h.get("stock_name", ""),
                    h.get("hold_shares"),
                    h.get("hold_ratio"),
                    h.get("hold_value"),
                    h.get("change_pct"),
                ))
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"保存北向持股缓存失败: {exc}")

    @staticmethod
    def _infer_sector(code: str, name: str) -> str:
        """简单行业推断（可按需完善）"""
        # 根据代码前缀和名称关键词推断
        if code.startswith("sh600") or code.startswith("sh601"):
            for kw in ["银行", "保险", "证券", "金融"]:
                if kw in name:
                    return "金融"
            for kw in ["石化", "石油", "能源", "煤", "电"]:
                if kw in name:
                    return "能源"
            for kw in ["地产", "置地", "保利", "招商蛇口"]:
                if kw in name:
                    return "地产"
            return "沪市主板"
        if code.startswith("sz00") or code.startswith("sz30"):
            for kw in ["科技", "电子", "半导", "芯", "通信", "计算机"]:
                if kw in name:
                    return "科技"
            for kw in ["医药", "生物", "医疗", "药"]:
                if kw in name:
                    return "医药"
            for kw in ["新能", "光伏", "锂电", "电池", "风电"]:
                if kw in name:
                    return "新能源"
            for kw in ["消费", "食品", "酒", "白酒", "家电"]:
                if kw in name:
                    return "消费"
            return "深市"
        return "其他"

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value in (None, "", "-", "--"):
            return None
        try:
            return float(str(value).replace(",", "").strip())
        except (TypeError, ValueError):
            return None


# 单例
_north_flow_service = None
_north_flow_lock = threading.Lock()


def get_north_flow_service() -> NorthFlowService:
    global _north_flow_service
    if _north_flow_service is None:
        with _north_flow_lock:
            if _north_flow_service is None:
                _north_flow_service = NorthFlowService()
    return _north_flow_service
