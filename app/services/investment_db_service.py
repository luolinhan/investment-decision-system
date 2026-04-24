"""
投资数据API服务 - 从本地数据库读取
"""
import sqlite3
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from app.db import get_sqlite_connection

DB_PATH = "data/investment.db"


class InvestmentDataService:
    """投资数据服务"""

    def __init__(self):
        self.db_path = DB_PATH

    def _get_db(self):
        return get_sqlite_connection(self.db_path)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            text = str(value).strip().replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            return parsed
        except Exception:
            return None

    @staticmethod
    def _safe_json_loads(value: Any) -> Optional[Dict[str, Any]]:
        if not value:
            return None
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, dict) else {"value": loaded}
        except Exception:
            return None

    def _get_table_columns(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        c = conn.cursor()
        c.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in c.fetchall()]

    def _ensure_snapshot_table(self):
        conn = self._get_db()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                snapshot_key TEXT PRIMARY KEY,
                payload_json TEXT,
                updated_at TEXT NOT NULL,
                expires_at TEXT,
                source TEXT,
                fetch_latency_ms INTEGER,
                notes TEXT
            )
        """)
        columns = set(self._get_table_columns(conn, "market_snapshots"))
        optional_columns = {
            "payload_json": "TEXT",
            "source": "TEXT",
            "updated_at": "TEXT",
            "expires_at": "TEXT",
            "age_seconds": "REAL",
            "is_fresh": "INTEGER",
            "fetch_latency_ms": "INTEGER",
            "notes": "TEXT",
        }
        for column, ddl_type in optional_columns.items():
            if column not in columns:
                c.execute(f"ALTER TABLE market_snapshots ADD COLUMN {column} {ddl_type}")
        conn.commit()
        conn.close()

    def get_market_snapshot(
        self,
        snapshot_key: str,
        ttl_seconds: int = 3600,
        max_age_seconds: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """读取市场快照缓存"""
        conn = None
        try:
            self._ensure_snapshot_table()
            conn = self._get_db()
            c = conn.cursor()
            columns = self._get_table_columns(conn, "market_snapshots")
            select_columns = [
                "snapshot_key",
                "payload_json" if "payload_json" in columns else None,
                "payload" if "payload" in columns else None,
                "source" if "source" in columns else None,
                "updated_at" if "updated_at" in columns else None,
                "expires_at" if "expires_at" in columns else None,
                "age_seconds" if "age_seconds" in columns else None,
                "is_fresh" if "is_fresh" in columns else None,
                "fetch_latency_ms" if "fetch_latency_ms" in columns else None,
                "notes" if "notes" in columns else None,
            ]
            c.execute(
                f"""
                SELECT {', '.join(col for col in select_columns if col)}
                FROM market_snapshots
                WHERE snapshot_key = ?
                """,
                (snapshot_key,),
            )
            row = c.fetchone()
            if not row:
                return None

            values = dict(zip([col for col in select_columns if col], row))
            payload = self._safe_json_loads(values.get("payload_json")) or self._safe_json_loads(values.get("payload"))
            updated_at = values.get("updated_at")
            expires_at = values.get("expires_at")
            updated_dt = self._parse_datetime(updated_at)
            expires_dt = self._parse_datetime(expires_at)
            now = datetime.now()

            age_seconds = values.get("age_seconds")
            if updated_dt is not None:
                age_seconds = int(max(0, (now - updated_dt).total_seconds()))

            freshness_ttl = max_age_seconds if max_age_seconds is not None else ttl_seconds
            is_fresh = True
            if values.get("is_fresh") is not None:
                is_fresh = bool(values.get("is_fresh"))
            if expires_dt is not None and expires_dt <= now:
                is_fresh = False
            if freshness_ttl is not None and age_seconds is not None and age_seconds > freshness_ttl:
                is_fresh = False

            if max_age_seconds is not None and age_seconds is not None and age_seconds > max_age_seconds:
                return None

            return {
                "payload": payload,
                "source": values.get("source"),
                "updated_at": updated_at,
                "expires_at": expires_at,
                "age_seconds": age_seconds,
                "is_fresh": is_fresh,
                "fetch_latency_ms": values.get("fetch_latency_ms"),
                "notes": values.get("notes"),
            }
        except Exception as exc:
            print(f"market snapshot read failed ({snapshot_key}): {exc}")
            return None
        finally:
            if conn:
                conn.close()

    def save_market_snapshot(self, snapshot_key: str, payload: Dict[str, Any], ttl_seconds: int = 3600,
                              source: str = "realtime", fetch_latency_ms: int = 0,
                              notes: str = "") -> Dict[str, Any]:
        """保存市场快照"""
        conn = None
        try:
            self._ensure_snapshot_table()
            now = datetime.now()
            expires = now + timedelta(seconds=ttl_seconds)
            conn = self._get_db()
            c = conn.cursor()
            columns = set(self._get_table_columns(conn, "market_snapshots"))
            payload_text = json.dumps(payload, ensure_ascii=False)
            row_values = {
                "snapshot_key": snapshot_key,
                "payload_json": payload_text,
                "payload": payload_text,
                "updated_at": now.isoformat(),
                "expires_at": expires.isoformat(),
                "source": source,
                "age_seconds": 0,
                "is_fresh": 1,
                "fetch_latency_ms": fetch_latency_ms,
                "notes": notes or "",
            }
            write_columns = [col for col in row_values if col in columns]
            placeholders = ", ".join(["?"] * len(write_columns))
            c.execute(
                f"""
                INSERT OR REPLACE INTO market_snapshots
                ({', '.join(write_columns)})
                VALUES ({placeholders})
                """,
                [row_values[col] for col in write_columns],
            )
            conn.commit()
            return {
                "updated_at": now.isoformat(),
                "expires_at": expires.isoformat(),
                "is_fresh": True,
            }
        except Exception as exc:
            print(f"market snapshot save failed ({snapshot_key}): {exc}")
            return {"updated_at": datetime.now().isoformat(), "is_fresh": False}
        finally:
            if conn:
                conn.close()

    def get_index_list(self) -> List[Dict]:
        """获取指数列表"""
        conn = self._get_db()
        c = conn.cursor()
        c.execute('''
            SELECT DISTINCT code, name FROM index_history ORDER BY code
        ''')
        result = [{"code": row[0], "name": row[1]} for row in c.fetchall()]
        conn.close()
        return result

    def get_index_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取指数历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, open, high, low, close, volume, change_pct
            FROM index_history
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "change_pct": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_index_latest(self, code: str) -> Optional[Dict]:
        """获取指数最新数据"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT code, name, trade_date, close, change_pct
            FROM index_history
            WHERE code = ?
            ORDER BY trade_date DESC LIMIT 1
        ''', (code,))

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "code": row[0],
                "name": row[1],
                "date": row[2],
                "close": row[3],
                "change_pct": row[4]
            }
        return None

    def get_all_indices_latest(self) -> Dict[str, Dict]:
        """获取所有指数最新数据"""
        conn = self._get_db()
        c = conn.cursor()

        # 获取每个指数的最新数据
        c.execute('''
            SELECT code, name, trade_date, close, change_pct
            FROM index_history
            WHERE (code, trade_date) IN (
                SELECT code, MAX(trade_date) FROM index_history GROUP BY code
            )
        ''')

        result = {}
        for row in c.fetchall():
            result[row[0]] = {
                "code": row[0],
                "name": row[1],
                "date": row[2],
                "close": row[3],
                "change_pct": row[4]
            }

        conn.close()
        return result

    def get_stock_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取股票历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, open, high, low, close, volume, change_pct
            FROM stock_daily
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "change_pct": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_interest_rates(self, days: int = 365) -> List[Dict]:
        """获取利率数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, shibor_overnight, shibor_1w, shibor_1m,
                   hibor_overnight, hibor_1w, hibor_1m
            FROM interest_rates
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "shibor_overnight": row[1],
            "shibor_1w": row[2],
            "shibor_1m": row[3],
            "hibor_overnight": row[4],
            "hibor_1w": row[5],
            "hibor_1m": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_interest_rates_latest(self) -> Optional[Dict]:
        """获取最新利率"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, shibor_overnight, shibor_1w, shibor_1m,
                   shibor_3m, shibor_6m, shibor_1y,
                   hibor_overnight, hibor_1w, hibor_1m, hibor_3m
            FROM interest_rates ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "shibor": {
                    "overnight": row[1],
                    "1w": row[2],
                    "1m": row[3],
                    "3m": row[4],
                    "6m": row[5],
                    "1y": row[6]
                },
                "hibor": {
                    "overnight": row[7],
                    "1w": row[8],
                    "1m": row[9],
                    "3m": row[10]
                }
            }
        return None

    def get_north_money(self, days: int = 180) -> List[Dict]:
        """获取北向资金数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow
            FROM north_money
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "sh_inflow": row[1],
            "sz_inflow": row[2],
            "total_inflow": row[3]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_market_sentiment_latest(self) -> Optional[Dict]:
        """获取最新市场情绪"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, up_count, down_count, flat_count, limit_up_count, limit_down_count
            FROM market_sentiment ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "up_count": row[1],
                "down_count": row[2],
                "flat_count": row[3],
                "limit_up_count": row[4],
                "limit_down_count": row[5]
            }
        return None

    def get_watch_list(self) -> List[Dict]:
        """获取关注股票列表"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT code, name, market, category, weight, notes
            FROM watch_list WHERE enabled=1 ORDER BY category, code
        ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "market": row[2],
            "category": row[3],
            "weight": row[4],
            "notes": row[5]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_vix_latest(self) -> Optional[Dict]:
        """获取最新VIX数据"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, vix_open, vix_high, vix_low, vix_close
            FROM vix_history ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4]
            }
        return None

    def get_vix_history(self, days: int = 365) -> List[Dict]:
        """获取VIX历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, vix_open, vix_high, vix_low, vix_close
            FROM vix_history
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_market_overview(self) -> Dict[str, Any]:
        """获取市场概览（整合数据）"""
        # 从数据库获取
        indices = self.get_all_indices_latest()
        rates = self.get_interest_rates_latest()
        sentiment = self.get_market_sentiment_latest()
        vix = self.get_vix_latest()

        # 实时获取股票行情（这个还是需要实时API）
        from app.services.investment_data import InvestmentDataService as RealtimeService
        realtime = RealtimeService()
        watch_stocks = realtime.get_watch_stocks()

        return {
            "update_time": datetime.now().isoformat(),
            "indices": indices,
            "rates": rates,
            "sentiment": sentiment,
            "vix": vix,
            "watch_stocks": watch_stocks
        }

    # ==================== 微观基本面模块 ====================

    def get_stock_fundamentals(self, codes: List[str] = None) -> List[Dict]:
        """获取股票基本面数据"""
        conn = self._get_db()
        c = conn.cursor()
        columns = set(self._get_table_columns(conn, "stock_financial"))
        revenue_col = "total_revenue" if "total_revenue" in columns else "revenue"

        if codes:
            placeholders = ','.join(['?' for _ in codes])
            c.execute(f'''
                SELECT code, name, market, report_date, pe_ttm, pb, ps_ttm,
                       roe, roa, gross_margin, net_margin, debt_ratio, current_ratio,
                       eps, bvps, {revenue_col}, revenue_yoy, net_profit, net_profit_yoy, dividend_yield
                FROM stock_financial
                WHERE code IN ({placeholders})
                ORDER BY code
            ''', codes)
        else:
            c.execute(f'''
                SELECT code, name, market, report_date, pe_ttm, pb, ps_ttm,
                       roe, roa, gross_margin, net_margin, debt_ratio, current_ratio,
                       eps, bvps, {revenue_col}, revenue_yoy, net_profit, net_profit_yoy, dividend_yield
                FROM stock_financial
                ORDER BY code
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "market": row[2],
            "report_date": row[3],
            "pe_ttm": row[4],
            "pb": row[5],
            "ps_ttm": row[6],
            "roe": row[7],
            "roa": row[8],
            "gross_margin": row[9],
            "net_margin": row[10],
            "debt_ratio": row[11],
            "current_ratio": row[12],
            "eps": row[13],
            "bvps": row[14],
            "revenue": row[15],
            "revenue_yoy": row[16],
            "net_profit": row[17],
            "net_profit_yoy": row[18],
            "dividend_yield": row[19]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_watch_stocks_fundamentals(self) -> List[Dict]:
        """获取关注股票的基本面数据 - 只返回最新数据"""
        conn = self._get_db()
        c = conn.cursor()

        # 从watch_list获取关注股票，关联stock_financial获取最新财务数据
        # 使用子查询获取每只股票的最新report_date
        c.execute('''
            SELECT w.code, w.name, w.market, w.category,
                   f.report_date, f.pe_ttm, f.pb, f.ps_ttm,
                   f.roe, f.roa, f.gross_margin, f.net_margin,
                   f.debt_ratio, f.current_ratio, f.eps, f.bvps,
                   f.total_revenue, f.revenue_yoy, f.net_profit, f.net_profit_yoy, f.dividend_yield
            FROM watch_list w
            LEFT JOIN stock_financial f ON w.code = f.code
                AND f.report_date = (
                    SELECT MAX(report_date) FROM stock_financial WHERE code = w.code
                )
            WHERE w.enabled = 1
            ORDER BY w.category, w.code
        ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "market": row[2],
            "category": row[3],
            "report_date": row[4],
            "pe_ttm": row[5],
            "pb": row[6],
            "ps_ttm": row[7],
            "roe": row[8],
            "roa": row[9],
            "gross_margin": row[10],
            "net_margin": row[11],
            "debt_ratio": row[12],
            "current_ratio": row[13],
            "eps": row[14],
            "bvps": row[15],
            "revenue": row[16],
            "revenue_yoy": row[17],
            "net_profit": row[18],
            "net_profit_yoy": row[19],
            "dividend_yield": row[20]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== 机会池模块 ====================

    @staticmethod
    def _normalize_stock_code(code: str) -> str:
        return (code or "").strip()

    @staticmethod
    def _code_without_prefix(code: str) -> str:
        text = (code or "").strip()
        if len(text) > 2 and text[:2].lower() in {"sh", "sz", "hk", "us"}:
            return text[2:]
        return text

    def _pool_filter_sql(self, pool_code: str, alias: str = "s", code_column: str = "symbol") -> tuple[str, List[Any]]:
        pool = (pool_code or "all").lower()
        ref = f"{alias}.{code_column}"
        if pool in {"all", "*"}:
            return "", []
        if pool in {"a", "ashare", "cn"}:
            return f" AND ({ref} GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]' OR lower({ref}) LIKE 'sh%' OR lower({ref}) LIKE 'sz%')", []
        if pool in {"hk", "hshare"}:
            return f" AND lower({ref}) LIKE 'hk%'", []
        if pool in {"us", "adr"}:
            return f" AND lower({ref}) LIKE 'us%'", []
        if pool in {"watch", "watchlist"}:
            return " AND EXISTS (SELECT 1 FROM watch_list w WHERE w.enabled = 1 AND (w.code = {0} OR substr(w.code, 3) = {0}))".format(ref), []
        return "", []

    def get_opportunity_pool_overview(self, pool_code: str = "all", limit: int = 180) -> Dict[str, Any]:
        """核心股票池总览与评分榜。

        这是一个只读聚合视图，优先使用 strategy_signals_v2；如果该表没有数据，
        降级到 stock_factor_snapshot。它不会创建新信号，也不会把半成品表写入主流程。
        """
        conn = self._get_db()
        c = conn.cursor()
        limit = max(1, min(int(limit or 180), 500))
        pool_sql, params = self._pool_filter_sql(pool_code, "s")

        try:
            latest = c.execute("SELECT MAX(as_of_date) FROM strategy_signals_v2").fetchone()[0]
        except Exception:
            latest = None

        leaderboard: List[Dict[str, Any]] = []
        if latest:
            c.execute(
                f"""
                SELECT
                    s.symbol,
                    COALESCE(NULLIF(s.symbol_name, ''), wf.name, wp.name, f.name, s.symbol) AS name,
                    COALESCE(wf.market, wp.market, f.market, '') AS market,
                    COALESCE(wf.category, wp.category, '') AS category,
                    s.as_of_date,
                    s.setup_type,
                    s.setup_subtype,
                    s.grade,
                    s.action,
                    s.action_reason,
                    s.score_total,
                    s.score_quality,
                    s.score_growth,
                    s.score_valuation,
                    s.score_technical,
                    s.score_flow,
                    s.risk_penalty,
                    s.risk_flags,
                    s.data_coverage_pct,
                    s.source_confidence
                FROM strategy_signals_v2 s
                LEFT JOIN watch_list wf
                    ON wf.enabled = 1 AND wf.code = s.symbol
                LEFT JOIN watch_list wp
                    ON wp.enabled = 1 AND substr(wp.code, 3) = s.symbol
                LEFT JOIN stock_financial f
                    ON f.code = s.symbol
                    AND f.report_date = (SELECT MAX(report_date) FROM stock_financial WHERE code = s.symbol)
                WHERE s.as_of_date = ?
                  AND COALESCE(s.eligibility_pass, 1) = 1
                  {pool_sql}
                ORDER BY COALESCE(s.score_total, 0) DESC, s.grade ASC, s.symbol ASC
                LIMIT ?
                """,
                [latest, *params, limit],
            )
            for row in c.fetchall():
                action = row[8] or "WATCH"
                setup_label = "/".join(str(item) for item in row[5:7] if item)
                leaderboard.append({
                    "code": row[0],
                    "display_code": row[0],
                    "name": row[1],
                    "market": row[2],
                    "category": row[3],
                    "as_of_date": row[4],
                    "setup_name": row[5],
                    "setup_label": setup_label or row[5],
                    "grade": row[7] or "C",
                    "action": action,
                    "action_label": action,
                    "action_reason": row[9],
                    "total_score": row[10],
                    "quality_score": row[11],
                    "growth_score": row[12],
                    "valuation_score": row[13],
                    "technical_score": row[14],
                    "flow_score": row[15],
                    "risk_score": row[16],
                    "risk_flags": row[17],
                    "data_coverage_pct": row[18],
                    "source_confidence": row[19],
                    "source_table": "strategy_signals_v2",
                })

        if not leaderboard:
            pool_sql, params = self._pool_filter_sql(pool_code, "s", "code")
            latest = c.execute("SELECT MAX(trade_date) FROM stock_factor_snapshot").fetchone()[0]
            c.execute(
                f"""
                SELECT
                    s.code,
                    COALESCE(w.name, f.name, s.code) AS name,
                    COALESCE(w.market, f.market, '') AS market,
                    COALESCE(w.category, '') AS category,
                    s.trade_date,
                    s.model,
                    s.quality,
                    s.growth,
                    s.valuation,
                    s.flow,
                    s.technical,
                    s.risk,
                    s.total
                FROM stock_factor_snapshot s
                LEFT JOIN watch_list w
                    ON w.enabled = 1 AND w.code = s.code
                LEFT JOIN stock_financial f
                    ON f.code = s.code
                    AND f.report_date = (SELECT MAX(report_date) FROM stock_financial WHERE code = s.code)
                WHERE s.trade_date = ?
                  {pool_sql}
                ORDER BY COALESCE(s.total, 0) DESC, s.code ASC
                LIMIT ?
                """,
                [latest, *params, limit],
            )
            for row in c.fetchall():
                total_score = row[12] or 0
                action = "WATCH" if total_score >= 55 else "SKIP"
                leaderboard.append({
                    "code": row[0],
                    "display_code": row[0],
                    "name": row[1],
                    "market": row[2],
                    "category": row[3],
                    "as_of_date": row[4],
                    "setup_name": row[5],
                    "setup_label": row[5],
                    "grade": "B" if total_score >= 65 else ("C" if total_score >= 45 else "D"),
                    "action": action,
                    "action_label": action,
                    "total_score": total_score,
                    "quality_score": row[6],
                    "growth_score": row[7],
                    "valuation_score": row[8],
                    "flow_score": row[9],
                    "technical_score": row[10],
                    "risk_score": row[11],
                    "source_table": "stock_factor_snapshot",
                })

        conn.close()
        total = len(leaderboard)
        buy_count = sum(
            1
            for item in leaderboard
            if str(item.get("action") or "").upper() in {"BUY", "ADD", "STRONG_BUY", "BUY_NOW"}
            or str(item.get("action") or "").upper().startswith("BUY")
        )
        watch_count = sum(1 for item in leaderboard if str(item.get("action") or "").upper() == "WATCH")
        avg_score = sum(float(item.get("total_score") or 0) for item in leaderboard) / max(1, total)
        summary = {
            "pool_code": pool_code,
            "as_of_date": leaderboard[0].get("as_of_date") if leaderboard else latest,
            "total_candidates": total,
            "buy_count": buy_count,
            "watch_count": watch_count,
            "average_score": round(avg_score, 1),
            "source_table": leaderboard[0].get("source_table") if leaderboard else None,
        }
        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "summary": summary,
            "leaderboard": leaderboard,
            "opportunities": leaderboard,
        }

    def get_opportunity_stock_detail(self, code: str, pool_code: str = "all") -> Dict[str, Any]:
        """单只股票机会池下钻。"""
        target = self._normalize_stock_code(code)
        target_raw = self._code_without_prefix(target)
        overview = self.get_opportunity_pool_overview(pool_code=pool_code, limit=500)
        match = None
        for item in overview.get("leaderboard") or []:
            item_code = self._normalize_stock_code(item.get("code"))
            if item_code == target or self._code_without_prefix(item_code) == target_raw:
                match = item
                break
        if not match:
            raise KeyError(code)

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "pool_code": pool_code,
            "signal": match,
            "fundamentals": self.get_stock_fundamentals([match["code"]]),
            "valuation": self.get_valuation_latest(match["code"]),
            "technical": self.get_technical_latest(match["code"]),
        }

    def sync_stock_pools(self, pool_code: str = "all") -> Dict[str, Any]:
        """机会池同步占位。

        当前机会池由 strategy_signals_v2 / stock_factor_snapshot 派生，不再伪装成
        独立采集任务。这个接口保留给前端按钮，返回只读状态。
        """
        overview = self.get_opportunity_pool_overview(pool_code=pool_code, limit=20)
        return {
            "status": "noop",
            "message": "机会池当前由本地评分表派生，无需单独同步。",
            "summary": overview.get("summary", {}),
        }

    def import_fundamentals_csv(self, csv_path: str) -> Dict:
        """从CSV导入财务数据"""
        import csv

        if not os.path.exists(csv_path):
            return {"status": "error", "message": f"文件不存在: {csv_path}"}

        conn = self._get_db()
        c = conn.cursor()

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                imported = 0
                for row in reader:
                    try:
                        c.execute('''
                            INSERT OR REPLACE INTO stock_financial
                            (code, name, market, report_date, pe_ttm, pb, ps_ttm,
                             roe, roa, gross_margin, net_margin, debt_ratio, current_ratio,
                             eps, bvps, revenue, revenue_yoy, net_profit, net_profit_yoy, dividend_yield)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row.get("code"),
                            row.get("name"),
                            row.get("market"),
                            row.get("report_date"),
                            float(row.get("pe_ttm")) if row.get("pe_ttm") and row.get("pe_ttm") != '-' else None,
                            float(row.get("pb")) if row.get("pb") and row.get("pb") != '-' else None,
                            float(row.get("ps_ttm")) if row.get("ps_ttm") and row.get("ps_ttm") != '-' else None,
                            float(row.get("roe")) if row.get("roe") and row.get("roe") != '-' else None,
                            float(row.get("roa")) if row.get("roa") and row.get("roa") != '-' else None,
                            float(row.get("gross_margin")) if row.get("gross_margin") and row.get("gross_margin") != '-' else None,
                            float(row.get("net_margin")) if row.get("net_margin") and row.get("net_margin") != '-' else None,
                            float(row.get("debt_ratio")) if row.get("debt_ratio") and row.get("debt_ratio") != '-' else None,
                            float(row.get("current_ratio")) if row.get("current_ratio") and row.get("current_ratio") != '-' else None,
                            float(row.get("eps")) if row.get("eps") and row.get("eps") != '-' else None,
                            float(row.get("bvps")) if row.get("bvps") and row.get("bvps") != '-' else None,
                            float(row.get("revenue")) if row.get("revenue") and row.get("revenue") != '-' else None,
                            float(row.get("revenue_yoy")) if row.get("revenue_yoy") and row.get("revenue_yoy") != '-' else None,
                            float(row.get("net_profit")) if row.get("net_profit") and row.get("net_profit") != '-' else None,
                            float(row.get("net_profit_yoy")) if row.get("net_profit_yoy") and row.get("net_profit_yoy") != '-' else None,
                            float(row.get("dividend_yield")) if row.get("dividend_yield") and row.get("dividend_yield") != '-' else None
                        ))
                        imported += 1
                    except Exception as e:
                        print(f"导入行失败: {e}")
                        continue

                conn.commit()
                conn.close()
                return {"status": "success", "imported": imported}

        except Exception as e:
            conn.close()
            return {"status": "error", "message": str(e)}

    # ==================== 估值水位模块 ====================

    def get_valuation_latest(self, code: str = None) -> List[Dict]:
        """获取最新估值水位"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, trade_date, pe_ttm, pe_percentile_3y, pe_percentile_5y, pe_percentile_10y,
                       pb, pb_percentile_3y, pb_percentile_5y, pb_percentile_10y, valuation_level
                FROM valuation_bands
                WHERE code = ?
                ORDER BY trade_date DESC LIMIT 1
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, trade_date, pe_ttm, pe_percentile_3y, pe_percentile_5y, pe_percentile_10y,
                       pb, pb_percentile_3y, pb_percentile_5y, pb_percentile_10y, valuation_level
                FROM valuation_bands
                WHERE (code, trade_date) IN (
                    SELECT code, MAX(trade_date) FROM valuation_bands GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "date": row[2],
            "pe_ttm": row[3],
            "pe_percentile_3y": row[4],
            "pe_percentile_5y": row[5],
            "pe_percentile_10y": row[6],
            "pb": row[7],
            "pb_percentile_3y": row[8],
            "pb_percentile_5y": row[9],
            "pb_percentile_10y": row[10],
            "valuation_level": row[11]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_valuation_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取估值历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, pe_ttm, pe_percentile_5y, pb, pb_percentile_5y
            FROM valuation_bands
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "pe_ttm": row[1],
            "pe_percentile_5y": row[2],
            "pb": row[3],
            "pb_percentile_5y": row[4]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== 技术指标模块 ====================

    def get_technical_latest(self, code: str = None) -> List[Dict]:
        """获取最新技术指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, trade_date, ma5, ma10, ma20, ma50, ma200,
                       macd, macd_signal, macd_hist, rsi_14, atr_14, atr_pct,
                       beta_1y, trend_signal
                FROM technical_indicators
                WHERE code = ?
                ORDER BY trade_date DESC LIMIT 1
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, trade_date, ma5, ma10, ma20, ma50, ma200,
                       macd, macd_signal, macd_hist, rsi_14, atr_14, atr_pct,
                       beta_1y, trend_signal
                FROM technical_indicators
                WHERE (code, trade_date) IN (
                    SELECT code, MAX(trade_date) FROM technical_indicators GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "date": row[2],
            "ma5": row[3],
            "ma10": row[4],
            "ma20": row[5],
            "ma50": row[6],
            "ma200": row[7],
            "macd": row[8],
            "macd_signal": row[9],
            "macd_hist": row[10],
            "rsi_14": row[11],
            "atr_14": row[12],
            "atr_pct": row[13],
            "beta_1y": row[14],
            "trend_signal": row[15]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_technical_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取技术指标历史"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, close, ma20, ma50, ma200, macd, macd_signal, rsi_14
            FROM technical_indicators t
            JOIN index_history i ON t.code = i.code AND t.trade_date = i.trade_date
            WHERE t.code = ? AND t.trade_date >= ?
            ORDER BY t.trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "close": row[1],
            "ma20": row[2],
            "ma50": row[3],
            "ma200": row[4],
            "macd": row[5],
            "macd_signal": row[6],
            "rsi_14": row[7]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== 行业模型模块 ====================

    def get_tmt_metrics(self, code: str = None) -> List[Dict]:
        """获取TMT行业指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, report_date, mau, dau, arpu, arppu, paying_ratio,
                       retention_d1, retention_d7, retention_d30,
                       revenue, revenue_yoy, net_profit, net_profit_yoy,
                       gross_margin, operating_margin, rd_ratio
                FROM sector_tmt
                WHERE code = ?
                ORDER BY report_date DESC
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, report_date, mau, dau, arpu, arppu, paying_ratio,
                       retention_d1, retention_d7, retention_d30,
                       revenue, revenue_yoy, net_profit, net_profit_yoy,
                       gross_margin, operating_margin, rd_ratio
                FROM sector_tmt
                WHERE (code, report_date) IN (
                    SELECT code, MAX(report_date) FROM sector_tmt GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "report_date": row[2],
            "mau": row[3],
            "dau": row[4],
            "arpu": row[5],
            "arppu": row[6],
            "paying_ratio": row[7],
            "retention_d1": row[8],
            "retention_d7": row[9],
            "retention_d30": row[10],
            "revenue": row[11],
            "revenue_yoy": row[12],
            "net_profit": row[13],
            "net_profit_yoy": row[14],
            "gross_margin": row[15],
            "operating_margin": row[16],
            "rd_ratio": row[17]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_biotech_pipeline(self, company_code: str = None, phase: str = None) -> List[Dict]:
        """获取创新药管线"""
        conn = self._get_db()
        c = conn.cursor()

        query = '''
            SELECT company_code, company_name, drug_name, drug_type, indication,
                   phase, phase_cn, start_date, expected_approval, status, region,
                   partner, market_size_est, notes
            FROM sector_biotech WHERE 1=1
        '''
        params = []

        if company_code:
            query += ' AND company_code = ?'
            params.append(company_code)
        if phase:
            query += ' AND phase = ?'
            params.append(phase)

        query += ' ORDER BY company_code, phase'

        c.execute(query, params)

        result = [{
            "company_code": row[0],
            "company_name": row[1],
            "drug_name": row[2],
            "drug_type": row[3],
            "indication": row[4],
            "phase": row[5],
            "phase_cn": row[6],
            "start_date": row[7],
            "expected_approval": row[8],
            "status": row[9],
            "region": row[10],
            "partner": row[11],
            "market_size_est": row[12],
            "notes": row[13]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_consumer_metrics(self, code: str = None) -> List[Dict]:
        """获取消费行业指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, report_date, revenue, revenue_yoy, same_store_sales_yoy,
                       store_count, store_change, online_ratio, gross_margin, operating_margin,
                       inventory_turnover, accounts_receivable_days, marketing_ratio,
                       member_count, member_growth_yoy
                FROM sector_consumer
                WHERE code = ?
                ORDER BY report_date DESC
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, report_date, revenue, revenue_yoy, same_store_sales_yoy,
                       store_count, store_change, online_ratio, gross_margin, operating_margin,
                       inventory_turnover, accounts_receivable_days, marketing_ratio,
                       member_count, member_growth_yoy
                FROM sector_consumer
                WHERE (code, report_date) IN (
                    SELECT code, MAX(report_date) FROM sector_consumer GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "report_date": row[2],
            "revenue": row[3],
            "revenue_yoy": row[4],
            "same_store_sales_yoy": row[5],
            "store_count": row[6],
            "store_change": row[7],
            "online_ratio": row[8],
            "gross_margin": row[9],
            "operating_margin": row[10],
            "inventory_turnover": row[11],
            "accounts_receivable_days": row[12],
            "marketing_ratio": row[13],
            "member_count": row[14],
            "member_growth_yoy": row[15]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== ETL日志模块 ====================

    def log_etl_job(self, job_name: str, job_type: str, start_time: str,
                    status: str, records_processed: int = 0, records_failed: int = 0,
                    error_message: str = None, details: str = None) -> int:
        """记录ETL任务日志"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            INSERT INTO etl_logs (job_name, job_type, start_time, status,
                                  records_processed, records_failed, error_message, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_name, job_type, start_time, status, records_processed,
              records_failed, error_message, details))

        log_id = c.lastrowid
        conn.commit()
        conn.close()
        return log_id

    def update_etl_job(self, log_id: int, end_time: str, status: str,
                       records_processed: int = None, records_failed: int = None,
                       error_message: str = None):
        """更新ETL任务日志"""
        conn = self._get_db()
        c = conn.cursor()

        updates = ["end_time = ?", "status = ?"]
        params = [end_time, status]

        if records_processed is not None:
            updates.append("records_processed = ?")
            params.append(records_processed)
        if records_failed is not None:
            updates.append("records_failed = ?")
            params.append(records_failed)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        params.append(log_id)

        c.execute(f'''
            UPDATE etl_logs SET {', '.join(updates)} WHERE id = ?
        ''', params)

        conn.commit()
        conn.close()

    def get_etl_logs(self, limit: int = 50, job_type: str = None) -> List[Dict]:
        """获取ETL日志"""
        conn = self._get_db()
        c = conn.cursor()

        if job_type:
            c.execute('''
                SELECT id, job_name, job_type, start_time, end_time, status,
                       records_processed, records_failed, error_message
                FROM etl_logs WHERE job_type = ?
                ORDER BY start_time DESC LIMIT ?
            ''', (job_type, limit))
        else:
            c.execute('''
                SELECT id, job_name, job_type, start_time, end_time, status,
                       records_processed, records_failed, error_message
                FROM etl_logs
                ORDER BY start_time DESC LIMIT ?
            ''', (limit,))

        result = [{
            "id": row[0],
            "job_name": row[1],
            "job_type": row[2],
            "start_time": row[3],
            "end_time": row[4],
            "status": row[5],
            "records_processed": row[6],
            "records_failed": row[7],
            "error_message": row[8]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== CSV导入功能 ====================

    def import_csv_to_table(self, table_name: str, csv_path: str) -> Dict:
        """从CSV导入数据到指定表"""
        import csv

        if not os.path.exists(csv_path):
            return {"status": "error", "message": f"文件不存在: {csv_path}"}

        conn = self._get_db()
        c = conn.cursor()

        # 获取表结构
        c.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in c.fetchall() if col[1] != 'id' and col[1] != 'created_at']

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                # 检查CSV列是否匹配
                csv_columns = reader.fieldnames
                common_columns = [col for col in columns if col in csv_columns]

                if not common_columns:
                    conn.close()
                    return {"status": "error", "message": "CSV列与表结构不匹配"}

                imported = 0
                for row in reader:
                    values = [row.get(col) for col in common_columns]
                    placeholders = ', '.join(['?' for _ in common_columns])
                    col_names = ', '.join(common_columns)

                    try:
                        c.execute(f'''
                            INSERT OR REPLACE INTO {table_name} ({col_names})
                            VALUES ({placeholders})
                        ''', values)
                        imported += 1
                    except Exception as e:
                        print(f"导入行失败: {e}")
                        continue

                conn.commit()
                conn.close()
                return {"status": "success", "imported": imported, "table": table_name}

        except Exception as e:
            conn.close()
            return {"status": "error", "message": str(e)}

    def get_data_health_overview(
        self,
        cache_keys: Dict[str, int] = None,
        snapshot_ttls: Dict[str, int] = None,
    ) -> Dict[str, Any]:
        """数据健康概览"""
        conn = None
        try:
            self._ensure_snapshot_table()
            conn = self._get_db()
            storage = []
            tracked_keys = cache_keys or snapshot_ttls or {}
            if tracked_keys:
                for key, ttl in tracked_keys.items():
                    snapshot = self.get_market_snapshot(key, ttl_seconds=ttl)
                    if snapshot:
                        storage.append({
                            "key": key,
                            "updated_at": snapshot.get("updated_at"),
                            "expires_at": snapshot.get("expires_at"),
                            "age_seconds": snapshot.get("age_seconds"),
                            "is_fresh": bool(snapshot.get("is_fresh")),
                            "source": snapshot.get("source"),
                            "fetch_latency_ms": snapshot.get("fetch_latency_ms"),
                            "notes": snapshot.get("notes"),
                        })
                    else:
                        storage.append({"key": key, "is_fresh": False, "source": "none"})
            else:
                c = conn.cursor()
                c.execute("SELECT snapshot_key FROM market_snapshots ORDER BY updated_at DESC")
                for (key,) in c.fetchall():
                    snapshot = self.get_market_snapshot(key)
                    if snapshot:
                        storage.append({
                            "key": key,
                            "updated_at": snapshot.get("updated_at"),
                            "expires_at": snapshot.get("expires_at"),
                            "age_seconds": snapshot.get("age_seconds"),
                            "is_fresh": bool(snapshot.get("is_fresh")),
                            "source": snapshot.get("source"),
                            "fetch_latency_ms": snapshot.get("fetch_latency_ms"),
                            "notes": snapshot.get("notes"),
                        })

            fresh_count = sum(1 for s in storage if s.get("is_fresh"))
            stale_count = len(storage) - fresh_count
            return {
                "summary": {
                    "total": len(storage),
                    "fresh": fresh_count,
                    "stale": stale_count,
                    "fresh_pct": round(fresh_count * 100.0 / max(1, len(storage)), 1),
                },
                "storage": storage,
            }
        except Exception as exc:
            return {
                "summary": {"total": 0, "fresh": 0, "stale": 0, "fresh_pct": 0.0},
                "storage": [],
                "error": str(exc)[:300],
            }
        finally:
            if conn:
                conn.close()

    # ==================== 数据资产治理 ====================

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _parse_asset_date(value: Any) -> Optional[datetime]:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if len(text) >= 10:
            text = text[:10]
        try:
            return datetime.strptime(text, "%Y-%m-%d")
        except Exception:
            return None

    def _get_table_latest_value(self, conn: sqlite3.Connection, table_name: str, columns: List[str]) -> Dict[str, Any]:
        candidates = [
            "trade_date",
            "date",
            "report_date",
            "as_of_date",
            "published",
            "fetched_at",
            "updated_at",
            "created_at",
            "start_time",
        ]
        c = conn.cursor()
        for column in candidates:
            if column not in columns:
                continue
            try:
                value = c.execute(f"SELECT MAX({column}) FROM {table_name}").fetchone()[0]
                if value:
                    return {"column": column, "value": value}
            except Exception:
                continue
        return {"column": None, "value": None}

    def _load_indicator_registry(self, conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
        if not self._table_exists(conn, "indicator_registry"):
            return {}
        rows = conn.execute("""
            SELECT indicator_key, layer, table_name, display_name, status, source,
                   freshness_sla_days, decision_usage, reason, updated_at
            FROM indicator_registry
        """).fetchall()
        registry = {}
        for row in rows:
            item = {
                "indicator_key": row[0],
                "layer": row[1],
                "table_name": row[2],
                "display_name": row[3],
                "status": row[4],
                "source": row[5],
                "freshness_sla_days": row[6],
                "decision_usage": row[7],
                "reason": row[8],
                "registry_updated_at": row[9],
            }
            registry.setdefault(row[2], item)
        return registry

    def get_data_asset_overview(self) -> Dict[str, Any]:
        """Return table-level asset status for pruning and UI gating."""
        conn = None
        try:
            conn = self._get_db()
            c = conn.cursor()
            tables = [
                row[0]
                for row in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
                if not row[0].startswith("sqlite_")
            ]
            registry_by_table = self._load_indicator_registry(conn)
            assets = []
            now = datetime.now()
            for table in tables:
                columns = self._get_table_columns(conn, table)
                row_count = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                latest = self._get_table_latest_value(conn, table, columns)
                registry = registry_by_table.get(table)
                status = (registry or {}).get("status")
                if not status:
                    status = "remove_candidate" if row_count == 0 else "review"

                latest_dt = self._parse_asset_date(latest.get("value"))
                freshness_sla_days = (registry or {}).get("freshness_sla_days")
                age_days = None
                health = "unknown"
                if row_count == 0:
                    health = "empty"
                elif latest_dt is not None:
                    age_days = max(0, (now - latest_dt).days)
                    if freshness_sla_days is not None and age_days > int(freshness_sla_days):
                        health = "stale"
                    else:
                        health = "fresh"
                if status in {"deprecated", "removed"}:
                    health = status

                assets.append({
                    "table": table,
                    "rows": row_count,
                    "latest_column": latest.get("column"),
                    "latest_value": latest.get("value"),
                    "age_days": age_days,
                    "status": status,
                    "health": health,
                    "indicator_key": (registry or {}).get("indicator_key"),
                    "layer": (registry or {}).get("layer"),
                    "display_name": (registry or {}).get("display_name"),
                    "decision_usage": (registry or {}).get("decision_usage"),
                    "reason": (registry or {}).get("reason"),
                })

            summary = {"total": len(assets), "by_status": {}, "by_health": {}}
            for item in assets:
                summary["by_status"][item["status"]] = summary["by_status"].get(item["status"], 0) + 1
                summary["by_health"][item["health"]] = summary["by_health"].get(item["health"], 0) + 1
            return {
                "generated_at": datetime.now().replace(microsecond=0).isoformat(),
                "summary": summary,
                "assets": assets,
            }
        except Exception as exc:
            return {
                "generated_at": datetime.now().replace(microsecond=0).isoformat(),
                "summary": {"total": 0, "by_status": {}, "by_health": {}},
                "assets": [],
                "error": str(exc)[:300],
            }
        finally:
            if conn:
                conn.close()

    # ==================== 策略回测与性能 ====================

    def get_strategy_perf_overview(self, setup_name: Optional[str] = None, windows: List[int] = None) -> Dict[str, Any]:
        """策略分层稳定性视图: 近N日胜率、盈亏比、回撤"""
        if windows is None:
            windows = [20, 60, 120]

        conn = self._get_db()
        c = conn.cursor()

        # 确保表存在
        try:
            c.execute("SELECT COUNT(*) FROM strategy_backtest_stats")
        except sqlite3.OperationalError:
            conn.close()
            return {"error": "回测表未初始化", "windows": [], "setups": [], "recent_signals": [], "as_of_date": ""}

        # 获取最新统计日期
        c.execute("SELECT MAX(stat_date) FROM strategy_backtest_stats")
        row = c.fetchone()
        latest_date = row[0] if row and row[0] else datetime.now().strftime("%Y-%m-%d")

        # 按窗口聚合: 前端期望 windows 格式
        window_stats = []
        for w in windows:
            where_clause = ""
            params: list = []
            if setup_name:
                where_clause = " WHERE setup_name = ?"
                params.append(setup_name)
            c.execute(f'''
                SELECT setup_name,
                       SUM(sample_size) as total_sample,
                       AVG(win_rate) as avg_win_rate,
                       AVG(profit_loss_ratio) as avg_pf,
                       AVG(avg_max_drawdown) as avg_dd
                FROM strategy_backtest_stats
                WHERE stat_date >= date('{latest_date}', '-{w} days')
                {where_clause}
                GROUP BY stat_date
                ORDER BY stat_date DESC
            ''', params)
            rows = c.fetchall()
            total_sample = sum(r[1] or 0 for r in rows)
            avg_wr = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
            avg_pf = (sum(r[3] or 0 for r in rows) / len(rows)) if rows else None
            avg_dd = (sum(r[4] or 0 for r in rows) / len(rows)) if rows else 0
            window_stats.append({
                "window_size": w,
                "sample_size": total_sample,
                "win_rate": round(avg_wr, 2),
                "profit_factor": round(avg_pf, 2) if avg_pf else None,
                "max_drawdown": round(avg_dd, 2),
            })

        # setup 分层对比
        setups = []
        where_clause = "WHERE stat_date = ?"
        params = [latest_date]
        if setup_name:
            where_clause += " AND setup_name = ?"
            params.append(setup_name)
        c.execute(f'''
            SELECT setup_name,
                   SUM(sample_size),
                   AVG(win_rate),
                   AVG(avg_return),
                   AVG(profit_loss_ratio),
                   AVG(avg_max_drawdown)
            FROM strategy_backtest_stats
            {where_clause}
            GROUP BY setup_name
            ORDER BY AVG(win_rate) DESC
        ''', params)
        for r in c.fetchall():
            setups.append({
                "setup_name": r[0],
                "setup_label": r[0],
                "sample_size": r[1] or 0,
                "win_rate": round(r[2], 2) if r[2] else 0,
                "avg_return": round(r[3], 2) if r[3] else 0,
                "profit_factor": round(r[4], 2) if r[4] else None,
                "max_drawdown": round(r[5], 2) if r[5] else 0,
            })

        # 最近信号日志
        c.execute('''
            SELECT signal_date, code, setup_name, hold_days, entry_price, exit_price,
                   max_gain, max_drawdown, win_flag, invalidated
            FROM signal_labels
            ORDER BY signal_date DESC LIMIT 50
        ''')
        recent_signals = []
        for r in c.fetchall():
            ret_pct = ((r[5] - r[4]) / r[4] * 100) if r[4] else 0
            recent_signals.append({
                "code": r[1],
                "name": r[1],
                "setup_name": r[2],
                "setup_label": r[2],
                "signal_time": r[0],
                "outcome_return": round(ret_pct, 2),
                "action": "win" if r[8] else "loss",
                "hold_days": r[3],
                "trigger_factors": [],
            })

        conn.close()
        return {
            "as_of_date": latest_date,
            "windows": window_stats,
            "setups": setups,
            "recent_signals": recent_signals,
        }

    def sync_strategy_runtime_data(self, windows: List[int] = None, allow_workbench_fallback: bool = True) -> Dict[str, Any]:
        """触发回测缓存刷新"""
        from quant_workbench.db_views import ensure_strategy_tables
        ensure_strategy_tables()

        try:
            from quant_workbench.backtest import refresh_backtest_cache
            result = refresh_backtest_cache()
            return {
                "status": "success",
                "processed_codes": result.get("processed_codes", 0),
                "labels": result.get("labels", 0),
                "stats": result.get("stats", 0),
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
            }

    def get_signal_journal(self, limit: int = 40, setup_name: Optional[str] = None) -> List[Dict]:
        """最近策略信号日志"""
        conn = self._get_db()
        c = conn.cursor()
        try:
            where = "1=1"
            params: list = []
            if setup_name:
                where += " AND setup_name = ?"
                params.append(setup_name)
            c.execute(f'''
                SELECT signal_date, code, setup_name, hold_days, entry_price, exit_price,
                       max_gain, max_drawdown, win_flag, invalidated
                FROM signal_labels
                WHERE {where}
                ORDER BY signal_date DESC LIMIT ?
            ''', params + [limit])
            result = []
            for r in c.fetchall():
                result.append({
                    "signal_date": r[0],
                    "code": r[1],
                    "setup_name": r[2],
                    "hold_days": r[3],
                    "entry_price": r[4],
                    "exit_price": r[5],
                    "max_gain": r[6],
                    "max_drawdown": r[7],
                    "win_flag": r[8],
                    "invalidated": r[9],
                    "return_pct": round(((r[5] - r[4]) / r[4] * 100), 2) if r[4] else 0,
                })
            return result
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()

    def get_hk_hot_rank_latest(self, limit: int = 50) -> List[Dict]:
        """获取最新港股热榜"""
        conn = self._get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT rank, code, name, price, change_pct, volume, turnover
                FROM hk_hot_rank
                WHERE trade_date = (SELECT MAX(trade_date) FROM hk_hot_rank)
                ORDER BY rank ASC LIMIT ?
            """, (limit,))
            result = []
            for r in c.fetchall():
                result.append({
                    "rank": r[0], "code": r[1], "name": r[2],
                    "price": r[3], "change_pct": r[4],
                    "volume": r[5], "turnover": r[6],
                })
            return result
        finally:
            conn.close()

    def get_hk_indices_latest(self) -> List[Dict]:
        """获取最新港股指数日线"""
        conn = self._get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT code, name, close, change_pct, trade_date
                FROM hk_indices
                WHERE trade_date = (SELECT MAX(trade_date) FROM hk_indices)
                ORDER BY code
            """)
            result = []
            for r in c.fetchall():
                result.append({
                    "code": r[0], "name": r[1], "close": r[2],
                    "change_pct": r[3], "date": r[4],
                })
            return result
        finally:
            conn.close()

    def get_hk_repurchase_latest(self, days: int = 30) -> List[Dict]:
        """获取最新港股回购数据"""
        conn = self._get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT code, name, repurchase_amount, trade_date
                FROM hk_repurchase
                ORDER BY trade_date DESC LIMIT 500
            """)
            result = []
            for r in c.fetchall():
                result.append({
                    "code": r[0], "name": r[1],
                    "repurchase_amount": r[2], "date": r[3],
                })
            return result
        finally:
            conn.close()


# 测试
if __name__ == "__main__":
    service = InvestmentDataService()

    print("=== 投资数据服务测试 ===\n")

    # 测试指数
    print("指数列表:")
    for idx in service.get_index_list():
        latest = service.get_index_latest(idx['code'])
        if latest:
            print(f"  {latest['name']}: {latest['close']} ({latest['date']})")

    # 测试利率
    print("\n最新利率:")
    rates = service.get_interest_rates_latest()
    if rates:
        print(f"  SHIBOR隔夜: {rates['shibor']['overnight']}")
        print(f"  HIBOR隔夜: {rates['hibor']['overnight']}")

    # 测试市场情绪
    print("\n市场情绪:")
    sentiment = service.get_market_sentiment_latest()
    if sentiment:
        print(f"  上涨: {sentiment['up_count']}, 下跌: {sentiment['down_count']}")
