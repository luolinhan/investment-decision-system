"""
投资数据API服务 - 从本地数据库读取
"""
import json
import sqlite3
import os
import re
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, List, Any, Optional

DB_PATH = "data/investment.db"
REPORTS_DB_PATH = "data/reports.db"

CORE_STOCK_POOL_CONFIGS = {
    "all": {"name": "核心池总览", "source": "local"},
    "hs300": {"name": "沪深300", "source": "sina_index", "symbol": "000300", "market": "A"},
    "star50": {"name": "科创50", "source": "sina_index", "symbol": "000688", "market": "A"},
    "hstech": {
        "name": "恒生科技",
        "source": "investing_hk",
        "url": "https://cn.investing.com/indices/hang-seng-tech-components",
        "market": "HK",
    },
    "ths_beauty100": {
        "name": "同花顺漂亮100",
        "source": "ths_concept",
        "code": "308718",
        "legulegu_url": "https://legulegu.com/stockdata/concepts/268",
        "market": "A",
    },
}

HSTECH_FALLBACK_CODE_MAP = {
    "比亚迪股份": "01211",
    "联想集团": "00992",
    "中芯国际": "00981",
    "金蝶国际": "00268",
    "比亚迪电子": "00285",
    "金山软件": "03888",
    "腾讯控股": "00700",
    "舜宇光学科技": "02382",
    "阿里健康": "00241",
    "华虹半导体": "01347",
    "小米集团": "01810",
    "小米集团-W": "01810",
    "美团": "03690",
    "美团-W": "03690",
    "同程艺龙": "00780",
    "同程旅行": "00780",
    "阿里巴巴": "09988",
    "阿里巴巴-SW": "09988",
    "网易": "09999",
    "网易-S": "09999",
    "京东集团": "09618",
    "京东集团-SW": "09618",
    "京东健康": "06618",
    "海尔智家": "06690",
    "快手": "01024",
    "快手-W": "01024",
    "百度集团": "09888",
    "百度集团-SW": "09888",
    "哔哩哔哩": "09626",
    "哔哩哔哩-W": "09626",
    "携程集团": "09961",
    "携程集团-S": "09961",
    "小鹏汽车": "09868",
    "小鹏汽车-W": "09868",
    "理想汽车": "02015",
    "理想汽车-W": "02015",
    "商汤": "00020",
    "商汤-W": "00020",
    "蔚来": "09866",
    "蔚来-SW": "09866",
    "零跑汽车": "09863",
    "腾讯音乐": "01698",
    "腾讯音乐-SW": "01698",
    "美的集团": "00300",
    "地平线机器人": "09660",
    "地平线机器人-W": "09660",
}

HSTECH_FALLBACK_CONSTITUENTS = [
    "比亚迪股份",
    "联想集团",
    "中芯国际",
    "金蝶国际",
    "比亚迪电子",
    "金山软件",
    "腾讯控股",
    "舜宇光学科技",
    "阿里健康",
    "华虹半导体",
    "小米集团-W",
    "美团-W",
    "同程艺龙",
    "阿里巴巴-SW",
    "网易-S",
    "京东集团-SW",
    "京东健康",
    "海尔智家",
    "快手",
    "百度集团",
    "哔哩哔哩",
    "携程集团",
    "小鹏汽车-W",
    "理想汽车-W",
    "商汤-W",
    "蔚来-SW",
    "零跑汽车",
    "腾讯音乐-SW",
    "美的集团",
    "地平线机器人-W",
]

SETUP_LABELS = {
    "quality_value_recovery": "质量估值修复",
    "earnings_revision_breakout": "预期上修突破",
    "risk_on_pullback_leader": "风险偏好回踩龙头",
    "sector_catalyst_confirmation": "行业催化确认",
    "unknown": "未分类",
    "ALL": "全策略",
}

SETUP_DEFAULT_FACTORS = {
    "quality_value_recovery": [
        "估值分位处于可接受区间",
        "中期趋势保持不破位",
        "盈利质量未明显恶化",
    ],
    "earnings_revision_breakout": [
        "盈利预期上修或景气改善",
        "量价突破确认",
        "资金流向未转弱",
    ],
    "risk_on_pullback_leader": [
        "市场风险偏好改善",
        "回踩后结构保持强势",
        "高Beta阶段仓位受控",
    ],
    "sector_catalyst_confirmation": [
        "行业催化事件临近",
        "板块相对强度抬升",
        "主线资金延续",
    ],
}

SETUP_DEFAULT_INVALIDATIONS = {
    "quality_value_recovery": [
        "跌破20日线且量能未恢复",
        "估值抬升但基本面未兑现",
    ],
    "earnings_revision_breakout": [
        "突破后3日内失守关键位",
        "预期上修信号反转",
    ],
    "risk_on_pullback_leader": [
        "市场转为risk_off",
        "回踩低点被有效跌破",
    ],
    "sector_catalyst_confirmation": [
        "催化兑现后资金未延续",
        "板块强度连续走弱",
    ],
}


class InvestmentDataService:
    """投资数据服务"""

    def __init__(self):
        self.db_path = DB_PATH
        self._ensure_runtime_tables()

    def _get_db(self):
        return sqlite3.connect(self.db_path)

    def _ensure_runtime_tables(self):
        """运行期快照表，用于将实时结果落到本地 SQLite。"""
        conn = self._get_db()
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                snapshot_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                expires_at TEXT,
                source TEXT,
                fetch_latency_ms INTEGER,
                notes TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_time TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                setup_name TEXT NOT NULL,
                setup_label TEXT,
                action TEXT,
                position_range TEXT,
                entry_price REAL,
                exit_price REAL,
                hold_days INTEGER,
                outcome_return REAL,
                max_drawdown REAL,
                win_flag INTEGER,
                invalidated INTEGER DEFAULT 0,
                trigger_factors TEXT,
                invalid_conditions TEXT,
                risk_flags TEXT,
                data_source TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_journal_unique
            ON signal_journal(signal_time, code, setup_name, hold_days, data_source)
            """
        )
        c.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signal_journal_setup_time
            ON signal_journal(setup_name, signal_time DESC)
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_perf_daily (
                trade_date TEXT NOT NULL,
                setup_name TEXT NOT NULL,
                setup_label TEXT,
                window_size INTEGER NOT NULL,
                sample_size INTEGER NOT NULL,
                win_rate REAL,
                avg_return REAL,
                profit_factor REAL,
                avg_max_drawdown REAL,
                max_drawdown REAL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(trade_date, setup_name, window_size)
            )
            """
        )
        c.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_strategy_perf_daily_setup_date
            ON strategy_perf_daily(setup_name, trade_date DESC)
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_regime_calibration (
                week_id TEXT NOT NULL,
                regime TEXT NOT NULL,
                setup_name TEXT NOT NULL,
                setup_label TEXT,
                sample_size INTEGER NOT NULL,
                win_rate REAL,
                avg_return REAL,
                profit_factor REAL,
                recommended_weight REAL,
                threshold_score REAL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(week_id, regime, setup_name)
            )
            """
        )
        c.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_strategy_regime_week
            ON strategy_regime_calibration(week_id, regime)
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_time TEXT NOT NULL,
                code TEXT NOT NULL,
                setup_name TEXT,
                side TEXT NOT NULL,
                signal_price REAL NOT NULL,
                executed_price REAL NOT NULL,
                quantity REAL,
                executed_time TEXT,
                source TEXT,
                note TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_execution_journal_time
            ON execution_journal(executed_time DESC)
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_pool_constituents (
                pool_code TEXT NOT NULL,
                pool_name TEXT NOT NULL,
                member_code TEXT NOT NULL,
                member_name TEXT,
                member_market TEXT,
                raw_code TEXT,
                weight REAL,
                as_of_date TEXT,
                source TEXT,
                extra_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(pool_code, member_code)
            )
            """
        )
        c.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_stock_pool_member_code
            ON stock_pool_constituents(member_code)
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_pool_status (
                pool_code TEXT PRIMARY KEY,
                pool_name TEXT NOT NULL,
                source TEXT,
                as_of_date TEXT,
                member_count INTEGER DEFAULT 0,
                sync_status TEXT,
                note TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def get_market_snapshot(self, snapshot_key: str, max_age_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """读取本地行情快照。"""
        conn = self._get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT payload_json, updated_at, expires_at, source, fetch_latency_ms, notes
            FROM market_snapshots
            WHERE snapshot_key = ?
            """,
            (snapshot_key,),
        )
        row = c.fetchone()
        conn.close()

        if not row:
            return None

        payload_json, updated_at, expires_at, source, fetch_latency_ms, notes = row
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            return None

        updated_dt = None
        expires_dt = None
        try:
            updated_dt = datetime.fromisoformat(updated_at) if updated_at else None
        except ValueError:
            updated_dt = None
        try:
            expires_dt = datetime.fromisoformat(expires_at) if expires_at else None
        except ValueError:
            expires_dt = None

        age_seconds = None
        if updated_dt:
            age_seconds = max(0, int((datetime.now() - updated_dt).total_seconds()))

        is_fresh = False
        if max_age_seconds is not None and age_seconds is not None:
            is_fresh = age_seconds <= max_age_seconds
        elif expires_dt:
            is_fresh = expires_dt >= datetime.now()

        return {
            "snapshot_key": snapshot_key,
            "payload": payload,
            "updated_at": updated_at,
            "expires_at": expires_at,
            "source": source,
            "fetch_latency_ms": fetch_latency_ms,
            "notes": notes,
            "age_seconds": age_seconds,
            "is_fresh": is_fresh,
        }

    def save_market_snapshot(
        self,
        snapshot_key: str,
        payload: Dict[str, Any],
        ttl_seconds: int,
        source: str = "realtime",
        fetch_latency_ms: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """保存本地行情快照。"""
        updated_at = datetime.now().replace(microsecond=0).isoformat()
        expires_at = (datetime.now() + timedelta(seconds=ttl_seconds)).replace(microsecond=0).isoformat()

        conn = self._get_db()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO market_snapshots (
                snapshot_key, payload_json, updated_at, expires_at, source, fetch_latency_ms, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at,
                source = excluded.source,
                fetch_latency_ms = excluded.fetch_latency_ms,
                notes = excluded.notes
            """,
            (
                snapshot_key,
                json.dumps(payload, ensure_ascii=False),
                updated_at,
                expires_at,
                source,
                fetch_latency_ms,
                notes,
            ),
        )
        conn.commit()
        conn.close()

        return {
            "snapshot_key": snapshot_key,
            "updated_at": updated_at,
            "expires_at": expires_at,
            "source": source,
            "fetch_latency_ms": fetch_latency_ms,
            "notes": notes,
        }

    def _table_exists(self, cursor: sqlite3.Cursor, table_name: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _table_columns(self, cursor: sqlite3.Cursor, table_name: str) -> List[str]:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [row[1] for row in cursor.fetchall()]

    def _safe_table_count(self, cursor: sqlite3.Cursor, table_name: str) -> int:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = cursor.fetchone()
        return int(row[0] if row else 0)

    def _safe_table_max(self, cursor: sqlite3.Cursor, table_name: str, column_name: str) -> Optional[str]:
        cursor.execute(f"SELECT MAX({column_name}) FROM {table_name}")
        row = cursor.fetchone()
        if not row:
            return None
        value = row[0]
        return None if value in (None, "") else str(value)

    def _snapshot_health(self, snapshot_key: str, ttl_seconds: int) -> Dict[str, Any]:
        snapshot = self.get_market_snapshot(snapshot_key, ttl_seconds)
        if not snapshot:
            return {
                "snapshot_key": snapshot_key,
                "updated_at": None,
                "age_seconds": None,
                "is_fresh": False,
                "source": None,
                "exists": False,
                "status": "missing",
            }

        status = "healthy" if snapshot.get("is_fresh") else "warning"
        return {
            "snapshot_key": snapshot_key,
            "updated_at": snapshot.get("updated_at"),
            "age_seconds": snapshot.get("age_seconds"),
            "is_fresh": bool(snapshot.get("is_fresh")),
            "source": snapshot.get("source"),
            "exists": True,
            "status": status,
        }

    def get_data_health_overview(self, snapshot_ttls: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """
        数据健康聚合:
        - storage: 关键快照状态
        - tables: 关键表健康
        - issues: 自动识别问题
        - summary: 健康分与告警计数
        """
        snapshot_ttls = snapshot_ttls or {
            "investment.market_overview.v2": 300,
            "investment.watch_stocks.v2": 300,
        }

        storage = [
            self._snapshot_health(snapshot_key, ttl_seconds)
            for snapshot_key, ttl_seconds in snapshot_ttls.items()
        ]

        table_specs = [
            {"name": "index_history", "date_cols": ["trade_date"], "min_rows": 300, "empty_severity": "error"},
            {"name": "watch_list", "date_cols": ["created_at"], "min_rows": 10, "empty_severity": "error"},
            {"name": "stock_daily", "date_cols": ["trade_date"], "min_rows": 100, "empty_severity": "error"},
            {"name": "stock_financial", "date_cols": ["report_date", "created_at"], "min_rows": 100, "empty_severity": "warning"},
            {"name": "interest_rates", "date_cols": ["trade_date"], "min_rows": 30, "empty_severity": "error"},
            {"name": "market_sentiment", "date_cols": ["trade_date"], "min_rows": 30, "empty_severity": "error"},
            {"name": "vix_history", "date_cols": ["trade_date"], "min_rows": 30, "empty_severity": "error"},
            {"name": "valuation_bands", "date_cols": ["trade_date", "created_at"], "min_rows": 20, "empty_severity": "warning"},
            {"name": "technical_indicators", "date_cols": ["trade_date", "created_at"], "min_rows": 20, "empty_severity": "warning"},
            {"name": "signal_journal", "date_cols": ["trade_date", "signal_time", "created_at"], "min_rows": 20, "empty_severity": "warning"},
            {"name": "strategy_perf_daily", "date_cols": ["trade_date", "updated_at"], "min_rows": 4, "empty_severity": "warning"},
            {"name": "strategy_regime_calibration", "date_cols": ["week_id", "updated_at"], "min_rows": 6, "empty_severity": "warning"},
            {"name": "execution_journal", "date_cols": ["executed_time", "created_at"], "min_rows": 1, "empty_severity": "warning"},
            {"name": "etl_logs", "date_cols": ["start_time", "created_at"], "min_rows": 1, "empty_severity": "warning"},
        ]

        tables: List[Dict[str, Any]] = []
        issues: List[Dict[str, Any]] = []

        conn = self._get_db()
        c = conn.cursor()
        try:
            for spec in table_specs:
                table_name = spec["name"]
                date_cols = spec.get("date_cols", [])
                min_rows = int(spec.get("min_rows", 0))
                empty_severity = spec.get("empty_severity", "warning")

                if not self._table_exists(c, table_name):
                    tables.append(
                        {
                            "name": table_name,
                            "exists": False,
                            "count": None,
                            "latest_date_col": None,
                            "latest_date": None,
                            "status": "missing",
                            "message": "table_not_found",
                        }
                    )
                    issues.append(
                        {
                            "severity": "error",
                            "code": "table_missing",
                            "target": table_name,
                            "message": f"关键表缺失: {table_name}",
                            "suggestion": "检查初始化脚本和数据库版本",
                        }
                    )
                    continue

                columns = self._table_columns(c, table_name)
                row_count = self._safe_table_count(c, table_name)
                chosen_date_col = None
                latest_date = None
                for col in date_cols:
                    if col in columns:
                        chosen_date_col = col
                        latest_date = self._safe_table_max(c, table_name, col)
                        break

                status = "healthy"
                message = "ok"
                if row_count == 0:
                    status = "error" if empty_severity == "error" else "warning"
                    message = "table_empty"
                    issues.append(
                        {
                            "severity": empty_severity,
                            "code": "table_empty",
                            "target": table_name,
                            "message": f"{table_name} 为空",
                            "suggestion": "执行对应采集/回填任务",
                        }
                    )
                elif row_count < min_rows:
                    status = "warning"
                    message = "insufficient_samples"
                    issues.append(
                        {
                            "severity": "warning",
                            "code": "insufficient_samples",
                            "target": table_name,
                            "message": f"{table_name} 样本偏少: {row_count} < {min_rows}",
                            "suggestion": "补充历史回填或提高同步频率",
                        }
                    )

                if chosen_date_col is None:
                    if status == "healthy":
                        status = "warning"
                    issues.append(
                        {
                            "severity": "warning",
                            "code": "date_column_missing",
                            "target": table_name,
                            "message": f"{table_name} 缺少可识别日期列",
                            "suggestion": "为该表增加时间字段，便于做新鲜度检查",
                        }
                    )
                    message = "date_column_missing"
                elif latest_date is None and row_count > 0:
                    if status == "healthy":
                        status = "warning"
                    issues.append(
                        {
                            "severity": "warning",
                            "code": "latest_date_null",
                            "target": table_name,
                            "message": f"{table_name}.{chosen_date_col} 无有效最大值",
                            "suggestion": "检查日期字段格式与数据质量",
                        }
                    )
                    message = "latest_date_null"

                tables.append(
                    {
                        "name": table_name,
                        "exists": True,
                        "count": row_count,
                        "latest_date_col": chosen_date_col,
                        "latest_date": latest_date,
                        "status": status,
                        "message": message,
                    }
                )
        finally:
            conn.close()

        for snapshot in storage:
            if not snapshot.get("exists"):
                issues.append(
                    {
                        "severity": "warning",
                        "code": "snapshot_missing",
                        "target": snapshot["snapshot_key"],
                        "message": f"快照不存在: {snapshot['snapshot_key']}",
                        "suggestion": "触发一次实时刷新以初始化快照",
                    }
                )
            elif not snapshot.get("is_fresh"):
                issues.append(
                    {
                        "severity": "warning",
                        "code": "snapshot_stale",
                        "target": snapshot["snapshot_key"],
                        "message": f"快照过期: {snapshot['snapshot_key']}",
                        "suggestion": "检查刷新任务或实时源连通性",
                    }
                )

        error_count = sum(1 for issue in issues if issue.get("severity") == "error")
        warning_count = sum(1 for issue in issues if issue.get("severity") == "warning")
        health_score = max(0, 100 - error_count * 18 - warning_count * 6)
        overall_status = "error" if error_count > 0 else "warning" if warning_count > 0 else "healthy"

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "storage": storage,
            "tables": tables,
            "issues": issues,
            "summary": {
                "health_score": health_score,
                "status": overall_status,
                "warning_count": warning_count,
                "error_count": error_count,
                "table_count": len(tables),
                "snapshot_count": len(storage),
            },
        }

    def _safe_float(self, value: Any, default: Optional[float] = None) -> Optional[float]:
        if value in (None, "", "-", "--"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _json_loads(self, value: Any) -> List[str]:
        if value in (None, "", "null"):
            return []
        if isinstance(value, list):
            return [str(item) for item in value if item not in (None, "")]
        try:
            payload = json.loads(value)
            if isinstance(payload, list):
                return [str(item) for item in payload if item not in (None, "")]
        except Exception:
            pass
        return [str(value)]

    def _json_dumps(self, values: Any) -> str:
        return json.dumps(values or [], ensure_ascii=False)

    def _clamp(self, value: Any, low: float = 0.0, high: float = 100.0) -> float:
        number = self._safe_float(value, 0.0) or 0.0
        return max(low, min(high, number))

    def _clean_security_name(self, value: Any) -> str:
        text = str(value or "").strip()
        translation = str.maketrans(
            {
                "－": "-",
                "—": "-",
                "–": "-",
                "―": "-",
                "Ｗ": "W",
                "Ｓ": "S",
                "Ａ": "A",
                "Ｂ": "B",
                "　": "",
                " ": "",
            }
        )
        return text.translate(translation)

    def _security_name_variants(self, value: Any) -> List[str]:
        base = self._clean_security_name(value)
        if not base:
            return []
        variants = {base}
        queue = [base]
        suffixes = ("-SW", "-W", "-S", "-B", "-A")
        while queue:
            current = queue.pop()
            for suffix in suffixes:
                if current.endswith(suffix):
                    trimmed = current[: -len(suffix)]
                    if trimmed and trimmed not in variants:
                        variants.add(trimmed)
                        queue.append(trimmed)
        return list(variants)

    def _normalize_security_code(self, value: Any, market: Optional[str] = None) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        lowered = raw.lower()
        if lowered.startswith(("sh", "sz")) and len(lowered) >= 8:
            return lowered[:8]
        if lowered.startswith("hk"):
            digits = re.sub(r"\D", "", lowered[2:])
            return f"hk{digits.zfill(5)}" if digits else "hk"
        if "." in raw:
            left, right = raw.split(".", 1)
            suffix = right.upper()
            left_digits = re.sub(r"\D", "", left)
            if suffix in {"SH", "SS"}:
                return f"sh{left_digits.zfill(6)}"
            if suffix == "SZ":
                return f"sz{left_digits.zfill(6)}"
            if suffix == "HK":
                return f"hk{left_digits.zfill(5)}"

        digits = re.sub(r"\D", "", raw)
        market_hint = (market or "").upper()
        if market_hint == "HK" or (not market_hint and len(digits) <= 5 and digits):
            return f"hk{digits.zfill(5)}"
        if len(digits) == 6:
            prefix = "sh" if digits.startswith(("5", "6", "9")) or digits.startswith("688") else "sz"
            return f"{prefix}{digits}"
        return lowered

    def _infer_market_from_code(self, code: str) -> str:
        normalized = self._normalize_security_code(code)
        if normalized.startswith("hk"):
            return "HK"
        if normalized.startswith(("sh", "sz")):
            return "A"
        if normalized.startswith("us"):
            return "US"
        return ""

    def _finance_code_for(self, code: str) -> str:
        normalized = self._normalize_security_code(code)
        if normalized.startswith(("sh", "sz")) and len(normalized) == 8:
            return normalized[2:]
        return normalized

    def _display_code(self, code: str) -> str:
        normalized = self._normalize_security_code(code)
        if normalized.startswith("sh") and len(normalized) == 8:
            return f"{normalized[2:]}.SH"
        if normalized.startswith("sz") and len(normalized) == 8:
            return f"{normalized[2:]}.SZ"
        if normalized.startswith("hk") and len(normalized) == 7:
            return f"{normalized[2:]}.HK"
        return normalized.upper()

    def _pool_configs(self, pool_code: str = "all") -> Dict[str, Dict[str, Any]]:
        if pool_code == "all":
            return {key: value for key, value in CORE_STOCK_POOL_CONFIGS.items() if key != "all"}
        if pool_code not in CORE_STOCK_POOL_CONFIGS:
            raise ValueError(f"未知股票池: {pool_code}")
        return {pool_code: CORE_STOCK_POOL_CONFIGS[pool_code]}

    def _pool_status_map(self) -> Dict[str, Dict[str, Any]]:
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT pool_code, pool_name, source, as_of_date, member_count,
                   sync_status, note, updated_at
            FROM stock_pool_status
            """
        )
        rows = {row["pool_code"]: dict(row) for row in c.fetchall()}
        conn.close()
        return rows

    def _lookup_hstech_code(self, name: str, dynamic_map: Optional[Dict[str, str]] = None) -> str:
        dynamic_map = dynamic_map or {}
        fallback_map = {self._clean_security_name(key): value for key, value in HSTECH_FALLBACK_CODE_MAP.items()}
        for candidate in self._security_name_variants(name):
            symbol = dynamic_map.get(candidate) or fallback_map.get(candidate)
            if symbol:
                return self._normalize_security_code(symbol, market="HK")
        return ""

    def _blend_score(
        self,
        primary: Optional[float],
        secondary: Optional[float],
        primary_weight: float = 0.65,
        default: float = 50.0,
    ) -> float:
        first = self._safe_float(primary, None)
        second = self._safe_float(secondary, None)
        if first is None and second is None:
            return default
        if first is None:
            return self._clamp(second)
        if second is None:
            return self._clamp(first)
        return self._clamp(first * primary_weight + second * (1 - primary_weight))

    def _setup_label(self, setup_name: Optional[str]) -> str:
        setup = (setup_name or "unknown").strip() or "unknown"
        return SETUP_LABELS.get(setup, setup)

    def _setup_defaults(self, setup_name: Optional[str]) -> Dict[str, Any]:
        setup = (setup_name or "unknown").strip() or "unknown"
        return {
            "factors": SETUP_DEFAULT_FACTORS.get(setup, ["信号触发条件待补充"]),
            "invalids": SETUP_DEFAULT_INVALIDATIONS.get(setup, ["失效条件待补充"]),
        }

    def _insert_signal_journal_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        conn = self._get_db()
        c = conn.cursor()
        before_changes = conn.total_changes
        c.executemany(
            """
            INSERT OR IGNORE INTO signal_journal (
                signal_time, trade_date, code, name, setup_name, setup_label,
                action, position_range, entry_price, exit_price, hold_days,
                outcome_return, max_drawdown, win_flag, invalidated,
                trigger_factors, invalid_conditions, risk_flags, data_source, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.get("signal_time"),
                    row.get("trade_date"),
                    row.get("code"),
                    row.get("name"),
                    row.get("setup_name"),
                    row.get("setup_label"),
                    row.get("action"),
                    row.get("position_range"),
                    row.get("entry_price"),
                    row.get("exit_price"),
                    row.get("hold_days"),
                    row.get("outcome_return"),
                    row.get("max_drawdown"),
                    row.get("win_flag"),
                    row.get("invalidated", 0),
                    self._json_dumps(row.get("trigger_factors")),
                    self._json_dumps(row.get("invalid_conditions")),
                    self._json_dumps(row.get("risk_flags")),
                    row.get("data_source"),
                    row.get("created_at") or datetime.now().replace(microsecond=0).isoformat(),
                )
                for row in rows
            ],
        )
        conn.commit()
        inserted = conn.total_changes - before_changes
        conn.close()
        return inserted

    def _backfill_signal_journal_from_labels(self, lookback_days: int = 540) -> Dict[str, int]:
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        conn = self._get_db()
        c = conn.cursor()
        try:
            c.execute(
                """
                SELECT
                    s.signal_date,
                    s.code,
                    COALESCE(w.name, s.code) AS name,
                    s.setup_name,
                    s.hold_days,
                    s.entry_price,
                    s.exit_price,
                    s.max_drawdown,
                    s.win_flag,
                    s.invalidated
                FROM signal_labels s
                LEFT JOIN watch_list w ON w.code = s.code
                WHERE s.signal_date >= ?
                ORDER BY s.signal_date DESC
                """,
                (cutoff,),
            )
            rows = c.fetchall()
        except sqlite3.OperationalError:
            rows = []
        finally:
            conn.close()

        payload: List[Dict[str, Any]] = []
        now_iso = datetime.now().replace(microsecond=0).isoformat()
        for row in rows:
            signal_date, code, name, setup_name, hold_days, entry_price, exit_price, max_drawdown, win_flag, invalidated = row
            setup = (setup_name or "unknown").strip() or "unknown"
            setup_label = self._setup_label(setup)
            defaults = self._setup_defaults(setup)
            entry = self._safe_float(entry_price, 0.0) or 0.0
            exit_p = self._safe_float(exit_price, None)
            if entry > 0 and exit_p is not None:
                outcome_return = round((exit_p - entry) * 100.0 / entry, 2)
            else:
                outcome_return = None
            risk_flags = []
            if (self._safe_float(max_drawdown, 0.0) or 0.0) <= -8:
                risk_flags.append("历史样本出现较大回撤")
            if int(invalidated or 0) == 1:
                risk_flags.append("历史标签触发失效条件")

            payload.append(
                {
                    "signal_time": f"{signal_date}T15:00:00",
                    "trade_date": signal_date,
                    "code": code,
                    "name": name or code,
                    "setup_name": setup,
                    "setup_label": setup_label,
                    "action": "watch" if int(invalidated or 0) == 0 else "avoid",
                    "position_range": "1%-3%",
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "hold_days": int(hold_days or 0),
                    "outcome_return": outcome_return,
                    "max_drawdown": max_drawdown,
                    "win_flag": int(win_flag) if win_flag is not None else None,
                    "invalidated": int(invalidated or 0),
                    "trigger_factors": defaults["factors"],
                    "invalid_conditions": defaults["invalids"],
                    "risk_flags": risk_flags,
                    "data_source": "signal_labels_backtest",
                    "created_at": now_iso,
                }
            )

        inserted = self._insert_signal_journal_rows(payload)
        return {"source_rows": len(payload), "inserted": inserted}

    def _backfill_signal_journal_from_workbench(self) -> Dict[str, int]:
        try:
            from quant_workbench.service import QuantWorkbenchService
        except Exception:
            return {"source_rows": 0, "inserted": 0}

        service = QuantWorkbenchService()
        try:
            opportunities = service.list_opportunities()
        except Exception:
            return {"source_rows": 0, "inserted": 0}

        signal_time = datetime.now().replace(microsecond=0).isoformat()
        trade_date = signal_time[:10]
        payload: List[Dict[str, Any]] = []
        for item in opportunities:
            strategy = item.get("strategy") or {}
            setup_name = (item.get("setup_name") or strategy.get("setup_name") or "unknown").strip() or "unknown"
            setup_label = item.get("setup_label") or strategy.get("setup_label") or self._setup_label(setup_name)
            ret_5 = self._safe_float(item.get("ret_5"), None)
            risk_flags = item.get("risk_flags") or []
            invalid_conditions = strategy.get("invalid_conditions") or self._setup_defaults(setup_name)["invalids"]

            payload.append(
                {
                    "signal_time": signal_time,
                    "trade_date": trade_date,
                    "code": item.get("code"),
                    "name": item.get("name") or item.get("code"),
                    "setup_name": setup_name,
                    "setup_label": setup_label,
                    "action": strategy.get("action") or item.get("action") or "watch",
                    "position_range": strategy.get("position_range") or item.get("position_range") or "1%-3%",
                    "entry_price": self._safe_float(item.get("close"), None),
                    "exit_price": None,
                    "hold_days": 5,
                    "outcome_return": ret_5,
                    "max_drawdown": None,
                    "win_flag": (1 if ret_5 is not None and ret_5 > 0 else 0 if ret_5 is not None else None),
                    "invalidated": 1 if (strategy.get("action") or item.get("action")) == "avoid" else 0,
                    "trigger_factors": strategy.get("factors") or self._setup_defaults(setup_name)["factors"],
                    "invalid_conditions": invalid_conditions,
                    "risk_flags": risk_flags,
                    "data_source": "workbench_snapshot",
                    "created_at": signal_time,
                }
            )

        inserted = self._insert_signal_journal_rows(payload)
        return {"source_rows": len(payload), "inserted": inserted}

    def _backfill_signal_journal_from_factor_snapshot(self) -> Dict[str, int]:
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        try:
            c.execute(
                """
                SELECT MAX(trade_date) AS trade_date
                FROM stock_factor_snapshot
                WHERE model = 'conservative'
                """
            )
            row = c.fetchone()
            trade_date = row["trade_date"] if row and row["trade_date"] else None
            if not trade_date:
                conn.close()
                return {"source_rows": 0, "inserted": 0}

            c.execute(
                """
                SELECT code, quality, growth, valuation, flow, technical, risk, total
                FROM stock_factor_snapshot
                WHERE model = 'conservative' AND trade_date = ?
                ORDER BY total DESC
                LIMIT 300
                """,
                (trade_date,),
            )
            snapshot_rows = c.fetchall()
            if not snapshot_rows:
                conn.close()
                return {"source_rows": 0, "inserted": 0}

            c.execute("SELECT code, name FROM watch_list WHERE enabled = 1")
            code_name = {item["code"]: item["name"] for item in c.fetchall()}
            conn.close()
        except sqlite3.OperationalError:
            conn.close()
            return {"source_rows": 0, "inserted": 0}

        price_cache: Dict[str, List[float]] = {}
        conn = self._get_db()
        c = conn.cursor()
        for row in snapshot_rows:
            code = row["code"]
            c.execute(
                """
                SELECT close
                FROM stock_daily
                WHERE code = ?
                ORDER BY trade_date DESC
                LIMIT 6
                """,
                (code,),
            )
            closes = [self._safe_float(item[0], None) for item in c.fetchall()]
            price_cache[code] = [value for value in closes if value is not None]
        conn.close()

        signal_time = f"{trade_date}T15:00:00"
        payload: List[Dict[str, Any]] = []
        for row in snapshot_rows:
            code = row["code"]
            total = self._safe_float(row["total"], 0.0) or 0.0
            risk = self._safe_float(row["risk"], 100.0) or 100.0
            flow = self._safe_float(row["flow"], 0.0) or 0.0
            technical = self._safe_float(row["technical"], 0.0) or 0.0

            if total >= 70 and risk <= 45:
                setup_name = "quality_value_recovery"
            elif total >= 62 and flow >= 55 and technical >= 55:
                setup_name = "earnings_revision_breakout"
            elif total >= 58 and risk <= 60:
                setup_name = "risk_on_pullback_leader"
            elif total >= 50:
                setup_name = "sector_catalyst_confirmation"
            else:
                setup_name = "unknown"

            if total >= 68 and risk <= 50:
                action = "buy"
                position_range = "3%-6%"
            elif total >= 55:
                action = "watch"
                position_range = "1%-3%"
            else:
                action = "avoid"
                position_range = "0%"

            closes = price_cache.get(code) or []
            entry_price = closes[0] if len(closes) >= 1 else None
            outcome_return = None
            if len(closes) >= 6 and closes[5]:
                outcome_return = round((closes[0] - closes[5]) * 100.0 / closes[5], 2)
            if outcome_return is None:
                proxy = (
                    (total - 55.0) * 0.24
                    + (flow - 50.0) * 0.08
                    + (technical - 50.0) * 0.08
                    - max(0.0, risk - 60.0) * 0.14
                )
                outcome_return = round(max(-20.0, min(20.0, proxy)), 2)
            max_drawdown = round(min(-0.5, outcome_return - abs(outcome_return) * 0.8), 2)

            factor_pairs = [
                ("quality", self._safe_float(row["quality"], 0.0) or 0.0),
                ("growth", self._safe_float(row["growth"], 0.0) or 0.0),
                ("valuation", self._safe_float(row["valuation"], 0.0) or 0.0),
                ("flow", flow),
                ("technical", technical),
            ]
            factor_pairs.sort(key=lambda item: item[1], reverse=True)
            trigger_factors = [f"{name}:{score:.1f}" for name, score in factor_pairs[:3]]

            payload.append(
                {
                    "signal_time": signal_time,
                    "trade_date": trade_date,
                    "code": code,
                    "name": code_name.get(code) or code,
                    "setup_name": setup_name,
                    "setup_label": self._setup_label(setup_name),
                    "action": action,
                    "position_range": position_range,
                    "entry_price": entry_price,
                    "exit_price": None,
                    "hold_days": 5,
                    "outcome_return": outcome_return,
                    "max_drawdown": max_drawdown,
                    "win_flag": (1 if outcome_return is not None and outcome_return > 0 else 0 if outcome_return is not None else None),
                    "invalidated": 1 if action == "avoid" else 0,
                    "trigger_factors": trigger_factors,
                    "invalid_conditions": self._setup_defaults(setup_name)["invalids"],
                    "risk_flags": ["风险分偏高"] if risk >= 70 else [],
                    "data_source": "factor_snapshot_bootstrap",
                    "created_at": datetime.now().replace(microsecond=0).isoformat(),
                }
            )

        inserted = self._insert_signal_journal_rows(payload)
        return {"source_rows": len(payload), "inserted": inserted}

    def _rebuild_strategy_perf_daily(self, trade_date: Optional[str] = None, windows: Optional[List[int]] = None) -> Dict[str, int]:
        trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")
        window_list = [int(x) for x in (windows or [20, 60, 120]) if int(x) > 0]

        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT setup_name, setup_label, signal_time, outcome_return, max_drawdown, win_flag
            FROM signal_journal
            WHERE outcome_return IS NOT NULL
            ORDER BY signal_time DESC
            """
        )
        rows = c.fetchall()

        grouped: Dict[str, List[sqlite3.Row]] = {"ALL": list(rows)}
        setup_labels: Dict[str, str] = {"ALL": SETUP_LABELS["ALL"]}
        for row in rows:
            setup = (row["setup_name"] or "unknown").strip() or "unknown"
            grouped.setdefault(setup, []).append(row)
            if setup not in setup_labels:
                setup_labels[setup] = row["setup_label"] or self._setup_label(setup)

        c.execute("DELETE FROM strategy_perf_daily WHERE trade_date = ?", (trade_date,))
        inserted = 0
        updated_at = datetime.now().replace(microsecond=0).isoformat()

        for setup_name, setup_rows in grouped.items():
            for window in window_list:
                bucket = setup_rows[:window]
                sample_size = len(bucket)
                if sample_size == 0:
                    continue

                returns = [self._safe_float(item["outcome_return"], 0.0) or 0.0 for item in bucket]
                max_drawdowns = [self._safe_float(item["max_drawdown"], None) for item in bucket]
                max_drawdowns = [val for val in max_drawdowns if val is not None]
                win_count = sum(1 for item in bucket if int(item["win_flag"] or 0) == 1)

                avg_return = round(sum(returns) / sample_size, 2)
                win_rate = round(win_count * 100.0 / sample_size, 1)

                win_sum = sum(value for value in returns if value > 0)
                loss_sum = abs(sum(value for value in returns if value < 0))
                profit_factor = round(win_sum / loss_sum, 2) if loss_sum > 0 else (99.0 if win_sum > 0 else None)

                avg_max_drawdown = round(sum(max_drawdowns) / len(max_drawdowns), 2) if max_drawdowns else None
                worst_drawdown = round(min(max_drawdowns), 2) if max_drawdowns else None

                c.execute(
                    """
                    INSERT OR REPLACE INTO strategy_perf_daily (
                        trade_date, setup_name, setup_label, window_size, sample_size,
                        win_rate, avg_return, profit_factor, avg_max_drawdown, max_drawdown, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_date,
                        setup_name,
                        setup_labels.get(setup_name, self._setup_label(setup_name)),
                        window,
                        sample_size,
                        win_rate,
                        avg_return,
                        profit_factor,
                        avg_max_drawdown,
                        worst_drawdown,
                        updated_at,
                    ),
                )
                inserted += 1

        conn.commit()
        conn.close()
        return {"rows": inserted, "window_count": len(window_list)}

    def sync_strategy_runtime_data(
        self,
        force_rebuild: bool = False,
        windows: Optional[List[int]] = None,
        allow_workbench_fallback: bool = False,
    ) -> Dict[str, Any]:
        windows = [int(x) for x in (windows or [20, 60, 120]) if int(x) > 0]
        conn = self._get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM signal_journal")
        journal_count_before = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM strategy_perf_daily WHERE trade_date = ?", (datetime.now().strftime("%Y-%m-%d"),))
        perf_today_count = int(c.fetchone()[0] or 0)
        conn.close()

        if force_rebuild:
            conn = self._get_db()
            c = conn.cursor()
            c.execute("DELETE FROM signal_journal")
            c.execute("DELETE FROM strategy_perf_daily")
            conn.commit()
            conn.close()
            journal_count_before = 0
            perf_today_count = 0

        labels_result = {"source_rows": 0, "inserted": 0}
        fallback_result = {"source_rows": 0, "inserted": 0}
        snapshot_fallback = {"source_rows": 0, "inserted": 0}

        if journal_count_before == 0 or force_rebuild:
            labels_result = self._backfill_signal_journal_from_labels()

        conn = self._get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM signal_journal")
        journal_count_mid = int(c.fetchone()[0] or 0)
        conn.close()

        if journal_count_mid == 0 and allow_workbench_fallback:
            fallback_result = self._backfill_signal_journal_from_workbench()

        conn = self._get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM signal_journal")
        journal_count_post_fallback = int(c.fetchone()[0] or 0)
        conn.close()

        if journal_count_post_fallback == 0:
            snapshot_fallback = self._backfill_signal_journal_from_factor_snapshot()

        # 已有当日结果时仍重建，保证窗口统计与最新样本一致
        perf_result = self._rebuild_strategy_perf_daily(windows=windows)

        conn = self._get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM signal_journal")
        journal_count_after = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM strategy_perf_daily WHERE trade_date = ?", (datetime.now().strftime("%Y-%m-%d"),))
        perf_today_after = int(c.fetchone()[0] or 0)
        conn.close()

        return {
            "journal_count_before": journal_count_before,
            "journal_count_after": journal_count_after,
            "perf_today_before": perf_today_count,
            "perf_today_after": perf_today_after,
            "labels_backfill": labels_result,
            "workbench_fallback": fallback_result,
            "snapshot_bootstrap": snapshot_fallback,
            "perf_rebuild": perf_result,
            "windows": windows,
            "allow_workbench_fallback": allow_workbench_fallback,
        }

    def get_signal_journal(self, limit: int = 40, setup_name: Optional[str] = None) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 40), 500))
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        if setup_name:
            c.execute(
                """
                SELECT signal_time, trade_date, code, name, setup_name, setup_label, action, position_range,
                       entry_price, exit_price, hold_days, outcome_return, max_drawdown, win_flag, invalidated,
                       trigger_factors, invalid_conditions, risk_flags, data_source
                FROM signal_journal
                WHERE setup_name = ?
                ORDER BY signal_time DESC
                LIMIT ?
                """,
                (setup_name, limit),
            )
        else:
            c.execute(
                """
                SELECT signal_time, trade_date, code, name, setup_name, setup_label, action, position_range,
                       entry_price, exit_price, hold_days, outcome_return, max_drawdown, win_flag, invalidated,
                       trigger_factors, invalid_conditions, risk_flags, data_source
                FROM signal_journal
                ORDER BY signal_time DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = c.fetchall()
        conn.close()

        result: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["trigger_factors"] = self._json_loads(item.get("trigger_factors"))
            item["invalid_conditions"] = self._json_loads(item.get("invalid_conditions"))
            item["risk_flags"] = self._json_loads(item.get("risk_flags"))
            result.append(item)
        return result

    def get_strategy_perf_overview(
        self,
        setup_name: Optional[str] = None,
        windows: Optional[List[int]] = None,
        trend_limit: int = 120,
    ) -> Dict[str, Any]:
        window_list = sorted({int(x) for x in (windows or [20, 60, 120]) if int(x) > 0})
        target_setup = (setup_name or "ALL").strip() or "ALL"

        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT MAX(trade_date) AS trade_date FROM strategy_perf_daily")
        latest_row = c.fetchone()
        latest_trade_date = latest_row["trade_date"] if latest_row and latest_row["trade_date"] else None
        if not latest_trade_date:
            conn.close()
            return {
                "as_of_date": None,
                "target_setup": target_setup,
                "windows": [],
                "setups": [],
                "trend_120d": [],
                "recent_signals": [],
            }

        placeholders = ",".join(["?" for _ in window_list]) if window_list else "20,60,120"
        c.execute(
            f"""
            SELECT window_size, sample_size, win_rate, avg_return, profit_factor, avg_max_drawdown, max_drawdown
            FROM strategy_perf_daily
            WHERE trade_date = ? AND setup_name = ? AND window_size IN ({placeholders})
            ORDER BY window_size
            """,
            [latest_trade_date, target_setup, *window_list],
        )
        windows_payload = [dict(row) for row in c.fetchall()]

        c.execute(
            """
            SELECT setup_name, setup_label, sample_size, win_rate, avg_return, profit_factor, max_drawdown
            FROM strategy_perf_daily
            WHERE trade_date = ? AND window_size = 120 AND setup_name <> 'ALL'
            ORDER BY sample_size DESC, win_rate DESC
            LIMIT 12
            """,
            (latest_trade_date,),
        )
        setup_rows = [dict(row) for row in c.fetchall()]

        c.execute(
            """
            SELECT trade_date, sample_size, win_rate, avg_return, profit_factor, max_drawdown
            FROM strategy_perf_daily
            WHERE setup_name = 'ALL' AND window_size = 120
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (max(20, min(int(trend_limit or 120), 240)),),
        )
        trend_rows = [dict(row) for row in c.fetchall()][::-1]
        conn.close()

        return {
            "as_of_date": latest_trade_date,
            "target_setup": target_setup,
            "windows": windows_payload,
            "setups": setup_rows,
            "trend_120d": trend_rows,
            "recent_signals": self.get_signal_journal(limit=20, setup_name=None if target_setup == "ALL" else target_setup),
        }

    @staticmethod
    def _iso_week_id(value: Optional[datetime] = None) -> str:
        now = value or datetime.now()
        iso = now.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    def _build_nearest_lookup(self, rows: List[sqlite3.Row], key: str, value_cols: List[str]) -> Dict[str, Dict[str, Any]]:
        payload: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            date_key = str(row[key])[:10]
            payload[date_key] = {col: row[col] for col in value_cols}
        return payload

    def _nearest_value_by_date(
        self,
        date_list: List[str],
        mapping: Dict[str, Dict[str, Any]],
        fallback: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not date_list:
            return {}
        fallback = fallback or {}
        sorted_keys = sorted(mapping.keys())
        result: Dict[str, Dict[str, Any]] = {}
        for date_str in date_list:
            chosen = fallback
            for key in sorted_keys:
                if key <= date_str:
                    chosen = mapping[key]
                else:
                    break
            result[date_str] = dict(chosen)
        return result

    def _compute_regime_label(
        self,
        up_count: Optional[float],
        down_count: Optional[float],
        north_inflow: Optional[float],
        shibor_on: Optional[float],
        shibor_1w: Optional[float],
        vix_close: Optional[float],
    ) -> str:
        up = self._safe_float(up_count, 0.0) or 0.0
        down = max(1.0, self._safe_float(down_count, 1.0) or 1.0)
        breadth_ratio = up / down
        north = self._safe_float(north_inflow, 0.0) or 0.0
        spread = (self._safe_float(shibor_1w, 0.0) or 0.0) - (self._safe_float(shibor_on, 0.0) or 0.0)
        vix = self._safe_float(vix_close, 20.0) or 20.0

        score = 0
        score += 1 if breadth_ratio >= 1.1 else -1 if breadth_ratio <= 0.9 else 0
        score += 1 if north >= 0 else -1
        score += 1 if spread <= 0.20 else -1 if spread >= 0.5 else 0
        score += 1 if vix <= 18 else -1 if vix >= 24 else 0
        if score >= 2:
            return "risk_on"
        if score <= -2:
            return "risk_off"
        return "neutral"

    def rebuild_strategy_regime_calibration(self, lookback_days: int = 365) -> Dict[str, Any]:
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        week_id = self._iso_week_id()

        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT trade_date, setup_name, setup_label, outcome_return, win_flag
            FROM signal_journal
            WHERE outcome_return IS NOT NULL AND trade_date >= ?
            ORDER BY trade_date DESC
            """,
            (cutoff,),
        )
        signals = c.fetchall()
        if not signals:
            conn.close()
            return {"week_id": week_id, "rows": 0, "signal_count": 0}

        trade_dates = sorted({str(row["trade_date"])[:10] for row in signals})
        c.execute(
            """
            SELECT trade_date, up_count, down_count
            FROM market_sentiment
            WHERE trade_date >= ?
            ORDER BY trade_date
            """,
            (cutoff,),
        )
        sentiment_map = self._build_nearest_lookup(c.fetchall(), "trade_date", ["up_count", "down_count"])
        c.execute(
            """
            SELECT trade_date, total_net_inflow
            FROM north_money
            WHERE trade_date >= ?
            ORDER BY trade_date
            """,
            (cutoff,),
        )
        north_map = self._build_nearest_lookup(c.fetchall(), "trade_date", ["total_net_inflow"])
        c.execute(
            """
            SELECT trade_date, shibor_overnight, shibor_1w
            FROM interest_rates
            WHERE trade_date >= ?
            ORDER BY trade_date
            """,
            (cutoff,),
        )
        rates_map = self._build_nearest_lookup(c.fetchall(), "trade_date", ["shibor_overnight", "shibor_1w"])
        c.execute(
            """
            SELECT trade_date, vix_close
            FROM vix_history
            WHERE trade_date >= ?
            ORDER BY trade_date
            """,
            (cutoff,),
        )
        vix_map = self._build_nearest_lookup(c.fetchall(), "trade_date", ["vix_close"])

        sentiment_by_date = self._nearest_value_by_date(trade_dates, sentiment_map, {"up_count": None, "down_count": None})
        north_by_date = self._nearest_value_by_date(trade_dates, north_map, {"total_net_inflow": None})
        rates_by_date = self._nearest_value_by_date(trade_dates, rates_map, {"shibor_overnight": None, "shibor_1w": None})
        vix_by_date = self._nearest_value_by_date(trade_dates, vix_map, {"vix_close": None})

        buckets: Dict[str, Dict[str, List[sqlite3.Row]]] = {"risk_on": {}, "neutral": {}, "risk_off": {}}
        for row in signals:
            trade_date = str(row["trade_date"])[:10]
            regime = self._compute_regime_label(
                sentiment_by_date.get(trade_date, {}).get("up_count"),
                sentiment_by_date.get(trade_date, {}).get("down_count"),
                north_by_date.get(trade_date, {}).get("total_net_inflow"),
                rates_by_date.get(trade_date, {}).get("shibor_overnight"),
                rates_by_date.get(trade_date, {}).get("shibor_1w"),
                vix_by_date.get(trade_date, {}).get("vix_close"),
            )
            setup = (row["setup_name"] or "unknown").strip() or "unknown"
            buckets[regime].setdefault(setup, []).append(row)
            buckets[regime].setdefault("ALL", []).append(row)

        c.execute("DELETE FROM strategy_regime_calibration WHERE week_id = ?", (week_id,))
        inserted = 0
        updated_at = datetime.now().replace(microsecond=0).isoformat()

        for regime, groups in buckets.items():
            for setup_name, rows in groups.items():
                sample_size = len(rows)
                if sample_size == 0:
                    continue
                returns = [self._safe_float(item["outcome_return"], 0.0) or 0.0 for item in rows]
                win_count = sum(1 for item in rows if int(item["win_flag"] or 0) == 1)
                win_rate = round(win_count * 100.0 / sample_size, 1)
                avg_return = round(sum(returns) / sample_size, 2)
                win_sum = sum(value for value in returns if value > 0)
                loss_sum = abs(sum(value for value in returns if value < 0))
                profit_factor = round(win_sum / loss_sum, 2) if loss_sum > 0 else (99.0 if win_sum > 0 else None)

                if sample_size >= 30 and win_rate >= 58 and avg_return >= 0:
                    recommended_weight = 1.0
                elif sample_size >= 15 and win_rate >= 52 and avg_return >= -0.5:
                    recommended_weight = 0.7
                elif win_rate < 45 or avg_return < -1:
                    recommended_weight = 0.2
                else:
                    recommended_weight = 0.4

                threshold_score = round(55 + recommended_weight * 20, 1)
                setup_label = SETUP_LABELS.get(setup_name, setup_name)
                c.execute(
                    """
                    INSERT OR REPLACE INTO strategy_regime_calibration (
                        week_id, regime, setup_name, setup_label, sample_size,
                        win_rate, avg_return, profit_factor, recommended_weight,
                        threshold_score, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        week_id,
                        regime,
                        setup_name,
                        setup_label,
                        sample_size,
                        win_rate,
                        avg_return,
                        profit_factor,
                        recommended_weight,
                        threshold_score,
                        updated_at,
                    ),
                )
                inserted += 1

        conn.commit()
        conn.close()
        return {"week_id": week_id, "rows": inserted, "signal_count": len(signals)}

    def get_strategy_regime_matrix(self, auto_rebuild: bool = True) -> Dict[str, Any]:
        week_id = self._iso_week_id()
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM strategy_regime_calibration WHERE week_id = ?", (week_id,))
        current_count = int(c.fetchone()[0] or 0)
        conn.close()

        rebuild_result: Dict[str, Any] = {}
        if auto_rebuild and current_count == 0:
            rebuild_result = self.rebuild_strategy_regime_calibration()

        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT week_id, regime, setup_name, setup_label, sample_size, win_rate,
                   avg_return, profit_factor, recommended_weight, threshold_score, updated_at
            FROM strategy_regime_calibration
            WHERE week_id = ?
            ORDER BY regime, recommended_weight DESC, sample_size DESC
            """,
            (week_id,),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(row["regime"], []).append(row)

        return {
            "week_id": week_id,
            "rows": rows,
            "matrix": grouped,
            "rebuild": rebuild_result,
            "updated_at": rows[0]["updated_at"] if rows else None,
        }

    def get_event_catalysts(self, days_ahead: int = 45, limit: int = 40) -> Dict[str, Any]:
        today = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=max(1, int(days_ahead)))).strftime("%Y-%m-%d")
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        try:
            c.execute(
                """
                SELECT
                    e.code,
                    COALESCE(w.name, e.code) AS name,
                    e.event_type,
                    e.event_date,
                    e.importance,
                    e.note
                FROM stock_event_calendar e
                LEFT JOIN watch_list w ON w.code = e.code
                WHERE e.event_date >= ? AND e.event_date <= ?
                ORDER BY COALESCE(e.importance, 0) DESC, e.event_date ASC
                LIMIT ?
                """,
                (today, end_date, max(1, min(int(limit), 300))),
            )
            items = [dict(row) for row in c.fetchall()]
        except sqlite3.OperationalError:
            items = []
        finally:
            conn.close()

        type_count: Dict[str, int] = {}
        for item in items:
            et = item.get("event_type") or "未分类"
            type_count[et] = type_count.get(et, 0) + 1

        return {
            "from_date": today,
            "to_date": end_date,
            "count": len(items),
            "types": type_count,
            "items": items,
        }

    def record_execution(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        side = str(payload.get("side") or "buy").lower()
        signal_price = self._safe_float(payload.get("signal_price"), None)
        executed_price = self._safe_float(payload.get("executed_price"), None)
        if signal_price is None or signal_price <= 0 or executed_price is None or executed_price <= 0:
            raise ValueError("signal_price/executed_price 必须为正数")

        signal_time = str(payload.get("signal_time") or datetime.now().replace(microsecond=0).isoformat())
        executed_time = str(payload.get("executed_time") or datetime.now().replace(microsecond=0).isoformat())
        created_at = datetime.now().replace(microsecond=0).isoformat()

        conn = self._get_db()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO execution_journal (
                signal_time, code, setup_name, side, signal_price, executed_price,
                quantity, executed_time, source, note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_time,
                payload.get("code"),
                payload.get("setup_name"),
                side,
                signal_price,
                executed_price,
                self._safe_float(payload.get("quantity"), None),
                executed_time,
                payload.get("source"),
                payload.get("note"),
                created_at,
            ),
        )
        insert_id = c.lastrowid
        conn.commit()
        conn.close()

        if side == "sell":
            slippage_bps = round((signal_price - executed_price) * 10000.0 / signal_price, 2)
        else:
            slippage_bps = round((executed_price - signal_price) * 10000.0 / signal_price, 2)

        return {
            "id": insert_id,
            "signal_time": signal_time,
            "code": payload.get("code"),
            "setup_name": payload.get("setup_name"),
            "side": side,
            "slippage_bps": slippage_bps,
            "executed_time": executed_time,
        }

    def get_slippage_overview(self, windows: Optional[List[int]] = None, limit: int = 200) -> Dict[str, Any]:
        window_list = sorted({int(x) for x in (windows or [20, 60, 120]) if int(x) > 0})
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT id, signal_time, code, setup_name, side, signal_price, executed_price,
                   quantity, executed_time, source, note, created_at
            FROM execution_journal
            ORDER BY COALESCE(executed_time, created_at) DESC
            LIMIT ?
            """,
            (max(20, min(int(limit), 1000)),),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        def _slippage(item: Dict[str, Any]) -> float:
            signal_price = self._safe_float(item.get("signal_price"), 0.0) or 0.0
            executed_price = self._safe_float(item.get("executed_price"), 0.0) or 0.0
            if signal_price <= 0 or executed_price <= 0:
                return 0.0
            if str(item.get("side") or "buy").lower() == "sell":
                return round((signal_price - executed_price) * 10000.0 / signal_price, 2)
            return round((executed_price - signal_price) * 10000.0 / signal_price, 2)

        for row in rows:
            row["slippage_bps"] = _slippage(row)

        window_stats: List[Dict[str, Any]] = []
        for window in window_list:
            bucket = rows[:window]
            if not bucket:
                continue
            values = [float(item.get("slippage_bps") or 0.0) for item in bucket]
            avg_slippage = round(sum(values) / len(values), 2)
            p90 = sorted(values)[int(0.9 * (len(values) - 1))]
            positive_ratio = round(sum(1 for value in values if value <= 0) * 100.0 / len(values), 1)
            window_stats.append(
                {
                    "window_size": window,
                    "sample_size": len(values),
                    "avg_slippage_bps": avg_slippage,
                    "p90_slippage_bps": round(p90, 2),
                    "favorable_ratio_pct": positive_ratio,
                }
            )

        setup_group: Dict[str, List[float]] = {}
        for row in rows:
            setup = (row.get("setup_name") or "unknown").strip() or "unknown"
            setup_group.setdefault(setup, []).append(float(row.get("slippage_bps") or 0.0))

        setup_stats = [
            {
                "setup_name": setup,
                "sample_size": len(values),
                "avg_slippage_bps": round(sum(values) / len(values), 2),
                "p90_slippage_bps": round(sorted(values)[int(0.9 * (len(values) - 1))], 2),
            }
            for setup, values in setup_group.items()
            if values
        ]
        setup_stats.sort(key=lambda item: item["sample_size"], reverse=True)

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "windows": window_stats,
            "setups": setup_stats[:12],
            "recent": rows[:20],
            "total_samples": len(rows),
        }

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

        if codes:
            placeholders = ','.join(['?' for _ in codes])
            c.execute(f'''
                SELECT code, name, market, report_date, pe_ttm, pb, ps_ttm,
                       roe, roa, gross_margin, net_margin, debt_ratio, current_ratio,
                       eps, bvps, revenue, revenue_yoy, net_profit, net_profit_yoy, dividend_yield
                FROM stock_financial
                WHERE code IN ({placeholders})
                ORDER BY code
            ''', codes)
        else:
            c.execute('''
                SELECT code, name, market, report_date, pe_ttm, pb, ps_ttm,
                       roe, roa, gross_margin, net_margin, debt_ratio, current_ratio,
                       eps, bvps, revenue, revenue_yoy, net_profit, net_profit_yoy, dividend_yield
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

    def _requests_session(self):
        import requests

        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )
        return session

    def _fetch_sina_index_constituents(self, pool_code: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        import akshare as ak

        try:
            df = ak.index_stock_cons_weight_csindex(symbol=config["symbol"])
            use_weight_source = True
        except Exception:
            df = ak.index_stock_cons(symbol=config["symbol"])
            use_weight_source = False
        items: List[Dict[str, Any]] = []
        as_of_date = datetime.now().strftime("%Y-%m-%d")
        for _, row in df.iterrows():
            raw_code = re.sub(r"\D", "", str(row.get("成分券代码") or row.get("品种代码") or ""))
            raw_name = str(row.get("成分券名称") or row.get("品种名称") or "").strip()
            included_at = str(row.get("日期") or row.get("纳入日期") or as_of_date)[:10]
            weight = self._safe_float(row.get("权重"), None) if use_weight_source else None
            if len(raw_code) != 6 or not raw_name:
                continue
            items.append(
                {
                    "pool_code": pool_code,
                    "pool_name": config["name"],
                    "member_code": self._normalize_security_code(raw_code, market="A"),
                    "member_name": raw_name,
                    "member_market": "A",
                    "raw_code": raw_code,
                    "weight": weight,
                    "as_of_date": included_at,
                    "source": "csindex_weight" if use_weight_source else config["source"],
                    "extra_json": self._json_dumps({"symbol": config["symbol"], "weight": weight}),
                }
            )
        dedup: Dict[str, Dict[str, Any]] = {}
        for item in items:
            dedup[item["member_code"]] = item
        return list(dedup.values())

    def _fetch_ths_concept_constituents(self, pool_code: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            import akshare as ak

            df = ak.stock_board_concept_cons_em(symbol=config["name"])
            items: List[Dict[str, Any]] = []
            as_of_date = datetime.now().strftime("%Y-%m-%d")
            for _, row in df.iterrows():
                raw_code = re.sub(r"\D", "", str(row.get("代码") or ""))
                raw_name = str(row.get("名称") or "").strip()
                if len(raw_code) != 6 or not raw_name:
                    continue
                items.append(
                    {
                        "pool_code": pool_code,
                        "pool_name": config["name"],
                        "member_code": self._normalize_security_code(raw_code, market="A"),
                        "member_name": raw_name,
                        "member_market": "A",
                        "raw_code": raw_code,
                        "weight": None,
                        "as_of_date": as_of_date,
                        "source": "eastmoney_concept",
                        "extra_json": self._json_dumps({"symbol": config["name"]}),
                    }
                )
            if items:
                return items
        except Exception:
            pass

        legulegu_url = str(config.get("legulegu_url") or "").strip()
        if legulegu_url:
            try:
                from bs4 import BeautifulSoup

                session = self._requests_session()
                html = session.get(legulegu_url, timeout=20).text
                soup = BeautifulSoup(html, "lxml")
                rows = soup.select("table tbody tr")
                items: List[Dict[str, Any]] = []
                as_of_date = datetime.now().strftime("%Y-%m-%d")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    stock_link = cells[1].find("a", href=True)
                    raw_name = " ".join((stock_link or cells[1]).get_text(" ", strip=True).split())
                    href = str(stock_link.get("href") or "") if stock_link else ""
                    match = re.search(r"/s/(\d{6})(?:\.(SZ|SH))?", href, flags=re.IGNORECASE)
                    raw_code = match.group(1) if match else re.sub(r"\D", "", cells[1].get_text(" ", strip=True))
                    if len(raw_code) != 6 or not raw_name:
                        continue
                    sw_level_1 = (
                        " ".join(cells[6].get_text(" ", strip=True).split())
                        if len(cells) > 6
                        else ""
                    )
                    sw_level_2 = (
                        " ".join(cells[7].get_text(" ", strip=True).split())
                        if len(cells) > 7
                        else ""
                    )
                    market_cap = cells[2].get_text(" ", strip=True) if len(cells) > 2 else ""
                    items.append(
                        {
                            "pool_code": pool_code,
                            "pool_name": config["name"],
                            "member_code": self._normalize_security_code(raw_code, market="A"),
                            "member_name": raw_name,
                            "member_market": "A",
                            "raw_code": raw_code,
                            "weight": None,
                            "as_of_date": as_of_date,
                            "source": "legulegu_concept",
                            "extra_json": self._json_dumps(
                                {
                                    "legulegu_url": legulegu_url,
                                    "sw_level_1": sw_level_1,
                                    "sw_level_2": sw_level_2,
                                    "market_cap": market_cap,
                                }
                            ),
                        }
                    )
                dedup: Dict[str, Dict[str, Any]] = {}
                for item in items:
                    dedup[item["member_code"]] = item
                if dedup:
                    return list(dedup.values())
            except Exception:
                pass

        import pandas as pd
        from bs4 import BeautifulSoup

        session = self._requests_session()
        base_url = f"https://q.10jqka.com.cn/gn/detail/code/{config['code']}/"
        html = session.get(base_url, timeout=20).text
        soup = BeautifulSoup(html, "lxml")
        page_info = soup.find("span", attrs={"class": "page_info"})
        page_count = 1
        if page_info:
            page_text = page_info.get_text(strip=True)
            if "/" in page_text:
                try:
                    page_count = max(1, int(page_text.split("/")[-1]))
                except ValueError:
                    page_count = 1

        items: List[Dict[str, Any]] = []
        as_of_date = datetime.now().strftime("%Y-%m-%d")
        for page in range(1, page_count + 1):
            url = base_url if page == 1 else f"{base_url}page/{page}/"
            page_html = session.get(url, timeout=20).text
            tables = pd.read_html(StringIO(page_html))
            if not tables:
                continue
            table = tables[0]
            code_col = next((col for col in table.columns if "代码" in str(col)), None)
            name_col = next((col for col in table.columns if "名称" in str(col)), None)
            if code_col is None or name_col is None:
                continue
            for _, row in table.iterrows():
                raw_code = re.sub(r"\D", "", str(row.get(code_col) or ""))
                raw_name = str(row.get(name_col) or "").strip()
                if len(raw_code) != 6 or not raw_name:
                    continue
                items.append(
                    {
                        "pool_code": pool_code,
                        "pool_name": config["name"],
                        "member_code": self._normalize_security_code(raw_code, market="A"),
                        "member_name": raw_name,
                        "member_market": "A",
                        "raw_code": raw_code,
                        "weight": None,
                        "as_of_date": as_of_date,
                        "source": config["source"],
                        "extra_json": self._json_dumps({"ths_code": config["code"], "page": page}),
                    }
                )
        dedup: Dict[str, Dict[str, Any]] = {}
        for item in items:
            dedup[item["member_code"]] = item
        return list(dedup.values())

    def _fetch_hstech_constituents(self, pool_code: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        from bs4 import BeautifulSoup

        dynamic_map: Dict[str, str] = {}
        member_names: List[str] = []
        try:
            session = self._requests_session()
            html = session.get(config["url"], timeout=20).text
            soup = BeautifulSoup(html, "lxml")
            script = soup.find("script", attrs={"id": "__NEXT_DATA__"})
            if script and script.string:
                try:
                    payload = json.loads(script.string)
                    quote_groups = (
                        payload.get("props", {})
                        .get("pageProps", {})
                        .get("state", {})
                        .get("quotesStore", {})
                        .get("quotes", [])
                    )
                    for group in quote_groups:
                        if not isinstance(group, list) or len(group) < 2 or not isinstance(group[1], dict):
                            continue
                        for item in group[1].get("_collection", []):
                            if not isinstance(item, dict):
                                continue
                            name = item.get("name")
                            flag = item.get("flag")
                            symbol = str(item.get("symbol") or "").strip()
                            if isinstance(name, dict):
                                label = name.get("label")
                            else:
                                label = item.get("Name")
                            market_code = ""
                            if isinstance(flag, dict):
                                market_code = str(flag.get("code") or "").upper()
                            if label and symbol and market_code == "HK":
                                dynamic_map[self._clean_security_name(label)] = symbol
                except Exception:
                    dynamic_map = {}

            tables = soup.find_all("table")
            if tables:
                rows = tables[0].find_all("tr")[1:]
                member_names = [
                    " ".join(row.find_all("td")[1].get_text(" ", strip=True).split())
                    for row in rows
                    if len(row.find_all("td")) >= 2
                ]
        except Exception:
            member_names = []

        if not member_names:
            member_names = list(HSTECH_FALLBACK_CONSTITUENTS)

        as_of_date = datetime.now().strftime("%Y-%m-%d")
        items: List[Dict[str, Any]] = []
        unresolved: List[str] = []
        for name in member_names:
            code = self._lookup_hstech_code(name, dynamic_map)
            if not code:
                unresolved.append(name)
                continue
            items.append(
                {
                    "pool_code": pool_code,
                    "pool_name": config["name"],
                    "member_code": code,
                    "member_name": name,
                    "member_market": "HK",
                    "raw_code": code[2:],
                    "weight": None,
                    "as_of_date": as_of_date,
                    "source": config["source"],
                    "extra_json": self._json_dumps({"url": config["url"]}),
                }
            )
        if unresolved:
            print(f"恒生科技存在未映射成员: {', '.join(unresolved[:8])}")
        dedup: Dict[str, Dict[str, Any]] = {}
        for item in items:
            dedup[item["member_code"]] = item
        if not dedup:
            raise ValueError("恒生科技本地兜底名单映射失败")
        return list(dedup.values())

    def sync_stock_pools(self, pool_code: str = "all") -> Dict[str, Any]:
        configs = self._pool_configs(pool_code)
        conn = self._get_db()
        c = conn.cursor()
        updated_at = datetime.now().replace(microsecond=0).isoformat()
        results: List[Dict[str, Any]] = []

        for code, config in configs.items():
            try:
                if config["source"] == "sina_index":
                    members = self._fetch_sina_index_constituents(code, config)
                elif config["source"] == "ths_concept":
                    members = self._fetch_ths_concept_constituents(code, config)
                elif config["source"] == "investing_hk":
                    members = self._fetch_hstech_constituents(code, config)
                else:
                    raise ValueError(f"未支持的股票池来源: {config['source']}")

                if not members:
                    raise ValueError("未抓取到成分股")

                c.execute("DELETE FROM stock_pool_constituents WHERE pool_code = ?", (code,))
                c.executemany(
                    """
                    INSERT OR REPLACE INTO stock_pool_constituents (
                        pool_code, pool_name, member_code, member_name, member_market,
                        raw_code, weight, as_of_date, source, extra_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            item["pool_code"],
                            item["pool_name"],
                            item["member_code"],
                            item["member_name"],
                            item["member_market"],
                            item.get("raw_code"),
                            item.get("weight"),
                            item.get("as_of_date"),
                            item.get("source"),
                            item.get("extra_json"),
                            updated_at,
                        )
                        for item in members
                    ],
                )
                as_of_date = max(str(item.get("as_of_date") or "")[:10] for item in members)
                c.execute(
                    """
                    INSERT OR REPLACE INTO stock_pool_status (
                        pool_code, pool_name, source, as_of_date, member_count,
                        sync_status, note, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        code,
                        config["name"],
                        config["source"],
                        as_of_date,
                        len(members),
                        "success",
                        "本地机会池已刷新",
                        updated_at,
                    ),
                )
                results.append(
                    {
                        "pool_code": code,
                        "pool_name": config["name"],
                        "member_count": len(members),
                        "status": "success",
                        "as_of_date": as_of_date,
                    }
                )
            except Exception as exc:
                c.execute(
                    """
                    INSERT OR REPLACE INTO stock_pool_status (
                        pool_code, pool_name, source, as_of_date, member_count,
                        sync_status, note, updated_at
                    )
                    VALUES (?, ?, ?, COALESCE((SELECT as_of_date FROM stock_pool_status WHERE pool_code = ?), ?),
                            COALESCE((SELECT member_count FROM stock_pool_status WHERE pool_code = ?), 0),
                            ?, ?, ?)
                    """,
                    (
                        code,
                        config["name"],
                        config["source"],
                        code,
                        datetime.now().strftime("%Y-%m-%d"),
                        code,
                        "error",
                        str(exc)[:300],
                        updated_at,
                    ),
                )
                results.append(
                    {
                        "pool_code": code,
                        "pool_name": config["name"],
                        "member_count": 0,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        conn.commit()
        conn.close()
        return {
            "generated_at": updated_at,
            "pool_code": pool_code,
            "results": results,
        }

    def get_stock_pool_status(self) -> List[Dict[str, Any]]:
        status_map = self._pool_status_map()
        conn = self._get_db()
        c = conn.cursor()
        rows: List[Dict[str, Any]] = []
        for code, config in CORE_STOCK_POOL_CONFIGS.items():
            if code == "all":
                continue
            count = c.execute(
                "SELECT COUNT(*) FROM stock_pool_constituents WHERE pool_code = ?",
                (code,),
            ).fetchone()[0]
            status = dict(status_map.get(code) or {})
            rows.append(
                {
                    "pool_code": code,
                    "pool_name": config["name"],
                    "source": config["source"],
                    "member_count": int(count or status.get("member_count") or 0),
                    "as_of_date": status.get("as_of_date"),
                    "sync_status": status.get("sync_status") or ("success" if count else "idle"),
                    "note": status.get("note"),
                    "updated_at": status.get("updated_at"),
                }
            )
        conn.close()
        return rows

    def _load_pool_members(self, pool_code: str = "all") -> List[Dict[str, Any]]:
        configs = self._pool_configs(pool_code)
        pool_codes = list(configs.keys())
        placeholders = ",".join(["?" for _ in pool_codes])
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            f"""
            SELECT pool_code, pool_name, member_code, member_name, member_market,
                   raw_code, weight, as_of_date, source, extra_json, updated_at
            FROM stock_pool_constituents
            WHERE pool_code IN ({placeholders})
            ORDER BY pool_code, member_code
            """,
            pool_codes,
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        return rows

    def _load_latest_financial_map(self, finance_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not finance_codes:
            return {}
        lookup_codes = sorted({code for code in finance_codes if code})
        for code in list(lookup_codes):
            if len(code) == 6 and code.isdigit():
                lookup_codes.append(f"sh{code}")
                lookup_codes.append(f"sz{code}")
        lookup_codes = sorted(set(lookup_codes))
        placeholders = ",".join(["?" for _ in lookup_codes])
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            f"""
            WITH latest AS (
                SELECT code, MAX(report_date) AS report_date
                FROM stock_financial
                WHERE code IN ({placeholders})
                GROUP BY code
            )
            SELECT sf.code, sf.name, sf.market, sf.report_date, sf.pe_ttm, sf.pb, sf.ps_ttm,
                   sf.roe, sf.roa, sf.gross_margin, sf.net_margin, sf.debt_ratio, sf.current_ratio,
                   sf.eps, sf.bvps, sf.total_revenue, sf.revenue_yoy, sf.net_profit,
                   sf.net_profit_yoy, sf.dividend_yield, sf.operating_cash_flow, sf.free_cash_flow
            FROM stock_financial sf
            JOIN latest l ON sf.code = l.code AND sf.report_date = l.report_date
            """,
            lookup_codes,
        )
        result: Dict[str, Dict[str, Any]] = {}
        for row in c.fetchall():
            payload = dict(row)
            raw_code = str(row["code"] or "")
            normalized = self._normalize_security_code(raw_code)
            result[raw_code] = payload
            if normalized:
                result[normalized] = payload
            if normalized.startswith(("sh", "sz")) and len(normalized) == 8:
                result[normalized[2:]] = payload
        conn.close()
        return result

    def _load_latest_valuation_map(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        placeholders = ",".join(["?" for _ in codes])
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            f"""
            WITH latest AS (
                SELECT code, MAX(trade_date) AS trade_date
                FROM valuation_bands
                WHERE code IN ({placeholders})
                GROUP BY code
            )
            SELECT v.code, v.name, v.trade_date, v.pe_ttm, v.pe_percentile_3y, v.pe_percentile_5y,
                   v.pe_percentile_10y, v.pb, v.pb_percentile_3y, v.pb_percentile_5y,
                   v.pb_percentile_10y, v.ps_ttm, v.ps_percentile_3y,
                   v.dividend_yield, v.dy_percentile_3y, v.valuation_level
            FROM valuation_bands v
            JOIN latest l ON v.code = l.code AND v.trade_date = l.trade_date
            """,
            codes,
        )
        result = {row["code"]: dict(row) for row in c.fetchall()}
        conn.close()
        return result

    def _load_latest_technical_map(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        placeholders = ",".join(["?" for _ in codes])
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            f"""
            WITH latest AS (
                SELECT code, MAX(trade_date) AS trade_date
                FROM technical_indicators
                WHERE code IN ({placeholders})
                GROUP BY code
            )
            SELECT t.code, t.name, t.trade_date, t.ma5, t.ma10, t.ma20, t.ma50, t.ma200,
                   t.macd, t.macd_signal, t.macd_hist, t.rsi_14, t.atr_14, t.atr_pct,
                   t.beta_1y, t.beta_3y, t.volatility_30d, t.volatility_90d, t.trend_signal
            FROM technical_indicators t
            JOIN latest l ON t.code = l.code AND t.trade_date = l.trade_date
            """,
            codes,
        )
        result = {row["code"]: dict(row) for row in c.fetchall()}
        conn.close()
        return result

    def _load_latest_factor_map(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        placeholders = ",".join(["?" for _ in codes])
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            f"""
            SELECT trade_date, code, model, quality, growth, valuation, flow, technical, risk, total
            FROM stock_factor_snapshot
            WHERE model = 'conservative'
              AND code IN ({placeholders})
              AND trade_date = (
                SELECT MAX(trade_date) FROM stock_factor_snapshot WHERE model = 'conservative'
              )
            """,
            codes,
        )
        result = {row["code"]: dict(row) for row in c.fetchall()}
        conn.close()
        return result

    def _load_recent_reports_map(self, names: List[str]) -> Dict[str, Dict[str, Any]]:
        clean_names = sorted({self._clean_security_name(name) for name in names if name})
        if not clean_names or not os.path.exists(REPORTS_DB_PATH):
            return {}

        conn = sqlite3.connect(REPORTS_DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM reports")
        total_reports = int(c.fetchone()[0] or 0)
        if total_reports <= 0:
            conn.close()
            return {}

        placeholders = ",".join(["?" for _ in clean_names])
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        c.execute(
            f"""
            SELECT stock_name, title, rating, publish_date
            FROM reports
            WHERE publish_date >= ?
              AND REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(stock_name, ' ', ''), '　', ''), '－', '-'), 'Ｗ', 'W'), 'Ｓ', 'S') IN ({placeholders})
            ORDER BY publish_date DESC, id DESC
            """,
            [cutoff, *clean_names],
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()

        positive_words = ("买入", "增持", "推荐", "上调", "超配", "改善", "复苏", "拐点", "突破")
        negative_words = ("减持", "卖出", "下调", "谨慎", "承压", "风险", "恶化", "回落", "失速")
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            clean_name = self._clean_security_name(row.get("stock_name"))
            item = grouped.setdefault(clean_name, {"coverage": 0, "sentiment": 0.0, "titles": []})
            title = str(row.get("title") or "")
            item["coverage"] += 1
            if any(word in title for word in positive_words):
                item["sentiment"] += 1.0
            if any(word in title for word in negative_words):
                item["sentiment"] -= 1.0
            if len(item["titles"]) < 5 and title:
                item["titles"].append(
                    {
                        "title": title,
                        "publish_date": row.get("publish_date"),
                        "rating": row.get("rating"),
                    }
                )

        for value in grouped.values():
            coverage = max(1, int(value.get("coverage") or 0))
            value["sentiment"] = round(float(value.get("sentiment") or 0.0) / coverage, 2)
        return grouped

    def _load_recent_reports_for_detail(self, name: str) -> List[Dict[str, Any]]:
        if not name or not os.path.exists(REPORTS_DB_PATH):
            return []
        conn = sqlite3.connect(REPORTS_DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM reports")
        if int(c.fetchone()[0] or 0) <= 0:
            conn.close()
            return []
        clean_name = self._clean_security_name(name)
        c.execute(
            """
            SELECT title, rating, institution, publish_date
            FROM reports
            WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(stock_name, ' ', ''), '　', ''), '－', '-'), 'Ｗ', 'W'), 'Ｓ', 'S') = ?
            ORDER BY publish_date DESC, id DESC
            LIMIT 8
            """,
            (clean_name,),
        )
        rows = [dict(row) for row in c.fetchall()]
        conn.close()
        return rows

    def _build_opportunity_stock_record(
        self,
        member: Dict[str, Any],
        financial_map: Dict[str, Dict[str, Any]],
        valuation_map: Dict[str, Dict[str, Any]],
        technical_map: Dict[str, Dict[str, Any]],
        factor_map: Dict[str, Dict[str, Any]],
        report_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        code = self._normalize_security_code(member.get("code"))
        finance_code = self._finance_code_for(code)
        fundamentals = dict(financial_map.get(finance_code) or {})
        valuation = dict(valuation_map.get(code) or {})
        technical = dict(technical_map.get(code) or {})
        factors = dict(factor_map.get(code) or {})
        reports = dict(report_map.get(self._clean_security_name(member.get("name"))) or {})

        roe = self._safe_float(fundamentals.get("roe"), None)
        gross_margin = self._safe_float(fundamentals.get("gross_margin"), None)
        net_margin = self._safe_float(fundamentals.get("net_margin"), None)
        debt_ratio = self._safe_float(fundamentals.get("debt_ratio"), None)
        pe_ttm = self._safe_float(fundamentals.get("pe_ttm"), None)
        pb = self._safe_float(fundamentals.get("pb"), None)
        revenue_yoy = self._safe_float(fundamentals.get("revenue_yoy"), None)
        profit_yoy = self._safe_float(fundamentals.get("net_profit_yoy"), None)
        dividend_yield = self._safe_float(fundamentals.get("dividend_yield"), None)
        ocf = self._safe_float(fundamentals.get("operating_cash_flow"), None)
        fcf = self._safe_float(fundamentals.get("free_cash_flow"), None)
        pe_pct5 = self._safe_float(valuation.get("pe_percentile_5y"), None)
        pb_pct5 = self._safe_float(valuation.get("pb_percentile_5y"), None)
        rsi_14 = self._safe_float(technical.get("rsi_14"), None)
        atr_pct = self._safe_float(technical.get("atr_pct"), None)
        report_coverage = int(reports.get("coverage") or 0)
        report_sentiment = self._safe_float(reports.get("sentiment"), 0.0) or 0.0
        trend_signal = str(technical.get("trend_signal") or "").lower()

        quality_heur = None
        if any(value is not None for value in (roe, gross_margin, net_margin, dividend_yield, ocf, fcf)):
            quality_heur = 50.0
            if roe is not None:
                quality_heur += max(-18.0, min(18.0, (roe - 10.0) * 1.6))
            if gross_margin is not None:
                quality_heur += max(-12.0, min(16.0, (gross_margin - 25.0) * 0.55))
            if net_margin is not None:
                quality_heur += max(-10.0, min(12.0, (net_margin - 10.0) * 0.8))
            if ocf is not None:
                quality_heur += 6.0 if ocf > 0 else -6.0
            if fcf is not None:
                quality_heur += 4.0 if fcf > 0 else -4.0
            if dividend_yield is not None:
                quality_heur += min(8.0, dividend_yield * 2.5)
            quality_heur = self._clamp(quality_heur)

        growth_heur = None
        if revenue_yoy is not None or profit_yoy is not None:
            growth_heur = 50.0
            if revenue_yoy is not None:
                growth_heur += max(-20.0, min(18.0, revenue_yoy * 0.55))
            if profit_yoy is not None:
                growth_heur += max(-22.0, min(22.0, profit_yoy * 0.22))
            if revenue_yoy is not None and profit_yoy is not None:
                if revenue_yoy > 0 and profit_yoy > 0:
                    growth_heur += 6.0
                elif revenue_yoy < 0 and profit_yoy < 0:
                    growth_heur -= 8.0
            growth_heur = self._clamp(growth_heur)

        valuation_heur = None
        if any(value is not None for value in (pe_ttm, pb, pe_pct5, pb_pct5)) or valuation.get("valuation_level"):
            valuation_heur = 50.0
            if pe_pct5 is not None:
                valuation_heur += max(-25.0, min(25.0, (55.0 - pe_pct5) * 0.7))
            if pb_pct5 is not None:
                valuation_heur += max(-20.0, min(20.0, (55.0 - pb_pct5) * 0.6))
            if pe_ttm is not None:
                if pe_ttm <= 0:
                    valuation_heur -= 12.0
                elif pe_ttm <= 20:
                    valuation_heur += 12.0
                elif pe_ttm <= 35:
                    valuation_heur += 4.0
                elif pe_ttm >= 60:
                    valuation_heur -= 12.0
            if pb is not None:
                if pb <= 3:
                    valuation_heur += 8.0
                elif pb >= 8:
                    valuation_heur -= 10.0
            valuation_level = str(valuation.get("valuation_level") or "")
            if valuation_level == "低估":
                valuation_heur += 10.0
            elif valuation_level == "高估":
                valuation_heur -= 10.0
            valuation_heur = self._clamp(valuation_heur)

        technical_heur = None
        if any(value is not None for value in (rsi_14, atr_pct)) or trend_signal:
            technical_heur = 50.0
            if trend_signal == "bullish":
                technical_heur += 15.0
            elif trend_signal == "neutral":
                technical_heur += 4.0
            elif trend_signal == "bearish":
                technical_heur -= 14.0
            if rsi_14 is not None:
                if 45 <= rsi_14 <= 68:
                    technical_heur += 12.0
                elif 35 <= rsi_14 < 45 or 68 < rsi_14 <= 75:
                    technical_heur += 5.0
                elif rsi_14 < 25 or rsi_14 > 80:
                    technical_heur -= 10.0
            if atr_pct is not None:
                if atr_pct <= 3:
                    technical_heur += 8.0
                elif atr_pct <= 5:
                    technical_heur += 3.0
                elif atr_pct >= 8:
                    technical_heur -= 10.0
            technical_heur = self._clamp(technical_heur)

        confirmation_heur = 45.0
        if report_coverage > 0:
            confirmation_heur += min(24.0, report_coverage * 4.0)
        confirmation_heur += report_sentiment * 12.0
        if trend_signal == "bullish":
            confirmation_heur += 5.0
        confirmation_heur = self._clamp(confirmation_heur)

        catalyst_heur = 45.0
        if report_coverage > 0:
            catalyst_heur += min(20.0, report_coverage * 3.5)
        catalyst_heur += report_sentiment * 18.0
        catalyst_heur = self._clamp(catalyst_heur)

        risk_heur = None
        if any(value is not None for value in (pe_ttm, roe, debt_ratio, atr_pct, revenue_yoy, profit_yoy)) or trend_signal:
            risk_heur = 40.0
            if pe_ttm is not None and pe_ttm <= 0:
                risk_heur += 12.0
            if roe is not None and roe < 8:
                risk_heur += 10.0
            if debt_ratio is not None and debt_ratio >= 65:
                risk_heur += 12.0
            if trend_signal == "bearish":
                risk_heur += 10.0
            if atr_pct is not None and atr_pct >= 6:
                risk_heur += 10.0
            if revenue_yoy is not None and profit_yoy is not None and revenue_yoy < 0 and profit_yoy < 0:
                risk_heur += 10.0
            if dividend_yield is not None and dividend_yield >= 3:
                risk_heur -= 6.0
            risk_heur = self._clamp(risk_heur)

        quality_score = round(self._blend_score(factors.get("quality"), quality_heur), 1)
        growth_score = round(self._blend_score(factors.get("growth"), growth_heur), 1)
        valuation_score = round(self._blend_score(factors.get("valuation"), valuation_heur), 1)
        technical_score = round(self._blend_score(factors.get("technical"), technical_heur), 1)
        confirmation_score = round(self._blend_score(factors.get("flow"), confirmation_heur, primary_weight=0.75, default=45.0), 1)
        catalyst_score = round(catalyst_heur, 1)
        risk_score = round(self._blend_score(factors.get("risk"), risk_heur, primary_weight=0.7), 1)

        coverage_flags = {
            "fundamental": bool(fundamentals.get("report_date")),
            "valuation": bool(valuation.get("trade_date")),
            "technical": bool(technical.get("trade_date")),
            "factor": bool(factors.get("trade_date")),
            "catalyst": report_coverage > 0,
        }
        coverage_score = round(sum(1 for value in coverage_flags.values() if value) * 100.0 / len(coverage_flags), 1)
        total_score = round(
            self._clamp(
                quality_score * 0.20
                + growth_score * 0.18
                + valuation_score * 0.14
                + technical_score * 0.14
                + confirmation_score * 0.10
                + catalyst_score * 0.08
                + coverage_score * 0.08
                + (100.0 - risk_score) * 0.08
            ),
            1,
        )

        gates = [
            {"key": "quality", "label": "质量", "value": quality_score, "passed": quality_score >= 60},
            {"key": "growth", "label": "增长", "value": growth_score, "passed": growth_score >= 55},
            {"key": "valuation", "label": "估值", "value": valuation_score, "passed": valuation_score >= 50},
            {"key": "technical", "label": "技术", "value": technical_score, "passed": technical_score >= 55},
            {"key": "confirmation", "label": "交易确认", "value": confirmation_score, "passed": confirmation_score >= 45},
            {"key": "risk", "label": "风险", "value": risk_score, "passed": risk_score <= 45},
            {"key": "coverage", "label": "覆盖", "value": coverage_score, "passed": coverage_score >= 55},
        ]
        passed_count = sum(1 for item in gates if item["passed"])

        if total_score >= 78 and passed_count >= 5 and risk_score <= 45 and coverage_score >= 55:
            grade = "A"
            action_key = "buy"
            action_label = "优先入池"
        elif total_score >= 68 and passed_count >= 4 and risk_score <= 60 and coverage_score >= 40:
            grade = "B"
            action_key = "watch"
            action_label = "观察入池"
        elif coverage_score < 40:
            grade = "C"
            action_key = "wait"
            action_label = "先补数据"
        else:
            grade = "C"
            action_key = "avoid"
            action_label = "暂不入池"

        if quality_score >= 70 and valuation_score >= 65:
            theme = "质量低估"
        elif growth_score >= 70 and technical_score >= 62:
            theme = "成长突破"
        elif technical_score >= 60 and risk_score <= 45:
            theme = "趋势确认"
        elif quality_score >= 65 and risk_score <= 45:
            theme = "稳健防守"
        else:
            theme = "等待确认"

        positives: List[str] = []
        negatives: List[str] = []
        if quality_score >= 60:
            positives.append("盈利质量与现金流约束未明显失真")
        if growth_score >= 60:
            positives.append("营收/利润增速处于可接受区间")
        if valuation_score >= 60:
            positives.append("估值未显著透支，具备赔率保护")
        if technical_score >= 60:
            positives.append("技术结构进入可观察区")
        if confirmation_score >= 50:
            positives.append("交易确认维度未明显逆风")
        if risk_score > 55:
            negatives.append("风险暴露偏高，入池需压仓位")
        if growth_score < 45:
            negatives.append("增长动能不足，胜率支撑偏弱")
        if technical_score < 45:
            negatives.append("技术结构未完成确认")
        if coverage_score < 50:
            negatives.append("本地数据覆盖不足，结论需降权使用")

        return {
            "code": code,
            "display_code": self._display_code(code),
            "name": member.get("name"),
            "market": member.get("market") or self._infer_market_from_code(code),
            "pool_memberships": member.get("pools") or [],
            "pool_names": [item.get("name") for item in member.get("pools") or []],
            "pool_count": len(member.get("pools") or []),
            "theme": theme,
            "grade": grade,
            "action_key": action_key,
            "action_label": action_label,
            "total_score": total_score,
            "quality_score": quality_score,
            "growth_score": growth_score,
            "valuation_score": valuation_score,
            "technical_score": technical_score,
            "confirmation_score": confirmation_score,
            "catalyst_score": catalyst_score,
            "risk_score": risk_score,
            "coverage_score": coverage_score,
            "passed_gates": passed_count,
            "gates": gates,
            "positives": positives[:4],
            "negatives": negatives[:4],
            "coverage_flags": coverage_flags,
            "report_date": fundamentals.get("report_date"),
            "factor_date": factors.get("trade_date"),
            "valuation_date": valuation.get("trade_date"),
            "technical_date": technical.get("trade_date"),
            "pe_ttm": pe_ttm,
            "pb": pb,
            "roe": roe,
            "gross_margin": gross_margin,
            "net_margin": net_margin,
            "revenue_yoy": revenue_yoy,
            "net_profit_yoy": profit_yoy,
            "dividend_yield": dividend_yield,
            "trend_signal": technical.get("trend_signal"),
            "rsi_14": rsi_14,
            "atr_pct": atr_pct,
            "pe_percentile_5y": pe_pct5,
            "pb_percentile_5y": pb_pct5,
            "valuation_level": valuation.get("valuation_level"),
            "fundamentals": fundamentals,
            "valuation": valuation,
            "technical": technical,
            "factors": factors,
            "reports": reports,
        }

    def get_opportunity_pool_overview(self, pool_code: str = "all", limit: int = 180) -> Dict[str, Any]:
        raw_members = self._load_pool_members(pool_code)
        pool_status = self.get_stock_pool_status()
        if not raw_members:
            return {
                "generated_at": datetime.now().replace(microsecond=0).isoformat(),
                "pool": {
                    "code": pool_code,
                    "name": CORE_STOCK_POOL_CONFIGS.get(pool_code, {}).get("name", pool_code),
                },
                "statuses": pool_status,
                "summary": {
                    "total_members": 0,
                    "finance_covered": 0,
                    "high_coverage_count": 0,
                    "grade_a_count": 0,
                    "actionable_count": 0,
                    "average_score": 0,
                },
                "leaderboard": [],
                "opportunities": [],
                "message": "本地尚未生成核心股票池，请先执行同步。",
            }

        member_map: Dict[str, Dict[str, Any]] = {}
        for row in raw_members:
            code = self._normalize_security_code(row.get("member_code"), market=row.get("member_market"))
            item = member_map.setdefault(
                code,
                {
                    "code": code,
                    "name": row.get("member_name"),
                    "market": row.get("member_market") or self._infer_market_from_code(code),
                    "pools": [],
                },
            )
            if not item.get("name"):
                item["name"] = row.get("member_name")
            item["pools"].append(
                {
                    "code": row.get("pool_code"),
                    "name": row.get("pool_name"),
                    "as_of_date": row.get("as_of_date"),
                }
            )

        members = list(member_map.values())
        codes = [item["code"] for item in members]
        finance_codes = sorted({self._finance_code_for(item["code"]) for item in members if self._finance_code_for(item["code"])})
        names = [item.get("name") for item in members if item.get("name")]

        financial_map = self._load_latest_financial_map(finance_codes)
        valuation_map = self._load_latest_valuation_map(codes)
        technical_map = self._load_latest_technical_map(codes)
        factor_map = self._load_latest_factor_map(codes)
        report_map = self._load_recent_reports_map(names)

        records = [
            self._build_opportunity_stock_record(
                member,
                financial_map=financial_map,
                valuation_map=valuation_map,
                technical_map=technical_map,
                factor_map=factor_map,
                report_map=report_map,
            )
            for member in members
        ]
        records.sort(
            key=lambda item: (
                {"A": 2, "B": 1, "C": 0}.get(item.get("grade"), 0),
                float(item.get("total_score") or 0),
                float(item.get("coverage_score") or 0),
            ),
            reverse=True,
        )

        limited = records[: max(20, min(int(limit or 180), 500))]
        actionable = [item for item in records if item.get("action_key") in {"buy", "watch"}]
        grade_a_count = sum(1 for item in records if item.get("grade") == "A")
        finance_covered = sum(1 for item in records if item.get("coverage_flags", {}).get("fundamental"))
        valuation_covered = sum(1 for item in records if item.get("coverage_flags", {}).get("valuation"))
        technical_covered = sum(1 for item in records if item.get("coverage_flags", {}).get("technical"))
        factor_covered = sum(1 for item in records if item.get("coverage_flags", {}).get("factor"))
        catalyst_covered = sum(1 for item in records if item.get("coverage_flags", {}).get("catalyst"))
        high_coverage_count = sum(1 for item in records if float(item.get("coverage_score") or 0) >= 60)
        average_score = round(sum(float(item.get("total_score") or 0) for item in records) / max(1, len(records)), 1)

        pool_metric_map: Dict[str, Dict[str, Any]] = {
            item["pool_code"]: {
                "pool_code": item["pool_code"],
                "pool_name": item["pool_name"],
                "member_count": item["member_count"],
                "grade_a_count": 0,
                "actionable_count": 0,
                "average_score": 0.0,
                "coverage_avg": 0.0,
            }
            for item in pool_status
        }
        pool_score_buckets: Dict[str, List[float]] = {key: [] for key in pool_metric_map}
        pool_coverage_buckets: Dict[str, List[float]] = {key: [] for key in pool_metric_map}
        for row in records:
            for pool in row.get("pool_memberships") or []:
                metric = pool_metric_map.get(pool.get("code"))
                if not metric:
                    continue
                pool_score_buckets[pool["code"]].append(float(row.get("total_score") or 0))
                pool_coverage_buckets[pool["code"]].append(float(row.get("coverage_score") or 0))
                if row.get("grade") == "A":
                    metric["grade_a_count"] += 1
                if row.get("action_key") in {"buy", "watch"}:
                    metric["actionable_count"] += 1
        for key, metric in pool_metric_map.items():
            scores = pool_score_buckets.get(key) or []
            coverages = pool_coverage_buckets.get(key) or []
            metric["average_score"] = round(sum(scores) / len(scores), 1) if scores else 0.0
            metric["coverage_avg"] = round(sum(coverages) / len(coverages), 1) if coverages else 0.0

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "pool": {
                "code": pool_code,
                "name": CORE_STOCK_POOL_CONFIGS.get(pool_code, {}).get("name", pool_code),
            },
            "statuses": [
                {
                    **item,
                    **pool_metric_map.get(item["pool_code"], {}),
                }
                for item in pool_status
            ],
            "summary": {
                "total_members": len(records),
                "finance_covered": finance_covered,
                "valuation_covered": valuation_covered,
                "technical_covered": technical_covered,
                "factor_covered": factor_covered,
                "catalyst_covered": catalyst_covered,
                "high_coverage_count": high_coverage_count,
                "grade_a_count": grade_a_count,
                "actionable_count": len(actionable),
                "average_score": average_score,
            },
            "leaderboard": limited,
            "opportunities": actionable[:12],
        }

    def get_opportunity_stock_detail(self, code: str, pool_code: str = "all") -> Dict[str, Any]:
        target_code = self._normalize_security_code(code)
        raw_members = self._load_pool_members(pool_code)
        member_map: Dict[str, Dict[str, Any]] = {}
        for row in raw_members:
            normalized = self._normalize_security_code(row.get("member_code"), market=row.get("member_market"))
            item = member_map.setdefault(
                normalized,
                {
                    "code": normalized,
                    "name": row.get("member_name"),
                    "market": row.get("member_market") or self._infer_market_from_code(normalized),
                    "pools": [],
                },
            )
            item["pools"].append(
                {
                    "code": row.get("pool_code"),
                    "name": row.get("pool_name"),
                    "as_of_date": row.get("as_of_date"),
                }
            )

        member = member_map.get(target_code)
        if not member:
            raise KeyError(target_code)

        financial_map = self._load_latest_financial_map([self._finance_code_for(target_code)])
        valuation_map = self._load_latest_valuation_map([target_code])
        technical_map = self._load_latest_technical_map([target_code])
        factor_map = self._load_latest_factor_map([target_code])
        report_map = self._load_recent_reports_map([member.get("name")])
        record = self._build_opportunity_stock_record(
            member,
            financial_map=financial_map,
            valuation_map=valuation_map,
            technical_map=technical_map,
            factor_map=factor_map,
            report_map=report_map,
        )

        reports = self._load_recent_reports_for_detail(member.get("name"))
        events: List[Dict[str, Any]] = []
        conn = self._get_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        try:
            c.execute(
                """
                SELECT event_type, event_date, importance, note
                FROM stock_event_calendar
                WHERE code = ?
                ORDER BY event_date ASC
                LIMIT 8
                """,
                (target_code,),
            )
            events = [dict(row) for row in c.fetchall()]
        except sqlite3.OperationalError:
            events = []
        finally:
            conn.close()

        next_checks = []
        if not record.get("coverage_flags", {}).get("fundamental"):
            next_checks.append("补齐最新财务报表")
        if not record.get("coverage_flags", {}).get("technical"):
            next_checks.append("补齐本地技术指标")
        if not record.get("coverage_flags", {}).get("factor"):
            next_checks.append("补齐六因子快照")
        if not reports:
            next_checks.append("补齐近90天研报/催化")

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "stock": record,
            "reports": reports,
            "events": events,
            "next_checks": next_checks[:4],
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
