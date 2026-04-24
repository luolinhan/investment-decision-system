"""
领先指标雷达主服务。

职责：
1. 统一读取 DuckDB 雷达库
2. 输出 macro / external / hk / thesis / policy / memory / pizza / gaps 面板
3. 保持对旧系统最小侵入，优先旁路接入
"""
from __future__ import annotations

import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb

from app.services.intelligence_service import IntelligenceService
from app.services.obsidian_memory_service import ObsidianMemoryService
from app.services.radar_store import RadarStore


RADAR_DB_PATH = Path("data/radar/radar.duckdb")
PARQUET_DIR = Path("data/radar/parquet")


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "--"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _safe_round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


def _display(value: Optional[float], unit: str = "", digits: int = 2, signed: bool = False) -> str:
    if value is None:
        return "-"
    fmt = f"{{:{'+' if signed else ''}.{digits}f}}"
    return f"{fmt.format(value)}{unit}"


def _json_loads(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback


def _parse_any_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "-", "--"):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        parsedate_to_datetime,
    ):
        try:
            parsed = parser(text)
            if parsed.tzinfo is not None:
                return parsed.astimezone().replace(tzinfo=None)
            return parsed
        except Exception:
            continue
    for pattern in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%Y/%m"):
        try:
            return datetime.strptime(text[: len(pattern)], pattern)
        except Exception:
            continue
    return None
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return fallback


REQUIRED_INDICATORS: List[Dict[str, Any]] = [
    {"panel": "macro", "name": "M1同比", "aliases": ["CN_M1_YOY", "china.m1_yoy", "cn.m1_yoy", "macro.cn.m1_yoy"], "priority": "P0", "source": "PBOC / Akshare", "status": "expected"},
    {"panel": "macro", "name": "M2同比", "aliases": ["CN_M2_YOY", "china.m2_yoy", "cn.m2_yoy", "macro.cn.m2_yoy"], "priority": "P0", "source": "PBOC / Akshare", "status": "expected"},
    {"panel": "macro", "name": "社融存量增速", "aliases": ["CN_TSF_YOY", "china.social_financing_yoy", "cn.social_financing_yoy"], "priority": "P0", "source": "PBOC / Akshare", "status": "expected"},
    {"panel": "macro", "name": "社融增量", "aliases": ["CN_NEW_LOANS", "china.social_financing_flow", "cn.social_financing_flow"], "priority": "P1", "source": "PBOC / Akshare", "status": "expected"},
    {"panel": "macro", "name": "核心CPI同比", "aliases": ["CN_CPI_CORE_YOY", "china.core_cpi_yoy", "cn.core_cpi_yoy"], "priority": "P0", "source": "NBS / manual or parser", "status": "expected"},
    {"panel": "macro", "name": "CPI同比", "aliases": ["CN_CPI_YOY", "china.cpi_yoy", "cn.cpi_yoy", "macro.cn.cpi_yoy"], "priority": "P0", "source": "NBS / Akshare", "status": "expected"},
    {"panel": "macro", "name": "PPI同比", "aliases": ["CN_PPI_YOY", "china.ppi_yoy", "cn.ppi_yoy", "macro.cn.ppi_yoy"], "priority": "P0", "source": "NBS / Akshare", "status": "expected"},
    {"panel": "macro", "name": "固定资产投资累计同比", "aliases": ["CN_FA_YOY", "CN_FAI_YTD_YOY", "china.fai_ytd_yoy", "cn.fai_ytd_yoy"], "priority": "P0", "source": "NBS / Akshare", "status": "expected"},
    {"panel": "macro", "name": "地产投资累计同比", "aliases": ["CN_RE_INVEST_YOY", "CN_RE_INVEST_INDEX", "china.real_estate_investment_yoy", "cn.real_estate_investment_yoy", "china.real_estate_index"], "priority": "P0", "source": "NBS / parser", "status": "expected"},
    {"panel": "macro", "name": "工业增加值同比", "aliases": ["CN_INDUSTRIAL_VA_YOY", "china.industrial_value_added_yoy", "cn.industrial_value_added_yoy"], "priority": "P1", "source": "NBS / Akshare", "status": "expected"},
    {"panel": "macro", "name": "产能利用率", "aliases": ["china.capacity_utilization", "cn.capacity_utilization"], "priority": "P1", "source": "NBS", "status": "expected"},
    {"panel": "macro", "name": "FDI累计同比", "aliases": ["CN_FDI_YOY", "china.fdi_ytd_yoy", "cn.fdi_ytd_yoy"], "priority": "P2", "source": "MOFCOM / Akshare", "status": "expected"},
    {"panel": "external", "name": "DXY", "aliases": ["DXY", "external.dxy", "dxy"], "priority": "P0", "source": "Yahoo / Stooq", "status": "expected"},
    {"panel": "external", "name": "美国2Y", "aliases": ["US_2Y_YIELD", "external.us2y", "us2y"], "priority": "P0", "source": "Treasury / Akshare", "status": "expected"},
    {"panel": "external", "name": "美国10Y", "aliases": ["US_10Y_YIELD", "external.us10y", "us10y"], "priority": "P0", "source": "Treasury / Akshare", "status": "expected"},
    {"panel": "external", "name": "2s10s", "aliases": ["US_2S10S", "external.us2s10s", "us2s10s"], "priority": "P0", "source": "derived", "status": "expected"},
    {"panel": "external", "name": "VIX", "aliases": ["VIX", "external.vix", "vix"], "priority": "P0", "source": "Yahoo", "status": "expected"},
    {"panel": "external", "name": "黄金", "aliases": ["GOLD", "external.gold", "gold"], "priority": "P1", "source": "Yahoo / Stooq", "status": "expected"},
    {"panel": "external", "name": "USD/CNH", "aliases": ["USD_CNH", "external.usdcnh", "usdcnh"], "priority": "P0", "source": "Yahoo / Stooq", "status": "expected"},
    {"panel": "external", "name": "富时中国A50", "aliases": ["external.ftse_a50", "ftse_a50"], "priority": "P1", "source": "Yahoo / Investing", "status": "expected"},
    {"panel": "external", "name": "YANG", "aliases": ["external.yang", "yang"], "priority": "P2", "source": "Yahoo", "status": "expected"},
    {"panel": "hk", "name": "南向资金", "aliases": ["CN_SOUTH_FLOW", "hk.southbound_total_net", "southbound_total_net"], "priority": "P0", "source": "HKEX / Akshare / sqlite", "status": "expected"},
    {"panel": "hk", "name": "北向资金", "aliases": ["CN_NORTH_FLOW", "hk.northbound_total_net", "northbound_total_net"], "priority": "P1", "source": "Akshare / sqlite", "status": "expected"},
    {"panel": "hk", "name": "恒指", "aliases": ["hk.hsi_close", "hsi_close"], "priority": "P0", "source": "Akshare", "status": "expected"},
    {"panel": "hk", "name": "国指", "aliases": ["hk.hscei_close", "hscei_close"], "priority": "P1", "source": "Akshare", "status": "expected"},
    {"panel": "hk", "name": "恒生科技", "aliases": ["hk.hstech_close", "hstech_close"], "priority": "P0", "source": "Akshare", "status": "expected"},
    {"panel": "hk", "name": "A/H溢价", "aliases": ["hk.ah_premium_index", "ah_premium_index"], "priority": "P0", "source": "Akshare / derived", "status": "expected"},
    {"panel": "hk", "name": "港股卖空占比", "aliases": ["hk.short_selling_ratio", "short_selling_ratio"], "priority": "P1", "source": "HKEX", "status": "expected"},
    {"panel": "hk", "name": "CCASS集中度", "aliases": ["hk.ccass_concentration", "ccass_concentration"], "priority": "P2", "source": "HKEX", "status": "expected"},
    {"panel": "hk", "name": "香港访港旅客", "aliases": ["hk.visitor_arrivals", "visitor_arrivals"], "priority": "P1", "source": "C&SD / HKTB", "status": "expected"},
    {"panel": "hk", "name": "香港客运量", "aliases": ["hk.passenger_traffic", "passenger_traffic"], "priority": "P1", "source": "ImmD / Airport stats", "status": "expected"},
    {"panel": "policy", "name": "五角大楼披萨指数", "aliases": ["policy.pizza_level", "pizza_level"], "priority": "P2", "source": "pizzint.watch", "status": "expected"},
    {"panel": "sector", "name": "创新药BD金额", "aliases": ["SECTOR_PHARMA_BD_VALUE", "sector.pharma_bd_value", "pharma_bd_value"], "priority": "P0", "source": "公告 / Intelligence", "status": "expected"},
    {"panel": "sector", "name": "创新药临床里程碑", "aliases": ["SECTOR_PHARMA_CLINICAL_MILESTONES", "sector.pharma_clinical_milestones", "pharma_clinical_milestones"], "priority": "P0", "source": "NMPA/CDE/FDA/Intelligence", "status": "expected"},
    {"panel": "sector", "name": "AI模型竞争力", "aliases": ["SECTOR_AI_MODEL_COMPETITIVENESS", "sector.ai_model_competitiveness", "ai_model_competitiveness"], "priority": "P0", "source": "LM Arena / Artificial Analysis / Intelligence", "status": "expected"},
    {"panel": "sector", "name": "AI CapEx", "aliases": ["SECTOR_AI_CAPEX", "sector.ai_capex", "ai_capex"], "priority": "P0", "source": "公司公告 / Research", "status": "expected"},
    {"panel": "sector", "name": "集成电路产量", "aliases": ["sector.semi_ic_output", "semi_ic_output"], "priority": "P0", "source": "NBS", "status": "expected"},
    {"panel": "sector", "name": "半导体国产替代", "aliases": ["SECTOR_SEMI_DOMESTIC_SUB", "sector.semi_domestic_substitution", "semi_domestic_substitution"], "priority": "P1", "source": "公告 / Research", "status": "expected"},
    {"panel": "sector", "name": "太阳能电池产量", "aliases": ["sector.pv_cell_output", "pv_cell_output"], "priority": "P0", "source": "NBS", "status": "expected"},
    {"panel": "sector", "name": "光伏供给纪律", "aliases": ["SECTOR_PV_SUPPLY_DISCIPLINE", "sector.pv_supply_discipline", "pv_supply_discipline"], "priority": "P1", "source": "公告 / Research", "status": "expected"},
    {"panel": "sector", "name": "能繁母猪", "aliases": ["sector.hog_sow_inventory", "hog_sow_inventory"], "priority": "P0", "source": "农业农村部 / NBS", "status": "expected"},
    {"panel": "sector", "name": "猪价", "aliases": ["sector.hog_price", "hog_price"], "priority": "P1", "source": "农业农村部 / commodity proxy", "status": "expected"},
]


SECTOR_BLUEPRINTS: List[Dict[str, Any]] = [
    {
        "key": "pharma",
        "name": "创新药",
        "keywords": ["fda", "nda", "bla", "drug", "clinical", "biotech", "pharma", "创新药", "临床", "授权"],
        "leading": ["BD金额", "海外授权", "关键读出", "NDA/BLA节点"],
        "confirming": ["临床推进", "附条件批准", "支付端反馈"],
        "risk": ["核心试验失败", "FDA延迟", "医保谈判压价"],
        "invalid": ["海外BD热度连续两个月回落且关键项目读出失败"],
        "watchlist": ["159992", "512290", "HK:06160", "HK:01801", "688235.SH"],
        "cycle": "周",
    },
    {
        "key": "ai",
        "name": "AI",
        "keywords": ["gpt", "deepseek", "llm", "model", "gpu", "server", "ai", "算力", "大模型", "推理"],
        "leading": ["模型榜单", "推理成本", "云厂商CapEx", "服务器/交换机订单"],
        "confirming": ["发布节奏", "企业采购", "基础设施交付"],
        "risk": ["CapEx低于预期", "模型性能落后", "监管压制"],
        "invalid": ["模型竞争力和CapEx连续两个观察窗同时走弱"],
        "watchlist": ["512720", "515980", "688111.SH", "002230.SZ", "09988.HK"],
        "cycle": "日",
    },
    {
        "key": "semi",
        "name": "半导体",
        "keywords": ["chip", "semiconductor", "foundry", "memory", "wafer", "半导体", "芯片", "晶圆"],
        "leading": ["集成电路产量", "高技术投资", "龙头扩产/并购"],
        "confirming": ["订单恢复", "库存去化", "设备验收"],
        "risk": ["减值扩大", "价格再下行", "资本开支缩减"],
        "invalid": ["产量回落且订单公告连续转弱"],
        "watchlist": ["512480", "159995", "688981.SH", "603501.SH", "00981.HK"],
        "cycle": "周",
    },
    {
        "key": "pv",
        "name": "光伏",
        "keywords": ["solar", "photovoltaic", "pv", "module", "cell", "光伏", "硅片", "电池片"],
        "leading": ["太阳能电池产量", "出口代理", "价格链条", "减产/停产公告"],
        "confirming": ["龙头资本开支下修", "并购整合", "开工率改善"],
        "risk": ["供给再扩张", "价格再下滑", "库存回升"],
        "invalid": ["价格链条再破位且停产纪律失效"],
        "watchlist": ["516180", "159857", "601012.SH", "300274.SZ", "600438.SH"],
        "cycle": "周",
    },
    {
        "key": "hog",
        "name": "猪周期",
        "keywords": ["hog", "pig", "swine", "pork", "生猪", "能繁母猪", "猪肉"],
        "leading": ["能繁母猪", "生猪存栏", "政策预警", "猪价"],
        "confirming": ["出栏增速", "去产能", "批发价"],
        "risk": ["补栏过快", "疫病扰动", "政策逆周期干预"],
        "invalid": ["能繁母猪重新拐头向上且猪价跌破成本线"],
        "watchlist": ["159865", "002714.SZ", "002157.SZ", "603477.SH"],
        "cycle": "周",
    },
]


class RadarService:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path or RADAR_DB_PATH)
        self.parquet_dir = PARQUET_DIR
        self.intelligence = IntelligenceService()
        self.memory = ObsidianMemoryService()
        self._overview_cache: Optional[Dict[str, Any]] = None
        self._overview_cache_at: Optional[datetime] = None
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        try:
            RadarStore(db_path=str(self.db_path), parquet_dir=str(self.parquet_dir)).ensure_schema()
            with duckdb.connect(str(self.db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pizza_daily (
                        as_of_date DATE,
                        level DOUBLE,
                        headline VARCHAR,
                        status VARCHAR,
                        description VARCHAR,
                        temperature_band VARCHAR,
                        source VARCHAR,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS hk_market_daily (
                        trade_date DATE,
                        metric_key VARCHAR,
                        value DOUBLE,
                        unit VARCHAR,
                        source VARCHAR,
                        metadata VARCHAR,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS hk_activity_daily (
                        activity_date DATE,
                        metric_key VARCHAR,
                        value DOUBLE,
                        unit VARCHAR,
                        source VARCHAR,
                        metadata VARCHAR,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
        except duckdb.IOException as exc:
            if "Could not set lock on file" in str(exc) and self.db_path.exists():
                return
            raise

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=True)

    def _table_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> List[str]:
        if not self._table_exists(conn, table_name):
            return []
        rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return [row[1] for row in rows]

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])

    def _observation_columns(self) -> Tuple[str, str]:
        with self._connect() as conn:
            cols = self._table_columns(conn, "indicator_observations")
        key_col = "indicator_code" if "indicator_code" in cols else "indicator_key"
        date_col = "obs_date" if "obs_date" in cols else "observation_date"
        return key_col, date_col

    def _catalog_columns(self) -> Dict[str, str]:
        with self._connect() as conn:
            cols = set(self._table_columns(conn, "indicator_catalog"))
        return {
            "key": "indicator_code" if "indicator_code" in cols else "indicator_key",
            "display": "display_name" if "display_name" in cols else "indicator_code",
            "type": "indicator_type" if "indicator_type" in cols else "type",
        }

    def _best_alias(self, aliases: Sequence[str]) -> Optional[str]:
        if not aliases:
            return None
        placeholders = ", ".join(["?"] * len(aliases))
        key_col, date_col = self._observation_columns()
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {key_col}, MAX({date_col}) AS latest_date, COUNT(*) AS row_count
                FROM indicator_observations
                WHERE {key_col} IN ({placeholders})
                  AND regexp_matches(CAST({date_col} AS VARCHAR), '^[0-9]{{4}}-[0-9]{{2}}(-[0-9]{{2}})?$')
                GROUP BY {key_col}
                ORDER BY latest_date DESC, row_count DESC
                LIMIT 1
                """,
                list(aliases),
            ).fetchone()
        return row[0] if row else None

    def _latest_for_aliases(self, aliases: Sequence[str]) -> Optional[Dict[str, Any]]:
        key = self._best_alias(aliases)
        if not key:
            return None
        key_col, date_col = self._observation_columns()
        with self._connect() as conn:
            cols = set(self._table_columns(conn, "indicator_observations"))
            value_text_col = "value_text" if "value_text" in cols else "notes"
            frequency_col = "frequency" if "frequency" in cols else "quality_flag"
            metadata_col = "metadata" if "metadata" in cols else "notes"
            row = conn.execute(
                f"""
                SELECT {key_col}, {date_col}, value, {value_text_col}, unit, source,
                       {frequency_col}, {metadata_col}
                FROM indicator_observations
                WHERE {key_col} = ? AND value IS NOT NULL AND isfinite(value)
                  AND regexp_matches(CAST({date_col} AS VARCHAR), '^[0-9]{{4}}-[0-9]{{2}}(-[0-9]{{2}})?$')
                ORDER BY {date_col} DESC
                LIMIT 1
                """,
                [key],
            ).fetchone()
        if not row:
            return None
        return {
            "indicator_key": row[0],
            "observation_date": str(row[1]),
            "value": _to_float(row[2]),
            "value_text": row[3],
            "unit": row[4] or "",
            "source": row[5],
            "frequency": row[6],
            "metadata": _json_loads(row[7], {}),
        }

    def _series_for_aliases(self, aliases: Sequence[str], limit: int = 36) -> List[Dict[str, Any]]:
        key = self._best_alias(aliases)
        if not key:
            return []
        key_col, date_col = self._observation_columns()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT {date_col}, value, unit
                FROM indicator_observations
                WHERE {key_col} = ? AND value IS NOT NULL AND isfinite(value)
                  AND regexp_matches(CAST({date_col} AS VARCHAR), '^[0-9]{{4}}-[0-9]{{2}}(-[0-9]{{2}})?$')
                ORDER BY {date_col} DESC
                LIMIT ?
                """,
                [key, max(1, min(int(limit or 36), 240))],
            ).fetchall()
        return [
            {"date": str(row[0]), "value": _to_float(row[1]), "unit": row[2] or ""}
            for row in reversed(rows)
            if _to_float(row[1]) is not None
        ]

    def _latest_table_value(
        self,
        table_name: str,
        date_col: str,
        value_col: str,
        where_clause: str = "",
        params: Optional[Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            if not self._table_exists(conn, table_name):
                return None
            row = conn.execute(
                f"""
                SELECT {date_col}, {value_col}
                FROM {table_name}
                {'WHERE ' + where_clause if where_clause else ''}
                ORDER BY {date_col} DESC
                LIMIT 1
                """,
                list(params or []),
            ).fetchone()
        if not row:
            return None
        return {
            "observation_date": str(row[0]),
            "value": _to_float(row[1]),
        }

    def _series_table_value(
        self,
        table_name: str,
        date_col: str,
        value_col: str,
        where_clause: str = "",
        params: Optional[Sequence[Any]] = None,
        limit: int = 60,
    ) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            if not self._table_exists(conn, table_name):
                return []
            rows = conn.execute(
                f"""
                SELECT {date_col}, {value_col}
                FROM {table_name}
                {'WHERE ' + where_clause if where_clause else ''}
                ORDER BY {date_col} DESC
                LIMIT ?
                """,
                [*(params or []), max(1, min(int(limit or 60), 240))],
            ).fetchall()
        return [
            {"date": str(row[0]), "value": _to_float(row[1]), "unit": ""}
            for row in reversed(rows)
            if _to_float(row[1]) is not None
        ]

    def _signal(self, label: str, value: Optional[float], good: Optional[float], bad: Optional[float], reverse: bool = False, unit: str = "", digits: int = 2, hint: str = "") -> Dict[str, Any]:
        if value is None:
            return {
                "label": label,
                "value": None,
                "display": "-",
                "signal": "missing",
                "score": 50,
                "hint": hint,
            }
        if reverse:
            positive = good is not None and value <= good
            negative = bad is not None and value >= bad
        else:
            positive = good is not None and value >= good
            negative = bad is not None and value <= bad
        signal = "positive" if positive else "negative" if negative else "neutral"
        score = 80 if signal == "positive" else 20 if signal == "negative" else 50
        return {
            "label": label,
            "value": value,
            "display": _display(value, unit=unit, digits=digits, signed=(good is not None and bad is not None and not reverse)),
            "signal": signal,
            "score": score,
            "hint": hint,
        }

    def _score_signals(self, signals: Sequence[Dict[str, Any]]) -> int:
        usable = [int(item.get("score") or 0) for item in signals if item.get("signal") != "missing"]
        if not usable:
            return 50
        return round(sum(usable) / max(1, len(usable)))

    def _series_stats(self, points: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        valid = [point for point in points if _to_float(point.get("value")) is not None]
        if not valid:
            return {
                "observations": 0,
                "start_date": None,
                "end_date": None,
                "latest": None,
                "previous": None,
                "delta": None,
            }
        latest = _to_float(valid[-1].get("value"))
        previous = _to_float(valid[-2].get("value")) if len(valid) > 1 else None
        delta = None if latest is None or previous is None else round(latest - previous, 4)
        return {
            "observations": len(valid),
            "start_date": valid[0].get("date"),
            "end_date": valid[-1].get("date"),
            "latest": latest,
            "previous": previous,
            "delta": delta,
        }

    def _panel_freshness(
        self,
        latest_rows: Sequence[Optional[Dict[str, Any]]],
        stale_after_days: int,
    ) -> Dict[str, Any]:
        parsed: List[datetime] = []
        for row in latest_rows:
            if not row:
                continue
            parsed_dt = _parse_any_datetime(
                row.get("observation_date")
                or row.get("event_time")
                or row.get("last_seen_at")
            )
            if parsed_dt:
                parsed.append(parsed_dt)
        if not parsed:
            return {
                "last_update": None,
                "stale_after_days": stale_after_days,
                "age_days": None,
                "is_stale": True,
            }
        latest_dt = max(parsed)
        age_days = max(0.0, round((datetime.now() - latest_dt).total_seconds() / 86400.0, 1))
        return {
            "last_update": latest_dt.date().isoformat(),
            "stale_after_days": stale_after_days,
            "age_days": age_days,
            "is_stale": age_days > stale_after_days,
        }

    def _coverage(self, aliases_list: Sequence[Sequence[str]]) -> float:
        total = max(1, len(list(aliases_list)))
        available = 0
        for aliases in aliases_list:
            if self._latest_for_aliases(aliases):
                available += 1
        return round(available * 100.0 / total, 1)

    def _recent_runs(self, limit: int = 12) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            cols = set(self._table_columns(conn, "source_runs"))
            if not cols:
                return []
            run_at = "run_at" if "run_at" in cols else "started_at"
            source_key = "source_key" if "source_key" in cols else "source_name"
            source_name = "source_name" if "source_name" in cols else "source_key"
            records_found = "records_found" if "records_found" in cols else "rows_read"
            records_added = "records_added" if "records_added" in cols else "rows_upserted"
            notes = "notes" if "notes" in cols else "error_message"
            rows = conn.execute(
                f"""
                SELECT {run_at}, {source_key}, {source_name}, status, {records_found}, {records_added}, {notes}
                FROM source_runs
                ORDER BY {run_at} DESC
                LIMIT ?
                """,
                [max(1, min(int(limit or 12), 50))],
            ).fetchall()
        return [
            {
                "run_at": str(row[0]),
                "source_key": row[1],
                "source_name": row[2],
                "status": row[3],
                "records_found": row[4] or 0,
                "records_added": row[5] or 0,
                "notes": row[6] or "",
            }
            for row in rows
        ]

    def _load_catalog(self) -> List[Dict[str, Any]]:
        catalog_cols = self._catalog_columns()
        with self._connect() as conn:
            if not self._table_exists(conn, "indicator_catalog"):
                return []
            rows = conn.execute(
                f"""
                SELECT {catalog_cols['key']}, {catalog_cols['display']}, category, {catalog_cols['type']}, frequency, direction,
                       half_life_days, affected_assets, affected_sectors, source,
                       confidence, last_update, status, notes
                FROM indicator_catalog
                """
            ).fetchall()
        items = [
            {
                "indicator_key": row[0],
                "display_name": row[1],
                "category": row[2],
                "type": row[3],
                "frequency": row[4],
                "direction": row[5],
                "half_life_days": row[6],
                "affected_assets": _json_loads(row[7], []),
                "affected_sectors": _json_loads(row[8], []),
                "source": row[9],
                "confidence": _to_float(row[10]),
                "last_update": str(row[11]) if row[11] else None,
                "status": row[12],
                "notes": row[13],
            }
            for row in rows
        ]
        items.sort(key=lambda item: ((item.get("category") or ""), (item.get("display_name") or item.get("indicator_key") or "")))
        return items

    def _get_gaps(self) -> Dict[str, Any]:
        catalog = self._load_catalog()
        latest_by_alias: Dict[str, Optional[Dict[str, Any]]] = {}
        items: List[Dict[str, Any]] = []
        required_items: List[Dict[str, Any]] = []
        for spec in REQUIRED_INDICATORS:
            latest = self._latest_for_aliases(spec["aliases"])
            latest_by_alias[spec["name"]] = latest
            status = "ready" if latest else "missing"
            item = (
                {
                    "panel": spec["panel"],
                    "name": spec["name"],
                    "priority": spec["priority"],
                    "source": spec["source"],
                    "status": status,
                    "last_update": latest.get("observation_date") if latest else None,
                    "current_value": latest.get("value") if latest else None,
                }
            )
            items.append(item)
            required_items.append(item)
        if catalog:
            for row in catalog:
                if row.get("status") in {"planned", "disabled"}:
                    items.append(
                        {
                            "panel": row.get("category"),
                            "name": row.get("display_name"),
                            "priority": "P2",
                            "source": row.get("source"),
                            "status": row.get("status"),
                            "last_update": row.get("last_update"),
                            "current_value": None,
                            "notes": row.get("notes"),
                        }
                    )
        items.sort(key=lambda item: (item.get("status") == "ready", item.get("priority"), item.get("panel"), item.get("name")))
        missing_count = sum(1 for item in required_items if item.get("status") != "ready")
        return {
            "summary": {
                "required_total": len(REQUIRED_INDICATORS),
                "ready_total": len(REQUIRED_INDICATORS) - missing_count,
                "missing_total": missing_count,
            },
            "items": items[:40],
        }

    def _get_macro_panel(self) -> Dict[str, Any]:
        specs = {
            "m1": ["CN_M1_YOY", "china.m1_yoy", "cn.m1_yoy", "macro.cn.m1_yoy"],
            "m2": ["CN_M2_YOY", "china.m2_yoy", "cn.m2_yoy", "macro.cn.m2_yoy"],
            "social_financing": ["CN_TSF_YOY", "china.social_financing_yoy", "cn.social_financing_yoy"],
            "cpi": ["CN_CPI_YOY", "china.cpi_yoy", "cn.cpi_yoy", "macro.cn.cpi_yoy"],
            "core_cpi": ["CN_CPI_CORE_YOY", "china.core_cpi_yoy", "cn.core_cpi_yoy"],
            "ppi": ["CN_PPI_YOY", "china.ppi_yoy", "cn.ppi_yoy", "macro.cn.ppi_yoy"],
            "fai": ["CN_FA_YOY", "CN_FAI_YTD_YOY", "china.fai_ytd_yoy", "cn.fai_ytd_yoy"],
            "real_estate": ["CN_RE_INVEST_YOY", "CN_RE_INVEST_INDEX", "china.real_estate_investment_yoy", "cn.real_estate_investment_yoy", "china.real_estate_index"],
            "industrial": ["CN_INDUSTRIAL_VA_YOY", "china.industrial_value_added_yoy", "cn.industrial_value_added_yoy"],
            "capacity": ["china.capacity_utilization", "cn.capacity_utilization"],
            "fdi": ["CN_FDI_YOY", "china.fdi_ytd_yoy", "cn.fdi_ytd_yoy"],
        }
        latest = {key: self._latest_for_aliases(aliases) for key, aliases in specs.items()}
        m1 = _to_float((latest.get("m1") or {}).get("value"))
        m2 = _to_float((latest.get("m2") or {}).get("value"))
        cpi = _to_float((latest.get("cpi") or {}).get("value"))
        ppi = _to_float((latest.get("ppi") or {}).get("value"))
        fai = _to_float((latest.get("fai") or {}).get("value"))
        real_estate_row = latest.get("real_estate") or {}
        real_estate = _to_float(real_estate_row.get("value"))
        real_estate_key = real_estate_row.get("indicator_key")
        industrial = _to_float((latest.get("industrial") or {}).get("value"))
        capacity = _to_float((latest.get("capacity") or {}).get("value"))
        if real_estate_key == "CN_RE_INVEST_INDEX":
            real_estate_signal = self._signal(
                "国房景气指数",
                real_estate,
                good=95.0,
                bad=90.0,
                unit="",
                digits=1,
                hint="国房景气指数高于 100 通常对应地产景气改善，这里用 95/90 做保守阈值。",
            )
            real_estate_drag = real_estate is not None and real_estate <= 90.0
            real_estate_credit_ok = real_estate is None or real_estate >= 95.0
        else:
            real_estate_signal = self._signal(
                "地产投资/价格代理",
                real_estate,
                good=-3.0,
                bad=-8.0,
                unit="%",
                digits=1,
                hint="地产仍弱时，顺周期弹性要打折。",
            )
            real_estate_drag = real_estate is not None and real_estate <= -8.0
            real_estate_credit_ok = real_estate is None or real_estate > -8.0
        signals = [
            self._signal("M1同比", m1, good=3.0, bad=-2.0, unit="%", digits=1, hint="M1回升通常对应风险偏好改善。"),
            self._signal("M2同比", m2, good=8.0, bad=6.0, unit="%", digits=1, hint="M2高位意味着货币环境偏宽。"),
            self._signal("CPI同比", cpi, good=0.6, bad=-0.5, unit="%", digits=1, hint="CPI脱离零轴后，通缩压力才算真正缓和。"),
            self._signal("PPI同比", ppi, good=-0.5, bad=-2.0, unit="%", digits=1, hint="PPI上修意味着工业价格压力缓解。"),
            self._signal("固定资产投资累计同比", fai, good=3.0, bad=0.0, unit="%", digits=1, hint="财政与信用扩张的确认项。"),
            real_estate_signal,
            self._signal("工业增加值同比", industrial, good=5.0, bad=3.0, unit="%", digits=1, hint="工业生产改善有助于验证景气修复。"),
            self._signal("产能利用率", capacity, good=74.5, bad=72.0, unit="%", digits=1, hint="产能利用率抬升，有助于价格和盈利修复。"),
        ]
        score = self._score_signals(signals)
        if (m2 or 0) >= 8.0:
            money_label = "宽货币"
        elif m2 is None:
            money_label = "货币缺数"
        else:
            money_label = "中性货币"
        if fai is not None and fai >= 3.0 and real_estate_credit_ok:
            credit_label = "宽信用"
        elif fai is None:
            credit_label = "信用缺数"
        else:
            credit_label = "信用分化"
        if cpi is None or ppi is None:
            inflation_label = "价格缺数"
        elif cpi >= 0.5 and ppi >= -0.5:
            inflation_label = "再通胀/通缩缓解"
        elif cpi >= 0.0:
            inflation_label = "通缩缓解中"
        else:
            inflation_label = "再通缩风险"
        missing = [label for label, row in latest.items() if not row]
        charts = [
            {"label": "M1同比", "points": self._series_for_aliases(specs["m1"], limit=24)},
            {"label": "M2同比", "points": self._series_for_aliases(specs["m2"], limit=24)},
            {"label": "CPI同比", "points": self._series_for_aliases(specs["cpi"], limit=24)},
            {"label": "PPI同比", "points": self._series_for_aliases(specs["ppi"], limit=24)},
        ]
        coverage = self._coverage(specs.values())
        return {
            "macro_regime": f"{money_label} / {credit_label}",
            "score": score,
            "inflation_state": inflation_label,
            "quadrant": {"money": money_label, "credit": credit_label},
            "signals": signals,
            "missing_keys": missing,
            "beneficiaries": ["创新药", "AI基础设施", "高股息(若外部压力未放大)"] if score >= 60 else ["高股息", "防御", "低波动"],
            "pressured_styles": ["地产链", "高负债顺周期"] if real_estate_drag else ["无明显压制风格"],
            "policy_divergence": ["政策表述偏积极，但社融/核心CPI仍需补齐验证。"] if "social_financing" in missing or "core_cpi" in missing else [],
            "charts": charts,
            "chart_stats": [dict(label=item["label"], **self._series_stats(item["points"])) for item in charts],
            "coverage_ratio": coverage,
            "freshness": self._panel_freshness(latest.values(), stale_after_days=45),
        }

    def _get_external_panel(self) -> Dict[str, Any]:
        specs = {
            "dxy": ["DXY", "external.dxy", "dxy"],
            "us2y": ["US_2Y_YIELD", "external.us2y", "us2y"],
            "us10y": ["US_10Y_YIELD", "external.us10y", "us10y"],
            "spread": ["US_2S10S", "external.us2s10s", "us2s10s"],
            "vix": ["VIX", "external.vix", "vix"],
            "gold": ["GOLD", "external.gold", "gold"],
            "usdcnh": ["USD_CNH", "external.usdcnh", "usdcnh"],
            "ftse_a50": ["external.ftse_a50", "ftse_a50"],
            "yang": ["external.yang", "yang"],
        }
        latest = {key: self._latest_for_aliases(aliases) for key, aliases in specs.items()}
        signals = [
            self._signal("DXY", _to_float((latest.get("dxy") or {}).get("value")), good=102.5, bad=105.0, reverse=True, digits=2, hint="美元回落时，中国风险资产压力通常减轻。"),
            self._signal("美国10Y", _to_float((latest.get("us10y") or {}).get("value")), good=4.0, bad=4.5, reverse=True, unit="%", digits=2, hint="长端利率过高会压制估值。"),
            self._signal("2s10s", _to_float((latest.get("spread") or {}).get("value")), good=0.0, bad=-0.5, unit="%", digits=2, hint="倒挂越深，全球风险偏好越脆弱。"),
            self._signal("VIX", _to_float((latest.get("vix") or {}).get("value")), good=16.0, bad=24.0, reverse=True, digits=2, hint="VIX是最直接的外部风险温度计。"),
            self._signal("黄金", _to_float((latest.get("gold") or {}).get("value")), good=2400.0, bad=2800.0, reverse=True, digits=0, hint="黄金过热常伴随避险升级。"),
            self._signal("USD/CNH", _to_float((latest.get("usdcnh") or {}).get("value")), good=7.10, bad=7.30, reverse=True, digits=3, hint="离岸人民币贬压会拖累港股弹性。"),
            self._signal("YANG", _to_float((latest.get("yang") or {}).get("value")), good=12.0, bad=18.0, reverse=True, digits=2, hint="反向做空中国ETF升温，说明外部压制增强。"),
        ]
        score = self._score_signals(signals)
        if score >= 65:
            risk_state = "risk_on"
        elif score <= 40:
            risk_state = "risk_off"
        else:
            risk_state = "neutral"
        charts = [
            {"label": "DXY", "points": self._series_for_aliases(specs["dxy"], limit=60)},
            {"label": "美国10Y", "points": self._series_for_aliases(specs["us10y"], limit=60)},
            {"label": "VIX", "points": self._series_for_aliases(specs["vix"], limit=60)},
            {"label": "USD/CNH", "points": self._series_for_aliases(specs["usdcnh"], limit=60)},
        ]
        asset_map = {
            "A股": "外部压力回落时，成长和顺周期弹性提升；压力抬升时，红利和内需防御更稳。",
            "港股": "对美元、CNH、南向情绪最敏感；risk_on 阶段高弹性更受益。",
            "高弹性主题": "需要 DXY/VIX/CNH 同时配合，不然只适合短波段。",
            "红利防御": "risk_off 下更占优，但要留意利率与人民币方向。",
        }
        return {
            "external_risk_score": score,
            "risk_state": risk_state,
            "signals": signals,
            "asset_map": asset_map,
            "missing_keys": [name for name, row in latest.items() if not row],
            "charts": charts,
            "chart_stats": [dict(label=item["label"], **self._series_stats(item["points"])) for item in charts],
            "coverage_ratio": self._coverage(specs.values()),
            "freshness": self._panel_freshness(latest.values(), stale_after_days=5),
        }

    def _get_hk_panel(self) -> Dict[str, Any]:
        southbound = self._latest_for_aliases(["CN_SOUTH_FLOW", "hk.southbound_total_net", "southbound_total_net"]) or self._latest_table_value("hk_south_flow", "trade_date", "total_net")
        northbound = self._latest_for_aliases(["CN_NORTH_FLOW", "hk.northbound_total_net", "northbound_total_net"]) or self._latest_table_value("hk_north_money", "trade_date", "total_net_inflow")
        hstech = self._latest_for_aliases(["hk.hstech_close", "hstech_close"]) or self._latest_table_value("hk_indices", "trade_date", "close", "index_code = ?", ["HSTECH"])
        ah = self._latest_for_aliases(["hk.ah_premium_index", "ah_premium_index"]) or self._latest_table_value("ah_premium", "trade_date", "ah_index")
        short_ratio = self._latest_for_aliases(["hk.short_selling_ratio", "short_selling_ratio"])
        visitors = self._latest_for_aliases(["hk.visitor_arrivals", "visitor_arrivals"]) or self._latest_table_value("hk_visitor_arrivals", "date", "arrivals_mainland_visitors")
        passengers = self._latest_for_aliases(["hk.passenger_traffic", "passenger_traffic"]) or self._latest_table_value("hk_visitor_arrivals", "date", "arrivals_total")
        signals = [
            self._signal("南向资金", _to_float((southbound or {}).get("value")), good=50.0, bad=-50.0, unit="亿", digits=1, hint="南向持续净流入是港股风格修复的核心驱动。"),
            self._signal("北向资金", _to_float((northbound or {}).get("value")), good=40.0, bad=-40.0, unit="亿", digits=1, hint="A/H联动强时，北向改善会强化港股共振。"),
            self._signal("恒生科技", _to_float((hstech or {}).get("value")), good=4000.0, bad=3200.0, digits=0, hint="高弹性风格的直接温度计。"),
            self._signal("A/H溢价", _to_float((ah or {}).get("value")), good=135.0, bad=150.0, reverse=True, digits=1, hint="溢价回落往往意味着港股折价修复。"),
            self._signal("港股卖空占比", _to_float((short_ratio or {}).get("value")), good=14.0, bad=20.0, reverse=True, unit="%", digits=1, hint="卖空占比抬升意味着风险偏好走弱。"),
            self._signal("访港旅客", _to_float((visitors or {}).get("value")), good=80000.0, bad=40000.0, unit="", digits=0, hint="香港活动代理，有助于验证本地消费与跨境热度。"),
            self._signal("客运量", _to_float((passengers or {}).get("value")), good=350000.0, bad=200000.0, unit="", digits=0, hint="与出入境和航空链条相关。"),
        ]
        score = self._score_signals(signals)
        if score >= 65:
            appetite = "risk_appetite_recovering"
        elif score <= 40:
            appetite = "risk_aversion"
        else:
            appetite = "mixed"
        charts = [
            {"label": "南向资金", "points": self._series_for_aliases(["CN_SOUTH_FLOW", "hk.southbound_total_net", "southbound_total_net"], limit=60) or self._series_table_value("hk_south_flow", "trade_date", "total_net", limit=60)},
            {"label": "恒生科技", "points": self._series_for_aliases(["hk.hstech_close", "hstech_close"], limit=60) or self._series_table_value("hk_indices", "trade_date", "close", "index_code = ?", ["HSTECH"], limit=60)},
            {"label": "A/H溢价", "points": self._series_for_aliases(["hk.ah_premium_index", "ah_premium_index"], limit=60) or self._series_table_value("ah_premium", "trade_date", "ah_index", limit=60)},
            {"label": "访港旅客", "points": self._series_for_aliases(["hk.visitor_arrivals", "visitor_arrivals"], limit=36) or self._series_table_value("hk_visitor_arrivals", "date", "arrivals_mainland_visitors", limit=36)},
        ]
        flags = []
        if _to_float((southbound or {}).get("value")) and _to_float((southbound or {}).get("value")) > 80:
            flags.append("南向抱团强化")
        if _to_float((short_ratio or {}).get("value")) and _to_float((short_ratio or {}).get("value")) > 18:
            flags.append("卖空压力偏高")
        if _to_float((ah or {}).get("value")) and _to_float((ah or {}).get("value")) < 130:
            flags.append("A/H修复已进入中段")
        return {
            "hk_liquidity_score": score,
            "risk_appetite": appetite,
            "signals": signals,
            "flags": flags,
            "missing_keys": [
                name
                for name, row in {
                    "southbound": southbound,
                    "northbound": northbound,
                    "hstech": hstech,
                    "ah": ah,
                    "short_ratio": short_ratio,
                    "visitors": visitors,
                    "passengers": passengers,
                }.items()
                if not row
            ],
            "charts": charts,
            "chart_stats": [dict(label=item["label"], **self._series_stats(item["points"])) for item in charts],
            "coverage_ratio": round((7 - len([
                name
                for name, row in {
                    "southbound": southbound,
                    "northbound": northbound,
                    "hstech": hstech,
                    "ah": ah,
                    "short_ratio": short_ratio,
                    "visitors": visitors,
                    "passengers": passengers,
                }.items()
                if not row
            ])) * 100.0 / 7.0, 1),
            "freshness": self._panel_freshness(
                [southbound, northbound, hstech, ah, short_ratio, visitors, passengers],
                stale_after_days=14,
            ),
        }

    def _count_intelligence_matches(self, blueprint: Dict[str, Any], events: Sequence[Dict[str, Any]], research: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        keywords = [token.lower() for token in blueprint.get("keywords", [])]
        event_count = 0
        research_count = 0
        for item in events:
            text = " ".join(
                str(item.get(field) or "")
                for field in ("title", "title_zh", "summary", "summary_zh", "impact_summary", "impact_summary_zh")
            ).lower()
            if any(keyword in text for keyword in keywords):
                event_count += 1
        for item in research:
            text = " ".join(
                str(item.get(field) or "")
                for field in ("title", "title_zh", "summary", "summary_zh", "thesis", "thesis_zh", "relevance", "relevance_zh")
            ).lower()
            if any(keyword in text for keyword in keywords):
                research_count += 1
        return {"events": event_count, "research": research_count}

    def _sector_indicator_bonus(self, blueprint_key: str) -> Tuple[int, List[str], List[str]]:
        key_map = {
            "pharma": [["SECTOR_PHARMA_BD_VALUE", "sector.pharma_bd_value", "pharma_bd_value"], ["SECTOR_PHARMA_CLINICAL_MILESTONES", "sector.pharma_clinical_milestones", "pharma_clinical_milestones"]],
            "ai": [["SECTOR_AI_MODEL_COMPETITIVENESS", "sector.ai_model_competitiveness", "ai_model_competitiveness"], ["SECTOR_AI_CAPEX", "sector.ai_capex", "ai_capex"]],
            "semi": [["CN_SW_SEMI_INDEX"], ["CN_SW_ELEC_INDEX"], ["SECTOR_SEMI_DOMESTIC_SUB", "sector.semi_domestic_substitution", "semi_domestic_substitution"], ["sector.semi_ic_output", "semi_ic_output"]],
            "pv": [["CN_SW_SOLAR_INDEX"], ["CN_SW_BATTERY_INDEX"], ["SECTOR_PV_SUPPLY_DISCIPLINE", "sector.pv_supply_discipline", "pv_supply_discipline"], ["sector.pv_cell_output", "pv_cell_output"]],
            "hog": [["CN_HOG_PRICE_INDEX"], ["CN_HOG_SPOT_PRICE"], ["CN_HOG_FUTURES_PRICE"], ["CN_SW_AGRI_INDEX"]],
        }
        aliases_list = key_map.get(blueprint_key, [])
        verified: List[str] = []
        missing: List[str] = []
        bonus = 0
        for aliases in aliases_list:
            latest = self._latest_for_aliases(aliases)
            label = aliases[0].split(".")[-1]
            if latest:
                bonus += 10
                verified.append(label)
            else:
                missing.append(label)
        return bonus, verified, missing

    def _get_sector_panel(self) -> Dict[str, Any]:
        try:
            events = self.intelligence.list_events(limit=120)
        except Exception:
            events = []
        try:
            research = self.intelligence.list_research(limit=120)
        except Exception:
            research = []
        cards = []
        for blueprint in SECTOR_BLUEPRINTS:
            match_counts = self._count_intelligence_matches(blueprint, events, research)
            bonus, verified, missing = self._sector_indicator_bonus(blueprint["key"])
            base_score = 35 + min(20, match_counts["events"] * 5) + min(15, match_counts["research"] * 3) + bonus
            score = max(20, min(95, base_score))
            confidence = round(min(0.92, 0.35 + (len(verified) * 0.12) + (match_counts["events"] * 0.03) + (match_counts["research"] * 0.02)), 2)
            cards.append(
                {
                    "key": blueprint["key"],
                    "name": blueprint["name"],
                    "score": score,
                    "confidence": confidence,
                    "leading_variables": blueprint["leading"],
                    "confirmed_variables": verified or blueprint["confirming"][:1],
                    "unverified_variables": missing or blueprint["confirming"],
                    "risk_variables": blueprint["risk"],
                    "invalid_conditions": blueprint["invalid"],
                    "watchlist": blueprint["watchlist"],
                    "review_cycle": blueprint["cycle"],
                    "evidence": match_counts,
                }
            )
        cards.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)
        overall_score = round(sum(item["score"] for item in cards) / max(1, len(cards)))
        overall_confidence = round(sum(item["confidence"] for item in cards) / max(1, len(cards)), 2)
        return {
            "sector_preposition_score": overall_score,
            "thesis_confidence": overall_confidence,
            "cards": cards,
            "evidence_totals": {
                "event_matches": sum(item["evidence"]["events"] for item in cards),
                "research_matches": sum(item["evidence"]["research"] for item in cards),
                "verified_indicator_hits": sum(len(item["confirmed_variables"]) for item in cards),
            },
        }

    def _classify_policy_event(self, item: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join(str(item.get(field) or "") for field in ("title", "title_zh", "summary", "summary_zh")).lower()
        if any(token in text for token in ("mlf", "omo", "fed", "rate", "利率", "降准", "降息")):
            bucket = "货币"
        elif any(token in text for token in ("fiscal", "bond", "财政", "专项债")):
            bucket = "财政"
        elif any(token in text for token in ("real estate", "property", "地产", "楼市")):
            bucket = "房地产"
        elif any(token in text for token in ("chip", "ai", "model", "科技", "半导体")):
            bucket = "科技产业"
        elif any(token in text for token in ("drug", "fda", "clinical", "医药", "药")):
            bucket = "医药"
        elif any(token in text for token in ("tariff", "trade", "export", "关税", "贸易")):
            bucket = "出海/贸易"
        else:
            bucket = "地缘政治"
        if any(token in text for token in ("easing", "support", "approve", "宽松", "支持", "批准")):
            bias = "宽松/利多"
        elif any(token in text for token in ("tighten", "ban", "restrict", "tariff", "收紧", "限制")):
            bias = "收紧/利空"
        else:
            bias = "中性"
        return {
            "bucket": bucket,
            "bias": bias,
            "title": item.get("title_zh") or item.get("title"),
            "priority": item.get("priority"),
            "impact_score": item.get("impact_score"),
            "event_time": item.get("event_time") or item.get("last_seen_at"),
            "url": item.get("primary_source_url"),
        }

    def _get_policy_panel(self) -> Dict[str, Any]:
        try:
            events = self.intelligence.list_events(limit=40)
        except Exception:
            events = []
        mapped = [self._classify_policy_event(item) for item in events[:20]]
        bucket_counts = Counter(item["bucket"] for item in mapped)
        return {
            "events": mapped[:12],
            "bucket_counts": [{"bucket": key, "count": value} for key, value in bucket_counts.most_common()],
        }

    def _get_pizza_panel(self) -> Dict[str, Any]:
        with self._connect() as conn:
            rows = []
            if self._table_exists(conn, "pizza_daily"):
                rows = conn.execute(
                    """
                    SELECT as_of_date, level, headline, status, description, temperature_band
                    FROM pizza_daily
                    WHERE level IS NOT NULL
                    ORDER BY as_of_date DESC
                    LIMIT 90
                    """
                ).fetchall()
            if (not rows) and self._table_exists(conn, "pentagon_pizza_history"):
                rows = conn.execute(
                    """
                    SELECT date, level, headline, status, description, temperature_band
                    FROM pentagon_pizza_history
                    WHERE level IS NOT NULL
                    ORDER BY date DESC
                    LIMIT 90
                    """
                ).fetchall()
        points = [
            {
                "date": str(row[0]),
                "level": _to_float(row[1]),
                "headline": row[2] or "",
                "status": row[3] or "",
                "description": row[4] or "",
                "temperature_band": row[5] or "",
            }
            for row in reversed(rows)
            if _to_float(row[1]) is not None
        ]
        if not points:
            latest = self._latest_for_aliases(["policy.pizza_level", "pizza_level"])
            if latest:
                points = [
                    {
                        "date": latest["observation_date"],
                        "level": latest["value"],
                        "headline": latest.get("value_text") or "单点快照",
                        "status": "single_point",
                        "description": "只有单点数据，尚未形成历史时间序列。",
                        "temperature_band": "neutral",
                    }
                ]
        latest_point = points[-1] if points else None
        levels = [point["level"] for point in points if point.get("level") is not None]
        percentile = None
        trend_7d = None
        if latest_point and levels:
            lower = sum(1 for level in levels if level <= latest_point["level"])
            percentile = round(lower * 100.0 / len(levels), 1)
        if len(levels) >= 7:
            trend_7d = round(levels[-1] - levels[-7], 2)
        band = (latest_point or {}).get("temperature_band") or "unknown"
        if latest_point and not latest_point.get("temperature_band"):
            level = latest_point.get("level")
            if level is not None:
                if level >= 85:
                    band = "过热"
                elif level >= 70:
                    band = "偏热"
                elif level <= 25:
                    band = "过冷"
                elif level <= 40:
                    band = "偏冷"
                else:
                    band = "中性"
        return {
            "latest": latest_point,
            "percentile_90d": percentile,
            "trend_7d": trend_7d,
            "band": band,
            "points": points,
            "interpretation": "只作为另类外部紧张度代理，不单独驱动仓位决策。",
            "freshness": self._panel_freshness(
                [{"observation_date": latest_point.get("date")}]
                if latest_point
                else [],
                stale_after_days=3,
            ),
            "series_stats": self._series_stats(
                [{"date": item["date"], "value": item["level"]} for item in points]
            ),
        }

    def _get_memory_panel(self) -> Dict[str, Any]:
        themes = [card["name"] for card in SECTOR_BLUEPRINTS] + ["宏观", "港股", "A股", "风险"]
        memory = self.memory.index_notes(themes=themes, limit=10)
        return memory

    def _get_pipeline_panel(self, runs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        recent = list(runs or [])
        now = datetime.now()
        failure_24h = 0
        status_counts = Counter()
        latest_sync: Optional[datetime] = None
        for item in recent:
            status_counts[item.get("status") or "unknown"] += 1
            run_at = _parse_any_datetime(item.get("run_at"))
            if run_at:
                latest_sync = max(latest_sync, run_at) if latest_sync else run_at
                if item.get("status") not in {"success", "partial"} and (now - run_at).total_seconds() <= 86400:
                    failure_24h += 1
        return {
            "recent_runs": recent[:10],
            "status_counts": [{"status": key, "count": value} for key, value in status_counts.items()],
            "failure_24h": failure_24h,
            "last_sync_at": latest_sync.replace(microsecond=0).isoformat() if latest_sync else None,
        }

    def get_overview(self, force_refresh: bool = False) -> Dict[str, Any]:
        if (
            not force_refresh
            and self._overview_cache is not None
            and self._overview_cache_at is not None
            and (datetime.now() - self._overview_cache_at).total_seconds() < 300
        ):
            return self._overview_cache
        macro = self._get_macro_panel()
        external = self._get_external_panel()
        hk = self._get_hk_panel()
        sectors = self._get_sector_panel()
        policy = self._get_policy_panel()
        pizza = self._get_pizza_panel()
        gaps = self._get_gaps()
        memory = self._get_memory_panel()
        runs = self._recent_runs(limit=12)
        pipeline = self._get_pipeline_panel(runs)
        overall_confidence = round(
            (
                sectors.get("thesis_confidence", 0.5)
                + macro.get("score", 50) / 100.0
                + external.get("external_risk_score", 50) / 100.0
                + hk.get("hk_liquidity_score", 50) / 100.0
            )
            / 4.0,
            2,
        )
        payload = {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "db_path": str(self.db_path),
            "summary": {
                "macro_regime": macro.get("macro_regime"),
                "external_risk_score": external.get("external_risk_score"),
                "hk_liquidity_score": hk.get("hk_liquidity_score"),
                "sector_preposition_score": sectors.get("sector_preposition_score"),
                "thesis_confidence": overall_confidence,
                "missing_indicators": gaps.get("summary", {}).get("missing_total", 0),
                "data_coverage": round(
                    (
                        (macro.get("coverage_ratio") or 0)
                        + (external.get("coverage_ratio") or 0)
                        + (hk.get("coverage_ratio") or 0)
                    )
                    / 3.0,
                    1,
                ),
                "last_data_sync": pipeline.get("last_sync_at"),
                "pipeline_failures_24h": pipeline.get("failure_24h"),
            },
            "macro": macro,
            "external": external,
            "hk": hk,
            "sectors": sectors,
            "policy": policy,
            "pizza": pizza,
            "gaps": gaps,
            "memory": memory,
            "pipeline": pipeline,
            "recent_source_runs": runs,
        }
        self._overview_cache = payload
        self._overview_cache_at = datetime.now()
        return payload

    def get_indicator_series(self, indicator_key: str, limit: int = 60) -> Dict[str, Any]:
        return {
            "indicator_key": indicator_key,
            "points": self._series_for_aliases([indicator_key], limit=limit),
        }
