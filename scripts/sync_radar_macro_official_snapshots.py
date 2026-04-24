# -*- coding: utf-8 -*-
"""Sync official macro/sector snapshot data via akshare into the radar DuckDB store.

Coverage (official snapshot sources where available, partial otherwise):
  - CN_CPI_CORE_YOY:          CPI national YoY (akshare has no dedicated "core CPI"
                              excluding food & energy; macro_china_cpi is used as
                              best available official proxy)
  - CN_GDP_YOY:               GDP quarterly YoY from akshare:macro_china_gdp
  - CN_INDUSTRIAL_VA_YOY:     Industrial value-added YoY from akshare
  - CN_FAI_YTD_YOY:           Fixed-asset investment cumulative YoY from akshare
  - CN_RE_INVEST_INDEX:       Real estate investment index (monthly, 1998–) from akshare
  - CN_SW_SEMI_INDEX:         Shenwan semiconductor sector index (daily)
  - CN_SW_SOLAR_INDEX:        Shenwan solar equipment sector index (daily)
  - CN_HOG_SUPPLY_INDEX:      Hog supply index (daily, short history) from akshare

Indicators that akshare does NOT expose via API are marked partial:
  - CN_CPI_CORE_YOY:   No core CPI function; using aggregate CPI YoY as proxy.
  - CN_CAPACITY_UTILIZATION: Not available in akshare (NBS publishes separately).
  - SECTOR_SEMI_IC_OUTPUT / SECTOR_PV_CELL_OUTPUT: No production volume API in akshare.
    Shenwan sector index prices are used as market-value proxies instead.
  - SECTOR_HOG_SOW_INVENTORY: No sow inventory API in akshare.
    futures_hog_supply (short history) is recorded as a partial proxy.

All data are snapshot-style: pull latest available history from akshare.

Run:
    python scripts/sync_radar_macro_official_snapshots.py
"""
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:7890")
os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:7890")

import pandas as pd

from app.services.radar_store import RadarStore

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# Catalog definitions
# ---------------------------------------------------------------------------

SNAPSHOT_INDICATORS = [
    {
        "indicator_code": "CN_CPI_CORE_YOY",
        "category": "macro_cn",
        "indicator_type": "inflation",
        "frequency": "monthly",
        "direction": "coincident",
        "half_life_days": 120,
        "affected_assets": ["CN bonds", "CNY"],
        "affected_sectors": ["consumer"],
        "source": "akshare:macro_china_cpi (aggregate CPI proxy; no core CPI API)",
        "confidence": 0.60,
        "status": "active",
        "notes": "CPI 全国同比（官方代理）：akshare 无核心 CPI（扣除食品能源）函数，"
                 "以 CPI 全国同比代理。数据来自宏观中国 CPI 官方快照，2008–至今。",
    },
    {
        "indicator_code": "CN_GDP_YOY",
        "category": "macro_cn",
        "indicator_type": "growth",
        "frequency": "quarterly",
        "direction": "coincident",
        "half_life_days": 365,
        "affected_assets": ["CN bonds", "CN equities"],
        "affected_sectors": ["broad"],
        "source": "akshare:macro_china_gdp",
        "confidence": 0.95,
        "status": "active",
        "notes": "GDP 季度同比（%）。数据来自国家统计局通过 akshare 快照，2005Q1–至今。",
    },
    {
        "indicator_code": "CN_INDUSTRIAL_VA_YOY",
        "category": "macro_cn",
        "indicator_type": "production",
        "frequency": "monthly",
        "direction": "coincident",
        "half_life_days": 90,
        "affected_assets": ["CN equities", "industrial commodities"],
        "affected_sectors": ["industrial"],
        "source": "akshare:macro_china_industrial_production_yoy",
        "confidence": 0.90,
        "status": "active",
        "notes": "规模以上工业增加值同比（%）。数据来自 akshare 宏观经济快照，1990–至今。",
    },
    {
        "indicator_code": "CN_FAI_YTD_YOY",
        "category": "macro_cn",
        "indicator_type": "investment",
        "frequency": "monthly",
        "direction": "leading",
        "half_life_days": 180,
        "affected_assets": ["CN bonds", "CN equities", "commodities"],
        "affected_sectors": ["construction", "industrial"],
        "source": "akshare:macro_china_gdzctz",
        "confidence": 0.90,
        "status": "active",
        "notes": "固定资产投资同比增长（%）。数据来自国家统计局固定资产投资月报通过 akshare，2008–至今。",
    },
    {
        "indicator_code": "CN_RE_INVEST_INDEX",
        "category": "macro_cn",
        "indicator_type": "real_estate",
        "frequency": "monthly",
        "direction": "leading",
        "half_life_days": 180,
        "affected_assets": ["CN real estate stocks", "CN bonds"],
        "affected_sectors": ["real_estate"],
        "source": "akshare:macro_china_real_estate",
        "confidence": 0.80,
        "status": "active",
        "notes": "国房景气指数（月频）。值 >100 表示景气。数据来自 akshare 官方快照，1998–2025/12。",
    },
    {
        "indicator_code": "CN_SW_SEMI_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["semiconductor stocks", "chip ETFs"],
        "affected_sectors": ["半导体"],
        "source": "akshare:index_hist_sw:801081",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万半导体二级行业指数收盘价（日频）。半导体产出景气代理，非直接产量数据。",
    },
    {
        "indicator_code": "CN_SW_SOLAR_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["solar stocks", "PV ETFs"],
        "affected_sectors": ["光伏设备"],
        "source": "akshare:index_hist_sw:801735",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万光伏设备二级行业指数收盘价（日频）。光伏产出景气代理，非直接产量数据。",
    },
    {
        "indicator_code": "CN_HOG_SUPPLY_INDEX",
        "category": "sector",
        "indicator_type": "supply",
        "frequency": "daily",
        "direction": "leading",
        "half_life_days": 14,
        "affected_assets": ["hog farming stocks", "pork ETFs"],
        "affected_sectors": ["养殖业"],
        "source": "akshare:futures_hog_supply",
        "confidence": 0.50,
        "status": "active",
        "notes": "生猪供应指数（日频，历史有限）。akshare 无直接能繁母猪存栏函数，"
                 "以 futures_hog_supply 作为供应侧短期代理，历史约 90 天。",
    },
    {
        "indicator_code": "CN_TSF_YOY",
        "category": "macro_cn",
        "indicator_type": "credit",
        "frequency": "monthly",
        "direction": "leading",
        "half_life_days": 90,
        "affected_assets": ["CN bonds", "CN equities", "CNY"],
        "affected_sectors": ["broad"],
        "source": "akshare:macro_china_shrzgm / macro_china_new_financial_credit",
        "confidence": 0.75,
        "status": "active",
        "notes": "社融同比。优先使用 macro_china_shrzgm；若 akshare 接口失败，则回退为新增金融信贷同比代理，"
                 "并在 quality_flag 中标记 estimated。",
    },
    {
        "indicator_code": "CN_NEW_LOANS",
        "category": "macro_cn",
        "indicator_type": "credit",
        "frequency": "monthly",
        "direction": "leading",
        "half_life_days": 60,
        "affected_assets": ["CN bonds", "CN equities", "CNY"],
        "affected_sectors": ["financials"],
        "source": "akshare:macro_rmb_loan",
        "confidence": 0.85,
        "status": "active",
        "notes": "新增人民币贷款总额（月频，亿元）。用于补充宽信用判断的月度趋势。",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val: Any) -> Optional[float]:
    try:
        v = float(val)
        if pd.isna(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


def _fetch_akshare(func_name: str, max_retries: int = 1, **kwargs) -> Optional[pd.DataFrame]:
    """Safely call an akshare function, returning None on failure."""
    for attempt in range(max_retries + 1):
        try:
            import akshare as ak
            func = getattr(ak, func_name, None)
            if func is None:
                logger.warning("[SKIP] akshare.%s not found", func_name)
                return None
            result = func(**kwargs)
            if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                logger.info("[EMPTY] akshare.%s returned empty", func_name)
                return None
            return result if isinstance(result, pd.DataFrame) else None
        except Exception as e:
            if attempt < max_retries:
                logger.info("[RETRY] akshare.%s attempt %d failed: %s", func_name, attempt + 1, e)
                time.sleep(2)
                continue
            logger.warning("[FAIL] akshare.%s after %d retries: %s", func_name, max_retries, e)
            return None
    return None


def _parse_china_month(raw: str) -> Optional[str]:
    """Parse '2026年03月份' -> '2026-03'.  Returns ISO month or None."""
    if not raw or not isinstance(raw, str):
        return None
    m = re.match(r"(\d{4})年(\d{2})月", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # Fallback: try YYYY-MM-DD or YYYY-MM
    m2 = re.match(r"(\d{4})-(\d{2})", raw)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}"
    return None


def _parse_rmb_loan_month(raw: str) -> Optional[str]:
    """Parse '2023-10' style strings from macro_rmb_loan."""
    if not raw or not isinstance(raw, str):
        return None
    match = re.match(r"(\d{4})-(\d{1,2})", raw.strip())
    if not match:
        return None
    return f"{match.group(1)}-{int(match.group(2)):02d}"


def _catalog_update(store: RadarStore) -> None:
    """Ensure snapshot indicator catalog entries exist."""
    conn = store.get_connection()
    try:
        for entry in SNAPSHOT_INDICATORS:
            existing = conn.execute(
                "SELECT indicator_code FROM indicator_catalog WHERE indicator_code = ?",
                (entry["indicator_code"],),
            ).fetchone()
            if existing is None:
                affected_assets = entry.get("affected_assets")
                if isinstance(affected_assets, list):
                    affected_assets = json.dumps(affected_assets, ensure_ascii=False)
                affected_sectors = entry.get("affected_sectors")
                if isinstance(affected_sectors, list):
                    affected_sectors = json.dumps(affected_sectors, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO indicator_catalog
                    (indicator_code, category, indicator_type, frequency,
                     direction, half_life_days, affected_assets, affected_sectors,
                     source, confidence, last_update, status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["indicator_code"],
                        entry.get("category"),
                        entry.get("indicator_type"),
                        entry.get("frequency"),
                        entry.get("direction"),
                        entry.get("half_life_days"),
                        affected_assets,
                        affected_sectors,
                        entry.get("source"),
                        entry.get("confidence"),
                        _now_utc(),
                        entry.get("status", "active"),
                        entry.get("notes"),
                    ),
                )
            else:
                conn.execute(
                    "UPDATE indicator_catalog SET last_update = ? WHERE indicator_code = ?",
                    (_now_utc(), entry["indicator_code"]),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------

def collect_cpi_proxy(store: RadarStore) -> Dict[str, Any]:
    """CPI 全国同比 as proxy for core CPI (akshare has no dedicated core CPI API)."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_china_cpi")
        if df is None:
            return _fail(started, "akshare.macro_china_cpi returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            obs_date = _parse_china_month(str(row.get("月份", "")))
            if obs_date is None:
                rows_read += 1
                continue
            val = _safe_float(row.get("全国-同比增长"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_CPI_CORE_YOY",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "pct",
                    "source": "akshare:macro_china_cpi",
                    "quality_flag": "good",
                    "notes": "CPI全国同比作为核心CPI代理；akshare无扣除食品能源的核心CPI函数",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"CPI全国同比代理核心CPI: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def collect_gdp(store: RadarStore) -> Dict[str, Any]:
    """GDP quarterly YoY from akshare:macro_china_gdp."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_china_gdp")
        if df is None:
            return _fail(started, "akshare.macro_china_gdp returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            quarter_str = str(row.get("季度", ""))
            if not quarter_str:
                rows_read += 1
                continue
            # Parse '2026年第1季度' or '2025年第1-4季度' -> YYYY-Qq
            m = re.search(r"(\d{4})年第(\d+)", quarter_str)
            if not m:
                rows_read += 1
                continue
            year, q = m.group(1), m.group(2)
            obs_date = f"{year}-Q{q}"
            val = _safe_float(row.get("国内生产总值-同比增长"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_GDP_YOY",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "pct",
                    "source": "akshare:macro_china_gdp",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"GDP季度同比: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def collect_industrial_va(store: RadarStore) -> Dict[str, Any]:
    """Industrial value-added YoY from akshare:macro_china_industrial_production_yoy."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_china_industrial_production_yoy")
        if df is None:
            return _fail(started, "akshare.macro_china_industrial_production_yoy returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            date_str = str(row.get("日期", ""))[:10]
            if not date_str or len(date_str) < 7:
                rows_read += 1
                continue
            # Dates are like '1990-03-01' -> normalize to '1990-03'
            obs_date = date_str[:7]
            val = _safe_float(row.get("今值"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_INDUSTRIAL_VA_YOY",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "pct",
                    "source": "akshare:macro_china_industrial_production_yoy",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"工业增加值同比: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def collect_fai(store: RadarStore) -> Dict[str, Any]:
    """Fixed-asset investment cumulative YoY from akshare:macro_china_gdzctz."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_china_gdzctz")
        if df is None:
            return _fail(started, "akshare.macro_china_gdzctz returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            obs_date = _parse_china_month(str(row.get("月份", "")))
            if obs_date is None:
                rows_read += 1
                continue
            val = _safe_float(row.get("同比增长"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_FAI_YTD_YOY",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "pct",
                    "source": "akshare:macro_china_gdzctz",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"固投同比: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def collect_real_estate(store: RadarStore) -> Dict[str, Any]:
    """Real estate investment index (国房景气指数) from akshare:macro_china_real_estate."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_china_real_estate")
        if df is None:
            return _fail(started, "akshare.macro_china_real_estate returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            date_val = row.get("日期")
            if date_val is None:
                rows_read += 1
                continue
            # date could be a datetime.date or string
            if hasattr(date_val, "strftime"):
                obs_date = date_val.strftime("%Y-%m")
            else:
                obs_date = str(date_val)[:7]
            val = _safe_float(row.get("最新值"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_RE_INVEST_INDEX",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "index",
                    "source": "akshare:macro_china_real_estate",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"国房景气指数: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def _collect_sw_index(
    store: RadarStore,
    symbol: str,
    indicator_code: str,
    source_tag: str,
) -> Dict[str, Any]:
    """Generic Shenwan sector index collector."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("index_hist_sw", symbol=symbol)
        if df is None:
            return {
                "started_at": started,
                "finished_at": _now_utc(),
                "status": "partial",
                "rows_read": 0,
                "rows_upserted": 0,
                "error_message": f"akshare index_hist_sw({symbol}) failed",
                "notes": f"申万指数 {symbol} 数据暂时不可用",
            }

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            date_str = str(row["日期"])[:10]
            val = _safe_float(row["收盘"])
            if val is not None:
                obs_rows.append({
                    "indicator_code": indicator_code,
                    "obs_date": date_str,
                    "value": val,
                    "unit": "index",
                    "source": f"akshare:sw_index:{symbol}",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"{source_tag}: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def collect_semi_index(store: RadarStore) -> Dict[str, Any]:
    """Shenwan semiconductor index as production-sentiment proxy."""
    return _collect_sw_index(store, "801081", "CN_SW_SEMI_INDEX", "半导体指数代理产出景气")


def collect_solar_index(store: RadarStore) -> Dict[str, Any]:
    """Shenwan solar equipment index as production-sentiment proxy."""
    return _collect_sw_index(store, "801735", "CN_SW_SOLAR_INDEX", "光伏设备指数代理产出景气")


def collect_hog_supply(store: RadarStore) -> Dict[str, Any]:
    """Hog supply index (short history, daily) as sow inventory proxy."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("futures_hog_supply")
        if df is None:
            return {
                "started_at": started,
                "finished_at": _now_utc(),
                "status": "partial",
                "rows_read": 0,
                "rows_upserted": 0,
                "error_message": "akshare.futures_hog_supply returned None",
                "notes": "生猪供应数据暂时不可用（akshare 无能繁母猪存栏API）",
            }

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            date_str = str(row.get("date", ""))[:10]
            val = _safe_float(row.get("value"))
            if val is not None and date_str:
                obs_rows.append({
                    "indicator_code": "CN_HOG_SUPPLY_INDEX",
                    "obs_date": date_str,
                    "value": val,
                    "unit": "index",
                    "source": "akshare:futures_hog_supply",
                    "quality_flag": "good",
                    "notes": "生猪供应短期代理指标；非直接能繁母猪存栏，历史约90天",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"生猪供应指数: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def _parse_tsf_official(
    store: RadarStore,
    df: pd.DataFrame,
    started: str,
) -> Dict[str, Any]:
    rows_read = 0
    obs_rows: List[Dict[str, Any]] = []
    columns = list(df.columns)
    date_col = columns[0] if columns else None
    yoy_col = next((col for col in columns[1:] if "同比" in str(col)), None)
    if not date_col or not yoy_col:
        return _fail(started, "macro_china_shrzgm missing expected date/yoy columns")

    for _, row in df.iterrows():
        obs_date = _parse_china_month(str(row.get(date_col, "")))
        if obs_date is None:
            rows_read += 1
            continue
        val = _safe_float(row.get(yoy_col))
        if val is not None:
            obs_rows.append({
                "indicator_code": "CN_TSF_YOY",
                "obs_date": obs_date,
                "value": val,
                "unit": "pct",
                "source": "akshare:macro_china_shrzgm",
                "quality_flag": "good",
            })
        rows_read += 1

    rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
    return {
        "started_at": started,
        "finished_at": _now_utc(),
        "status": "success" if obs_rows else "partial",
        "rows_read": rows_read,
        "rows_upserted": rows_upserted,
        "error_message": None,
        "notes": f"社融同比(官方): {rows_upserted} obs, "
                 f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
    }


def collect_tsf_proxy(store: RadarStore) -> Dict[str, Any]:
    """TSF YoY, falling back to new financial credit YoY when the official endpoint is unavailable."""
    started = _now_utc()
    rows_read = 0

    official_df = _fetch_akshare("macro_china_shrzgm", max_retries=1)
    if official_df is not None:
        logger.info("macro_china_shrzgm succeeded, using official TSF data")
        return _parse_tsf_official(store, official_df, started)

    logger.info("macro_china_shrzgm unavailable, falling back to macro_china_new_financial_credit")
    df = _fetch_akshare("macro_china_new_financial_credit")
    if df is None:
        return _fail(started, "both macro_china_shrzgm and macro_china_new_financial_credit failed")

    obs_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        obs_date = _parse_china_month(str(row.get("月份", "")))
        if obs_date is None:
            rows_read += 1
            continue
        val = _safe_float(row.get("当月-同比增长"))
        if val is not None:
            obs_rows.append({
                "indicator_code": "CN_TSF_YOY",
                "obs_date": obs_date,
                "value": val,
                "unit": "pct",
                "source": "akshare:macro_china_new_financial_credit",
                "quality_flag": "estimated",
                "notes": "新增金融信贷同比代理社融景气；官方社融接口不可用时启用",
            })
        rows_read += 1

    rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
    return {
        "started_at": started,
        "finished_at": _now_utc(),
        "status": "success" if obs_rows else "partial",
        "rows_read": rows_read,
        "rows_upserted": rows_upserted,
        "error_message": None,
        "notes": f"社融同比代理(新增信贷): {rows_upserted} obs, "
                 f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
    }


def collect_new_loans(store: RadarStore) -> Dict[str, Any]:
    """New RMB loans from akshare macro_rmb_loan."""
    started = _now_utc()
    rows_read = 0
    try:
        df = _fetch_akshare("macro_rmb_loan")
        if df is None:
            return _fail(started, "akshare.macro_rmb_loan returned None")

        obs_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            obs_date = _parse_rmb_loan_month(str(row.get("月份", "")))
            if obs_date is None:
                rows_read += 1
                continue
            val = _safe_float(row.get("新增人民币贷款-总额"))
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_NEW_LOANS",
                    "obs_date": obs_date,
                    "value": val,
                    "unit": "hundred_million_cny",
                    "source": "akshare:macro_rmb_loan",
                    "quality_flag": "good",
                })
            rows_read += 1

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success" if obs_rows else "partial",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"新增人民币贷款: {rows_upserted} obs, "
                     f"范围 {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        return _fail(started, str(e))


def _fail(started: str, error: str) -> Dict[str, Any]:
    return {
        "started_at": started,
        "finished_at": _now_utc(),
        "status": "failed",
        "rows_read": 0,
        "rows_upserted": 0,
        "error_message": error,
        "notes": f"采集失败: {error}",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COLLECTORS = [
    ("Snapshot: CPI National YoY (core proxy)", collect_cpi_proxy),
    ("Snapshot: GDP Quarterly YoY", collect_gdp),
    ("Snapshot: Industrial VA YoY", collect_industrial_va),
    ("Snapshot: FAI YTD YoY", collect_fai),
    ("Snapshot: Real Estate Investment Index", collect_real_estate),
    ("Snapshot: Semiconductor Index (output proxy)", collect_semi_index),
    ("Snapshot: Solar Equipment Index (output proxy)", collect_solar_index),
    ("Snapshot: Hog Supply Index (sow proxy)", collect_hog_supply),
    ("Snapshot: TSF YoY (credit cycle proxy)", collect_tsf_proxy),
    ("Snapshot: New RMB Loans", collect_new_loans),
]

UNAVAILABLE_NOTES = {
    "CN_CAPACITY_UTILIZATION": "国家统计局工业产能利用率：akshare 无对应 API，"
                               "需从 stats.gov.cn 手工抓取或等 akshare 更新。",
    "SECTOR_SEMI_IC_OUTPUT": "集成电路产量（月度）：akshare 无直接产量 API，"
                             "暂以申万半导体指数代理景气度。",
    "SECTOR_PV_CELL_OUTPUT": "太阳能电池/光伏电池产量（月度）：akshare 无直接产量 API，"
                             "暂以申万光伏设备指数代理景气度。",
    "SECTOR_HOG_SOW_INVENTORY": "能繁母猪存栏量：akshare 无对应 API，"
                                "以 futures_hog_supply 短期指标代理。",
}


def main() -> None:
    started_at = _now_utc()
    print("=" * 60)
    print(f"Radar Macro/sector Official Snapshot Sync - {started_at}")
    print("=" * 60)

    store = RadarStore()
    store.ensure_schema()
    _catalog_update(store)

    results: List[Dict[str, Any]] = []
    for name, collector in COLLECTORS:
        print(f"\n{'-' * 40}")
        print(f"Collecting: {name}")
        try:
            result = collector(store)
            results.append(result)
            status = result.get("status", "unknown")
            obs = result.get("rows_upserted", 0)
            notes = result.get("notes", "")
            print(f"  [{status.upper()}] {obs} obs - {notes}")
            store.record_source_run(
                source_name=f"sync_radar_macro_official_snapshots:{name}",
                target_table="indicator_observations",
                started_at=result["started_at"],
                finished_at=result["finished_at"],
                status=result.get("status", "unknown"),
                rows_read=result.get("rows_read", 0),
                rows_upserted=result.get("rows_upserted", 0),
                error_message=result.get("error_message"),
                notes=result.get("notes"),
            )
        except Exception as e:
            print(f"  [FAILED] {e}")
            traceback.print_exc()
            store.record_source_run(
                source_name=f"sync_radar_macro_official_snapshots:{name}",
                target_table="indicator_observations",
                started_at=_now_utc(),
                finished_at=_now_utc(),
                status="failed",
                error_message=str(e),
                notes=traceback.format_exc()[:500],
            )

    # Record unavailable indicators as info-only source runs
    for code, note in UNAVAILABLE_NOTES.items():
        print(f"\n[UNAVAILABLE] {code}: {note}")
        store.record_source_run(
            source_name=f"sync_radar_macro_official_snapshots:{code} (unavailable)",
            target_table="indicator_observations",
            started_at=_now_utc(),
            finished_at=_now_utc(),
            status="partial",
            rows_read=0,
            rows_upserted=0,
            error_message=None,
            notes=f"指标 {code} 暂无 akshare API 支持：{note}",
        )

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary:")
    total_ok = sum(1 for r in results if r.get("status") == "success")
    total_partial = sum(1 for r in results if r.get("status") == "partial")
    total_fail = sum(1 for r in results if r.get("status") == "failed")
    total_obs = sum(r.get("rows_upserted", 0) for r in results)
    print(f"  Success: {total_ok}  |  Partial: {total_partial}  |  Failed: {total_fail}")
    print(f"  Total observations upserted: {total_obs}")
    print(f"  Unavailable indicators (logged): {len(UNAVAILABLE_NOTES)}")
    print("=" * 60)
    metrics = {
        "run_timestamp": started_at,
        "total_collectors": len(COLLECTORS),
        "success": total_ok,
        "partial": total_partial,
        "failed": total_fail,
        "total_observations": total_obs,
        "unavailable_count": len(UNAVAILABLE_NOTES),
        "details": [],
    }
    for item in results:
        metrics["details"].append({
            "status": item.get("status"),
            "rows_upserted": item.get("rows_upserted", 0),
            "notes": item.get("notes"),
        })
    for code, note in UNAVAILABLE_NOTES.items():
        metrics["details"].append({
            "indicator": code,
            "status": "unavailable",
            "notes": note,
        })
    print("ETL_METRICS_JSON=" + json.dumps(metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()
