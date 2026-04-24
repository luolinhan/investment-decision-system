# -*- coding: utf-8 -*-
"""Sync sector thesis data (semiconductor, solar/PV, pig cycle) into the radar DuckDB store.

Data coverage:
  Semiconductor: Shenwan semiconductor & electronics sector indices
  Solar/PV:      Shenwan solar equipment & battery sector indices
  Pig cycle:     Hog spot price index, spot price, futures price, agriculture index

All data sourced via akshare (Shenwan sector indices, Soozhu hog data).
Failing items are logged in source_runs and catalog notes — never fabricated.

Run:
    python scripts/sync_radar_sector.py
"""
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("HTTP_PROXY", "http://127.0.0.1:7890")
os.environ.setdefault("HTTPS_PROXY", "http://127.0.0.1:7890")

import pandas as pd

from app.services.radar_store import RadarStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SECTOR_INDICATORS = [
    # Semiconductor
    {
        "indicator_code": "CN_SW_SEMI_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["semiconductor stocks", "chip ETFs"],
        "affected_sectors": ["半导体"],
        "source": "akshare:sw_index:801081",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万半导体二级行业指数收盘价",
    },
    {
        "indicator_code": "CN_SW_ELEC_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["electronics stocks"],
        "affected_sectors": ["电子"],
        "source": "akshare:sw_index:801080",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万电子一级行业指数收盘价（半导体上位指数代理）",
    },
    # Solar/PV
    {
        "indicator_code": "CN_SW_SOLAR_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["solar stocks", "PV ETFs"],
        "affected_sectors": ["光伏设备"],
        "source": "akshare:sw_index:801735",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万光伏设备二级行业指数收盘价",
    },
    {
        "indicator_code": "CN_SW_BATTERY_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["battery stocks"],
        "affected_sectors": ["电池"],
        "source": "akshare:sw_index:801737",
        "confidence": 0.85,
        "status": "active",
        "notes": "申万电池二级行业指数收盘价（储能/光伏配套代理）",
    },
    # Pig cycle
    {
        "indicator_code": "CN_HOG_PRICE_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 14,
        "affected_assets": ["hog farming stocks", "pork ETFs"],
        "affected_sectors": ["养殖业", "猪肉"],
        "source": "akshare:index_hog_spot_price",
        "confidence": 0.80,
        "status": "active",
        "notes": "生猪价格指数（预售/成交均价加权代理）",
    },
    {
        "indicator_code": "CN_HOG_SPOT_PRICE",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 14,
        "affected_assets": ["hog farming stocks"],
        "affected_sectors": ["养殖业"],
        "source": "akshare:spot_hog_year_trend_soozhu",
        "confidence": 0.75,
        "status": "active",
        "notes": "生猪现货价格（元/公斤），来源：搜猪网",
    },
    {
        "indicator_code": "CN_HOG_FUTURES_PRICE",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 14,
        "affected_assets": ["hog futures", "hog farming stocks"],
        "affected_sectors": ["养殖业"],
        "source": "akshare:futures_hog_core",
        "confidence": 0.80,
        "status": "active",
        "notes": "生猪期货主力合约结算价（元/kg）",
    },
    {
        "indicator_code": "CN_SW_AGRI_INDEX",
        "category": "sector",
        "indicator_type": "price",
        "frequency": "daily",
        "direction": "coincident",
        "half_life_days": 30,
        "affected_assets": ["agriculture stocks"],
        "affected_sectors": ["农林牧渔"],
        "source": "akshare:sw_index:801010",
        "confidence": 0.70,
        "status": "active",
        "notes": "申万农林牧渔一级行业指数收盘价（猪周期上位指数）",
    },
]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_akshare(func_name: str, **kwargs) -> Optional[pd.DataFrame]:
    """Safely call an akshare function, returning None on failure."""
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
        logger.warning("[FAIL] akshare.%s: %s", func_name, e)
        return None


def _catalog_update(store: RadarStore) -> None:
    """Ensure sector indicator catalog entries exist and are up to date."""
    # Only upsert entries that don't have recent updates
    conn = store.get_connection()
    try:
        for entry in SECTOR_INDICATORS:
            existing = conn.execute(
                "SELECT last_update FROM indicator_catalog WHERE indicator_code = ?",
                (entry["indicator_code"],),
            ).fetchone()
            if existing is None:
                # New entry – insert full record
                affected_assets = entry.get("affected_assets")
                if isinstance(affected_assets, list):
                    import json
                    affected_assets = json.dumps(affected_assets, ensure_ascii=False)
                affected_sectors = entry.get("affected_sectors")
                if isinstance(affected_sectors, list):
                    import json
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
                # Existing – just bump timestamp
                conn.execute(
                    "UPDATE indicator_catalog SET last_update = ? WHERE indicator_code = ?",
                    (_now_utc(), entry["indicator_code"]),
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sector collectors
# ---------------------------------------------------------------------------

def _collect_sw_index(
    store: RadarStore,
    symbol: str,
    indicator_code: str,
    source_tag: str,
) -> Dict[str, Any]:
    """Generic Shenwan sector index collector."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
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
            "notes": f"{source_tag}: {rows_upserted} obs from {rows_read} rows, "
                     f"date range {obs_rows[0]['obs_date'] if obs_rows else 'N/A'} ~ {obs_rows[-1]['obs_date'] if obs_rows else 'N/A'}",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_sw_index(%s) failed", symbol)
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "failed",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": error,
            "notes": f"申万指数 {symbol} 采集失败: {error}",
        }


def collect_semiconductor_indices(store: RadarStore) -> Dict[str, Any]:
    """Collect Shenwan semiconductor & electronics sector indices."""
    results = []
    # Collect semiconductor (二级行业 801081)
    results.append(_collect_sw_index(store, "801081", "CN_SW_SEMI_INDEX", "半导体"))
    # Collect electronics (一级行业 801080) as a broader proxy
    results.append(_collect_sw_index(store, "801080", "CN_SW_ELEC_INDEX", "电子"))

    total_obs = sum(r["rows_upserted"] for r in results)
    total_read = sum(r["rows_read"] for r in results)
    any_failed = any(r["status"] == "failed" for r in results)
    any_partial = any(r["status"] == "partial" for r in results)

    status = "success"
    if any_failed:
        status = "failed"
    elif any_partial:
        status = "partial"

    notes = "; ".join(r["notes"] for r in results if r.get("notes"))
    return {
        "started_at": results[0]["started_at"],
        "finished_at": _now_utc(),
        "status": status,
        "rows_read": total_read,
        "rows_upserted": total_obs,
        "error_message": next((r["error_message"] for r in results if r["error_message"]), None),
        "notes": notes,
    }


def collect_solar_pv_indices(store: RadarStore) -> Dict[str, Any]:
    """Collect Shenwan solar equipment & battery sector indices."""
    results = []
    # Solar equipment (二级行业 801735)
    results.append(_collect_sw_index(store, "801735", "CN_SW_SOLAR_INDEX", "光伏设备"))
    # Battery (二级行业 801737) as a solar-storage proxy
    results.append(_collect_sw_index(store, "801737", "CN_SW_BATTERY_INDEX", "电池"))

    total_obs = sum(r["rows_upserted"] for r in results)
    total_read = sum(r["rows_read"] for r in results)
    any_failed = any(r["status"] == "failed" for r in results)
    any_partial = any(r["status"] == "partial" for r in results)

    status = "success"
    if any_failed:
        status = "failed"
    elif any_partial:
        status = "partial"

    notes = "; ".join(r["notes"] for r in results if r.get("notes"))
    return {
        "started_at": results[0]["started_at"],
        "finished_at": _now_utc(),
        "status": status,
        "rows_read": total_read,
        "rows_upserted": total_obs,
        "error_message": next((r["error_message"] for r in results if r["error_message"]), None),
        "notes": notes,
    }


def collect_pig_cycle_indices(store: RadarStore) -> Dict[str, Any]:
    """Collect hog price indices and agriculture sector index for pig cycle thesis."""
    started = _now_utc()
    obs_rows: List[Dict[str, Any]] = []
    rows_read = 0
    notes_parts: List[str] = []

    # 1. Hog spot price index (index_hog_spot_price)
    hog_idx = _fetch_akshare("index_hog_spot_price")
    if hog_idx is not None:
        for _, row in hog_idx.iterrows():
            date_str = str(row.get("日期", ""))[:10]
            val = _safe_float(row.get("指数"))
            if val is not None and date_str:
                obs_rows.append({
                    "indicator_code": "CN_HOG_PRICE_INDEX",
                    "obs_date": date_str,
                    "value": val,
                    "unit": "index",
                    "source": "akshare:index_hog_spot_price",
                    "quality_flag": "good",
                })
            rows_read += 1
        notes_parts.append(f"猪价指数: {len([r for r in obs_rows if r['indicator_code']=='CN_HOG_PRICE_INDEX'])} obs")
    else:
        notes_parts.append("猪价指数: 数据不可用")

    # 2. Hog spot price in CNY/kg (spot_hog_year_trend_soozhu)
    hog_spot = _fetch_akshare("spot_hog_year_trend_soozhu")
    if hog_spot is not None:
        spot_count = 0
        for _, row in hog_spot.iterrows():
            date_str = str(row.get("日期", ""))[:10]
            val = _safe_float(row.get("价格"))
            if val is not None and date_str:
                obs_rows.append({
                    "indicator_code": "CN_HOG_SPOT_PRICE",
                    "obs_date": date_str,
                    "value": val,
                    "unit": "cny_per_kg",
                    "source": "akshare:spot_hog_year_trend_soozhu",
                    "quality_flag": "good",
                })
                spot_count += 1
            rows_read += 1
        notes_parts.append(f"生猪现货价: {spot_count} obs")
    else:
        notes_parts.append("生猪现货价: 数据不可用")

    # 3. Hog futures price (futures_hog_core)
    hog_fut = _fetch_akshare("futures_hog_core")
    if hog_fut is not None:
        fut_count = 0
        for _, row in hog_fut.iterrows():
            date_str = str(row.get("date", ""))[:10]
            val = _safe_float(row.get("value"))
            if val is not None and date_str:
                obs_rows.append({
                    "indicator_code": "CN_HOG_FUTURES_PRICE",
                    "obs_date": date_str,
                    "value": val,
                    "unit": "cny_per_kg",
                    "source": "akshare:futures_hog_core",
                    "quality_flag": "good",
                })
                fut_count += 1
            rows_read += 1
        notes_parts.append(f"生猪期货价: {fut_count} obs")
    else:
        notes_parts.append("生猪期货价: 数据不可用")

    # 4. Agriculture sector index (农林牧渔 801010) as a broader pig cycle proxy
    agri = _fetch_akshare("index_hist_sw", symbol="801010")
    if agri is not None:
        agri_count = 0
        for _, row in agri.iterrows():
            date_str = str(row["日期"])[:10]
            val = _safe_float(row["收盘"])
            if val is not None:
                obs_rows.append({
                    "indicator_code": "CN_SW_AGRI_INDEX",
                    "obs_date": date_str,
                    "value": val,
                    "unit": "index",
                    "source": "akshare:sw_index:801010",
                    "quality_flag": "good",
                })
                agri_count += 1
            rows_read += 1
        notes_parts.append(f"农林牧渔指数: {agri_count} obs")
    else:
        notes_parts.append("农林牧渔指数: 数据不可用")

    rows_upserted = 0
    status = "success"
    error = None
    if obs_rows:
        rows_upserted = store.upsert_indicator_observations(obs_rows)
    else:
        status = "partial"
        error = "All pig cycle sources returned empty"

    notes = "; ".join(notes_parts)
    return {
        "started_at": started,
        "finished_at": _now_utc(),
        "status": status,
        "rows_read": rows_read,
        "rows_upserted": rows_upserted,
        "error_message": error,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COLLECTORS = [
    ("Sector: Semiconductor Indices", collect_semiconductor_indices),
    ("Sector: Solar/PV Indices", collect_solar_pv_indices),
    ("Sector: Pig Cycle Indices", collect_pig_cycle_indices),
]


def main() -> None:
    started_at = _now_utc()
    print("=" * 60)
    print(f"Radar Sector Thesis Sync – {started_at}")
    print("=" * 60)

    store = RadarStore()
    store.ensure_schema()

    # Ensure catalog entries exist
    _catalog_update(store)

    results: List[Dict[str, Any]] = []
    for name, collector in COLLECTORS:
        print(f"\n{'─' * 40}")
        print(f"Collecting: {name}")
        try:
            result = collector(store)
            results.append(result)
            status = result.get("status", "unknown")
            obs = result.get("rows_upserted", 0)
            notes = result.get("notes", "")
            print(f"  [{status.upper()}] {obs} obs — {notes}")
            store.record_source_run(
                source_name=f"sync_radar_sector:{name}",
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
                source_name=f"sync_radar_sector:{name}",
                target_table="indicator_observations",
                started_at=_now_utc(),
                finished_at=_now_utc(),
                status="failed",
                error_message=str(e),
                notes=traceback.format_exc()[:500],
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
    print("=" * 60)


if __name__ == "__main__":
    main()
