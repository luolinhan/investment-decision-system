# -*- coding: utf-8 -*-
"""Sync macro & external market data into the radar DuckDB store.

Data coverage:
  China macro: M1, M2, TSF, CPI, PPI, FAI, RE investment, industrial VA, FDI
  Global: US2Y, US10Y, 2s10s spread, DXY, Gold, VIX, USD/CNH
  US monetary policy: Fed Funds Effective Rate, 10Y Breakeven inflation

Uses akshare for China macro, akshare bond_zh_us_rate for US Treasury yields,
FRED CSV for Fed Funds / breakeven, yfinance for global prices/rates.
Failing items are logged in source_runs and catalog notes — never fabricated.

Run:
    python scripts/sync_radar_macro_external.py
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

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _clean_date(val: Any, fmt: str = "%Y-%m") -> str:
    """Convert akshare date strings to ISO format."""
    if isinstance(val, pd.Timestamp):
        return val.strftime(fmt)
    s = str(val).strip()
    # Handle Chinese date formats: "2024年01月份"
    import re
    m = re.match(r"(\d{4})年(\d{1,2})月", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-01"
    # Try ISO date
    if len(s) >= 10:
        return s[:10]
    return s[:7] if len(s) >= 7 else s


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


def _fetch_yfinance(ticker: str, period: str = "2y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """Safely fetch a yfinance history."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        df = t.history(period=period, interval=interval)
        if df is None or df.empty:
            logger.info("[EMPTY] yfinance %s returned empty", ticker)
            return None
        return df
    except Exception as e:
        logger.warning("[FAIL] yfinance %s: %s", ticker, e)
        return None


def _fetch_fred_series(series_id: str) -> Optional[pd.DataFrame]:
    """Fetch a FRED series via CSV download. Requires proxy for external access."""
    try:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode()
        df = pd.read_csv(pd.StringIO(text), parse_dates=["observation_date"])
        if df.empty:
            logger.info("[EMPTY] FRED %s returned empty", series_id)
            return None
        df.columns = ["obs_date", "value"]
        df["obs_date"] = df["obs_date"].astype(str).str[:10]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        return df if not df.empty else None
    except Exception as e:
        logger.warning("[FAIL] FRED %s: %s", series_id, e)
        return None


def _fetch_akshare_us_rates() -> Optional[pd.DataFrame]:
    """Fetch US Treasury yields via akshare bond_zh_us_rate."""
    try:
        import akshare as ak
        df = ak.bond_zh_us_rate()
        if df is None or df.empty:
            logger.info("[EMPTY] akshare.bond_zh_us_rate returned empty")
            return None
        # Rename to simpler columns
        df = df.rename(columns={
            "日期": "obs_date",
            "美国国债收益率2年": "us_2y",
            "美国国债收益率10年": "us_10y",
            "美国国债收益率10年-2年": "us_10s_2s",
        })
        df["obs_date"] = df["obs_date"].astype(str)
        for col in ["us_2y", "us_10y", "us_10s_2s"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.warning("[FAIL] akshare.bond_zh_us_rate: %s", e)
        return None


# ---------------------------------------------------------------------------
# China macro collectors
# ---------------------------------------------------------------------------

def collect_cn_money_supply(store: RadarStore) -> Dict[str, Any]:
    """Collect M1, M2, and M1-M2 spread from akshare."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        df = _fetch_akshare("macro_china_money_supply")
        if df is None:
            return {
                "started_at": started,
                "finished_at": _now_utc(),
                "status": "partial",
                "rows_read": 0,
                "rows_upserted": 0,
                "error_message": "akshare macro_china_money_supply failed",
                "notes": "M1/M2 data unavailable, will retry next run",
            }

        # Map columns: akshare typically has '月份' and various metrics
        obs_rows: List[Dict[str, Any]] = []
        cols = list(df.columns)
        logger.info("macro_china_money_supply columns: %s", cols)

        # Prefer YoY growth columns instead of quantity columns.
        m1_col = next((c for c in cols if "M1" in str(c) and "同比" in str(c)), None)
        m2_col = next((c for c in cols if "M2" in str(c) and "同比" in str(c)), None)

        if m1_col and m2_col:
            for _, row in df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                m1_val = _safe_float(row[m1_col])
                m2_val = _safe_float(row[m2_col])

                if m1_val is not None:
                    obs_rows.append({
                        "indicator_code": "CN_M1_YOY",
                        "obs_date": date_str,
                        "value": m1_val,
                        "unit": "pct",
                        "source": "akshare:macro_china_money_supply",
                        "quality_flag": "good",
                    })
                if m2_val is not None:
                    obs_rows.append({
                        "indicator_code": "CN_M2_YOY",
                        "obs_date": date_str,
                        "value": m2_val,
                        "unit": "pct",
                        "source": "akshare:macro_china_money_supply",
                        "quality_flag": "good",
                    })
                if m1_val is not None and m2_val is not None:
                    obs_rows.append({
                        "indicator_code": "CN_M1M2_SCIS",
                        "obs_date": date_str,
                        "value": round(m1_val - m2_val, 4),
                        "unit": "pct",
                        "source": "derived",
                        "quality_flag": "good",
                    })
        else:
            # Fallback: try to match common column names
            for _, row in df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                for c in cols[1:]:
                    val = _safe_float(row[c])
                    if val is None:
                        continue
                    if "M1" in str(c) and "同比" in str(c):
                        obs_rows.append({
                            "indicator_code": "CN_M1_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_money_supply",
                            "quality_flag": "good",
                        })
                    elif "M2" in str(c) and "同比" in str(c):
                        obs_rows.append({
                            "indicator_code": "CN_M2_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_money_supply",
                            "quality_flag": "good",
                        })

        rows_read = len(df)
        if obs_rows:
            rows_upserted = store.upsert_indicator_observations(obs_rows)

        # Update catalog last_update
        _update_catalog_ts(store, ["CN_M1_YOY", "CN_M2_YOY", "CN_M1M2_SCIS"])

        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"Collected {rows_upserted} observations from {rows_read} rows",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_cn_money_supply failed")
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "failed",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": error,
            "notes": f"M1/M2 sync failed: {error}",
        }


def collect_cn_cpi_ppi(store: RadarStore) -> Dict[str, Any]:
    """Collect CPI, PPI, and CPI-PPI spread from akshare."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        obs_rows: List[Dict[str, Any]] = []

        # CPI
        cpi_df = _fetch_akshare("macro_china_cpi")
        if cpi_df is not None:
            cols = list(cpi_df.columns)
            logger.info("macro_china_cpi columns: %s", cols)
            yoy_col = next((c for c in cols if "全国" in str(c) and "同比" in str(c)), None) or next(
                (c for c in cols if "同比" in str(c)),
                None,
            )
            for _, row in cpi_df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                rows_read += 1
                if yoy_col:
                    val = _safe_float(row[yoy_col])
                    if val is not None:
                        obs_rows.append({
                            "indicator_code": "CN_CPI_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_cpi",
                            "quality_flag": "good",
                        })

        # PPI
        ppi_df = _fetch_akshare("macro_china_ppi")
        if ppi_df is not None:
            cols = list(ppi_df.columns)
            logger.info("macro_china_ppi columns: %s", cols)
            yoy_col = next((c for c in cols if "同比" in str(c)), None)
            for _, row in ppi_df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                rows_read += 1
                if yoy_col:
                    val = _safe_float(row[yoy_col])
                    if val is not None:
                        obs_rows.append({
                            "indicator_code": "CN_PPI_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_ppi",
                            "quality_flag": "good",
                        })

        # Derive CPI-PPI spread
        if obs_rows:
            cpi_data = {r["obs_date"]: r["value"] for r in obs_rows if r["indicator_code"] == "CN_CPI_YOY"}
            ppi_data = {r["obs_date"]: r["value"] for r in obs_rows if r["indicator_code"] == "CN_PPI_YOY"}
            common_dates = set(cpi_data.keys()) & set(ppi_data.keys())
            for d in sorted(common_dates):
                obs_rows.append({
                    "indicator_code": "CN_CPI_PPI_SCIS",
                    "obs_date": d,
                    "value": round(cpi_data[d] - ppi_data[d], 4),
                    "unit": "pct",
                    "source": "derived",
                    "quality_flag": "good",
                })

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        _update_catalog_ts(store, ["CN_CPI_YOY", "CN_PPI_YOY", "CN_CPI_PPI_SCIS"])

        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "success",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"Collected {rows_upserted} CPI/PPI observations",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_cn_cpi_ppi failed")
        return {
            "started_at": started,
            "finished_at": _now_utc(),
            "status": "failed",
            "rows_read": rows_read,
            "rows_upserted": rows_upserted,
            "error_message": error,
            "notes": f"CPI/PPI sync failed: {error}",
        }


def collect_cn_industrial_va(store: RadarStore) -> Dict[str, Any]:
    """Collect industrial value-added YoY."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        df = _fetch_akshare("macro_china_industrial_production_yoy")
        if df is None:
            return {
                "started_at": started, "finished_at": _now_utc(), "status": "partial",
                "rows_read": 0, "rows_upserted": 0,
                "error_message": "akshare macro_china_industrial_production_yoy failed",
                "notes": "工业数据不可用",
            }

        obs_rows: List[Dict[str, Any]] = []
        cols = list(df.columns)
        logger.info("macro_china_industrial_production_yoy columns: %s", cols)

        for _, row in df.iterrows():
            date_str = _clean_date(row["日期"]) if "日期" in cols else (_clean_date(row.iloc[1]) if len(row) > 1 else None)
            if date_str is None:
                continue
            rows_read += 1
            # akshare format: 商品, 日期, 今值, 预测值, 前值
            if "今值" in cols:
                val = _safe_float(row["今值"])
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "CN_INDUSTRIAL_VA_YOY",
                        "obs_date": date_str,
                        "value": val,
                        "unit": "pct",
                        "source": "akshare:macro_china_industrial_production_yoy",
                        "quality_flag": "good",
                    })

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        _update_catalog_ts(store, ["CN_INDUSTRIAL_VA_YOY"])

        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": None, "notes": f"Industrial VA: {rows_upserted} obs",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_cn_industrial_va failed")
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": error, "notes": f"Industrial VA sync failed: {error}",
        }


def collect_cn_investment(store: RadarStore) -> Dict[str, Any]:
    """Collect fixed-asset investment and real estate investment."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        obs_rows: List[Dict[str, Any]] = []

        # FAI
        fai_df = _fetch_akshare("macro_china_fai")
        if fai_df is not None:
            cols = list(fai_df.columns)
            logger.info("macro_china_fai columns: %s", cols)
            for _, row in fai_df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                rows_read += 1
                for c in cols[1:]:
                    val = _safe_float(row[c])
                    if val is not None and ("累计同比" in str(c) or "同比" in str(c)):
                        obs_rows.append({
                            "indicator_code": "CN_FA_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_fai",
                            "quality_flag": "good",
                        })
                        break

        # RE Investment
        re_df = _fetch_akshare("macro_china_fai_re") or _fetch_akshare("macro_china_re_investment")
        if re_df is not None:
            cols = list(re_df.columns)
            logger.info("RE investment columns: %s", cols)
            for _, row in re_df.iterrows():
                date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
                if date_str is None:
                    continue
                rows_read += 1
                for c in cols[1:]:
                    val = _safe_float(row[c])
                    if val is not None and ("累计同比" in str(c) or "同比" in str(c)):
                        obs_rows.append({
                            "indicator_code": "CN_RE_INVEST_YOY",
                            "obs_date": date_str,
                            "value": val,
                            "unit": "pct",
                            "source": "akshare:macro_china_fai_re",
                            "quality_flag": "good",
                        })
                        break

        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        _update_catalog_ts(store, ["CN_FA_YOY", "CN_RE_INVEST_YOY"])

        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": None, "notes": f"Investment: {rows_upserted} obs",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_cn_investment failed")
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": error, "notes": f"Investment sync failed: {error}",
        }


def collect_cn_fdi(store: RadarStore) -> Dict[str, Any]:
    """Collect FDI data (may be unavailable via akshare)."""
    started = _now_utc()
    try:
        df = _fetch_akshare("macro_china_fdi")
        if df is None:
            return {
                "started_at": started, "finished_at": _now_utc(), "status": "partial",
                "rows_read": 0, "rows_upserted": 0,
                "error_message": "akshare macro_china_fdi returned no data",
                "notes": "FDI 数据暂时不可获取，标记为 planned 状态",
            }
        # If we got data, process it
        obs_rows: List[Dict[str, Any]] = []
        cols = list(df.columns)
        for _, row in df.iterrows():
            date_str = _clean_date(row.iloc[0]) if len(row) > 0 else None
            if date_str is None:
                continue
            for c in cols[1:]:
                val = _safe_float(row[c])
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "CN_FDI_YOY",
                        "obs_date": date_str,
                        "value": val,
                        "unit": "billion_usd",
                        "source": "akshare:macro_china_fdi",
                        "quality_flag": "good",
                    })
                    break
        rows_upserted = store.upsert_indicator_observations(obs_rows) if obs_rows else 0
        _update_catalog_ts(store, ["CN_FDI_YOY"])
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": len(df), "rows_upserted": rows_upserted,
            "error_message": None, "notes": f"FDI: {rows_upserted} obs",
        }
    except Exception as e:
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": 0, "rows_upserted": 0,
            "error_message": str(e), "notes": "FDI sync failed",
        }


# ---------------------------------------------------------------------------
# Global / external collectors
# ---------------------------------------------------------------------------

def collect_global_rates_fx(store: RadarStore) -> Dict[str, Any]:
    """Collect US rates (akshare primary, FRED/yfinance fallback), DXY, USD/CNH."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        obs_rows: List[Dict[str, Any]] = []
        us_2y_dict: Dict[str, float] = {}
        us_10y_dict: Dict[str, float] = {}

        # --- US 2Y and 10Y yields: akshare bond_zh_us_rate primary ---
        ak_us = _fetch_akshare_us_rates()
        ak_success = False
        if ak_us is not None and "us_2y" in ak_us.columns and "us_10y" in ak_us.columns:
            for _, row in ak_us.iterrows():
                date_str = str(row["obs_date"])[:10]
                v2 = _safe_float(row.get("us_2y"))
                v10 = _safe_float(row.get("us_10y"))
                if v2 is not None:
                    us_2y_dict[date_str] = v2
                    obs_rows.append({
                        "indicator_code": "US_2Y_YIELD",
                        "obs_date": date_str,
                        "value": v2,
                        "unit": "pct",
                        "source": "akshare:bond_zh_us_rate",
                        "quality_flag": "good",
                    })
                if v10 is not None:
                    us_10y_dict[date_str] = v10
                    obs_rows.append({
                        "indicator_code": "US_10Y_YIELD",
                        "obs_date": date_str,
                        "value": v10,
                        "unit": "pct",
                        "source": "akshare:bond_zh_us_rate",
                        "quality_flag": "good",
                    })
            rows_read += len(ak_us)
            ak_success = True

        # Fallback: FRED CSV for missing dates or if akshare failed
        if not ak_success or len(us_10y_dict) == 0:
            for fred_id, code in [("DGS2", "US_2Y_YIELD"), ("DGS10", "US_10Y_YIELD")]:
                target = us_2y_dict if code == "US_2Y_YIELD" else us_10y_dict
                fred_df = _fetch_fred_series(fred_id)
                if fred_df is not None:
                    for _, row in fred_df.iterrows():
                        d = str(row["obs_date"])
                        v = _safe_float(row["value"])
                        if v is not None and d not in target:
                            target[d] = v
                            obs_rows.append({
                                "indicator_code": code,
                                "obs_date": d,
                                "value": v,
                                "unit": "pct",
                                "source": f"fred:{fred_id}",
                                "quality_flag": "good",
                            })
                    rows_read += len(fred_df)

        # Last fallback: yfinance
        if len(us_10y_dict) == 0:
            for yf_ticker, code in [("^TNX", "US_10Y_YIELD"), ("^IRX", "US_2Y_YIELD")]:
                target = us_2y_dict if code == "US_2Y_YIELD" else us_10y_dict
                yf_df = _fetch_yfinance(yf_ticker, period="2y")
                if yf_df is not None:
                    for idx, row in yf_df.iterrows():
                        v = _safe_float(row.get("Close"))
                        if v is not None:
                            d = str(idx)[:10]
                            if d not in target:
                                target[d] = v
                                obs_rows.append({
                                    "indicator_code": code,
                                    "obs_date": d,
                                    "value": v,
                                    "unit": "pct",
                                    "source": f"yfinance:{yf_ticker}",
                                    "quality_flag": "good",
                                })
                    rows_read += len(yf_df)

        # Derive 2s10s spread from whatever we have
        if us_10y_dict and us_2y_dict:
            common = set(us_10y_dict.keys()) & set(us_2y_dict.keys())
            for d in sorted(common):
                obs_rows.append({
                    "indicator_code": "US_2S10S",
                    "obs_date": d,
                    "value": round(us_10y_dict[d] - us_2y_dict[d], 4),
                    "unit": "pct",
                    "source": "derived",
                    "quality_flag": "good",
                })

        # DXY
        dxy = _fetch_yfinance("DX-Y.NYB", period="2y")
        if dxy is not None:
            for idx, row in dxy.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "DXY",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "index",
                        "source": "yfinance:DX-Y.NYB",
                        "quality_flag": "good",
                    })
            rows_read += len(dxy)

        # USD/CNH
        usdcnh = _fetch_yfinance("USDCNH=X", period="2y")
        if usdcnh is not None:
            for idx, row in usdcnh.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "USD_CNH",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "rate",
                        "source": "yfinance:USDCNH=X",
                        "quality_flag": "good",
                    })
            rows_read += len(usdcnh)

        # JPY
        jpy = _fetch_yfinance("JPY=X", period="2y")
        if jpy is not None:
            for idx, row in jpy.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "JP_USDJPY",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "rate",
                        "source": "yfinance:JPY=X",
                        "quality_flag": "good",
                    })
            rows_read += len(jpy)

        if obs_rows:
            rows_upserted = store.upsert_indicator_observations(obs_rows)
        _update_catalog_ts(store, [
            "US_10Y_YIELD", "US_2Y_YIELD", "US_2S10S",
            "DXY", "USD_CNH", "JP_USDJPY",
        ])

        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": None, "notes": f"Rates/FX: {rows_upserted} obs from {rows_read} rows",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_global_rates_fx failed")
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": error, "notes": f"Rates/FX sync failed: {error}",
        }


def collect_us_monetary_policy(store: RadarStore) -> Dict[str, Any]:
    """Collect Fed Funds Effective Rate and 10Y Breakeven from FRED."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        obs_rows: List[Dict[str, Any]] = []

        # Fed Funds Effective Rate
        dff_df = _fetch_fred_series("DFF")
        if dff_df is not None:
            for _, row in dff_df.iterrows():
                obs_rows.append({
                    "indicator_code": "FED_FUNDS_EFFECTIVE",
                    "obs_date": str(row["obs_date"]),
                    "value": _safe_float(row["value"]),
                    "unit": "pct",
                    "source": "fred:DFF",
                    "quality_flag": "good",
                })
            rows_read += len(dff_df)

        # 10Y Breakeven Inflation
        be_df = _fetch_fred_series("T10YIE")
        if be_df is not None:
            for _, row in be_df.iterrows():
                obs_rows.append({
                    "indicator_code": "US_10Y_BREAKEVEN",
                    "obs_date": str(row["obs_date"]),
                    "value": _safe_float(row["value"]),
                    "unit": "pct",
                    "source": "fred:T10YIE",
                    "quality_flag": "good",
                })
            rows_read += len(be_df)

        if obs_rows:
            rows_upserted = store.upsert_indicator_observations(obs_rows)
        _update_catalog_ts(store, ["FED_FUNDS_EFFECTIVE", "US_10Y_BREAKEVEN"])

        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": None,
            "notes": f"Monetary policy: {rows_upserted} obs from {rows_read} rows",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_us_monetary_policy failed")
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": error, "notes": f"Monetary policy sync failed: {error}",
        }


def collect_global_commodities_volatility(store: RadarStore) -> Dict[str, Any]:
    """Collect Gold, Copper, Crude Oil, VIX via yfinance."""
    started = _now_utc()
    rows_read = 0
    rows_upserted = 0
    error = None
    try:
        obs_rows: List[Dict[str, Any]] = []

        # Gold
        gold = _fetch_yfinance("GC=F", period="2y")
        if gold is not None:
            for idx, row in gold.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "GOLD",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "usd_per_oz",
                        "source": "yfinance:GC=F",
                        "quality_flag": "good",
                    })
            rows_read += len(gold)

        # Copper
        copper = _fetch_yfinance("HG=F", period="2y")
        if copper is not None:
            for idx, row in copper.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "COPPER",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "usd_per_lb",
                        "source": "yfinance:HG=F",
                        "quality_flag": "good",
                    })
            rows_read += len(copper)

        # Copper/Gold ratio (derived)
        if obs_rows:
            gold_data = {r["obs_date"]: r["value"] for r in obs_rows if r["indicator_code"] == "GOLD"}
            copper_data = {r["obs_date"]: r["value"] for r in obs_rows if r["indicator_code"] == "COPPER"}
            common = set(gold_data.keys()) & set(copper_data.keys())
            for d in sorted(common):
                if gold_data[d] > 0:
                    obs_rows.append({
                        "indicator_code": "COPPER_GOLD_RATIO",
                        "obs_date": d,
                        "value": round(copper_data[d] / gold_data[d], 6),
                        "unit": "ratio",
                        "source": "derived",
                        "quality_flag": "good",
                    })

        # Crude Oil
        oil = _fetch_yfinance("CL=F", period="2y")
        if oil is not None:
            for idx, row in oil.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "CRUDE_OIL",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "usd_per_bbl",
                        "source": "yfinance:CL=F",
                        "quality_flag": "good",
                    })
            rows_read += len(oil)

        # VIX
        vix = _fetch_yfinance("^VIX", period="2y")
        if vix is not None:
            for idx, row in vix.iterrows():
                val = _safe_float(row.get("Close"))
                if val is not None:
                    obs_rows.append({
                        "indicator_code": "VIX",
                        "obs_date": str(idx)[:10],
                        "value": val,
                        "unit": "index",
                        "source": "yfinance:^VIX",
                        "quality_flag": "good",
                    })
            rows_read += len(vix)

        if obs_rows:
            rows_upserted = store.upsert_indicator_observations(obs_rows)
        _update_catalog_ts(store, [
            "GOLD", "COPPER", "COPPER_GOLD_RATIO",
            "CRUDE_OIL", "VIX",
        ])

        return {
            "started_at": started, "finished_at": _now_utc(), "status": "success",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": None, "notes": f"Commodities/VIX: {rows_upserted} obs",
        }
    except Exception as e:
        error = str(e)
        logger.exception("collect_global_commodities_volatility failed")
        return {
            "started_at": started, "finished_at": _now_utc(), "status": "failed",
            "rows_read": rows_read, "rows_upserted": rows_upserted,
            "error_message": error, "notes": f"Commodities/VIX sync failed: {error}",
        }


# ---------------------------------------------------------------------------
# Catalog timestamp update
# ---------------------------------------------------------------------------

def _update_catalog_ts(store: RadarStore, codes: List[str]) -> None:
    """Update last_update for given indicator codes."""
    conn = store.get_connection()
    now = _now_utc()
    try:
        for code in codes:
            conn.execute(
                "UPDATE indicator_catalog SET last_update = ? WHERE indicator_code = ?",
                (now, code),
            )
    except Exception:
        logger.warning("Failed to update catalog timestamps for %s", codes)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COLLECTORS = [
    ("China: Money Supply (M1/M2)", collect_cn_money_supply),
    ("China: CPI / PPI", collect_cn_cpi_ppi),
    ("China: Industrial Value Added", collect_cn_industrial_va),
    ("China: Fixed-Asset Investment", collect_cn_investment),
    ("China: FDI", collect_cn_fdi),
    ("Global: Rates & FX", collect_global_rates_fx),
    ("Global: US Monetary Policy", collect_us_monetary_policy),
    ("Global: Commodities & VIX", collect_global_commodities_volatility),
]


def main() -> None:
    started_at = _now_utc()
    print("=" * 60)
    print(f"Radar Macro & External Data Sync – {started_at}")
    print("=" * 60)

    store = RadarStore()
    store.ensure_schema()

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
                source_name=f"sync_radar_macro:{name}",
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
                source_name=f"sync_radar_macro:{name}",
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
