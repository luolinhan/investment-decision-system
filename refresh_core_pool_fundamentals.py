"""
Refresh core pool fundamentals and valuation bands.
"""
from __future__ import annotations

import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Disable all proxies to avoid AkShare/EastMoney proxy failures.
for proxy_var in [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
]:
    os.environ.pop(proxy_var, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

requests.Session.trust_env = False
_original_session = requests.Session


class NoProxySession(requests.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trust_env = False
        self.proxies = {}


requests.Session = NoProxySession

import akshare as ak
import pandas as pd

DB_PATH = Path(__file__).resolve().parent / "data" / "investment.db"
MARKET_DIR = Path(__file__).resolve().parent / "data" / "quant_workbench" / "market"
REPORT_COL_RE = re.compile(r"^\d{8}$")


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        if isinstance(value, str) and "%" in value:
            return round(float(value.replace("%", "")) / 100, 4)
        return round(float(value), 4)
    except (ValueError, TypeError):
        return None


def _normalize_code(raw: str, market: Optional[str] = None) -> str:
    raw = str(raw or "").strip()
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


def _finance_code(code: str) -> str:
    normalized = _normalize_code(code)
    if normalized.startswith(("sh", "sz")) and len(normalized) == 8:
        return normalized[2:]
    return normalized


def _valuation_level(pe_ttm: Optional[float], pb: Optional[float]) -> Optional[str]:
    if pe_ttm is None and pb is None:
        return None
    pe = pe_ttm if pe_ttm is not None else 9999
    pbv = pb if pb is not None else 9999
    if pe <= 15 or pbv <= 1.5:
        return "低估"
    if pe >= 40 or pbv >= 5:
        return "高估"
    return "合理"


def load_pool_members() -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT pool_code, pool_name, member_code, member_name, member_market
        FROM stock_pool_constituents
        """
    )
    rows = [dict(row) for row in c.fetchall()]
    conn.close()
    return rows


def _format_report_date(raw: str) -> str:
    value = str(raw or "").strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value[:10]


def _latest_report_column(df: pd.DataFrame) -> Optional[str]:
    cols = [str(col) for col in df.columns if REPORT_COL_RE.match(str(col))]
    if not cols:
        return None
    return sorted(cols, reverse=True)[0]


def _extract_indicator(df: pd.DataFrame, indicator: str, report_col: str) -> Optional[float]:
    if "指标" not in df.columns:
        return None
    subset = df[df["指标"] == indicator]
    if subset.empty:
        return None
    return _safe_float(subset.iloc[0].get(report_col))


def _extract_first_available(df: pd.DataFrame, indicators: List[str], report_col: str) -> Optional[float]:
    for indicator in indicators:
        value = _extract_indicator(df, indicator, report_col)
        if value is not None:
            return value
    return None


def _calc_yoy(df: pd.DataFrame, indicator: str, report_cols: List[str]) -> Optional[float]:
    if len(report_cols) < 2:
        return None
    current = _extract_indicator(df, indicator, report_cols[0])
    previous = _extract_indicator(df, indicator, report_cols[1])
    if current is None or previous in (None, 0):
        return None
    return round((current - previous) / abs(previous), 4)


def _load_last_close_from_parquet(code: str) -> Optional[Tuple[str, float]]:
    path = MARKET_DIR / f"{code}_1d.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    df = df.sort_values("date")
    last = df.iloc[-1]
    trade_date = str(last.get("date") or "")[:10]
    close_price = _safe_float(last.get("close"))
    if not trade_date or close_price is None:
        return None
    return trade_date, float(close_price)


def _load_last_close_from_akshare(code: str, market: str) -> Optional[Tuple[str, float]]:
    try:
        if market == "A":
            symbol = code[2:] if code.startswith(("sh", "sz")) else code
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
            if df is None or df.empty:
                return None
            df = df.rename(columns={"日期": "date", "收盘": "close"})
        elif market == "HK" and hasattr(ak, "stock_hk_hist"):
            symbol = code[2:] if code.startswith("hk") else code
            df = ak.stock_hk_hist(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None
            df = df.rename(columns={"日期": "date", "收盘": "close"})
        else:
            return None
    except Exception:
        return None
    df = df.sort_values("date")
    last = df.iloc[-1]
    trade_date = str(last.get("date") or "")[:10]
    close_price = _safe_float(last.get("close"))
    if not trade_date or close_price is None:
        return None
    return trade_date, float(close_price)


def fetch_last_close(code: str, market: str) -> Tuple[Optional[str], Optional[float]]:
    local = _load_last_close_from_parquet(code)
    if local:
        return local
    remote = _load_last_close_from_akshare(code, market)
    if remote:
        return remote
    return None, None


def _calc_ratio(price: Optional[float], base: Optional[float]) -> Optional[float]:
    if price is None or base in (None, 0):
        return None
    if base <= 0:
        return None
    return round(price / base, 4)


def fetch_a_financial(code: str, retries: int = 2, delay: float = 0.8) -> Optional[Dict[str, Any]]:
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            df = ak.stock_financial_abstract(symbol=code)
            if df is None or df.empty:
                raise ValueError("empty response")
            report_col = _latest_report_column(df)
            if not report_col:
                raise ValueError("missing report column")
            report_cols = sorted(
                [str(col) for col in df.columns if REPORT_COL_RE.match(str(col))],
                reverse=True,
            )
            report_date = _format_report_date(report_col)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(delay * (attempt + 1))
                continue
            print(f"[fundamental] {code} failed: {exc}")
            return None

        roe = _extract_first_available(df, ["净资产收益率(ROE)", "净资产收益率"], report_col)
        roa = _extract_first_available(df, ["总资产报酬率(ROA)", "总资产报酬率", "总资产净利率"], report_col)
        gross_margin = _extract_first_available(df, ["毛利率", "销售毛利率"], report_col)
        net_margin = _extract_first_available(df, ["销售净利率"], report_col)
        debt_ratio = _extract_first_available(df, ["资产负债率"], report_col)
        current_ratio = _extract_first_available(df, ["流动比率"], report_col)
        quick_ratio = _extract_first_available(df, ["速动比率"], report_col)
        eps = _extract_first_available(
            df,
            ["基本每股收益", "每股收益", "稀释每股收益", "摊薄每股收益_最新股数"],
            report_col,
        )
        bvps = _extract_first_available(df, ["每股净资产"], report_col)
        total_revenue = _extract_first_available(df, ["营业总收入"], report_col)
        net_profit = _extract_first_available(df, ["归母净利润", "净利润"], report_col)
        operating_cash_flow = _extract_first_available(df, ["经营现金流量净额"], report_col)

        net_profit_yoy = _calc_yoy(df, "归母净利润", report_cols)
        if net_profit_yoy is None:
            net_profit_yoy = _calc_yoy(df, "净利润", report_cols)
        revenue_yoy = _calc_yoy(df, "营业总收入", report_cols)

        return {
            "code": code,
            "report_date": report_date,
            "roe": roe,
            "roa": roa,
            "gross_margin": gross_margin,
            "net_margin": net_margin,
            "debt_ratio": debt_ratio,
            "current_ratio": current_ratio,
            "quick_ratio": quick_ratio,
            "eps": eps,
            "bvps": bvps,
            "total_revenue": total_revenue,
            "net_profit": net_profit,
            "net_profit_yoy": net_profit_yoy,
            "revenue_yoy": revenue_yoy,
            "operating_cash_flow": operating_cash_flow,
            "dividend_yield": None,
        }
    if last_exc is not None:
        print(f"[fundamental] {code} failed: {last_exc}")
    return None


def main() -> None:
    members = load_pool_members()
    if not members:
        print("no pool members found")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    a_codes: Dict[str, str] = {}
    hk_codes: Dict[str, str] = {}
    for row in members:
        normalized = _normalize_code(row.get("member_code"), row.get("member_market"))
        name = row.get("member_name")
        if normalized.startswith(("sh", "sz")):
            a_codes[normalized] = name
        elif normalized.startswith("hk"):
            hk_codes[normalized] = name

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        SELECT code, name, report_date, eps, bvps
        FROM stock_financial
        WHERE report_date IS NOT NULL AND report_date <> ''
        """
    )
    financial_map = {
        str(row[0]): {
            "code": row[0],
            "name": row[1],
            "report_date": row[2],
            "eps": row[3],
            "bvps": row[4],
        }
        for row in c.fetchall()
    }

    saved_financial = 0
    saved_valuation = 0

    for idx, (normalized, name) in enumerate(a_codes.items(), start=1):
        symbol = normalized[2:]
        fin = financial_map.get(symbol)
        fin_fetched = False
        if not fin:
            fin = fetch_a_financial(symbol, retries=2, delay=0.9)
            fin_fetched = fin is not None
        trade_date, last_close = fetch_last_close(normalized, "A")
        pe = _calc_ratio(last_close, fin.get("eps") if fin else None)
        pb = _calc_ratio(last_close, fin.get("bvps") if fin else None)
        dy = None
        if fin_fetched and fin:
            fin_name = name
            report_date = fin.get("report_date") or trade_date or today
            c.execute(
                """
                INSERT OR REPLACE INTO stock_financial
                (code, name, market, report_date, report_type, roe, roa, gross_margin, net_margin,
                 debt_ratio, current_ratio, quick_ratio, eps, bvps, pe_ttm, pb, total_revenue,
                 net_profit, net_profit_yoy, revenue_yoy, operating_cash_flow, dividend_yield)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    fin_name,
                    "A",
                    report_date,
                    "akshare",
                    fin.get("roe"),
                    fin.get("roa"),
                    fin.get("gross_margin"),
                    fin.get("net_margin"),
                    fin.get("debt_ratio"),
                    fin.get("current_ratio"),
                    fin.get("quick_ratio"),
                    fin.get("eps"),
                    fin.get("bvps"),
                    pe,
                    pb,
                    fin.get("total_revenue"),
                    fin.get("net_profit"),
                    fin.get("net_profit_yoy"),
                    fin.get("revenue_yoy"),
                    fin.get("operating_cash_flow"),
                    dy,
                ),
            )
            saved_financial += 1
        elif fin and (pe is not None or pb is not None):
            c.execute(
                "UPDATE stock_financial SET pe_ttm = ?, pb = ? WHERE code = ?",
                (pe, pb, symbol),
            )

        if pe is not None or pb is not None or dy is not None:
            valuation_date = trade_date or today
            c.execute(
                """
                INSERT OR REPLACE INTO valuation_bands
                (code, name, trade_date, pe_ttm, pb, dividend_yield, valuation_level)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized,
                    name,
                    valuation_date,
                    pe,
                    pb,
                    dy,
                    _valuation_level(pe, pb),
                ),
            )
            saved_valuation += 1
        time.sleep(0.6)
        if idx % 40 == 0:
            time.sleep(2.5)

    conn.commit()
    conn.close()

    print(
        {
            "a_members": len(a_codes),
            "hk_members": len(hk_codes),
            "saved_financial": saved_financial,
            "saved_valuation": saved_valuation,
        }
    )


if __name__ == "__main__":
    main()
