"""
Refresh technical_indicators for core pool (A-share primary).
"""
from __future__ import annotations

import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Disable all proxies to avoid AkShare proxy issues.
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


def _fetch_a_hist(code: str) -> pd.DataFrame:
    local = _load_local_hist(code)
    if not local.empty:
        return local
    pure = code[2:] if code.startswith(("sh", "sz")) else code
    try:
        df = ak.stock_zh_a_hist(symbol=pure, period="daily", adjust="qfq")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
        }
    )
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def _fetch_hk_hist(code: str) -> pd.DataFrame:
    local = _load_local_hist(code)
    if not local.empty:
        return local
    symbol = code[2:] if code.startswith("hk") else code
    try:
        df = ak.stock_hk_hist(symbol=symbol, adjust="qfq")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
        }
    )
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def _load_local_hist(code: str) -> pd.DataFrame:
    path = MARKET_DIR / f"{code}_1d.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    if "date" not in df.columns and "trade_date" in df.columns:
        df = df.rename(columns={"trade_date": "date"})
    cols = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    if len(cols) < 6:
        return pd.DataFrame()
    df = df[cols].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def compute_indicators(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or df.empty or len(df) < 60:
        return None
    df = df.sort_values("date")
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - macd_signal

    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_pct = (atr / close) * 100

    last_idx = df.index[-1]
    trend_signal = "neutral"
    if pd.notna(ma20.loc[last_idx]) and pd.notna(ma50.loc[last_idx]) and pd.notna(macd_hist.loc[last_idx]):
        if ma20.loc[last_idx] > ma50.loc[last_idx] and macd_hist.loc[last_idx] > 0:
            trend_signal = "bullish"
        elif ma20.loc[last_idx] < ma50.loc[last_idx] and macd_hist.loc[last_idx] < 0:
            trend_signal = "bearish"

    return {
        "trade_date": df.loc[last_idx, "date"],
        "ma5": float(ma5.loc[last_idx]) if pd.notna(ma5.loc[last_idx]) else None,
        "ma10": float(ma10.loc[last_idx]) if pd.notna(ma10.loc[last_idx]) else None,
        "ma20": float(ma20.loc[last_idx]) if pd.notna(ma20.loc[last_idx]) else None,
        "ma50": float(ma50.loc[last_idx]) if pd.notna(ma50.loc[last_idx]) else None,
        "ma200": float(ma200.loc[last_idx]) if pd.notna(ma200.loc[last_idx]) else None,
        "macd": float(macd.loc[last_idx]) if pd.notna(macd.loc[last_idx]) else None,
        "macd_signal": float(macd_signal.loc[last_idx]) if pd.notna(macd_signal.loc[last_idx]) else None,
        "macd_hist": float(macd_hist.loc[last_idx]) if pd.notna(macd_hist.loc[last_idx]) else None,
        "rsi_14": float(rsi.loc[last_idx]) if pd.notna(rsi.loc[last_idx]) else None,
        "atr_14": float(atr.loc[last_idx]) if pd.notna(atr.loc[last_idx]) else None,
        "atr_pct": float(atr_pct.loc[last_idx]) if pd.notna(atr_pct.loc[last_idx]) else None,
        "trend_signal": trend_signal,
    }


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT member_code, member_name, member_market FROM stock_pool_constituents")
    members = [dict(row) for row in c.fetchall()]

    saved = 0
    for item in members:
        code = _normalize_code(item.get("member_code"), item.get("member_market"))
        name = item.get("member_name")
        if code.startswith(("sh", "sz")):
            df = _fetch_a_hist(code)
        elif code.startswith("hk"):
            df = _fetch_hk_hist(code)
        else:
            continue

        payload = compute_indicators(df)
        if not payload:
            continue

        c.execute(
            """
            INSERT OR REPLACE INTO technical_indicators
            (code, name, trade_date, ma5, ma10, ma20, ma50, ma200, macd, macd_signal,
             macd_hist, rsi_14, atr_14, atr_pct, beta_1y, beta_3y, volatility_30d,
             volatility_90d, trend_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code,
                name,
                payload["trade_date"],
                payload["ma5"],
                payload["ma10"],
                payload["ma20"],
                payload["ma50"],
                payload["ma200"],
                payload["macd"],
                payload["macd_signal"],
                payload["macd_hist"],
                payload["rsi_14"],
                payload["atr_14"],
                payload["atr_pct"],
                None,
                None,
                None,
                None,
                payload["trend_signal"],
            ),
        )
        saved += 1
        time.sleep(0.12)

    conn.commit()
    conn.close()
    print({"saved": saved})


if __name__ == "__main__":
    main()
