"""
行情抓取
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class YahooChartClient:
    """基于 Yahoo Finance chart 接口的轻量数据源。"""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        self.session.trust_env = True
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_chart(self, symbol: str, interval: str, range_: str) -> pd.DataFrame:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        resp = self.session.get(
            url,
            params={"interval": interval, "range": range_, "includePrePost": "false"},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        result = payload.get("chart", {}).get("result") or []
        if not result:
            return pd.DataFrame()

        data = result[0]
        timestamps = data.get("timestamp") or []
        quote = data.get("indicators", {}).get("quote", [{}])[0]
        if not timestamps:
            return pd.DataFrame()

        df = pd.DataFrame(
            {
                "ts": pd.to_datetime(timestamps, unit="s"),
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
                "volume": quote.get("volume", []),
            }
        )
        df = df.dropna(subset=["close"]).copy()
        if df.empty:
            return df

        if interval == "1d":
            df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None).dt.normalize()
        else:
            df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)

        df["date"] = df["ts"].dt.strftime("%Y-%m-%d")
        df["updated_at"] = datetime.now().isoformat()
        return df.reset_index(drop=True)


def safe_pct_change(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current in (None, 0) or base in (None, 0):
        return None
    return round((current - base) / base * 100, 2)
