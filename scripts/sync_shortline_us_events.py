"""
美股信号采集脚本 — 用 yfinance 拉取近 N 日行情，生成结构化事件。

用法:
    python -m scripts.sync_shortline_us_events
    python -m scripts.sync_shortline_us_events --tickers NVDA MSFT TSLA --days 10
"""
import argparse
import json
from datetime import datetime, timedelta

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None  # graceful fallback for envs without yfinance


# ETF mapping: sector -> ETF ticker
SECTOR_ETFS = {
    "AI": "AIQ",
    "半导体": "SMH",
    "机器人": "ROBO",
    "创新药": "XBI",
    "光伏": "TAN",
    "核电": "URA",
    "养猪": "DBA",
    "红利": "VYM",
}


def fetch_us_data(tickers: list[str], days: int = 5) -> pd.DataFrame:
    """
    Fetch OHLCV data for the given tickers over the last ``days`` trading days.
    Returns a DataFrame with columns: ticker, date, open, high, low, close, volume.
    """
    if yf is None:
        raise ImportError("yfinance is not installed. pip install yfinance")

    end = datetime.now()
    start = end - timedelta(days=days + 10)  # extra buffer for weekends

    frames = []
    for ticker in tickers:
        try:
            hist = yf.Ticker(ticker).history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
            if hist.empty:
                continue
            hist = hist.tail(days)
            hist = hist.reset_index()
            hist["ticker"] = ticker
            hist["date"] = hist["Date"].dt.strftime("%Y-%m-%d")
            hist = hist.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
            frames.append(hist[["ticker", "date", "open", "high", "low", "close", "volume"]])
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close", "volume"])

    return pd.concat(frames, ignore_index=True)


def detect_signals(df: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """
    Given OHLCV data, detect events:
      - price_breakout: close > max(high) of prior days
      - etf_breakout: sector ETF breakout (simulated)
      - sector_rotation: volume spike + price change
      - earnings_spillover: gap-up/gap-down after hours (simulated)

    Returns a list of event dicts.
    """
    events: list[dict] = []
    today = datetime.now().strftime("%Y-%m-%d")

    for ticker in tickers:
        tdf = df[df["ticker"] == ticker].sort_values("date")
        if len(tdf) < 2:
            continue

        closes = tdf["close"].values
        highs = tdf["high"].values
        lows = tdf["low"].values
        volumes = tdf["volume"].values
        last_close = closes[-1]
        prev_close = closes[-2]

        # --- price_breakout ---
        if len(highs) >= 3 and last_close > max(highs[:-1]):
            severity = min(1.0, round((last_close / max(highs[:-1]) - 1) * 10 + 0.5, 4))
            events.append({
                "us_ticker": ticker,
                "event_type": "price_breakout",
                "event_date": today,
                "severity": severity,
                "detail_json": json.dumps({
                    "close": float(last_close),
                    "prev_high": float(max(highs[:-1])),
                    "pct_breakout": round((last_close / max(highs[:-1]) - 1) * 100, 2),
                }),
            })

        # --- sector_rotation (volume spike + price move) ---
        if len(volumes) >= 3:
            avg_vol = sum(volumes[:-1]) / (len(volumes) - 1)
            if avg_vol > 0 and volumes[-1] > avg_vol * 1.5:
                pct_change = abs((last_close - prev_close) / prev_close) if prev_close > 0 else 0
                if pct_change > 0.01:
                    severity = min(1.0, round(pct_change * 5 + 0.3, 4))
                    events.append({
                        "us_ticker": ticker,
                        "event_type": "sector_rotation",
                        "event_date": today,
                        "severity": severity,
                        "detail_json": json.dumps({
                            "volume_ratio": round(volumes[-1] / avg_vol, 2),
                            "pct_change": round(pct_change * 100, 2),
                        }),
                    })

        # --- earnings_spillover (gap detection) ---
        gap = (last_close - prev_close) / prev_close if prev_close > 0 else 0
        if abs(gap) > 0.02:
            severity = min(1.0, round(abs(gap) * 5 + 0.3, 4))
            events.append({
                "us_ticker": ticker,
                "event_type": "earnings_spillover",
                "event_date": today,
                "severity": severity,
                "detail_json": json.dumps({
                    "gap_pct": round(gap * 100, 2),
                    "direction": "up" if gap > 0 else "down",
                }),
            })

    # --- etf_breakout: check sector ETFs for tickers that have events ---
    tickers_with_events = {e["us_ticker"] for e in events}
    if tickers_with_events:
        for sector, etf_ticker in SECTOR_ETFS.items():
            try:
                etf_df = df[df["ticker"] == etf_ticker]
                if etf_df.empty and yf is not None:
                    end = datetime.now()
                    start = end - timedelta(days=15)
                    etf_hist = yf.Ticker(etf_ticker).history(
                        start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d")
                    )
                    if not etf_hist.empty:
                        etf_hist = etf_hist.tail(5).reset_index()
                        etf_hist["ticker"] = etf_ticker
                        etf_hist["date"] = etf_hist["Date"].dt.strftime("%Y-%m-%d")
                        etf_hist = etf_hist.rename(columns={
                            "Open": "open", "High": "high", "Low": "low",
                            "Close": "close", "Volume": "volume",
                        })
                        etf_df = etf_hist[["ticker", "date", "open", "high", "low", "close", "volume"]]

                if len(etf_df) >= 3:
                    etf_closes = etf_df["close"].values
                    etf_highs = etf_df["high"].values
                    if etf_closes[-1] > max(etf_highs[:-1]):
                        severity = min(1.0, round((etf_closes[-1] / max(etf_highs[:-1]) - 1) * 10 + 0.5, 4))
                        events.append({
                            "us_ticker": etf_ticker,
                            "event_type": "etf_breakout",
                            "event_date": today,
                            "severity": severity,
                            "detail_json": json.dumps({
                                "etf": etf_ticker,
                                "sector": sector,
                                "close": float(etf_closes[-1]),
                                "pct_breakout": round((etf_closes[-1] / max(etf_highs[:-1]) - 1) * 100, 2),
                            }),
                        })
            except Exception:
                continue

    return events


def main():
    parser = argparse.ArgumentParser(description="Sync US market events via yfinance")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to scan")
    parser.add_argument("--days", type=int, default=5, help="Lookback days")
    args = parser.parse_args()

    from app.services.shortline_service import ShortlineService

    svc = ShortlineService()
    result = svc.sync_us_market_events(tickers=args.tickers, days=args.days)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
