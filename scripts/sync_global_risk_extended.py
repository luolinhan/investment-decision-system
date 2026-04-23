# -*- coding: utf-8 -*-
"""
同步全球风险扩展数据: DXY、商品历史、美国国债收益率

数据源: yfinance (DXY), akshare (bond_zh_us_rate)
"""
import sqlite3
import sys
import os
from datetime import datetime

os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import akshare as ak
except ImportError:
    print("[FAIL] akshare not installed")
    sys.exit(1)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")


def safe_float(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 60)
    print(f"Global Risk Extended Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # 1. US Treasury + DXY from akshare bond_zh_us_rate
    try:
        df = ak.bond_zh_us_rate()
        if df is not None and not df.empty:
            cols = list(df.columns)
            print(f"  US10Y: {len(df)} records, columns={cols}")
            added = 0
            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[0])[:10]
                    us_10y = safe_float(row.iloc[1]) if len(row) > 1 else None
                    us_10y_2y = safe_float(row.iloc[2]) if len(row) > 2 else None
                    conn.execute("""
                        INSERT OR REPLACE INTO us_treasury_history
                        (trade_date, us_10y, us_2y, us_10y_2y_spread)
                        VALUES (?, ?, ?, ?)
                    """, (date, us_10y, us_10y_2y,
                          (us_10y - us_10y_2y) if us_10y and us_10y_2y else None))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] US Treasury: {added} records")
    except Exception as e:
        print(f"  US Treasury FAIL: {e}")

    # 2. DXY from yfinance
    try:
        import yfinance as yf
        dxy = yf.Ticker("DX-Y.NYB")
        hist = dxy.history(period="1y")
        if hist is not None and not hist.empty:
            print(f"  DXY: {len(hist)} records")
            added = 0
            for date, row in hist.iterrows():
                date_str = str(date)[:10]
                close = safe_float(row.get("Close"))
                if close and close > 0:
                    conn.execute("""
                        INSERT OR REPLACE INTO us_treasury_history
                        (trade_date, dxy)
                        VALUES (?, ?)
                    """, (date_str, close))
                    added += 1
            conn.commit()
            print(f"  [OK] DXY: {added} records")
    except Exception as e:
        print(f"  DXY FAIL: {e}")

    conn.close()
    print("\n[OK] Global Risk Extended sync done")


if __name__ == "__main__":
    main()
