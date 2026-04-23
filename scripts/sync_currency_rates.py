# -*- coding: utf-8 -*-
"""
同步汇率数据 (USD/CNY)

数据源 (降级策略):
1. akshare currency_boc_safe (中国外汇交易中心官方数据)
2. akshare currency_boc_sina (新浪-中行, 历史数据到2023年底)
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


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS currency_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            usd_cny REAL, usd_cnh REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def extract_rate(row, ncols):
    """从行数据中提取汇率值。BOC牌价以100为单位，需要÷100。"""
    rate = None
    for ci in range(1, min(ncols, 6)):
        try:
            val = float(row.iloc[ci])
            if 500 < val < 800:  # per-100 range: 680-750
                rate = val / 100
                break
            elif 5 < val < 10:  # already per-unit (some sources)
                rate = val
                break
        except (ValueError, TypeError):
            pass
    return rate


def upsert_from_df(conn, df, source_name):
    """从DataFrame提取并入库"""
    if df is None or df.empty:
        return 0
    ncols = len(df.columns)
    added = 0
    for _, row in df.iterrows():
        try:
            date = str(row.iloc[0])[:10]
            rate = extract_rate(row, ncols)
            if rate and date[:4].isdigit():
                conn.execute(
                    "INSERT OR REPLACE INTO currency_rates (trade_date, usd_cny) VALUES (?, ?)",
                    (date, rate)
                )
                added += 1
        except Exception:
            continue
    return added


def main():
    print("=" * 60)
    print(f"Currency Rates Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    # Source 1: PBOC official (currency_boc_safe)
    try:
        df = ak.currency_boc_safe()
        if df is not None and not df.empty:
            latest = str(df.iloc[-1, 0])[:10]
            print(f"  BOC SAFE: {len(df)} records, latest={latest}")
            if latest >= "2024-01-01":
                added = upsert_from_df(conn, df, "boc_safe")
                conn.commit()
                print(f"  [OK] Upserted {added} USD/CNY records from BOC SAFE")
                conn.close()
                return
            else:
                print(f"  [WARN] BOC SAFE data only up to {latest}, trying fallback...")
    except Exception as e:
        print(f"  BOC SAFE FAIL: {e}")

    # Source 2: Sina fallback (historical only, stops ~2023-11)
    try:
        df = ak.currency_boc_sina(symbol="美元")
        if df is not None and not df.empty:
            latest = str(df.iloc[-1, 0])[:10]
            print(f"  BOC SINA: {len(df)} records, latest={latest}")
            added = upsert_from_df(conn, df, "boc_sina")
            conn.commit()
            print(f"  [OK] Upserted {added} USD/CNY records from BOC SINA")
    except Exception as e:
        print(f"  BOC SINA FAIL: {e}")

    conn.close()


if __name__ == "__main__":
    main()
