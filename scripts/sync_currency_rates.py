# -*- coding: utf-8 -*-
"""
同步汇率数据 (USD/CNY)

数据源: akshare currency_boc_sina
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


def main():
    print("=" * 60)
    print(f"Currency Rates Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    try:
        df = ak.currency_boc_sina(symbol="美元")
        ncols = len(df.columns)
        print(f"  BOC USD/CNY: {len(df)} records, {ncols} columns")

        added = 0
        for _, row in df.iterrows():
            try:
                date = str(row.iloc[0])[:10]
                # Column index 3 or 4 is typically the middle/spot rate for USD
                rate = None
                for ci in range(1, min(ncols, 6)):
                    try:
                        val = float(row.iloc[ci])
                        if 5 < val < 10:
                            rate = val
                            break
                    except (ValueError, TypeError):
                        pass
                if rate:
                    conn.execute(
                        "INSERT OR REPLACE INTO currency_rates (trade_date, usd_cny) VALUES (?, ?)",
                        (date, rate)
                    )
                    added += 1
            except Exception:
                continue
        conn.commit()
        print(f"  [OK] Upserted {added} USD/CNY records")
    except Exception as e:
        print(f"  FAIL: {e}")

    conn.close()


if __name__ == "__main__":
    main()
