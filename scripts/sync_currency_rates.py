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

    # USD/CNY from Bank of China
    try:
        df = ak.currency_boc_sina(symbol="美元")
        print(f"  BOC USD/CNY: {len(df)} records, columns: {list(df.columns)}")
        added = 0
        if len(df.columns) >= 2:
            date_col = df.columns[0]
            rate_col = df.columns[1]
            for _, row in df.iterrows():
                try:
                    date = str(row[date_col])[:10]
                    rate = float(row[rate_col]) if row[rate_col] is not None else None
                    if rate and rate > 5 and rate < 10:
                        conn.execute("""
                            INSERT OR REPLACE INTO currency_rates
                            (trade_date, usd_cny) VALUES (?, ?)
                        """, (date, rate))
                        added += 1
                except Exception:
                    continue
            conn.commit()
        print(f"  [OK] Upserted {added} USD/CNY records")
    except Exception as e:
        print(f"  USD/CNY FAIL: {e}")

    conn.close()


if __name__ == "__main__":
    main()
