# -*- coding: utf-8 -*-
"""
同步南向资金 (港股通) 数据 - 修正版

数据源: akshare stock_hsgt_hist_em() 不带 symbol 参数
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
        CREATE TABLE IF NOT EXISTS south_flow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            hk_sz_net REAL, hk_hgt_net REAL, total_net REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def main():
    print("=" * 60)
    print(f"South Flow Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    cursor = conn.cursor()

    try:
        # stock_hsgt_hist_em() without symbol param returns all HSGT data
        df = ak.stock_hsgt_hist_em()
        print(f"  Fetched {len(df)} records (columns: {list(df.columns)})")

        added = 0
        for _, row in df.iterrows():
            try:
                date = str(row.iloc[0])[:10]
                if len(row) >= 6:
                    val = row.iloc[5]
                    if val is not None and not (isinstance(val, float) and val != val):
                        net = float(val)
                        cursor.execute("""
                            INSERT OR REPLACE INTO south_flow
                            (trade_date, total_net)
                            VALUES (?, ?)
                        """, (date, net))
                        added += 1
            except Exception:
                continue
        conn.commit()
        print(f"[OK] Upserted {added} records")
    except Exception as e:
        print(f"[FAIL] {e}")
        conn.close()
        sys.exit(1)

    cursor.execute("SELECT trade_date, total_net FROM south_flow ORDER BY trade_date DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    main()
