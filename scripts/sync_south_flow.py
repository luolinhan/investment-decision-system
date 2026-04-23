# -*- coding: utf-8 -*-
"""
同步南向资金 (港股通) 数据

数据源: akshare stock_hsgt_hist_em()
修复: 使用日期范围参数获取最近缺失数据
"""
import sqlite3
import sys
import os
from datetime import datetime, timedelta

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


def get_latest_date(conn):
    """获取数据库中最新的南向资金日期"""
    row = conn.execute("SELECT MAX(trade_date) FROM south_flow").fetchone()
    if row and row[0]:
        return row[0]
    return None


def fetch_data(start_date):
    """获取南向资金数据，支持日期范围"""
    try:
        df = ak.stock_hsgt_hist_em(symbol="港股通", start_date=start_date, end_date=datetime.now().strftime("%Y%m%d"))
        return df
    except Exception:
        return None


def main():
    print("=" * 60)
    print(f"South Flow Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    cursor = conn.cursor()

    latest = get_latest_date(conn)
    if latest:
        # 从最新日期往前2天开始拉取，确保覆盖周末/节假日
        start = (datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y%m%d")
        print(f"  DB latest: {latest}, fetching from {start}")
    else:
        start = "20140101"
        print(f"  No existing data, fetching all history from {start}")

    try:
        df = fetch_data(start)
        if df is not None and not df.empty:
            print(f"  Fetched {len(df)} records (columns: {list(df.columns)})")
            added = 0
            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[0])[:10]
                    if not date[:4].isdigit():
                        continue
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

            cursor.execute("SELECT trade_date, total_net FROM south_flow ORDER BY trade_date DESC LIMIT 5")
            for r in cursor.fetchall():
                print(f"  {r[0]}: {r[1]}")
        else:
            print("[WARN] No data received from API")
    except Exception as e:
        print(f"[FAIL] {e}")
        conn.close()
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
