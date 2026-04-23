# -*- coding: utf-8 -*-
"""
同步融资融券余额数据

数据源: akshare stock_margin_sse (上交所融资融券，包含全市场主要数据)
注意: stock_margin_szse 返回格式与SSE不同且仅1行汇总数据，故仅用SSE
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
        CREATE TABLE IF NOT EXISTS margin_balance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            sh_rzye REAL, sz_rzye REAL, total_rzye REAL,
            sh_rqyl REAL, sz_rqyl REAL, total_rqyl REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def safe_float(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 60)
    print(f"Margin Balance Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    cursor = conn.cursor()

    try:
        df = ak.stock_margin_sse()
        cols = list(df.columns)
        print(f"  SSE: {len(df)} records, columns={cols}")

        # Use iloc with verified SSE column positions:
        # col[0]=日期, col[1]=融资余额, col[3]=融券余量, col[6]=融资融券余额(total)
        date_idx = 0
        rzye_idx = 1  # 融资余额
        rqyl_idx = 3  # 融券余量
        total_idx = 6 if len(cols) > 6 else None  # 融资融券余额

        added = 0
        for _, row in df.iterrows():
            try:
                date = str(row.iloc[date_idx])[:10]
                if not date[:4].isdigit():
                    continue
                rzye = safe_float(row.iloc[rzye_idx])
                rqyl = safe_float(row.iloc[rqyl_idx])
                total = safe_float(row.iloc[total_idx]) if total_idx is not None else None

                cursor.execute("""
                    INSERT OR REPLACE INTO margin_balance
                    (trade_date, sh_rzye, sh_rqyl, total_rzye, total_rqyl)
                    VALUES (?, ?, ?, ?, ?)
                """, (date, rzye, rqyl, total, rqyl))
                added += 1
            except Exception:
                continue

        conn.commit()
        print(f"\n[OK] Upserted {added} records")

        cursor.execute("SELECT trade_date, total_rzye, total_rqyl FROM margin_balance ORDER BY trade_date DESC LIMIT 5")
        for row in cursor.fetchall():
            print(f"  {row[0]}: 融资余额={row[1]} 融券余量={row[2]}")
    except Exception as e:
        print(f"  FAIL: {e}")

    conn.close()


if __name__ == "__main__":
    main()
