# -*- coding: utf-8 -*-
"""
同步融资融券余额数据

数据源: akshare stock_margin_sse + stock_margin_szse
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


def main():
    print("=" * 60)
    print(f"Margin Balance Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    cursor = conn.cursor()

    added = 0
    for exchange, col_map in [
        ("sse", {"date": 0, "rzye": 1, "rqyl": 4}),
        ("szse", {"date": 0, "rzye": 1, "rqyl": 4}),
    ]:
        try:
            fn = ak.stock_margin_sse if exchange == "sse" else ak.stock_margin_szse
            df = fn()
            key = "sh" if exchange == "sse" else "sz"
            print(f"  {exchange}: {len(df)} records fetched")

            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[col_map["date"]])[:10]
                    rzye = float(row.iloc[col_map["rzye"]]) if row.iloc[col_map["rzye"]] is not None else None
                    rqyl = float(row.iloc[col_map["rqyl"]]) if row.iloc[col_map["rqyl"]] is not None else None

                    cursor.execute(f"""
                        INSERT OR REPLACE INTO margin_balance
                        (trade_date, {key}_rzye, {key}_rqyl)
                        VALUES (?, ?, ?)
                    """, (date, rzye, rqyl))
                    added += 1
                except Exception:
                    continue
        except Exception as e:
            print(f"  {exchange}: FAIL - {e}")

    # Update totals
    cursor.execute("""
        UPDATE margin_balance SET
            total_rzye = COALESCE(sh_rzye,0) + COALESCE(sz_rzye,0),
            total_rqyl = COALESCE(sh_rqyl,0) + COALESCE(sz_rqyl,0)
        WHERE total_rzye IS NULL
    """)

    conn.commit()
    print(f"\n[OK] Upserted {added} records")

    cursor.execute("SELECT trade_date, total_rzye, total_rqyl FROM margin_balance ORDER BY trade_date DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f"  {row[0]}: 融资余额={row[1]} 融券余量={row[2]}")

    conn.close()


if __name__ == "__main__":
    main()
