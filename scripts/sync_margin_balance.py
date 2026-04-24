# -*- coding: utf-8 -*-
"""
同步融资融券余额数据

数据源: akshare stock_margin_sse (上交所融资融券)
注意: stock_margin_szse 返回格式与SSE不同，仅用SSE
"""
import sqlite3
import sys
import os
import json
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


def parse_margin_date(raw):
    """解析多种日期格式: '2023-09-22', '20230922', '2023/09/22'"""
    s = str(raw).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) >= 10 and s[:4].isdigit():
        return s[:10].replace("/", "-")
    return None


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

    if os.getenv("FORCE_MARGIN_SYNC", "").strip() != "1":
        print("  [DEPRECATED] margin_balance source is stale; sync disabled until a reliable source is selected.")
        try:
            cursor.execute("SELECT MAX(trade_date) FROM margin_balance")
            latest = cursor.fetchone()[0]
            print(f"  latest_existing={latest}")
        finally:
            conn.close()
        print("ETL_METRICS_JSON=" + json.dumps({
            "records_processed": 0,
            "records_failed": 0,
            "records_skipped": 1,
            "status": "deprecated",
        }, ensure_ascii=False))
        return

    try:
        df = ak.stock_margin_sse()
        cols = list(df.columns)
        print(f"  SSE: {len(df)} records, columns={cols}")

        date_idx = 0
        rzye_idx = 1
        rqyl_idx = 3
        total_idx = 6 if len(cols) > 6 else None

        added = 0
        for _, row in df.iterrows():
            try:
                date = parse_margin_date(row.iloc[date_idx])
                if date is None:
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
