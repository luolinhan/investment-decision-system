# -*- coding: utf-8 -*-
"""
同步宏观指标: 中国PMI、铜金比(计算)

数据源:
- 中国PMI: akshare macro_china_pmi
- 铜金比: 从 global_risk 的 gold + LME铜价计算 (如果可获取)
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
        CREATE TABLE IF NOT EXISTS macro_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            indicator_type TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            value REAL,
            unit TEXT,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, indicator_name)
        )
    """)
    conn.commit()


def upsert(conn, date, name, value, unit, source, itype="macro"):
    conn.execute(
        "INSERT OR REPLACE INTO macro_indicators "
        "(trade_date, indicator_type, indicator_name, value, unit, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (date, itype, name, value, unit, source),
    )


def main():
    print("=" * 60)
    print(f"Macro Indicators Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    # 1. China PMI
    try:
        df = ak.macro_china_pmi()
        if df is not None and not df.empty:
            print(f"  PMI: {len(df)} records")
            row = df.iloc[-1]
            # Parse date - handle Chinese format like "2008年01月份"
            raw_date = str(row.iloc[0]).replace("年", "-").replace("月", "-").replace("份", "").strip()
            try:
                dt = datetime.strptime(raw_date, "%Y-%m-")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                date = raw_date[:10]

            value = float(row.iloc[1]) if len(row) > 1 else None
            if value:
                upsert(conn, date, "china_manufacturing_pmi", value, "", "akshare")
                print(f"    {date}: PMI={value}")
                if len(row) > 2:
                    nmi = float(row.iloc[2]) if row.iloc[2] is not None else None
                    if nmi:
                        upsert(conn, date, "china_non_manufacturing_pmi", nmi, "", "akshare")
                        print(f"    {date}: NMI={nmi}")
                conn.commit()
    except Exception as e:
        print(f"  PMI FAIL: {e}")

    # 2. Copper/Gold ratio (from existing data)
    try:
        gold_row = conn.execute(
            "SELECT MAX(trade_date), MAX(close) FROM vix_history WHERE vix_close > 0"
        ).fetchone()

        # Get gold from commodity_prices if exists
        try:
            gold = conn.execute(
                "SELECT trade_date, MAX(close) FROM commodity_prices LIMIT 1"
            ).fetchone()
        except Exception:
            gold = None

        print("  Copper/Gold Ratio: skipped (need external copper source)")
    except Exception as e:
        print(f"  Copper/Gold FAIL: {e}")

    conn.close()
    print("\n[OK] Macro indicators sync done")


if __name__ == "__main__":
    main()
