# -*- coding: utf-8 -*-
"""
同步宏观指标: LME铜价、铜金比、中国PMI

数据源:
- LME铜: akshare futures_foreign_hist
- 中国PMI: akshare macro_china_pmi
- 铜金比: LME铜价 / 国际金价 (计算指标)
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


def upsert_indicator(conn, date, name, value, unit, source, itype="commodity"):
    conn.execute("""
        INSERT OR REPLACE INTO macro_indicators
        (trade_date, indicator_type, indicator_name, value, unit, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (date, itype, name, value, unit, source))


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
            date = str(row.iloc[0])[:10] if len(row) > 0 else "unknown"
            value = float(row.iloc[1]) if len(row) > 1 else None
            if value:
                upsert_indicator(conn, date, "china_manufacturing_pmi", value, "", "akshare", "macro")
                if len(row) > 2:
                    nmi = float(row.iloc[2]) if row.iloc[2] is not None else None
                    if nmi:
                        upsert_indicator(conn, date, "china_non_manufacturing_pmi", nmi, "", "akshare", "macro")
                print(f"    {date}: PMI={value}")
                conn.commit()
    except Exception as e:
        print(f"  PMI FAIL: {e}")

    # 2. LME Copper via futures
    try:
        df = ak.futures_foreign_hist(symbol="铜")
        if df is not None and not df.empty:
            print(f"  LME Copper: {len(df)} records")
            row = df.iloc[-1]
            date = str(row.iloc[0])[:10] if len(row) > 0 else "unknown"
            close = float(row.iloc[3]) if len(row) > 3 else None
            if close:
                upsert_indicator(conn, date, "lme_copper_usd", close, "USD/ton", "akshare", "commodity")
                print(f"    {date}: LME Cu={close}")
                conn.commit()
    except Exception as e:
        print(f"  LME Copper FAIL: {e}")

    # 3. Compute Copper/Gold Ratio
    try:
        copper_row = conn.execute(
            "SELECT value FROM macro_indicators WHERE indicator_name='lme_copper_usd' ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        gold_row = conn.execute(
            "SELECT latest FROM commodity_prices WHERE commodity_type='gold' ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        if copper_row and gold_row:
            copper_usd = copper_row[0]
            gold_cny_g = gold_row[0]
            # Approximate copper/gold ratio: (copper USD/ton) / (gold CNY/g * 31.1035 g/oz / 7 * USD/CNY)
            # Simplified: use copper price in USD per ton
            ratio = copper_usd / gold_cny_g if gold_cny_g else None
            if ratio:
                today = datetime.now().strftime("%Y-%m-%d")
                upsert_indicator(conn, today, "copper_gold_ratio", ratio, "", "computed", "commodity")
                print(f"  Copper/Gold Ratio: {ratio:.4f}")
                conn.commit()
    except Exception as e:
        print(f"  Copper/Gold Ratio FAIL: {e}")

    conn.close()
    print("\n[OK] Macro indicators sync done")


if __name__ == "__main__":
    main()
