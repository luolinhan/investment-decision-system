# -*- coding: utf-8 -*-
"""
同步宏观指标: 中国PMI、铜金比(计算)

数据源:
- 中国PMI: akshare macro_china_pmi
- 铜金比: 从 commodity_prices 的 gold + LME铜价计算 (如果可获取)
"""
import re
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


def parse_chinese_date(raw):
    """解析 '2008年01月份' → '2008-01-01'"""
    m = re.match(r"(\d{4})年(\d{1,2})月", raw)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return f"{year:04d}-{month:02d}-01"
    # Fallback: try first 10 chars
    return str(raw)[:10]


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
            print(f"  PMI: {len(df)} records, columns={list(df.columns)}")
            row = df.iloc[-1]
            date = parse_chinese_date(str(row.iloc[0]))

            # Column structure: [月份, 制造业-指数, 制造业-同比增减, 非制造业-指数, 非制造业-同比增减]
            # PMI = col[1], NMI = col[3]
            pmi_val = None
            nmi_val = None
            if len(df.columns) > 1:
                col1 = df.columns[1]
                try:
                    pmi_val = float(row[col1]) if row[col1] is not None else None
                except (ValueError, TypeError):
                    pass
            if len(df.columns) > 3:
                col3 = df.columns[3]
                try:
                    nmi_val = float(row[col3]) if row[col3] is not None else None
                except (ValueError, TypeError):
                    pass

            if pmi_val and 20 < pmi_val < 80:  # PMI sanity check
                upsert(conn, date, "china_manufacturing_pmi", pmi_val, "", "akshare")
                print(f"    {date}: PMI={pmi_val}")
            else:
                print(f"    [WARN] PMI value {pmi_val} out of range, skipped")

            if nmi_val and 20 < nmi_val < 80:  # NMI sanity check
                upsert(conn, date, "china_non_manufacturing_pmi", nmi_val, "", "akshare")
                print(f"    {date}: NMI={nmi_val}")
            elif nmi_val:
                print(f"    [WARN] NMI value {nmi_val} out of range, skipped")

            conn.commit()
    except Exception as e:
        print(f"  PMI FAIL: {e}")

    conn.close()
    print("\n[OK] Macro indicators sync done")


if __name__ == "__main__":
    main()
