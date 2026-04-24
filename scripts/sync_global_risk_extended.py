# -*- coding: utf-8 -*-
"""
同步全球风险扩展数据: DXY、商品历史、美国国债收益率

数据源: yfinance (DXY), akshare (bond_zh_us_rate)
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


def safe_float(val):
    if val is None or (isinstance(val, float) and val != val):
        return None


def table_columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS us_treasury_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            us_2y_yield REAL,
            us_5y_yield REAL,
            us_10y_yield REAL,
            us_30y_yield REAL,
            us_10y_2y_spread REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT,
            dxy REAL
        )
    """)
    columns = table_columns(conn, "us_treasury_history")
    for column, ddl_type in {
        "us_2y_yield": "REAL",
        "us_5y_yield": "REAL",
        "us_10y_yield": "REAL",
        "us_30y_yield": "REAL",
        "us_10y_2y_spread": "REAL",
        "dxy": "REAL",
        "updated_at": "TEXT",
    }.items():
        if column not in columns:
            conn.execute(f"ALTER TABLE us_treasury_history ADD COLUMN {column} {ddl_type}")
    conn.commit()


def update_or_insert(conn, trade_date, updates):
    existing = conn.execute(
        "SELECT rowid FROM us_treasury_history WHERE trade_date = ? ORDER BY rowid LIMIT 1",
        (trade_date,),
    ).fetchone()
    updates = {k: v for k, v in updates.items() if v is not None}
    updates["updated_at"] = datetime.now().isoformat()
    if existing:
        set_clause = ", ".join(f"{key} = ?" for key in updates)
        conn.execute(
            f"UPDATE us_treasury_history SET {set_clause} WHERE rowid = ?",
            [*updates.values(), existing[0]],
        )
    else:
        columns = ["trade_date", *updates.keys()]
        placeholders = ", ".join(["?"] * len(columns))
        conn.execute(
            f"INSERT INTO us_treasury_history ({', '.join(columns)}) VALUES ({placeholders})",
            [trade_date, *updates.values()],
        )


def sync_dxy_yfinance(conn):
    import yfinance as yf
    dxy = yf.Ticker("DX-Y.NYB")
    hist = dxy.history(period="1y")
    if hist is None or hist.empty:
        return 0
    added = 0
    for date, row in hist.iterrows():
        date_str = str(date)[:10]
        close = safe_float(row.get("Close"))
        if close and 70 <= close <= 130:
            update_or_insert(conn, date_str, {"dxy": close})
            added += 1
    return added


def sync_dxy_stooq(conn):
    import pandas as pd
    url = "https://stooq.com/q/d/l/?s=dx.f&i=d"
    df = pd.read_csv(url)
    if df is None or df.empty:
        return 0
    added = 0
    for _, row in df.tail(260).iterrows():
        date_str = str(row.get("Date") or "")[:10]
        close = safe_float(row.get("Close"))
        if date_str and close and 70 <= close <= 130:
            update_or_insert(conn, date_str, {"dxy": close})
            added += 1
    return added
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 60)
    print(f"Global Risk Extended Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    ensure_schema(conn)
    metrics = {"records_processed": 0, "records_failed": 0, "records_skipped": 0}

    # 1. US Treasury + DXY from akshare bond_zh_us_rate
    try:
        df = ak.bond_zh_us_rate()
        if df is not None and not df.empty:
            cols = list(df.columns)
            print(f"  US10Y: {len(df)} records, columns={cols}")
            added = 0
            first_error = None
            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[0])[:10]
                    # Columns: [日期, 中国2Y, 中国5Y, 中国10Y, 中国30Y, 中国10Y-2Y, 中国GDP, 美国2Y, 美国5Y, 美国10Y, 美国30Y, 美国10Y-2Y, 美国GDP]
                    us_2y = safe_float(row.iloc[7]) if len(row) > 7 else None
                    us_10y = safe_float(row.iloc[9]) if len(row) > 9 else None
                    spread = safe_float(row.iloc[11]) if len(row) > 11 else None
                    update_or_insert(conn, date, {
                        "us_10y_yield": us_10y,
                        "us_2y_yield": us_2y,
                        "us_10y_2y_spread": spread,
                    })
                    added += 1
                except Exception as exc:
                    if first_error is None:
                        first_error = str(exc)[:200]
                    metrics["records_skipped"] += 1
                    continue
            conn.commit()
            print(f"  [OK] US Treasury: {added} records")
            if first_error:
                print(f"  [WARN] first skipped US Treasury row: {first_error}")
            metrics["records_processed"] += added
    except Exception as e:
        metrics["records_failed"] += 1
        print(f"  US Treasury FAIL: {e}")

    # 2. DXY from yfinance, fallback to Stooq when Yahoo fails
    added = 0
    try:
        added = sync_dxy_yfinance(conn)
    except Exception as e:
        print(f"  DXY yfinance FAIL: {e}")
    if added == 0:
        try:
            print("  DXY yfinance returned no rows, trying Stooq")
            added = sync_dxy_stooq(conn)
        except Exception as e:
            metrics["records_failed"] += 1
            print(f"  DXY Stooq FAIL: {e}")
    conn.commit()
    print(f"  [OK] DXY: {added} records")
    metrics["records_processed"] += added

    conn.close()
    print("\n[OK] Global Risk Extended sync done")
    print("ETL_METRICS_JSON=" + json.dumps(metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()
