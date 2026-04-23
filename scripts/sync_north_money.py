# -*- coding: utf-8 -*-
"""
同步北向资金数据到数据库 - 多源降级策略

降级策略:
1. akshare stock_hsgt_hist_em (东方财富)
2. akshare stock_hsgt_hold_stock_em (东方财富持股)
3. 腾讯实时北向资金接口
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
    print("[FAIL] akshare not installed, run: pip install akshare")
    sys.exit(1)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")


def ensure_table_exists(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS north_money (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL UNIQUE,
            sh_net_inflow REAL,
            sz_net_inflow REAL,
            total_net_inflow REAL,
            sh_accumulated REAL,
            sz_accumulated REAL,
            total_accumulated REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("[OK] Table north_money ensured")


def fetch_akshare_hsgt_hist():
    """Source 1: akshare stock_hsgt_hist_em - 历史数据"""
    print("[1] Fetching via akshare stock_hsgt_hist_em...")
    try:
        df = ak.stock_hsgt_hist_em()
        if df is not None and not df.empty:
            print(f"    Fetched {len(df)} records, latest: {df.iloc[-1, 0] if len(df) > 0 else 'N/A'}")
            return df
    except Exception as e:
        print(f"    FAIL: {e}")
    return None


def fetch_tencent_north_money():
    """Source 2: 腾讯实时北向资金 - 降级方案"""
    print("[2] Fetching via tencent real-time north money...")
    try:
        import urllib.request
        url = "https://push2.eastmoney.com/api/qt/kamt.rtmin/get"
        params = "?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55,f56&ut=b2884a393a59ad64002292a3e90d46a5&cb=&_=1"
        full_url = url + params

        req = urllib.request.Request(full_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://data.eastmoney.com/'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('utf-8')
            import json
            jdata = json.loads(data)
            records = []
            if jdata.get("data") and jdata["data"].get("s2n"):
                lines = jdata["data"]["s2n"]
                for line in lines:
                    parts = line.split(",")
                    if len(parts) >= 4:
                        records.append({
                            "date": parts[0][:10],
                            "sh_net": float(parts[1]) if parts[1] != "-" else 0,
                            "sz_net": float(parts[2]) if parts[2] != "-" else 0,
                            "total_net": float(parts[3]) if parts[3] != "-" else 0,
                        })
                if records:
                    print(f"    Fetched {len(records)} records, latest: {records[-1]['date']}")
                    import pandas as pd
                    return pd.DataFrame(records)
    except Exception as e:
        print(f"    FAIL: {e}")
    return None


def upsert_hist_data(conn: sqlite3.Connection, df):
    """插入历史数据 (akshare格式)"""
    cursor = conn.cursor()
    added = 0

    for _, row in df.iterrows():
        try:
            trade_date = str(row.iloc[0])[:10]
            total_net = None

            # Try column 5 (net inflow) first
            if len(row) >= 6:
                val = row.iloc[5]
                if val is not None and not (isinstance(val, float) and val != val):
                    try:
                        total_net = float(val)
                    except ValueError:
                        pass

            # Fallback: buy - sell
            if total_net is None and len(row) >= 4:
                try:
                    buy = float(row.iloc[2])
                    sell = float(row.iloc[3])
                    total_net = buy - sell
                except (ValueError, TypeError):
                    pass

            if total_net is None:
                continue

            cursor.execute("""
                INSERT OR REPLACE INTO north_money
                (trade_date, total_net_inflow)
                VALUES (?, ?)
            """, (trade_date, total_net))
            added += 1
        except Exception:
            continue

    conn.commit()
    print(f"[OK] Upserted {added} records")
    return added


def upsert_realtime_data(conn: sqlite3.Connection, df):
    """插入实时数据 (腾讯格式, 包含沪/深拆分)"""
    cursor = conn.cursor()
    added = 0

    for _, row in df.iterrows():
        try:
            trade_date = row["date"]
            cursor.execute("""
                INSERT OR REPLACE INTO north_money
                (trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow)
                VALUES (?, ?, ?, ?)
            """, (trade_date, row["sh_net"], row["sz_net"], row["total_net"]))
            added += 1
        except Exception:
            continue

    conn.commit()
    print(f"[OK] Upserted {added} records from realtime source")
    return added


def show_latest_data(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow
        FROM north_money
        WHERE total_net_inflow != 0
        ORDER BY trade_date DESC LIMIT 10
    """)

    print("\nLatest north money data:")
    for row in cursor.fetchall():
        sh = f"{row[1]:.2f}" if row[1] is not None else "-"
        sz = f"{row[2]:.2f}" if row[2] is not None else "-"
        total = f"{row[3]:.2f}" if row[3] is not None else "-"
        print(f"  {row[0]}: 沪={sh}亿 深={sz}亿 合计={total}亿")


def main():
    print("=" * 60)
    print(f"North Money Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    ensure_table_exists(conn)

    # Try source 1: akshare history
    df = fetch_akshare_hsgt_hist()
    if df is not None and not df.empty:
        print("\n[3] Saving to database...")
        added = upsert_hist_data(conn, df)
    else:
        print("\n[WARN] akshare source failed, trying fallback...")
        # Try source 2: tencent real-time
        df2 = fetch_tencent_north_money()
        if df2 is not None and not df2.empty:
            print("\n[3] Saving to database (fallback)...")
            added = upsert_realtime_data(conn, df2)
        else:
            print("[FAIL] All sources failed")
            conn.close()
            sys.exit(1)

    show_latest_data(conn)
    conn.close()

    print("\n" + "=" * 60)
    print(f"Sync completed at {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
