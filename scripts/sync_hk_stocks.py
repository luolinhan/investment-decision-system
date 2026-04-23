# -*- coding: utf-8 -*-
"""
同步港股数据: 热榜、指数日线、回购

数据源: akshare stock_hk_*_em()
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


def safe_float(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def main():
    print("=" * 60)
    print(f"HK Stocks Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    trade_date = datetime.now().strftime("%Y-%m-%d")

    # 1. HK Hot Rank
    try:
        df = ak.stock_hk_hot_rank_em()
        if df is not None and not df.empty:
            cols = list(df.columns)
            print(f"  Hot Rank: {len(df)} records, columns={cols}")
            added = 0
            for i, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO hk_hot_rank
                        (trade_date, rank, code, name, price, change_pct, volume, turnover)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_date, i+1,
                          str(row.iloc[0]) if len(row) > 0 else None,
                          str(row.iloc[1]) if len(row) > 1 else None,
                          safe_float(row.iloc[2]) if len(row) > 2 else None,
                          safe_float(row.iloc[3]) if len(row) > 3 else None,
                          safe_float(row.iloc[4]) if len(row) > 4 else None,
                          safe_float(row.iloc[5]) if len(row) > 5 else None))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] Hot Rank: {added} records for {trade_date}")
    except Exception as e:
        print(f"  Hot Rank FAIL: {e}")

    # 2. HK Indices
    try:
        df = ak.stock_hk_index_daily_em()
        if df is not None and not df.empty:
            print(f"  HK Indices: {len(df)} records")
            added = 0
            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[0])[:10]
                    cursor.execute("""
                        INSERT OR REPLACE INTO hk_indices
                        (trade_date, code, name, close, change_pct)
                        VALUES (?, ?, ?, ?, ?)
                    """, (date,
                          str(row.iloc[1]) if len(row) > 1 else None,
                          str(row.iloc[2]) if len(row) > 2 else None,
                          safe_float(row.iloc[3]) if len(row) > 3 else None,
                          safe_float(row.iloc[4]) if len(row) > 4 else None))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] HK Indices: {added} records")
    except Exception as e:
        print(f"  HK Indices FAIL: {e}")

    # 3. HK Repurchase
    try:
        df = ak.stock_hk_repurchase_em()
        if df is not None and not df.empty:
            print(f"  HK Repurchase: {len(df)} records")
            added = 0
            for _, row in df.iterrows():
                try:
                    date = str(row.iloc[0])[:10] if len(row) > 0 else trade_date
                    cursor.execute("""
                        INSERT OR REPLACE INTO hk_repurchase
                        (trade_date, code, name, repurchase_amount)
                        VALUES (?, ?, ?, ?)
                    """, (date,
                          str(row.iloc[1]) if len(row) > 1 else None,
                          str(row.iloc[2]) if len(row) > 2 else None,
                          safe_float(row.iloc[3]) if len(row) > 3 else None))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] HK Repurchase: {added} records")
    except Exception as e:
        print(f"  HK Repurchase FAIL: {e}")

    conn.close()
    print("\n[OK] HK Stocks sync done")


if __name__ == "__main__":
    main()
