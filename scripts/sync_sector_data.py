# -*- coding: utf-8 -*-
"""
同步板块数据: 板块列表、资金流、成分股

数据源: akshare stock_board_industry_*_em(), stock_sector_fund_flow_rank()
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
    print(f"Sector Data Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    trade_date = datetime.now().strftime("%Y-%m-%d")

    # 1. Sector Performance (板块列表)
    try:
        df = ak.stock_board_industry_name_em()
        if df is not None and not df.empty:
            cols = list(df.columns)
            print(f"  Sectors: {len(df)} records, columns={cols}")
            added = 0
            for _, row in df.iterrows():
                try:
                    # Typical columns: 板块名称, 涨跌幅, 换手率, 总市值, 领涨股票, ...
                    cursor.execute("""
                        INSERT OR REPLACE INTO sector_performance
                        (trade_date, sector_name, change_pct, turnover, leader_stock)
                        VALUES (?, ?, ?, ?, ?)
                    """, (trade_date,
                          str(row.iloc[0]) if len(row) > 0 else None,
                          safe_float(row.iloc[1]) if len(row) > 1 else None,
                          safe_float(row.iloc[2]) if len(row) > 2 else None,
                          str(row.iloc[4]) if len(row) > 4 else None))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] Sector Performance: {added} records")
    except Exception as e:
        print(f"  Sector Performance FAIL: {e}")

    # 2. Sector Fund Flow
    try:
        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        if df is not None and not df.empty:
            cols = list(df.columns)
            print(f"  Sector Flow: {len(df)} records, columns={cols}")
            added = 0
            for i, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO sector_fund_flow
                        (trade_date, sector_name, net_inflow, net_inflow_pct, rank)
                        VALUES (?, ?, ?, ?, ?)
                    """, (trade_date,
                          str(row.iloc[0]) if len(row) > 0 else None,
                          safe_float(row.iloc[1]) if len(row) > 1 else None,
                          safe_float(row.iloc[2]) if len(row) > 2 else None,
                          i + 1))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] Sector Fund Flow: {added} records")
    except Exception as e:
        print(f"  Sector Fund Flow FAIL: {e}")

    # 3. Sector Stock Map (成分股) - 只对前20个板块拉取成分股，避免API过载
    try:
        df_sectors = ak.stock_board_industry_name_em()
        if df_sectors is not None and not df_sectors.empty:
            top_n = min(20, len(df_sectors))
            print(f"  Fetching constituent stocks for top {top_n} sectors...")
            added = 0
            for i in range(top_n):
                sector_name = str(df_sectors.iloc[i, 0])
                try:
                    df_stocks = ak.stock_board_industry_cons_em(symbol=sector_name)
                    if df_stocks is not None and not df_stocks.empty:
                        for _, row in df_stocks.iterrows():
                            cursor.execute("""
                                INSERT OR REPLACE INTO sector_stock_map
                                (sector_name, code, name, close, change_pct, turnover)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (sector_name,
                                  str(row.iloc[0]) if len(row) > 0 else None,
                                  str(row.iloc[1]) if len(row) > 1 else None,
                                  safe_float(row.iloc[2]) if len(row) > 2 else None,
                                  safe_float(row.iloc[3]) if len(row) > 3 else None,
                                  safe_float(row.iloc[4]) if len(row) > 4 else None))
                            added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] Sector Stock Map: {added} records")
    except Exception as e:
        print(f"  Sector Stock Map FAIL: {e}")

    conn.close()
    print("\n[OK] Sector Data sync done")


if __name__ == "__main__":
    main()
