# -*- coding: utf-8 -*-
"""
同步港股数据: 热榜、指数日线、回购

数据源: akshare stock_hk_spot (Sina), stock_hk_index_spot_sina (Sina)
注意: eastmoney API 在阿里云 Windows 上被网络策略阻断，改用 Sina 数据源
"""
import sqlite3
import sys
import os
from datetime import datetime

# Clear proxy env vars — eastmoney blocked, Sina works without proxy
for k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'all_proxy', 'ALL_PROXY'):
    os.environ.pop(k, None)

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

    # 1. HK Hot Rank — build from stock_hk_spot sorted by turnover (成交额)
    try:
        df = ak.stock_hk_spot()
        if df is not None and not df.empty:
            # Columns: 日期时间, 代码, 中文名称, 英文名称, 交易类型, 最新价, 涨跌额, 涨跌幅, 昨收, 今开, 最高, 最低, 成交量, 成交额, 买一, 卖一
            df = df.sort_values('成交额', ascending=False).reset_index(drop=True)
            print(f"  Hot Rank: {len(df)} stocks sorted by turnover")
            added = 0
            for i, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO hk_hot_rank
                        (trade_date, rank, code, name, price, change_pct, volume, turnover)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (trade_date, i + 1,
                          str(row['代码']),
                          str(row['中文名称']),
                          safe_float(row['最新价']),
                          safe_float(row['涨跌幅']),
                          safe_float(row['成交量']),
                          safe_float(row['成交额'])))
                    added += 1
                except Exception:
                    continue
                if added >= 100:  # Only keep top 100
                    break
            conn.commit()
            print(f"  [OK] Hot Rank: {added} records for {trade_date}")
    except Exception as e:
        print(f"  Hot Rank FAIL: {e}")

    # 2. HK Indices — from stock_hk_index_spot_sina
    try:
        df = ak.stock_hk_index_spot_sina()
        if df is not None and not df.empty:
            # Columns: 代码, 名称, 最新价, 涨跌额, 涨跌幅, 总手, 总金额, 最高价, 最低价
            print(f"  HK Indices: {len(df)} records")
            added = 0
            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO hk_indices
                        (trade_date, code, name, close, change_pct)
                        VALUES (?, ?, ?, ?, ?)
                    """, (trade_date,
                          str(row['代码']),
                          str(row['名称']),
                          safe_float(row['最新价']),
                          safe_float(row['涨跌幅'])))
                    added += 1
                except Exception:
                    continue
            conn.commit()
            print(f"  [OK] HK Indices: {added} records")
    except Exception as e:
        print(f"  HK Indices FAIL: {e}")

    # 3. HK Repurchase — no direct Sina source, skip gracefully
    # stock_hk_repurchase_em() uses eastmoney which is blocked
    print("  HK Repurchase: skipped (Sina source not available)")

    conn.close()
    print("\n[OK] HK Stocks sync done")


if __name__ == "__main__":
    main()
