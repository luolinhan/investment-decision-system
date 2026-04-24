#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
香港流动性雷达数据同步 - 南向/北向、指数、A/H溢价、访客流量

数据覆盖:
- 南向资金历史 (south_flow)
- 北向资金历史 (north_money)
- 恒指/国指/恒生科技指数 (akshare)
- A/H溢价 (akshare + catalog placeholder)
- 访客流量 (政府统计处) - 可选

目标库: data/radar/radar.duckdb
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import akshare as ak
    import duckdb
    import pandas as pd
except ImportError as e:
    print(f"[FAIL] Missing dependency: {e}")
    print("Run: pip install akshare duckdb pandas")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RADAR_DB_PATH = os.path.join(BASE_DIR, "data", "radar", "radar.duckdb")
INVESTMENT_DB_PATH = os.path.join(BASE_DIR, "data", "investment.db")


def ensure_radar_db():
    """确保 radar.duckdb 存在并创建所需表"""
    os.makedirs(os.path.dirname(RADAR_DB_PATH), exist_ok=True)

    con = duckdb.connect(RADAR_DB_PATH)
    try:
        # 南向资金表
        con.execute("""
            CREATE TABLE IF NOT EXISTS hk_south_flow (
                trade_date DATE PRIMARY KEY,
                hk_sz_net DOUBLE,
                hk_hgt_net DOUBLE,
                total_net DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 北向资金表
        con.execute("""
            CREATE TABLE IF NOT EXISTS hk_north_money (
                trade_date DATE PRIMARY KEY,
                sh_net_inflow DOUBLE,
                sz_net_inflow DOUBLE,
                total_net_inflow DOUBLE,
                sh_accumulated DOUBLE,
                sz_accumulated DOUBLE,
                total_accumulated DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 恒生指数表
        con.execute("""
            CREATE TABLE IF NOT EXISTS hk_indices (
                trade_date DATE,
                index_code VARCHAR,
                index_name VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (trade_date, index_code)
            )
        """)

        # A/H溢价表
        con.execute("""
            CREATE TABLE IF NOT EXISTS ah_premium (
                trade_date DATE PRIMARY KEY,
                ah_index DOUBLE,  -- 恒生沪深港通AH股溢价指数
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 访客流量表
        con.execute("""
            CREATE TABLE IF NOT EXISTS hk_visitor_arrivals (
                month DATE PRIMARY KEY,
                arrivals_total INTEGER,
                arrivals_air INTEGER,
                arrivals_land INTEGER,
                arrivals_sea INTEGER,
                departures_total INTEGER,
                departures_air INTEGER,
                departures_land INTEGER,
                departures_sea INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        print("[OK] Radar DB tables ensured")
    finally:
        con.close()


def backfill_from_investment_db(con: duckdb.DuckDBPyConnection) -> Tuple[int, int]:
    """从 investment.db 回填南向/北向资金历史"""
    added_south = 0
    added_north = 0

    if not os.path.exists(INVESTMENT_DB_PATH):
        print("[WARN] investment.db not found, skipping backfill")
        return 0, 0

    sqlite_conn = sqlite3.connect(INVESTMENT_DB_PATH)
    try:
        # 回填南向资金
        cursor = sqlite_conn.execute("""
            SELECT trade_date, hk_sz_net, hk_hgt_net, total_net
            FROM south_flow
            WHERE trade_date IS NOT NULL
        """)

        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["trade_date", "hk_sz_net", "hk_hgt_net", "total_net"])
            df["trade_date"] = pd.to_datetime(df["trade_date"])

            existing = con.execute("""
                SELECT COUNT(*) FROM hk_south_flow
                WHERE trade_date IN (SELECT trade_date FROM df)
            """).fetchone()[0]

            if len(df) > existing:
                con.execute("""
                    INSERT OR REPLACE INTO hk_south_flow
                    (trade_date, hk_sz_net, hk_hgt_net, total_net)
                    SELECT trade_date, hk_sz_net, hk_hgt_net, total_net FROM df
                """)
                added_south = len(df) - existing
                print(f"[OK] Backfilled {added_south} south flow records from investment.db")

        # 回填北向资金
        cursor = sqlite_conn.execute("""
            SELECT trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow,
                   sh_accumulated, sz_accumulated, total_accumulated
            FROM north_money
            WHERE trade_date IS NOT NULL
        """)

        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=[
                "trade_date", "sh_net_inflow", "sz_net_inflow", "total_net_inflow",
                "sh_accumulated", "sz_accumulated", "total_accumulated"
            ])
            df["trade_date"] = pd.to_datetime(df["trade_date"])

            existing = con.execute("""
                SELECT COUNT(*) FROM hk_north_money
                WHERE trade_date IN (SELECT trade_date FROM df)
            """).fetchone()[0]

            if len(df) > existing:
                con.execute("""
                    INSERT OR REPLACE INTO hk_north_money
                    (trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow,
                     sh_accumulated, sz_accumulated, total_accumulated)
                    SELECT trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow,
                           sh_accumulated, sz_accumulated, total_accumulated FROM df
                """)
                added_north = len(df) - existing
                print(f"[OK] Backfilled {added_north} north money records from investment.db")
    finally:
        sqlite_conn.close()

    return added_south, added_north


def fetch_hk_indices() -> Optional[pd.DataFrame]:
    """获取恒指、国指、恒生科技指数历史数据"""
    print("[1] Fetching HK indices (HSI, HSCC, HSTECH)...")

    try:
        # 恒生指数
        df_hsi = ak.index_hk_hist(symbol="恒生指数", period="daily",
                                   start_date="20000101", end_date="22220101")
        # 国企指数
        df_hsc = ak.index_hk_hist(symbol="国企指数", period="daily",
                                   start_date="20000101", end_date="22220101")
        # 恒生科技指数
        df_hst = ak.index_hk_hist(symbol="恒生科技指数", period="daily",
                                   start_date="20200101", end_date="22220101")

        all_dfs = []
        for df, code, name in [
            (df_hsi, "HSI", "恒生指数"),
            (df_hsc, "HSCC", "国企指数"),
            (df_hst, "HSTECH", "恒生科技指数")
        ]:
            if df is not None and not df.empty:
                df = df.copy()
                df["index_code"] = code
                df["index_name"] = name
                all_dfs.append(df)
                print(f"    {name}: {len(df)} records, latest {df.iloc[-1]['date'] if 'date' in df.columns else 'N/A'}")

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            return combined
    except Exception as e:
        print(f"    FAIL: {e}")

    return None


def fetch_ah_premium() -> Optional[pd.DataFrame]:
    """获取A/H溢价指数"""
    print("[2] Fetching A/H premium index...")

    try:
        # 恒生沪深港通AH股溢价指数
        df = ak.index_hk_hist(symbol="AH股溢价指数", period="daily",
                               start_date="20150101", end_date="22220101")
        if df is not None and not df.empty:
            print(f"    AH溢价指数: {len(df)} records, latest {df.iloc[-1]['date'] if 'date' in df.columns else 'N/A'}")
            return df
    except Exception as e:
        print(f"    FAIL: {e}")

    # 降级: 返回 catalog-ready placeholder
    print("    [WARN] Using catalog placeholder for AH premium")
    return pd.DataFrame({
        "date": [datetime.now().strftime("%Y-%m-%d")],
        "value": [None],
        "notes": ["AH premium data source TBD - placeholder for catalog"]
    })


def fetch_visitor_arrivals() -> Optional[pd.DataFrame]:
    """获取香港访客流量数据 (可选)"""
    print("[3] Fetching HK visitor arrivals (optional)...")

    # 暂时不做实时抓取，保留 catalog 结构
    print("    [SKIP] Visitor arrivals - manual collection recommended")
    return None


def upsert_indices(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """插入指数数据"""
    if df is None or df.empty:
        return 0

    # 标准化列名
    df = df.copy()
    if "date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["date"])
    elif "datetime" in df.columns:
        df["trade_date"] = pd.to_datetime(df["datetime"])

    required_cols = ["trade_date", "index_code", "index_name", "open", "high", "low", "close"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    existing = con.execute("""
        SELECT COUNT(*) FROM hk_indices
        WHERE (trade_date, index_code) IN (
            SELECT trade_date, index_code FROM df
        )
    """).fetchone()[0]

    con.execute("""
        INSERT OR REPLACE INTO hk_indices
        (trade_date, index_code, index_name, open, high, low, close, volume)
        SELECT trade_date, index_code, index_name,
               TRY_CAST(open AS DOUBLE), TRY_CAST(high AS DOUBLE),
               TRY_CAST(low AS DOUBLE), TRY_CAST(close AS DOUBLE),
               TRY_CAST(volume AS DOUBLE)
        FROM df
    """)

    added = len(df) - existing
    if added > 0:
        print(f"[OK] Upserted {added} index records")
    return added


def upsert_ah_premium(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """插入A/H溢价数据"""
    if df is None or df.empty:
        return 0

    df = df.copy()
    if "date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["date"])
    else:
        df["trade_date"] = pd.to_datetime(df.get("datetime", datetime.now()))

    if "value" in df.columns:
        df["ah_index"] = df["value"]
    elif "close" in df.columns:
        df["ah_index"] = df["close"]

    existing = con.execute("""
        SELECT COUNT(*) FROM ah_premium
        WHERE trade_date IN (SELECT trade_date FROM df)
    """).fetchone()[0]

    con.execute("""
        INSERT OR REPLACE INTO ah_premium
        (trade_date, ah_index)
        SELECT trade_date, TRY_CAST(ah_index AS DOUBLE) FROM df
    """)

    added = len(df) - existing
    if added > 0:
        print(f"[OK] Upserted {added} AH premium records")
    return added


def show_summary(con: duckdb.DuckDBPyConnection):
    """显示数据摘要"""
    print("\n" + "=" * 60)
    print("Radar HK Data Summary")
    print("=" * 60)

    tables = [
        ("hk_south_flow", "南向资金", "trade_date"),
        ("hk_north_money", "北向资金", "trade_date"),
        ("hk_indices", "恒指/国指/科技", "trade_date"),
        ("ah_premium", "A/H溢价", "trade_date"),
        ("hk_visitor_arrivals", "访客流量", "month"),
    ]

    for table, label, date_col in tables:
        try:
            row = con.execute(f"""
                SELECT COUNT(*), MIN({date_col}), MAX({date_col})
                FROM {table}
            """).fetchone()
            cnt, min_date, max_date = row
            if cnt > 0:
                print(f"  {label:12s}: {cnt:4d} records ({min_date} ~ {max_date})")
            else:
                print(f"  {label:12s}: (empty)")
        except Exception as e:
            print(f"  {label:12s}: ERROR - {e}")


def main():
    print("=" * 60)
    print(f"HK Radar Sync - {datetime.now()}")
    print("=" * 60)

    ensure_radar_db()
    con = duckdb.connect(RADAR_DB_PATH)

    try:
        # Step 1: 从 investment.db 回填资金流
        backfill_from_investment_db(con)

        # Step 2: 获取恒生指数
        df_indices = fetch_hk_indices()
        if df_indices is not None:
            upsert_indices(con, df_indices)

        # Step 3: 获取A/H溢价
        df_ah = fetch_ah_premium()
        if df_ah is not None:
            upsert_ah_premium(con, df_ah)

        # Step 4: 显示摘要
        show_summary(con)

    except Exception as e:
        print(f"\n[FAIL] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()

    print("\n" + "=" * 60)
    print(f"Sync completed at {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
