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

        try:
            visitor_info = con.execute("PRAGMA table_info('hk_visitor_arrivals')").fetchall()
        except Exception:
            visitor_info = []
        visitor_cols = {row[1] for row in visitor_info}
        if visitor_cols and "date" not in visitor_cols:
            con.execute("ALTER TABLE hk_visitor_arrivals RENAME TO hk_visitor_arrivals_legacy")

        con.execute("""
            CREATE TABLE IF NOT EXISTS hk_visitor_arrivals (
                date DATE PRIMARY KEY,
                arrivals_total INTEGER,
                arrivals_hk_residents INTEGER,
                arrivals_mainland_visitors INTEGER,
                arrivals_other_visitors INTEGER,
                departures_total INTEGER,
                departures_hk_residents INTEGER,
                departures_mainland_visitors INTEGER,
                departures_other_visitors INTEGER,
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
        existing_tables = {
            row[0]
            for row in sqlite_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        # 回填南向资金
        if "south_flow" in existing_tables:
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
        else:
            print("[WARN] south_flow table not found in investment.db, skipping southbound backfill")

        # 回填北向资金
        if "north_money" in existing_tables:
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
        else:
            print("[WARN] north_money table not found in investment.db, skipping northbound backfill")
    finally:
        sqlite_conn.close()

    return added_south, added_north


def fetch_hk_indices() -> Optional[pd.DataFrame]:
    """获取恒指、国指、恒生科技指数历史数据"""
    print("[1] Fetching HK indices (HSI, HSCC, HSTECH)...")

    def _normalize(df: pd.DataFrame, code: str, name: str) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None
        work = df.copy()
        lower_map = {str(col).lower(): col for col in work.columns}
        if "date" not in work.columns:
            for candidate in ("date", "日期", "trade_date"):
                if candidate in work.columns:
                    work["date"] = pd.to_datetime(work[candidate])
                    break
                raw = lower_map.get(candidate.lower())
                if raw:
                    work["date"] = pd.to_datetime(work[raw])
                    break
        rename_map = {}
        for target, options in {
            "open": ["open", "开盘", "open_price"],
            "high": ["high", "最高", "high_price"],
            "low": ["low", "最低", "low_price"],
            "close": ["close", "收盘", "close_price", "最新价"],
            "volume": ["volume", "成交量"],
        }.items():
            for option in options:
                if option in work.columns:
                    rename_map[option] = target
                    break
                raw = lower_map.get(option.lower())
                if raw:
                    rename_map[raw] = target
                    break
        work = work.rename(columns=rename_map)
        work["index_code"] = code
        work["index_name"] = name
        return work

    try:
        all_dfs = []
        fetchers = [
            ("HSI", "恒生指数", [("index_hk_hist", {"symbol": "恒生指数", "period": "daily", "start_date": "20000101", "end_date": "22220101"}), ("stock_hk_index_daily_sina", {"symbol": "HSI"})]),
            ("HSCC", "国企指数", [("index_hk_hist", {"symbol": "国企指数", "period": "daily", "start_date": "20000101", "end_date": "22220101"}), ("stock_hk_index_daily_sina", {"symbol": "HSCEI"})]),
            ("HSTECH", "恒生科技指数", [("index_hk_hist", {"symbol": "恒生科技指数", "period": "daily", "start_date": "20200101", "end_date": "22220101"}), ("stock_hk_index_daily_sina", {"symbol": "HSTECH"})]),
        ]
        for code, name, methods in fetchers:
            data = None
            for func_name, kwargs in methods:
                func = getattr(ak, func_name, None)
                if not func:
                    continue
                try:
                    data = func(**kwargs)
                    if data is not None and not data.empty:
                        break
                except Exception:
                    continue
            data = _normalize(data, code, name)
            if data is not None and not data.empty:
                all_dfs.append(data)
                latest = data.iloc[-1]["date"] if "date" in data.columns else "N/A"
                print(f"    {name}: {len(data)} records, latest {latest}")

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
    """
    获取香港出入境旅客流量数据

    数据源: 入境事务处官方 CSV
    """
    print("[3] Fetching HK visitor arrivals from immd.gov.hk...")

    try:
        csv_url = "https://www.immd.gov.hk/opendata/eng/transport/immigration_clearance/statistics_on_daily_passenger_traffic.csv"
        df = pd.read_csv(csv_url, encoding="utf-8")
        if df.empty:
            print("    [WARN] CSV is empty")
            return None

        df.columns = df.columns.str.strip()
        df["date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
        grouped = df.groupby(["date", "Arrival / Departure"]).agg({
            "Hong Kong Residents": "sum",
            "Mainland Visitors": "sum",
            "Other Visitors": "sum",
            "Total": "sum",
        }).reset_index()
        pivoted = grouped.pivot(
            index="date",
            columns="Arrival / Departure",
            values=["Hong Kong Residents", "Mainland Visitors", "Other Visitors", "Total"],
        ).reset_index()
        pivoted.columns = [
            "date",
            "arrivals_hk_residents", "departures_hk_residents",
            "arrivals_mainland_visitors", "departures_mainland_visitors",
            "arrivals_other_visitors", "departures_other_visitors",
            "arrivals_total", "departures_total",
        ]
        latest_date = pivoted["date"].max().date() if not pivoted.empty else "N/A"
        print(f"    [OK] Loaded {len(pivoted)} daily records, latest: {latest_date}")
        return pivoted
    except Exception as e:
        print(f"    [FAIL] Error fetching visitor arrivals: {e}")
        import traceback
        traceback.print_exc()
        return None


def upsert_visitor_arrivals(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """插入访客流量数据"""
    if df is None or df.empty:
        return 0

    existing = con.execute("""
        SELECT COUNT(*) FROM hk_visitor_arrivals
        WHERE date IN (SELECT date FROM df)
    """).fetchone()[0]

    con.execute("""
        INSERT OR REPLACE INTO hk_visitor_arrivals
        (date, arrivals_total, arrivals_hk_residents, arrivals_mainland_visitors,
         arrivals_other_visitors, departures_total, departures_hk_residents,
         departures_mainland_visitors, departures_other_visitors)
        SELECT date,
               arrivals_total,
               arrivals_hk_residents, arrivals_mainland_visitors, arrivals_other_visitors,
               departures_total,
               departures_hk_residents, departures_mainland_visitors, departures_other_visitors
        FROM df
    """)

    added = len(df) - existing
    if added > 0:
        print(f"[OK] Upserted {added} visitor arrival records")
    else:
        print("[OK] No new visitor arrival records")
    return added


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
        ("hk_visitor_arrivals", "访客流量", "date"),
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

    etl_metrics = {
        "hk_south_flow": 0,
        "hk_north_money": 0,
        "hk_indices": 0,
        "ah_premium": 0,
        "hk_visitor_arrivals": 0,
        "status": "partial",
    }

    ensure_radar_db()
    con = duckdb.connect(RADAR_DB_PATH)

    try:
        # Step 1: 从 investment.db 回填资金流
        added_south, added_north = backfill_from_investment_db(con)
        etl_metrics["hk_south_flow"] = added_south
        etl_metrics["hk_north_money"] = added_north

        # Step 2: 获取恒生指数
        df_indices = fetch_hk_indices()
        if df_indices is not None:
            etl_metrics["hk_indices"] = upsert_indices(con, df_indices)

        # Step 3: 获取A/H溢价
        df_ah = fetch_ah_premium()
        if df_ah is not None:
            etl_metrics["ah_premium"] = upsert_ah_premium(con, df_ah)

        # Step 4: 获取访客流量
        df_visitor = fetch_visitor_arrivals()
        if df_visitor is not None:
            etl_metrics["hk_visitor_arrivals"] = upsert_visitor_arrivals(con, df_visitor)

        # Step 5: 显示摘要
        show_summary(con)
        etl_metrics["status"] = "success"

    except Exception as e:
        print(f"\n[FAIL] {e}")
        import traceback
        traceback.print_exc()
        etl_metrics["status"] = "failed"
        sys.exit(1)
    finally:
        con.close()

    print("\n" + "=" * 60)
    print(f"Sync completed at {datetime.now()}")
    print("=" * 60)
    print(f"ETL_METRICS_JSON={etl_metrics}")


if __name__ == "__main__":
    main()
