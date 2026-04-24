#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
五角大楼披萨指数历史数据同步

数据源: https://pizzint.watch
指标含义:
- Level 1 (过冷): 披萨订单非常少，国防承包商活动低迷
- Level 2 (偏冷): 订单较少
- Level 3 (中性): 正常水平
- Level 4 (偏热): 订单增加，活动上升
- Level 5 (过热): 订单激增，重大国防活动信号

目标库: data/radar/radar.duckdb
"""

import sys
import os
from datetime import datetime
from typing import Optional, List, Tuple
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import requests
    import duckdb
    import pandas as pd
except ImportError as e:
    print(f"[FAIL] Missing dependency: {e}")
    print("Run: pip install requests duckdb pandas")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RADAR_DB_PATH = os.path.join(BASE_DIR, "data", "radar", "radar.duckdb")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REQUEST_TIMEOUT = 30


def ensure_radar_db():
    """确保 radar.duckdb 存在并创建 pentagon_pizza 表"""
    os.makedirs(os.path.dirname(RADAR_DB_PATH), exist_ok=True)

    con = duckdb.connect(RADAR_DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS pentagon_pizza_history (
                date DATE PRIMARY KEY,
                level INTEGER CHECK (level BETWEEN 1 AND 5),
                headline VARCHAR,
                status VARCHAR,
                description VARCHAR,
                temperature_band VARCHAR CHECK (
                    temperature_band IN ('过冷', '偏冷', '中性', '偏热', '过热')
                ),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("[OK] Pentagon pizza table ensured")
    finally:
        con.close()


def scrape_pizzint_watch() -> Optional[pd.DataFrame]:
    """
    抓取 pizzint.watch 当前状态作为每日快照
    """
    print("[1] Scraping pizzint.watch current status...")

    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

        resp = session.get("https://pizzint.watch", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html = resp.text

        status_match = re.search(r"STATUS:\s*([A-Z]+)", html)
        status = status_match.group(1).strip() if status_match else "UNKNOWN"
        doughcon_match = re.search(r"DOUGHCON\s+(\d)", html)
        doughcon_level = int(doughcon_match.group(1)) if doughcon_match else None
        has_alert = bool(re.search(r"INCREASED\s+INTELLIGENCE\s+WATCH", html))

        if doughcon_level is not None:
            level = max(1, min(5, 6 - doughcon_level))
        elif status == "OPERATIONAL" and has_alert:
            level = 4
        elif status == "OPERATIONAL":
            level = 3
        else:
            level = 2

        records = [{
            "date": datetime.now().strftime("%Y-%m-%d"),
            "level": level,
            "headline": f"DOUGHCON {doughcon_level if doughcon_level else 'N/A'} - {status}",
            "status": status.lower(),
            "description": f"Current operational status. Alert: {has_alert}",
            "temperature_band": level_to_band(level),
        }]
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        print(f"    [OK] Captured today's snapshot: Level {level} ({level_to_band(level)}), Status: {status}")
        return df

    except Exception as e:
        print(f"    [FAIL] Error scraping pizzint.watch: {e}")
        import traceback
        traceback.print_exc()
        return None


def level_to_band(level: int) -> str:
    """将 level (1-5) 转换为温度带"""
    bands = {
        1: "过冷",
        2: "偏冷",
        3: "中性",
        4: "偏热",
        5: "过热"
    }
    return bands.get(level, "中性")


def generate_sample_data() -> pd.DataFrame:
    """生成示例数据用于 catalog 测试"""
    sample_dates = pd.date_range(end=datetime.now(), periods=30, freq="D")
    records = []
    for i, date in enumerate(sample_dates):
        level = 3 + ((i * 7) % 5) - 2  # 伪随机 1-5
        level = max(1, min(5, level))
        records.append({
            "date": date.strftime("%Y-%m-%d"),
            "level": level,
            "headline": f"Sample Level {level}",
            "status": "sample",
            "description": "Sample data for catalog testing",
            "temperature_band": level_to_band(level),
        })
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    return df


def upsert_history(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> Tuple[int, int]:
    """插入历史数据（每日快照），返回 (新增, 更新)"""
    if df is None or df.empty:
        return 0, 0

    # 检查现有记录
    existing_dates = con.execute("""
        SELECT date FROM pentagon_pizza_history
        WHERE date IN (SELECT DISTINCT date FROM df)
    """).fetchdf()["date"].tolist()

    existing_set = set(existing_dates)
    df_new = df[~df["date"].isin(existing_set)]

    added = 0
    updated = 0
    if len(df_new) > 0:
        con.execute("""
            INSERT INTO pentagon_pizza_history
            (date, level, headline, status, description, temperature_band)
            SELECT date, level, headline, status, description, temperature_band
            FROM df_new
        """)
        added = len(df_new)
        print(f"[OK] Inserted {added} new snapshot records")
    else:
        print("[OK] No new records (today's snapshot may already exist)")

    return added, updated


def show_latest(con: duckdb.DuckDBPyConnection, limit: int = 10):
    """显示最新数据"""
    print("\n" + "=" * 60)
    print(f"Latest Pentagon Pizza Data (last {limit} days)")
    print("=" * 60)

    rows = con.execute(f"""
        SELECT date, level, temperature_band, headline
        FROM pentagon_pizza_history
        ORDER BY date DESC
        LIMIT {limit}
    """).fetchall()

    for row in rows:
        date_str = row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0])
        print(f"  {date_str} | Level {row[1]} ({row[2]}) | {row[3]}")


def show_stats(con: duckdb.DuckDBPyConnection):
    """显示统计信息"""
    print("\n" + "=" * 60)
    print("Pentagon Pizza Statistics")
    print("=" * 60)

    total = con.execute("SELECT COUNT(*) FROM pentagon_pizza_history").fetchone()[0]
    date_range = con.execute("""
        SELECT MIN(date), MAX(date) FROM pentagon_pizza_history
    """).fetchone()
    min_date, max_date = date_range

    print(f"  Total records: {total}")
    print(f"  Date range: {min_date} ~ {max_date}")

    # 按温度带统计
    print("\n  Distribution by temperature band:")
    dist = con.execute("""
        SELECT temperature_band, COUNT(*) as cnt
        FROM pentagon_pizza_history
        GROUP BY temperature_band
        ORDER BY
            CASE temperature_band
                WHEN '过冷' THEN 1
                WHEN '偏冷' THEN 2
                WHEN '中性' THEN 3
                WHEN '偏热' THEN 4
                WHEN '过热' THEN 5
            END
    """).fetchall()

    for band, cnt in dist:
        print(f"    {band:6s}: {cnt:4d} days ({cnt/total*100:.1f}%)")


def main():
    print("=" * 60)
    print(f"Pentagon Pizza History Sync - {datetime.now()}")
    print("=" * 60)

    ensure_radar_db()
    con = duckdb.connect(RADAR_DB_PATH)

    try:
        df = scrape_pizzint_watch()
        if df is not None:
            added, updated = upsert_history(con, df)
            show_latest(con)
            show_stats(con)
        else:
            print("[FAIL] No data retrieved")
            sys.exit(1)

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
