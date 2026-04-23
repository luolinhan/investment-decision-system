"""
同步全球风险雷达数据到数据库

用途：
1. 预先获取 VIX、Pentagon Pizza、US10Y 等数据
2. 写入 global_risk_snapshot 表
3. 让 API 优先从数据库读取，而不是每次实时请求

运行时机：每日定时任务（建议 09:00 和 15:30）
"""
import sqlite3
import json
import sys
import os
from datetime import datetime

# 确保代理设置
os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.investment_data import InvestmentDataService

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")


def ensure_table_exists(conn: sqlite3.Connection):
    """确保 global_risk_snapshot 表存在"""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS global_risk_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL UNIQUE,
            us10y_value REAL,
            us10y_as_of TEXT,
            vix_value REAL,
            vix_as_of TEXT,
            vix_zone TEXT,
            vix_commentary TEXT,
            pentagon_level INTEGER,
            pentagon_headline TEXT,
            pentagon_status TEXT,
            pentagon_description TEXT,
            gold_value REAL,
            oil_value REAL,
            dxy_value REAL,
            yield_spread_value REAL,
            composite_score REAL,
            composite_level TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("[OK] Table global_risk_snapshot ensured")


def upsert_snapshot(conn: sqlite3.Connection, data: dict):
    """插入或更新风险快照"""
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    us10y = data.get("us10y", {})
    vix = data.get("vix", {})
    pizza = data.get("pentagon_pizza", {})
    gold = data.get("gold", {})
    oil = data.get("oil", {})
    dxy = data.get("dxy", {})
    yield_spread = data.get("yield_spread", {})
    composite = data.get("composite_risk", {})

    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO global_risk_snapshot
        (snapshot_date, us10y_value, us10y_as_of, vix_value, vix_as_of, vix_zone, vix_commentary,
         pentagon_level, pentagon_headline, pentagon_status, pentagon_description,
         gold_value, oil_value, dxy_value, yield_spread_value,
         composite_score, composite_level)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot_date,
        us10y.get("latest"),
        us10y.get("as_of"),
        vix.get("latest"),
        vix.get("as_of"),
        vix.get("zone"),
        vix.get("commentary"),
        pizza.get("level"),
        pizza.get("headline"),
        pizza.get("watch_status"),
        pizza.get("description"),
        gold.get("latest"),
        oil.get("latest"),
        dxy.get("latest"),
        yield_spread.get("latest"),
        composite.get("score"),
        composite.get("level"),
    ))
    conn.commit()
    print(f"[OK] Snapshot saved for {snapshot_date}")
    return snapshot_date


def main():
    print("=" * 60)
    print(f"Global Risk Radar Sync - {datetime.now()}")
    print("=" * 60)

    # 初始化数据库
    conn = sqlite3.connect(DB_PATH)
    ensure_table_exists(conn)

    # 获取数据
    print("\n[1] Fetching risk radar data...")
    service = InvestmentDataService()

    # 获取完整风险雷达数据（180天历史）
    data = service.get_global_risk_radar(days=180)

    if not data:
        print("[FAIL] No data received")
        conn.close()
        sys.exit(1)

    # 显示获取结果摘要
    print(f"\n  US10Y: {data.get('us10y', {}).get('latest', 'N/A')} ({data.get('us10y', {}).get('as_of', 'N/A')})")
    print(f"  VIX: {data.get('vix', {}).get('latest', 'N/A')} ({data.get('vix', {}).get('as_of', 'N/A')})")
    print(f"  Pentagon Pizza: Level {data.get('pentagon_pizza', {}).get('level', 'N/A')} - {data.get('pentagon_pizza', {}).get('headline', 'N/A')}")
    print(f"  Gold: {data.get('gold', {}).get('latest', 'N/A')}")
    print(f"  Composite Risk: {data.get('composite_risk', {}).get('score', 'N/A')} ({data.get('composite_risk', {}).get('level', 'N/A')})")

    # 写入数据库
    print("\n[2] Saving to database...")
    snapshot_date = upsert_snapshot(conn, data)

    # 同步VIX历史（如果成功获取）
    if data.get("vix", {}).get("latest"):
        print("\n[3] Syncing VIX history...")
        try:
            from sync_market_reference_data import upsert_vix
            vix_raw = service._fetch_vix_yahoo()
            if vix_raw and vix_raw.get("value"):
                upsert_vix(conn, vix_raw)
                print("[OK] VIX history updated")
        except Exception as e:
            print(f"[SKIP] VIX history sync failed: {e}")

    conn.close()

    print("\n" + "=" * 60)
    print(f"Sync completed at {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    main()