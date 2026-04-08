"""
同步市场参考数据到 SQLite

用途:
1. 把实时服务拿到的当前指数 / VIX / 利率 / 市场情绪落回本地库
2. 给历史接口、数据库回退和定时任务提供可信快照
"""
import sqlite3
from datetime import datetime

from app.services.investment_data import InvestmentDataService

DB_PATH = "data/investment.db"


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}


def upsert_indices(conn: sqlite3.Connection, indices: dict) -> int:
    saved = 0
    cursor = conn.cursor()

    for code, item in (indices or {}).items():
        if item.get("close") is None:
            continue

        trade_date = (item.get("date") or datetime.now().strftime("%Y-%m-%d"))[:10]
        cursor.execute(
            """
            INSERT OR REPLACE INTO index_history
            (code, name, trade_date, close, change_pct)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                code,
                item.get("name") or code,
                trade_date,
                item.get("close"),
                item.get("change_pct"),
            ),
        )
        saved += 1

    return saved


def upsert_vix(conn: sqlite3.Connection, vix: dict) -> bool:
    if not vix or vix.get("value") is None:
        return False

    trade_date = (vix.get("quote_time") or datetime.now().strftime("%Y-%m-%d"))[:10]
    columns = get_table_columns(conn, "vix_history")

    if {"vix_open", "vix_high", "vix_low", "vix_close"}.issubset(columns):
        conn.execute(
            """
            INSERT OR REPLACE INTO vix_history
            (trade_date, vix_open, vix_high, vix_low, vix_close)
            VALUES (?, ?, ?, ?, ?)
            """,
            (trade_date, None, None, None, vix.get("value")),
        )
    elif "vix_change" in columns:
        conn.execute(
            """
            INSERT OR REPLACE INTO vix_history
            (trade_date, vix_close, vix_change)
            VALUES (?, ?, ?)
            """,
            (trade_date, vix.get("value"), vix.get("change_pct")),
        )
    else:
        conn.execute(
            """
            INSERT OR REPLACE INTO vix_history
            (trade_date, vix_close)
            VALUES (?, ?)
            """,
            (trade_date, vix.get("value")),
        )
    return True


def upsert_rates(conn: sqlite3.Connection, rates: dict) -> bool:
    if not rates:
        return False

    shibor = rates.get("shibor") or {}
    hibor = rates.get("hibor") or {}
    if not shibor and not hibor:
        return False

    trade_date = (rates.get("date") or datetime.now().strftime("%Y-%m-%d"))[:10]
    conn.execute(
        """
        INSERT OR REPLACE INTO interest_rates
        (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y,
         hibor_overnight, hibor_1w, hibor_1m, hibor_3m)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_date,
            shibor.get("overnight"),
            shibor.get("1w"),
            shibor.get("1m"),
            shibor.get("3m"),
            shibor.get("6m"),
            shibor.get("1y"),
            hibor.get("overnight"),
            hibor.get("1w"),
            hibor.get("1m"),
            hibor.get("3m"),
        ),
    )
    return True


def upsert_sentiment(conn: sqlite3.Connection, sentiment: dict) -> bool:
    if not sentiment:
        return False

    conn.execute(
        """
        INSERT OR REPLACE INTO market_sentiment
        (trade_date, up_count, down_count, flat_count, limit_up_count, limit_down_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            sentiment.get("date") or datetime.now().strftime("%Y-%m-%d"),
            sentiment.get("up_count"),
            sentiment.get("down_count"),
            sentiment.get("flat_count"),
            sentiment.get("limit_up_count"),
            sentiment.get("limit_down_count"),
        ),
    )
    return True


def main():
    print("=" * 60)
    print(f"同步市场参考数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    service = InvestmentDataService()
    overview = service.get_market_overview(force_refresh=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        saved_indices = upsert_indices(conn, overview.get("indices"))
        saved_vix = upsert_vix(conn, overview.get("fear_greed", {}).get("vix"))
        saved_rates = upsert_rates(conn, overview.get("rates"))
        saved_sentiment = upsert_sentiment(conn, overview.get("sentiment"))
        conn.commit()
    finally:
        conn.close()

    print(f"indices: {saved_indices} 条")
    print(f"vix: {'OK' if saved_vix else 'SKIP'}")
    print(f"rates: {'OK' if saved_rates else 'SKIP'}")
    print(f"sentiment: {'OK' if saved_sentiment else 'SKIP'}")


if __name__ == "__main__":
    main()
