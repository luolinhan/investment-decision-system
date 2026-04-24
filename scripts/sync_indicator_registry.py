# -*- coding: utf-8 -*-
"""Seed indicator registry for data governance.

The registry is the source of truth for whether a dataset is allowed into the
main UI/scoring path. It does not delete data; deprecated datasets remain in the
database for audit/backfill until a cleanup migration is explicitly approved.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")


INDICATORS = [
    {
        "indicator_key": "global_risk.dxy",
        "layer": "external_risk",
        "table_name": "us_treasury_history",
        "display_name": "美元指数 DXY",
        "status": "active",
        "source": "sqlite fallback from yfinance/stooq",
        "freshness_sla_days": 5,
        "decision_usage": "external_risk_score",
        "reason": "DXY can be read from local snapshot even when live source fails.",
    },
    {
        "indicator_key": "global_risk.oil",
        "layer": "external_risk",
        "table_name": "commodity_prices",
        "display_name": "油价",
        "status": "experimental",
        "source": "commodity_prices",
        "freshness_sla_days": 21,
        "decision_usage": "excluded_when_stale",
        "reason": "Historical oil field contains invalid values; current API excludes stale/out-of-range rows.",
    },
    {
        "indicator_key": "global_risk.vix",
        "layer": "external_risk",
        "table_name": "vix_history",
        "display_name": "VIX",
        "status": "active",
        "source": "yfinance/sqlite",
        "freshness_sla_days": 3,
        "decision_usage": "external_risk_score",
        "reason": "Stable recent data and used by global risk scoring.",
    },
    {
        "indicator_key": "macro.pmi",
        "layer": "macro_regime",
        "table_name": "macro_indicators",
        "display_name": "中国 PMI/NMI",
        "status": "active",
        "source": "akshare macro_china_pmi",
        "freshness_sla_days": 45,
        "decision_usage": "macro_regime",
        "reason": "Latest row is selected by parsed date, avoiding 2008 stale row bug.",
    },
    {
        "indicator_key": "flow.northbound",
        "layer": "risk_preference",
        "table_name": "north_money",
        "display_name": "北向资金",
        "status": "active",
        "source": "akshare/tencent fallback",
        "freshness_sla_days": 5,
        "decision_usage": "risk_preference",
        "reason": "Currently used in decision-center and macro overview.",
    },
    {
        "indicator_key": "flow.southbound",
        "layer": "hk_liquidity",
        "table_name": "south_flow",
        "display_name": "南向资金",
        "status": "active",
        "source": "akshare stock_hsgt_hist_em",
        "freshness_sla_days": 5,
        "decision_usage": "hk_liquidity_score",
        "reason": "Core HK liquidity input.",
    },
    {
        "indicator_key": "flow.margin_balance",
        "layer": "risk_preference",
        "table_name": "margin_balance",
        "display_name": "融资融券余额",
        "status": "deprecated",
        "source": "akshare stock_margin_sse",
        "freshness_sla_days": 5,
        "decision_usage": "none",
        "reason": "Source returns stale 2023 data; sync is disabled until a reliable source is selected.",
    },
    {
        "indicator_key": "hk.repurchase",
        "layer": "hk_liquidity",
        "table_name": "hk_repurchase",
        "display_name": "港股回购",
        "status": "deprecated",
        "source": "unreliable/empty",
        "freshness_sla_days": 5,
        "decision_usage": "none",
        "reason": "Table is empty and source was blocked/unavailable.",
    },
    {
        "indicator_key": "sector.market_performance",
        "layer": "sector_validation",
        "table_name": "sector_performance",
        "display_name": "行业板块表现",
        "status": "deprecated",
        "source": "schema mismatch",
        "freshness_sla_days": 3,
        "decision_usage": "none",
        "reason": "Table is empty due to service/init schema mismatch.",
    },
    {
        "indicator_key": "sector.market_flow",
        "layer": "sector_validation",
        "table_name": "sector_fund_flow",
        "display_name": "行业资金流",
        "status": "deprecated",
        "source": "schema mismatch",
        "freshness_sla_days": 3,
        "decision_usage": "none",
        "reason": "Table is empty due to service/init schema mismatch.",
    },
    {
        "indicator_key": "news.bloomberg_rss",
        "layer": "external_news",
        "table_name": "news_articles",
        "display_name": "Bloomberg RSS",
        "status": "experimental",
        "source": "rss/sqlite fallback",
        "freshness_sla_days": 3,
        "decision_usage": "brief_context",
        "reason": "Windows live RSS currently fails; API uses DB fallback and exposes source_status.",
    },
]


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS indicator_registry (
            indicator_key TEXT PRIMARY KEY,
            layer TEXT NOT NULL,
            table_name TEXT,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT,
            freshness_sla_days INTEGER,
            decision_usage TEXT,
            reason TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()


def main() -> int:
    print("=" * 60)
    print(f"Indicator Registry Sync - {datetime.now()}")
    print("=" * 60)
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    ensure_table(conn)
    now = datetime.now().isoformat()
    for item in INDICATORS:
        conn.execute(
            """
            INSERT OR REPLACE INTO indicator_registry
            (indicator_key, layer, table_name, display_name, status, source,
             freshness_sla_days, decision_usage, reason, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["indicator_key"],
                item["layer"],
                item.get("table_name"),
                item["display_name"],
                item["status"],
                item.get("source"),
                item.get("freshness_sla_days"),
                item.get("decision_usage"),
                item.get("reason"),
                now,
            ),
        )
    conn.commit()
    conn.close()
    status_counts = {}
    for item in INDICATORS:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
    print(f"[OK] registered {len(INDICATORS)} indicators: {status_counts}")
    print("ETL_METRICS_JSON=" + json.dumps({
        "records_processed": len(INDICATORS),
        "records_failed": 0,
        "records_skipped": 0,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
