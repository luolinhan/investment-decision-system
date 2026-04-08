"""SQLite adapters for setup-driven Quant Workbench data."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from quant_workbench.config import INVESTMENT_DB_PATH


_DDL_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS stock_factor_snapshot (
        trade_date TEXT,
        code TEXT,
        model TEXT,
        quality REAL,
        growth REAL,
        valuation REAL,
        flow REAL,
        technical REAL,
        risk REAL,
        total REAL,
        PRIMARY KEY(trade_date, code, model)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signal_labels (
        signal_date TEXT,
        code TEXT,
        setup_name TEXT,
        hold_days INTEGER,
        entry_price REAL,
        exit_price REAL,
        max_gain REAL,
        max_drawdown REAL,
        win_flag INTEGER,
        invalidated INTEGER DEFAULT 0,
        PRIMARY KEY(signal_date, code, setup_name, hold_days)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_backtest_stats (
        stat_date TEXT,
        setup_name TEXT,
        hold_days INTEGER,
        sample_size INTEGER,
        win_rate REAL,
        avg_return REAL,
        avg_max_drawdown REAL,
        profit_loss_ratio REAL,
        PRIMARY KEY(stat_date, setup_name, hold_days)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_event_calendar (
        code TEXT,
        event_type TEXT,
        event_date TEXT,
        importance INTEGER,
        note TEXT,
        PRIMARY KEY(code, event_type, event_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_flow_daily (
        trade_date TEXT,
        code TEXT,
        northbound_net REAL,
        southbound_net REAL,
        main_inflow REAL,
        margin_balance REAL,
        turnover_rank REAL,
        PRIMARY KEY(trade_date, code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_estimate (
        consensus_date TEXT,
        code TEXT,
        eps_fy1 REAL,
        eps_fy2 REAL,
        eps_rev_30d REAL,
        eps_rev_90d REAL,
        target_price REAL,
        target_upside REAL,
        coverage INTEGER,
        PRIMARY KEY(consensus_date, code)
    )
    """,
)


def ensure_strategy_tables(db_path: Path = INVESTMENT_DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        for ddl in _DDL_STATEMENTS:
            conn.execute(ddl)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signal_labels_setup_date
            ON signal_labels(setup_name, signal_date)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_strategy_backtest_setup
            ON strategy_backtest_stats(setup_name, stat_date)
            """
        )
        conn.commit()


def _row_to_dict(row: sqlite3.Row | None) -> Dict[str, Any]:
    return dict(row) if row is not None else {}


def _parse_iso_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value)[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def summarize_event_summary(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "事件日历未接入，按周复核"

    today = date.today()
    upcoming = []
    historical = []
    for item in events:
        event_day = _parse_iso_date(item.get("event_date"))
        if event_day is None:
            continue
        if event_day >= today:
            upcoming.append((event_day, item))
        else:
            historical.append((event_day, item))

    if upcoming:
        event_day, item = sorted(upcoming, key=lambda pair: pair[0])[0]
        importance = item.get("importance")
        note = item.get("note")
        parts = [f"{event_day.isoformat()} {item.get('event_type', '事件')}"]
        if importance not in (None, ""):
            parts.append(f"重要度 {importance}")
        if note:
            parts.append(str(note))
        return " / ".join(parts)

    if not historical:
        return "事件日历未接入，按周复核"

    event_day, item = sorted(historical, key=lambda pair: pair[0], reverse=True)[0]
    return f"最近事件 {event_day.isoformat()} {item.get('event_type', '事件')}"


def summarize_sector_context(sector: Dict[str, Any]) -> str:
    sector_type = sector.get("type")
    if not sector_type:
        return "行业先行指标未接入"

    if sector_type == "tmt":
        parts = []
        if sector.get("revenue_yoy") not in (None, ""):
            parts.append(f"收入同比 {sector['revenue_yoy']}%")
        if sector.get("rd_ratio") not in (None, ""):
            parts.append(f"研发费率 {sector['rd_ratio']}%")
        if sector.get("retention_d30") not in (None, ""):
            parts.append(f"30日留存 {sector['retention_d30']}%")
        return "TMT: " + (" / ".join(parts) if parts else "数据待补")

    if sector_type == "consumer":
        parts = []
        if sector.get("same_store_sales_yoy") not in (None, ""):
            parts.append(f"同店增速 {sector['same_store_sales_yoy']}%")
        if sector.get("store_change") not in (None, ""):
            parts.append(f"门店变化 {sector['store_change']}")
        if sector.get("member_growth_yoy") not in (None, ""):
            parts.append(f"会员增速 {sector['member_growth_yoy']}%")
        return "消费: " + (" / ".join(parts) if parts else "数据待补")

    if sector_type == "biotech":
        parts = []
        if sector.get("phase_cn"):
            parts.append(f"阶段 {sector['phase_cn']}")
        if sector.get("expected_approval"):
            parts.append(f"审批预期 {sector['expected_approval']}")
        if sector.get("partner"):
            parts.append(f"合作方 {sector['partner']}")
        return "医药: " + (" / ".join(parts) if parts else "数据待补")

    return "行业先行指标未接入"


class QuantWorkbenchDBViews:
    def __init__(self, db_path: Path = INVESTMENT_DB_PATH) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        ensure_strategy_tables(self.db_path)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def load_feature_context(self, code: str) -> Dict[str, Any]:
        with self._connect() as conn:
            valuation = _row_to_dict(
                conn.execute(
                    """
                    SELECT trade_date, pe_ttm, pe_percentile_5y, pb, pb_percentile_5y,
                           dividend_yield, dy_percentile_3y, valuation_level
                    FROM valuation_bands
                    WHERE code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (code,),
                ).fetchone()
            )
            technical = _row_to_dict(
                conn.execute(
                    """
                    SELECT trade_date, trend_signal, rsi_14, atr_pct, beta_1y, volatility_30d, volatility_90d
                    FROM technical_indicators
                    WHERE code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (code,),
                ).fetchone()
            )
            flow = _row_to_dict(
                conn.execute(
                    """
                    SELECT trade_date, northbound_net, southbound_net, main_inflow,
                           margin_balance, turnover_rank
                    FROM stock_flow_daily
                    WHERE code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """,
                    (code,),
                ).fetchone()
            )
            estimate = _row_to_dict(
                conn.execute(
                    """
                    SELECT consensus_date, eps_fy1, eps_fy2, eps_rev_30d, eps_rev_90d,
                           target_price, target_upside, coverage
                    FROM stock_estimate
                    WHERE code = ?
                    ORDER BY consensus_date DESC
                    LIMIT 1
                    """,
                    (code,),
                ).fetchone()
            )
            market_flow = _row_to_dict(
                conn.execute(
                    """
                    SELECT trade_date, total_net_inflow, total_accumulated
                    FROM north_money
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                ).fetchone()
            )
            events = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT event_type, event_date, importance, note
                    FROM stock_event_calendar
                    WHERE code = ?
                    ORDER BY event_date ASC
                    LIMIT 5
                    """,
                    (code,),
                ).fetchall()
            ]
            sector = self._load_sector_context(conn, code)

        return {
            "valuation": valuation,
            "technical": technical,
            "flow": flow,
            "estimate": estimate,
            "market_flow": market_flow,
            "events": events,
            "event_summary": summarize_event_summary(events),
            "sector": sector,
            "sector_summary": summarize_sector_context(sector),
        }

    def load_backtest_stats_map(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._connect() as conn:
            latest_stat_date = conn.execute(
                "SELECT MAX(stat_date) AS stat_date FROM strategy_backtest_stats"
            ).fetchone()["stat_date"]
            if not latest_stat_date:
                return {}

            rows = conn.execute(
                """
                SELECT stat_date, setup_name, hold_days, sample_size, win_rate,
                       avg_return, avg_max_drawdown, profit_loss_ratio
                FROM strategy_backtest_stats
                WHERE stat_date = ?
                ORDER BY setup_name, hold_days
                """,
                (latest_stat_date,),
            ).fetchall()

        payload: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            payload.setdefault(row["setup_name"], []).append(dict(row))
        return payload

    def _load_sector_context(self, conn: sqlite3.Connection, code: str) -> Dict[str, Any]:
        tmt = conn.execute(
            """
            SELECT report_date, revenue_yoy, rd_ratio, retention_d30, notes
            FROM sector_tmt
            WHERE code = ?
            ORDER BY report_date DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()
        if tmt is not None:
            payload = dict(tmt)
            payload["type"] = "tmt"
            return payload

        consumer = conn.execute(
            """
            SELECT report_date, same_store_sales_yoy, store_change, member_growth_yoy, notes
            FROM sector_consumer
            WHERE code = ?
            ORDER BY report_date DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()
        if consumer is not None:
            payload = dict(consumer)
            payload["type"] = "consumer"
            return payload

        biotech = conn.execute(
            """
            SELECT phase_cn, expected_approval, partner, notes, updated_at
            FROM sector_biotech
            WHERE company_code = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()
        if biotech is not None:
            payload = dict(biotech)
            payload["type"] = "biotech"
            return payload

        return {}
