"""Phase-one setup label generation and aggregate backtest stats."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List

import pandas as pd

from quant_workbench.config import INVESTMENT_DB_PATH
from quant_workbench.db_views import ensure_strategy_tables
from quant_workbench.factors import enrich_price_features
from quant_workbench.storage import available_market_files, read_parquet
from quant_workbench.setups import SETUP_ORDER

HOLD_DAYS = (5, 10, 20)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_trade_date(row: pd.Series) -> str:
    for key in ("date", "trade_date", "ts"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value)[:10]
    return datetime.now().strftime("%Y-%m-%d")


def _load_static_fundamentals(conn: sqlite3.Connection, code: str) -> Dict[str, float]:
    row = conn.execute(
        """
        SELECT roe, pe_ttm, pb
        FROM stock_financial
        WHERE code = ?
        ORDER BY report_date DESC
        LIMIT 1
        """,
        (code,),
    ).fetchone()
    if row is None:
        return {"roe": 0.0, "pe_ttm": 0.0, "pb": 0.0}
    return {"roe": _safe_float(row[0]), "pe_ttm": _safe_float(row[1]), "pb": _safe_float(row[2])}


def _detect_proxy_setups(row: pd.Series, fundamentals: Dict[str, float]) -> List[str]:
    ret_5 = _safe_float(row.get("ret_5"))
    ret_20 = _safe_float(row.get("ret_20"))
    close_vs_ma_20 = _safe_float(row.get("close_vs_ma_20"))
    vol_ratio = _safe_float(row.get("vol_ratio"))
    efi_13 = _safe_float(row.get("efi_13"))
    ma_20_slope = _safe_float(row.get("ma_20_slope"))
    ma_60_slope = _safe_float(row.get("ma_60_slope"))
    atr_pct = _safe_float(row.get("atr_pct_14"))

    roe = fundamentals.get("roe", 0.0)
    pe_ttm = fundamentals.get("pe_ttm", 0.0)
    pb = fundamentals.get("pb", 0.0)

    setups: List[str] = []
    if (
        (roe >= 10 or (0 < pb <= 3.5) or (0 < pe_ttm <= 24))
        and ma_20_slope >= 0
        and -12 <= ret_20 <= 16
        and close_vs_ma_20 >= -2
    ):
        setups.append("quality_value_recovery")

    if ret_20 >= 8 and vol_ratio >= 1.15 and close_vs_ma_20 >= 1 and efi_13 > 0:
        setups.append("earnings_revision_breakout")

    if ret_5 >= -3 and ret_5 <= 4 and vol_ratio >= 0.9 and efi_13 > 0 and ma_60_slope >= -0.1:
        setups.append("risk_on_pullback_leader")

    if ret_20 >= 4 and ret_5 >= 0 and vol_ratio >= 1.05 and atr_pct <= 7:
        setups.append("sector_catalyst_confirmation")

    return [name for name in SETUP_ORDER if name in setups]


def _build_label_rows(code: str, frame: pd.DataFrame, fundamentals: Dict[str, float]) -> List[tuple[Any, ...]]:
    rows: List[tuple[Any, ...]] = []
    active_previous = {setup: False for setup in SETUP_ORDER}

    for index in range(60, len(frame) - 1):
        current = frame.iloc[index]
        active_now = set(_detect_proxy_setups(current, fundamentals))
        entry_price = _safe_float(current.get("close"))
        entry_ma_20 = _safe_float(current.get("ma_20"))
        if entry_price <= 0:
            active_previous = {setup: setup in active_now for setup in SETUP_ORDER}
            continue

        for setup_name in active_now:
            if active_previous.get(setup_name):
                continue

            signal_date = _extract_trade_date(current)
            for hold_days in HOLD_DAYS:
                end_index = min(index + hold_days, len(frame) - 1)
                future = frame.iloc[index + 1 : end_index + 1]
                if future.empty:
                    continue

                exit_price = _safe_float(future.iloc[-1].get("close"))
                if exit_price <= 0:
                    continue

                future_high = _safe_float(future["high"].max()) if "high" in future.columns else exit_price
                future_low = _safe_float(future["low"].min()) if "low" in future.columns else exit_price
                max_gain = ((future_high / entry_price) - 1) * 100 if future_high > 0 else 0.0
                max_drawdown = ((future_low / entry_price) - 1) * 100 if future_low > 0 else 0.0
                invalidated = 1 if max_drawdown <= -8 or (entry_ma_20 > 0 and future_low < entry_ma_20) else 0
                win_flag = 1 if exit_price > entry_price else 0

                rows.append(
                    (
                        signal_date,
                        code,
                        setup_name,
                        hold_days,
                        round(entry_price, 4),
                        round(exit_price, 4),
                        round(max_gain, 2),
                        round(max_drawdown, 2),
                        win_flag,
                        invalidated,
                    )
                )

        active_previous = {setup: setup in active_now for setup in SETUP_ORDER}

    return rows


def _rebuild_backtest_stats(conn: sqlite3.Connection, stat_date: str) -> int:
    conn.execute("DELETE FROM strategy_backtest_stats WHERE stat_date = ?", (stat_date,))
    rows = conn.execute(
        """
        SELECT setup_name,
               hold_days,
               COUNT(*) AS sample_size,
               AVG(CASE WHEN entry_price != 0 THEN (exit_price - entry_price) * 100.0 / entry_price END) AS avg_return,
               AVG(max_drawdown) AS avg_max_drawdown,
               AVG(win_flag * 100.0) AS win_rate,
               AVG(CASE
                   WHEN win_flag = 1 AND entry_price != 0
                   THEN (exit_price - entry_price) * 100.0 / entry_price
               END) AS avg_win_return,
               AVG(CASE
                   WHEN win_flag = 0 AND entry_price != 0
                   THEN ABS((exit_price - entry_price) * 100.0 / entry_price)
               END) AS avg_loss_abs
        FROM signal_labels
        WHERE signal_date >= date(?, '-365 day')
        GROUP BY setup_name, hold_days
        HAVING COUNT(*) >= 1
        """,
        (stat_date,),
    ).fetchall()

    for row in rows:
        avg_win_return = _safe_float(row[6])
        avg_loss_abs = _safe_float(row[7])
        profit_loss_ratio = round(avg_win_return / avg_loss_abs, 2) if avg_loss_abs > 0 else None
        conn.execute(
            """
            INSERT OR REPLACE INTO strategy_backtest_stats (
                stat_date, setup_name, hold_days, sample_size, win_rate,
                avg_return, avg_max_drawdown, profit_loss_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stat_date,
                row[0],
                row[1],
                row[2],
                round(_safe_float(row[5]), 2),
                round(_safe_float(row[3]), 2),
                round(_safe_float(row[4]), 2),
                profit_loss_ratio,
            ),
        )

    return len(rows)


def refresh_backtest_cache(codes: Iterable[str] | None = None) -> Dict[str, int]:
    ensure_strategy_tables()
    allowed_codes = set(codes or [])
    processed_codes = 0
    label_count = 0
    stat_date = datetime.now().strftime("%Y-%m-%d")

    with sqlite3.connect(INVESTMENT_DB_PATH) as conn:
        for path in available_market_files():
            if not path.name.endswith("_1d.parquet"):
                continue

            code = path.stem.rsplit("_", 1)[0]
            if allowed_codes and code not in allowed_codes:
                continue
            if not code.startswith(("sh", "sz", "hk")):
                continue

            frame = read_parquet(path)
            if frame.empty or len(frame) < 90:
                continue

            frame = enrich_price_features(frame)
            fundamentals = _load_static_fundamentals(conn, code)
            labels = _build_label_rows(code, frame, fundamentals)

            conn.execute("DELETE FROM signal_labels WHERE code = ?", (code,))
            if labels:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO signal_labels (
                        signal_date, code, setup_name, hold_days, entry_price, exit_price,
                        max_gain, max_drawdown, win_flag, invalidated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    labels,
                )
                label_count += len(labels)

            processed_codes += 1

        stats_count = _rebuild_backtest_stats(conn, stat_date)
        conn.commit()

    return {
        "processed_codes": processed_codes,
        "labels": label_count,
        "stats": stats_count,
    }
