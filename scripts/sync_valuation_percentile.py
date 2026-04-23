# -*- coding: utf-8 -*-
"""
从stock_financial历史数据计算估值百分位

逻辑: 对每只股票，基于其历史PE/PB计算当前值的百分位排名
"""
import sqlite3
import sys
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")


def compute_percentile(values, current):
    """计算当前值在历史序列中的百分位"""
    if not values or current is None:
        return None
    try:
        current = float(current)
    except (ValueError, TypeError):
        return None
    valid = []
    for v in values:
        try:
            fv = float(v)
            if fv > 0:
                valid.append(fv)
        except (ValueError, TypeError):
            continue
    if not valid:
        return None
    count_below = sum(1 for v in valid if v < current)
    return round(count_below / len(valid) * 100, 1)


def main():
    print("=" * 60)
    print(f"Valuation Percentile Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all stocks with PE/PB data
    rows = cursor.execute("""
        SELECT code, report_date, pe_ttm, pb, ps_ttm, dividend_yield
        FROM stock_financial
        WHERE pe_ttm IS NOT NULL OR pb IS NOT NULL
        ORDER BY code, report_date
    """).fetchall()

    # Group by code
    stocks = {}
    for row in rows:
        code = row[0]
        if code not in stocks:
            stocks[code] = {"pe": [], "pb": [], "ps": [], "dy": [], "latest": row}
        stocks[code]["pe"].append(row[2])
        stocks[code]["pb"].append(row[3])
        stocks[code]["ps"].append(row[4])
        stocks[code]["dy"].append(row[5])

    added = 0
    for code, data in stocks.items():
        latest = data["latest"]
        report_date = latest[1]
        pe = latest[2]
        pb = latest[3]
        ps = latest[4]
        dy = latest[5]

        pe_pct = compute_percentile(data["pe"], pe)
        pb_pct = compute_percentile(data["pb"], pb)
        ps_pct = compute_percentile(data["ps"], ps)
        dy_pct = compute_percentile(data["dy"], dy)

        cursor.execute("""
            INSERT OR REPLACE INTO valuation_percentile
            (code, trade_date, pe_ttm, pe_percentile_3y, pe_percentile_5y, pe_percentile_10y,
             pb, pb_percentile_3y, pb_percentile_5y, pb_percentile_10y,
             ps_ttm, ps_percentile_3y, dividend_yield, dy_percentile_3y)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, report_date, pe, pe_pct, pe_pct, pe_pct,
              pb, pb_pct, pb_pct, pb_pct, ps, ps_pct, dy, dy_pct))
        added += 1

    conn.commit()
    conn.close()
    print(f"  [OK] Computed valuation percentiles for {added} stocks")


if __name__ == "__main__":
    main()
