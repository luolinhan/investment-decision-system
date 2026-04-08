"""
Clean empty shell records in stock_financial.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "investment.db"


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM stock_financial")
    total_before = int(c.fetchone()[0] or 0)

    c.execute(
        """
        DELETE FROM stock_financial
        WHERE (report_date IS NULL OR report_date = '')
          AND pe_ttm IS NULL
          AND pb IS NULL
          AND roe IS NULL
          AND gross_margin IS NULL
          AND net_margin IS NULL
          AND revenue_yoy IS NULL
          AND net_profit_yoy IS NULL
          AND dividend_yield IS NULL
          AND total_revenue IS NULL
          AND net_profit IS NULL
        """
    )
    deleted = c.rowcount if c.rowcount is not None else 0

    conn.commit()

    c.execute("SELECT COUNT(*) FROM stock_financial")
    total_after = int(c.fetchone()[0] or 0)
    conn.close()

    print(
        {
            "total_before": total_before,
            "deleted": deleted,
            "total_after": total_after,
        }
    )


if __name__ == "__main__":
    main()
