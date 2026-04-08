"""
Merge reports from a source SQLite DB into a target reports.db.
Usage:
  python merge_reports_db.py --target path/to/reports.db --source path/to/reports.db
"""
from __future__ import annotations

import argparse
import sqlite3


def ensure_reports_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            stock_code TEXT,
            stock_name TEXT,
            institution TEXT,
            author TEXT,
            rating TEXT,
            publish_date DATE,
            pdf_url TEXT,
            local_pdf_path TEXT,
            summary TEXT,
            raw_content TEXT,
            source TEXT,
            external_id TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_code ON reports(stock_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(publish_date)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()

    target_conn = sqlite3.connect(args.target)
    ensure_reports_table(target_conn)
    source_conn = sqlite3.connect(args.source)

    columns = [
        "title",
        "stock_code",
        "stock_name",
        "institution",
        "author",
        "rating",
        "publish_date",
        "pdf_url",
        "local_pdf_path",
        "summary",
        "raw_content",
        "source",
        "external_id",
    ]
    select_sql = f"SELECT {', '.join(columns)} FROM reports"
    insert_sql = f"INSERT OR IGNORE INTO reports ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"

    count = 0
    for row in source_conn.execute(select_sql):
        target_conn.execute(insert_sql, row)
        count += 1

    target_conn.commit()
    source_conn.close()
    target_conn.close()
    print({"merged": count})


if __name__ == "__main__":
    main()
