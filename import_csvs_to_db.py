"""
Import CSV dumps into SQLite tables (INSERT OR REPLACE).
Usage:
  python import_csvs_to_db.py --db path/to/investment.db --mapping table=csv_path [table=csv_path ...]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def import_csv(conn: sqlite3.Connection, table: str, csv_path: Path) -> int:
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        headers = next(reader, None)
        if not headers:
            return 0
        placeholders = ",".join(["?"] * len(headers))
        columns = ",".join(headers)
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        count = 0
        for row in reader:
            conn.execute(sql, row)
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument(
        "--mapping",
        action="append",
        default=[],
        help="table=csv_path (repeatable)",
    )
    args = parser.parse_args()
    if not args.mapping:
        raise SystemExit("no mappings provided")

    conn = sqlite3.connect(args.db)
    total = 0
    for item in args.mapping:
        if "=" not in item:
            raise SystemExit(f"invalid mapping: {item}")
        table, path = item.split("=", 1)
        imported = import_csv(conn, table.strip(), Path(path.strip()))
        total += imported
        conn.commit()
        print({"table": table.strip(), "rows": imported})
    conn.close()
    print({"total": total})


if __name__ == "__main__":
    main()
