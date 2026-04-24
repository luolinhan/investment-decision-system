# -*- coding: utf-8 -*-
"""Audit Investment Hub SQLite tables and code references.

This script is intentionally read-only. It helps decide which datasets should be
active, deprecated, or removed before any physical cleanup migration is written.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "investment.db"
SCAN_DIRS = ["app", "scripts", "templates"]
DATE_COLUMN_CANDIDATES = [
    "trade_date",
    "date",
    "report_date",
    "as_of_date",
    "published",
    "fetched_at",
    "updated_at",
    "created_at",
    "start_time",
]


def table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def latest_value(conn: sqlite3.Connection, table_name: str, columns: List[str]) -> Dict[str, Any]:
    for column in DATE_COLUMN_CANDIDATES:
        if column not in columns:
            continue
        try:
            value = conn.execute(f"SELECT MAX({column}) FROM {table_name}").fetchone()[0]
            if value:
                return {"column": column, "value": value}
        except Exception:
            continue
    return {"column": None, "value": None}


def code_reference_count(root: Path, table_name: str) -> int:
    count = 0
    for scan_dir in SCAN_DIRS:
        path = root / scan_dir
        if not path.exists():
            continue
        for file_path in path.rglob("*"):
            if file_path.suffix.lower() not in {".py", ".html", ".js", ".ts", ".md"}:
                continue
            try:
                text = file_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if table_name in text:
                count += 1
    return count


def classify(row_count: int, latest: Dict[str, Any], refs: int) -> str:
    if row_count == 0 and refs == 0:
        return "remove_candidate"
    if row_count == 0:
        return "deprecated_empty"
    if refs == 0:
        return "orphaned_data"
    if latest.get("value") is None:
        return "needs_date_contract"
    return "review"


def load_registry(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='indicator_registry'"
    ).fetchone()
    if not table:
        return {}
    rows = conn.execute("""
        SELECT indicator_key, table_name, display_name, status, freshness_sla_days, reason
        FROM indicator_registry
    """).fetchall()
    return {
        row[1]: {
            "indicator_key": row[0],
            "display_name": row[2],
            "registry_status": row[3],
            "freshness_sla_days": row[4],
            "registry_reason": row[5],
        }
        for row in rows
    }


def audit(db_path: Path) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path)
    registry = load_registry(conn)
    tables = [
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        if not row[0].startswith("sqlite_")
    ]
    assets = []
    for table in tables:
        columns = table_columns(conn, table)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        latest = latest_value(conn, table, columns)
        refs = code_reference_count(ROOT, table)
        registry_item = registry.get(table, {})
        status_hint = registry_item.get("registry_status") or classify(row_count, latest, refs)
        assets.append({
            "table": table,
            "rows": row_count,
            "latest_column": latest["column"],
            "latest_value": latest["value"],
            "code_reference_count": refs,
            "status_hint": status_hint,
            **registry_item,
        })
    conn.close()
    summary = {}
    for item in assets:
        summary[item["status_hint"]] = summary.get(item["status_hint"], 0) + 1
    return {
        "db_path": str(db_path),
        "table_count": len(assets),
        "summary": summary,
        "assets": assets,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    result = audit(db_path)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(f"DB: {result['db_path']}")
    print(f"Tables: {result['table_count']}  Summary: {result['summary']}")
    print("table\trows\tlatest\trefs\tstatus")
    for item in result["assets"]:
        latest = f"{item['latest_column']}={item['latest_value']}" if item["latest_column"] else "-"
        registry_label = item.get("indicator_key") or "-"
        print(f"{item['table']}\t{item['rows']}\t{latest}\t{item['code_reference_count']}\t{item['status_hint']}\t{registry_label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
