#!/usr/bin/env python3
"""Create and backfill the Lead-Lag V3 Evidence Vault."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.evidence_vault import EvidenceVaultService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate local SQLite data into the V3 Evidence Vault.")
    parser.add_argument("--db-path", default="data/investment.db", help="SQLite database path.")
    parser.add_argument("--archive-root", default="data/archive", help="Local archive root.")
    parser.add_argument("--limit", type=int, default=None, help="Optional per-table backfill row limit.")
    parser.add_argument("--no-backfill", action="store_true", help="Only create schema and archive directories.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable ETL metrics.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = EvidenceVaultService(db_path=args.db_path, archive_root=args.archive_root)
    schema = service.ensure_schema()
    backfill = {"skipped": True} if args.no_backfill else service.backfill_from_existing(limit=args.limit)
    quality = service.source_quality_summary()
    payload = {"schema": schema, "backfill": backfill, "quality": quality}
    if args.json:
        print(f"ETL_METRICS_JSON={json.dumps(payload, ensure_ascii=False, sort_keys=True)}")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
