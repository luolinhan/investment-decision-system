#!/usr/bin/env python3
"""Create and seed the Lead-Lag V3 Opportunity Universe Registry."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.opportunity_universe import OpportunityUniverseRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate the local Opportunity Universe Registry.")
    parser.add_argument("--db-path", default="data/investment.db", help="SQLite database path.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable ETL metrics.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = OpportunityUniverseRegistry(db_path=args.db_path)
    schema = registry.ensure_schema()
    seeded = registry.seed_defaults()
    summary = registry.registry_summary()
    payload = {"schema": schema, "seeded": seeded, "summary": summary}
    if args.json:
        print(f"ETL_METRICS_JSON={json.dumps(payload, ensure_ascii=False, sort_keys=True)}")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
