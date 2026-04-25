"""Sync US-led shortline events into investment.db."""
from __future__ import annotations

import argparse
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from app.services.shortline_service import ShortlineService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync US shortline events.")
    parser.add_argument("--db-path", default=os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db")))
    parser.add_argument("--build-candidates", action="store_true", help="Also build CN candidates after syncing events.")
    parser.add_argument("--include-official", action="store_true", help="Also sync SEC official filing events.")
    parser.add_argument("--translate", action="store_true", help="Translate latest T0 events with Bailian after syncing.")
    parser.add_argument("--max-items", type=int, default=24)
    args = parser.parse_args()

    service = ShortlineService(args.db_path)
    result = service.sync_us_market_events(max_items=max(8, int(args.max_items or 24)))
    if args.include_official:
        result["official"] = service.sync_sec_filings()
    if args.translate:
        result["translate"] = service.translate_recent_events(limit=20)
    if args.build_candidates:
        result["candidates"] = service.build_candidates()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
