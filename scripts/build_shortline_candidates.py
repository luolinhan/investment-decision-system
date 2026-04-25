"""Build shortline CN candidates from latest US-led events."""
from __future__ import annotations

import argparse
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from app.services.shortline_service import ShortlineService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Build shortline candidates.")
    parser.add_argument("--db-path", default=os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db")))
    parser.add_argument("--refresh-events", action="store_true", help="Refresh US events before building candidates.")
    parser.add_argument("--max-age-hours", type=int, default=36)
    args = parser.parse_args()

    service = ShortlineService(args.db_path)
    result = {"ok": True}
    if args.refresh_events:
        result["sync"] = service.sync_us_market_events()
    result["candidates"] = service.build_candidates(max_age_hours=max(1, int(args.max_age_hours or 36)))
    result["overview"] = service.get_overview()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
