"""Translate shortline official events using Bailian."""
from __future__ import annotations

import argparse
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from app.services.shortline_service import ShortlineService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Translate shortline T0 events.")
    parser.add_argument("--db-path", default=os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db")))
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    service = ShortlineService(args.db_path)
    result = service.translate_recent_events(limit=max(1, int(args.limit or 20)))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
