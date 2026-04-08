"""
Generate and persist daily LLM intelligence brief.

Recommended schedule:
- preopen: 08:35
- midday: 12:20
- postclose: 18:10
"""
from __future__ import annotations

import argparse
from datetime import datetime

from app.services.coding_plan_service import CodingPlanService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily LLM intelligence brief")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh and bypass local brief cache",
    )
    parser.add_argument(
        "--label",
        default="daily",
        help="Label for current run, used in logs only",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 72)
    print(f"[generate_llm_daily_brief] start {now} label={args.label}")
    print("=" * 72)

    service = CodingPlanService()
    payload = service.generate_daily_brief(
        force_refresh=bool(args.force_refresh),
        persist=True,
    )

    print(
        "result:",
        {
            "generated_at": payload.get("generated_at"),
            "source_type": payload.get("source_type"),
            "model": payload.get("model"),
            "confidence": payload.get("confidence"),
            "error": payload.get("error"),
        },
    )
    print("[generate_llm_daily_brief] done")


if __name__ == "__main__":
    main()
