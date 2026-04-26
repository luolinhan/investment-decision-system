from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.lead_lag_service import LeadLagService


def build_snapshot() -> dict:
    service = LeadLagService(data_dir=os.getenv("LEAD_LAG_DATA_DIR"))
    overview = service.overview()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "collector_mode": os.getenv("LEAD_LAG_COLLECTOR_MODE", "demo"),
        "as_of": overview.get("as_of"),
        "summary": {
            "model_family_count": overview.get("model_family_count"),
            "sector_thesis_count": overview.get("sector_thesis_count"),
            "opportunity_count": overview.get("opportunity_count"),
            "event_count": overview.get("event_count"),
            "watchlist_count": overview.get("watchlist_count"),
        },
        "source_health": overview.get("source_health", {}),
        "stage_counts": overview.get("stage_counts", {}),
        "top_batons": overview.get("top_batons", {}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Lead-Lag collector snapshots for Aliyun handoff.")
    parser.add_argument("--output", default="data/lead_lag/collector_snapshot.json", help="Snapshot output path.")
    parser.add_argument("--health-only", action="store_true", help="Emit only source health.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    args = parser.parse_args()

    snapshot = build_snapshot()
    if args.health_only:
        snapshot = {
            "generated_at": snapshot["generated_at"],
            "collector_mode": snapshot["collector_mode"],
            "source_health": snapshot["source_health"],
        }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2 if args.pretty else None),
        encoding="utf-8",
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
