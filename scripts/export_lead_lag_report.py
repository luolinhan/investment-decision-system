from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.lead_lag_service import LeadLagService


def _render_list(items: Iterable[str]) -> str:
    rows = [f"- {item}" for item in items if item]
    return "\n".join(rows) if rows else "- none"


def build_report(service: LeadLagService, report_type: str, theme: str | None = None) -> str:
    overview = service.get_overview()
    models = service.list_models(limit=10)
    opportunities = service.list_opportunities(limit=10)
    thesis_cards = service.list_sector_thesis()
    events = service.get_events_calendar(limit=10)
    replay = service.get_replay_validation()
    memory = service.get_obsidian_memory(limit=6)

    if theme:
        theme_lower = theme.lower()
        thesis_cards = [item for item in thesis_cards if theme_lower in str(item.get("name", "")).lower() or theme_lower in str(item.get("summary", "")).lower()]
        opportunities = [item for item in opportunities if theme_lower in str(item.get("title", "")).lower() or theme_lower in str(item.get("rationale", "")).lower()]

    header = [
        f"# Lead-Lag {report_type.title()} Report",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- as_of: {overview.get('as_of')}",
        f"- source: {overview.get('source')}",
        f"- theme: {theme or 'all'}",
        "",
        "## Overview",
        "",
        f"- headline: {overview.get('headline')}",
        f"- summary: {overview.get('summary')}",
        f"- status: {overview.get('status_text')}",
        "",
        "## Top Models",
        "",
        _render_list(
            f"{item.get('name')} | stage={item.get('status')} | confidence={item.get('confidence')}"
            for item in models[:5]
        ),
        "",
        "## Opportunity Board",
        "",
        _render_list(
            f"{item.get('title')} | score={item.get('score')} | driver={item.get('driver')} | risk={item.get('risk')}"
            for item in opportunities[:6]
        ),
        "",
        "## Sector Thesis",
        "",
        _render_list(
            f"{item.get('name')} | crowding={item.get('crowding')} | invalidation={item.get('invalidation')}"
            for item in thesis_cards[:5]
        ),
        "",
        "## Event Calendar",
        "",
        _render_list(
            f"{item.get('date')} | {item.get('title')} | {item.get('importance')}"
            for item in events[:6]
        ),
        "",
        "## Replay Validation",
        "",
        _render_list(
            f"{item.get('title')} | outcome={item.get('outcome')} | hit_rate={item.get('hit_rate')}"
            for item in replay[:5]
        ),
        "",
        "## Obsidian Memory",
        "",
        _render_list(
            f"{item.get('title')} | tags={','.join(item.get('tags', []))}"
            for item in memory[:5]
        ),
        "",
    ]
    return "\n".join(header)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Lead-Lag markdown reports.")
    parser.add_argument("--type", default="daily", choices=["daily", "weekly", "monthly", "theme"], help="Report type.")
    parser.add_argument("--theme", default=None, help="Optional theme filter.")
    parser.add_argument("--output", default=None, help="Optional output path.")
    args = parser.parse_args()

    service = LeadLagService()
    report = build_report(service, args.type, theme=args.theme)
    output = Path(args.output) if args.output else Path("reports") / "lead_lag" / f"{args.type}-{datetime.now().date().isoformat()}.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
