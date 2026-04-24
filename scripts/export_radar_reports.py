#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export Investment Radar reports (daily, weekly, monthly, thesis-tracking) to Obsidian vault.
Only exports to an independent subdirectory (e.g., Investment-Radar-Reports/) to avoid touching original notes.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure we can import from app/
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.services.intelligence_service import IntelligenceService


class RadarReportExporter:
    def __init__(self, db_path: Optional[str] = None):
        db_path = db_path or os.getenv(
            "INVESTMENT_DB_PATH",
            os.path.join(BASE_DIR, "data", "investment.db"),
        )
        self.service = IntelligenceService(db_path)

    def get_radar_data(
        self,
        report_type: str,
        lookback_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Fetch relevant intelligence data for the report."""
        overview = self.service.get_overview()
        events = overview["latest_events"]
        research = overview["research"]
        source_health = overview["source_health"]
        last_run = overview["last_run"]

        # Filter by time window if needed
        if lookback_days:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=lookback_days)
            ).isoformat()
            events = [
                e
                for e in events
                if (e.get("last_seen_at") or e.get("event_time") or "").startswith(
                    cutoff[:10]
                )
            ]
            research = [
                r
                for r in research
                if (r.get("fetched_at") or r.get("published_at") or "").startswith(
                    cutoff[:10]
                )
            ]

        # Categorize events
        ai_events = [e for e in events if e.get("category") == "ai_model"]
        biotech_events = [e for e in events if e.get("category") == "biotech"]
        hk_events = [e for e in events if e.get("category") == "hk_market"]
        external_events = [e for e in events if e.get("category") not in ("ai_model", "biotech", "hk_market")]

        return {
            "report_type": report_type,
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "lookback_days": lookback_days,
            "summary": overview["summary"],
            "events": {
                "ai_model": ai_events,
                "biotech": biotech_events,
                "hk_market": hk_events,
                "external": external_events,
            },
            "research": research,
            "source_health": source_health,
            "last_run": last_run,
            "gaps": self._detect_gaps(source_health, last_run),
        }

    @staticmethod
    def _detect_gaps(
        source_health: List[Dict[str, Any]],
        last_run: Optional[Dict[str, Any]],
    ) -> List[str]:
        """Detect collection or coverage gaps."""
        gaps = []
        if not last_run:
            gaps.append("No recent collection run detected.")
        elif last_run.get("status") != "success":
            gaps.append(f"Last collection run failed: {last_run.get('error')}")
        else:
            recent = datetime.fromisoformat(last_run["finished_at"].replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - recent) > timedelta(hours=6):
                gaps.append(f"Last successful run was >6 hours ago ({recent.isoformat()}).")
        failed_sources = [
            s["source_key"] for s in source_health if s.get("enabled") and s.get("last_error")
        ]
        if failed_sources:
            gaps.append(f"Failed sources: {', '.join(failed_sources[:5])}")
        return gaps

    @staticmethod
    def _format_event(event: Dict[str, Any]) -> str:
        """Format a single event as markdown."""
        lines = [
            f"- **{event.get('title', '')}**",
            f"  - Priority: {event.get('priority', 'P2')} | Confidence: {event.get('confidence', 0):.0%}",
            f"  - Category: {event.get('category', '')}",
            f"  - Impact: {event.get('impact_summary', '')}",
            f"  - Last seen: {event.get('last_seen_at', '')}",
        ]
        if event.get("summary"):
            lines.append(f"  - Summary: {event.get('summary')}")
        return "\n".join(lines)

    @staticmethod
    def _format_research(research: Dict[str, Any]) -> str:
        """Format a research item as markdown."""
        lines = [
            f"- **{research.get('title', '')}**",
            f"  - Source: {research.get('source_name', '')}",
            f"  - Type: {research.get('report_type', '')}",
            f"  - Fetched: {research.get('fetched_at', '')}",
        ]
        if research.get("summary"):
            lines.append(f"  - Summary: {research.get('summary')}")
        if research.get("thesis"):
            lines.append(f"  - Thesis: {research.get('thesis')}")
        return "\n".join(lines)

    def generate_report_markdown(self, data: Dict[str, Any]) -> str:
        """Generate markdown content for the report."""
        report_type = data["report_type"]
        generated_at = data["generated_at"]
        summary = data["summary"]

        lines = [
            f"# Investment Radar - {report_type.title()} Report",
            "",
            f"_Generated: {generated_at}_",
            "",
            "## Summary",
            f"- Active events: {summary.get('active_events', 0)}",
            f"- P0 events: {summary.get('p0_events', 0)}",
            f"- P1 events: {summary.get('p1_events', 0)}",
            f"- Research reports: {summary.get('research_reports', 0)}",
            f"- Enabled sources: {summary.get('enabled_sources', 0)}",
            "",
        ]

        # Gaps section
        if data.get("gaps"):
            lines.extend(
                [
                    "## Gaps / Issues",
                    "",
                ]
                + [f"- {gap}" for gap in data["gaps"]]
                + [""]
            )

        # Events by category
        for category_name, events in data["events"].items():
            if not events:
                continue
            lines.extend(
                [
                    f"## {category_name.replace('_', ' ').title()} Events",
                    "",
                ]
                + [self._format_event(e) for e in events]
                + [""]
            )

        # Research
        if data["research"]:
            lines.extend(
                [
                    "## Recent Research",
                    "",
                ]
                + [self._format_research(r) for r in data["research"]]
                + [""]
            )

        # Source health
        failed = [
            s
            for s in data["source_health"]
            if s.get("enabled") and s.get("last_error")
        ]
        if failed:
            lines.extend(
                [
                    "## Source Health Issues",
                    "",
                ]
                + [
                    f"- **{s.get('source_key', '')}**: {s.get('last_error', '')[:120]}"
                    for s in failed[:10]
                ]
                + [""]
            )

        # Last run
        if data.get("last_run"):
            run = data["last_run"]
            lines.extend(
                [
                    "## Last Collection Run",
                    "",
                    f"- Started: {run.get('started_at', '')}",
                    f"- Finished: {run.get('finished_at', '')}",
                    f"- Status: {run.get('status', '')}",
                    f"- Records found: {run.get('records_found', 0)}",
                ]
            )
            if run.get("error"):
                lines.append(f"- Error: {run.get('error', '')}")

        return "\n".join(lines)

    def export_to_obsidian(
        self,
        report_type: str,
        content: str,
        vault_path: Optional[str] = None,
    ) -> Path:
        """Export report to Obsidian vault subdirectory."""
        # Determine output directory
        if not vault_path:
            vault_path = os.getenv(
                "OBSIDIAN_VAULT_PATH",
                os.getenv("HOME", "/tmp") + "/Documents/Obsidian/投资决策系统",
            )
        reports_dir = Path(vault_path) / "Investment-Radar-Reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        now = datetime.now()
        if report_type == "daily":
            filename = f"Radar-Daily-{now.strftime('%Y%m%d')}.md"
        elif report_type == "weekly":
            filename = f"Radar-Weekly-{now.strftime('%Y%U')}.md"
        elif report_type == "monthly":
            filename = f"Radar-Monthly-{now.strftime('%Y%m')}.md"
        else:
            filename = f"Radar-{report_type}-{now.strftime('%Y%m%d-%H%M')}.md"

        filepath = reports_dir / filename
        filepath.write_text(content, encoding="utf-8")
        return filepath

    def export_thesis_reports(self, data: Dict[str, Any], vault_path: Optional[str] = None) -> List[Path]:
        """Export per-thesis tracking reports."""
        # Determine output directory
        if not vault_path:
            vault_path = os.getenv(
                "OBSIDIAN_VAULT_PATH",
                os.getenv("HOME", "/tmp") + "/Documents/Obsidian/投资决策系统",
            )
        thesis_dir = Path(vault_path) / "Investment-Radar-Reports" / "Thesis-Tracking"
        thesis_dir.mkdir(parents=True, exist_ok=True)

        paths = []
        for research in data["research"]:
            thesis = research.get("thesis") or research.get("title") or "untitled"
            # Create a clean filename from thesis
            filename = "".join(c if c.isalnum() or c in (" ", "-", "_") else "_" for c in thesis[:80]).strip()
            filename = f"Thesis-{filename.replace(' ', '_')}-{datetime.now().strftime('%Y%m%d')}.md"

            content_lines = [
                f"# Thesis Tracking: {thesis}",
                "",
                f"_Generated: {data['generated_at']}_",
                "",
                "## Source Research",
                "",
                self._format_research(research),
                "",
                "## Related Events",
                "",
            ]
            # Find events related to this thesis
            related_events = [
                e for e in data["events"]["ai_model"] + data["events"]["biotech"] + data["events"]["external"]
                if research.get("title", "").lower() in (e.get("title", "") + e.get("summary", "")).lower()
                or any(term in (e.get("title", "") + e.get("summary", "")).lower() for term in thesis.lower().split()[:3])
            ]
            if related_events:
                content_lines.extend([self._format_event(e) for e in related_events])
            else:
                content_lines.append("_No directly related events found._")

            content_lines.extend([
                "",
                "## Action Items",
                "",
                "- [ ] Verify thesis against recent market data",
                "- [ ] Check for new supporting/contradicting evidence",
                "- [ ] Update investment thesis card if needed",
            ])

            filepath = thesis_dir / filename
            filepath.write_text("\n".join(content_lines), encoding="utf-8")
            paths.append(filepath)

        return paths

    def export(
        self,
        report_type: str,
        vault_path: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Main export entry point."""
        # Fetch data
        lookback_days = {"daily": 1, "weekly": 7, "monthly": 30}.get(report_type, 7)
        data = self.get_radar_data(report_type, lookback_days=lookback_days)

        # Generate content
        content = self.generate_report_markdown(data)

        # Export
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            now = datetime.now()
            if report_type == "daily":
                filename = f"radar-daily-{now.strftime('%Y%m%d')}.md"
            elif report_type == "weekly":
                filename = f"radar-weekly-{now.strftime('%Y%U')}.md"
            elif report_type == "monthly":
                filename = f"radar-monthly-{now.strftime('%Y%m')}.md"
            elif report_type == "thesis":
                filename = f"radar-thesis-{now.strftime('%Y%m%d-%H%M')}.md"
            else:
                filename = f"radar-{report_type}-{now.strftime('%Y%m%d-%H%M')}.md"
            filepath = output_path / filename
            filepath.write_text(content, encoding="utf-8")
            result_path = filepath
            result_type = "file"
        elif report_type == "thesis":
            paths = self.export_thesis_reports(data, vault_path=vault_path)
            result_path = paths[0] if paths else None
            result_type = "thesis_reports"
        else:
            result_path = self.export_to_obsidian(report_type, content, vault_path=vault_path)
            result_type = "obsidian"

        return {
            "success": True,
            "report_type": report_type,
            "result_type": result_type,
            "path": str(result_path) if result_path else None,
            "data": data,
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export Investment Radar reports to Obsidian or local directory."
    )
    parser.add_argument(
        "--report",
        choices=["daily", "weekly", "monthly", "thesis"],
        required=True,
        help="Report type to generate",
    )
    parser.add_argument(
        "--vault-path",
        default=None,
        help="Obsidian vault path (default: $OBSIDIAN_VAULT_PATH or ~/Documents/Obsidian/投资决策系统)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Local output directory (bypasses Obsidian; useful for testing)",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite database path (default: data/investment.db)",
    )
    args = parser.parse_args()

    exporter = RadarReportExporter(db_path=args.db_path)
    result = exporter.export(
        report_type=args.report,
        vault_path=args.vault_path,
        output_dir=args.output_dir,
    )

    print("=" * 60)
    print(f"Radar Report Export - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"Report type: {result['report_type']}")
    print(f"Result type: {result['result_type']}")
    if result.get("path"):
        print(f"Saved to: {result['path']}")
    summary = result["data"]["summary"]
    print(
        f"Summary: {summary.get('active_events')} active events, "
        f"{summary.get('p0_events')} P0, "
        f"{summary.get('p1_events')} P1, "
        f"{summary.get('research_reports')} research reports"
    )
    if result["data"].get("gaps"):
        print(f"Gaps/issues: {len(result['data']['gaps'])}")
    print("[OK] Export completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
