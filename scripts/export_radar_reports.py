#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export Investment Radar reports into an isolated Obsidian subdirectory.

The exporter reads the unified RadarService payload so the written reports stay
consistent with the dashboard, DuckDB snapshots and pipeline health signals.

Supported report modes:
  - daily
  - weekly
  - monthly
  - thesis
  - due   (daily + thesis, weekly on Monday, monthly on day 1)
  - all
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from app.services.radar_service import RadarService  # noqa: E402


DEFAULT_VAULT_CANDIDATES = [
    os.getenv("INVESTMENT_OBSIDIAN_VAULT"),
    os.getenv("OBSIDIAN_VAULT_PATH"),
    r"C:\Users\Administrator\Documents\Obsidian\知识库",
    os.path.expanduser("~/Documents/Obsidian/知识库"),
]
REPORT_ROOT_NAME = "Investment-Radar-Reports"


def _safe_slug(text: str, limit: int = 64) -> str:
    cleaned = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", (text or "").strip(), flags=re.UNICODE)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return (cleaned or "untitled")[:limit]


def _is_duckdb_lock_error(exc: Exception) -> bool:
    text = str(exc)
    return (
        "Could not set lock on file" in text
        or "File is already open in" in text
        or "无法访问" in text
    )


def _pick_text(*values: Any, fallback: str = "-") -> str:
    for value in values:
        if value not in (None, ""):
            return str(value)
    return fallback


def _fmt_date(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return str(value).replace("T", " ")[:19]
    except Exception:
        return str(value)


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        number = float(value)
    except Exception:
        return str(value)
    return f"{number:.{digits}f}".rstrip("0").rstrip(".")


def _bullet_list(items: Sequence[str], empty: str = "-") -> List[str]:
    if not items:
        return [f"- {empty}"]
    return [f"- {item}" for item in items]


def _resolve_output_root(vault_path: str | None, output_dir: str | None) -> Path:
    if output_dir:
        root = Path(output_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    candidates = [vault_path, *DEFAULT_VAULT_CANDIDATES]
    for candidate in candidates:
        if not candidate:
            continue
        candidate_path = Path(candidate).expanduser()
        try:
            candidate_path.mkdir(parents=True, exist_ok=True)
            root = candidate_path / REPORT_ROOT_NAME
            root.mkdir(parents=True, exist_ok=True)
            return root
        except Exception:
            continue

    root = (BASE_DIR / "reports" / REPORT_ROOT_NAME).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


class RadarReportExporter:
    def __init__(self, db_path: str | None = None):
        self.service = self._open_service(db_path=db_path)

    @staticmethod
    def _open_service(db_path: str | None = None) -> RadarService:
        last_error: Exception | None = None
        for _ in range(6):
            try:
                return RadarService(db_path=db_path)
            except Exception as exc:
                last_error = exc
                if not _is_duckdb_lock_error(exc):
                    raise
                time.sleep(1.0)
        if last_error:
            raise last_error
        return RadarService(db_path=db_path)

    def get_overview(self, force_refresh: bool = False) -> Dict[str, Any]:
        return self.service.get_overview(force_refresh=force_refresh)

    def _frontmatter(self, title: str, report_type: str, generated_at: str) -> List[str]:
        return [
            "---",
            f"title: {title}",
            f"report_type: {report_type}",
            f"generated_at: {generated_at}",
            "tags:",
            "  - investment",
            "  - radar",
            f"  - {report_type}",
            "---",
            "",
        ]

    def _render_summary(self, overview: Dict[str, Any]) -> List[str]:
        summary = overview.get("summary", {})
        return [
            "## Summary",
            f"- `macro_regime`: {_pick_text(summary.get('macro_regime'))}",
            f"- `external_risk_score`: {_fmt_num(summary.get('external_risk_score'))}",
            f"- `hk_liquidity_score`: {_fmt_num(summary.get('hk_liquidity_score'))}",
            f"- `sector_preposition_score`: {_fmt_num(summary.get('sector_preposition_score'))}",
            f"- `thesis_confidence`: {_fmt_num(summary.get('thesis_confidence'))}",
            f"- `data_coverage`: {_fmt_num(summary.get('data_coverage'))}%",
            f"- `missing_indicators`: {_pick_text(summary.get('missing_indicators'))}",
            f"- `last_data_sync`: {_fmt_date(summary.get('last_data_sync'))}",
            f"- `pipeline_failures_24h`: {_pick_text(summary.get('pipeline_failures_24h'))}",
            "",
        ]

    def _render_panel(self, title: str, panel: Dict[str, Any], headline_lines: Sequence[str]) -> List[str]:
        freshness = panel.get("freshness") or {}
        lines = [
            f"## {title}",
            *headline_lines,
            f"- 更新日期: {_fmt_date(freshness.get('last_update'))}",
            f"- 是否 stale: {_pick_text(freshness.get('is_stale'))}",
        ]
        if panel.get("coverage_ratio") is not None:
            lines.append(f"- 覆盖率: {_fmt_num(panel.get('coverage_ratio'))}%")
        signals = panel.get("signals") or []
        if signals:
            lines.extend(["", "### Drivers"])
            for item in signals:
                lines.append(
                    f"- **{_pick_text(item.get('label'))}**: {_pick_text(item.get('display'))} | "
                    f"{_pick_text(item.get('signal'))} | {_pick_text(item.get('hint'))}"
                )
        missing = panel.get("missing_keys") or []
        if missing:
            lines.extend(["", "### Missing"])
            lines.extend(_bullet_list([str(item) for item in missing]))
        charts = panel.get("chart_stats") or []
        if charts:
            lines.extend(["", "### Trend Windows"])
            for item in charts:
                lines.append(
                    f"- **{_pick_text(item.get('label'))}**: "
                    f"{_pick_text(item.get('start_date'))} -> {_pick_text(item.get('end_date'))} | "
                    f"{_pick_text(item.get('observations'))} points | "
                    f"delta={_fmt_num(item.get('delta'))}"
                )
        lines.append("")
        return lines

    def _render_sectors(self, overview: Dict[str, Any]) -> List[str]:
        panel = overview.get("sectors", {})
        evidence = panel.get("evidence_totals") or {}
        lines = [
            "## Thesis Board",
            f"- sector_preposition_score: {_fmt_num(panel.get('sector_preposition_score'))}",
            f"- thesis_confidence: {_fmt_num(panel.get('thesis_confidence'))}",
            f"- event_matches: {_pick_text(evidence.get('event_matches'))}",
            f"- research_matches: {_pick_text(evidence.get('research_matches'))}",
            "",
        ]
        for card in panel.get("cards") or []:
            lines.extend(
                [
                    f"### {card.get('name')}",
                    f"- score: {_fmt_num(card.get('score'))}",
                    f"- confidence: {_fmt_num(card.get('confidence'))}",
                    f"- review_cycle: {_pick_text(card.get('review_cycle'))}",
                    "- leading_variables:",
                    *[f"  - {item}" for item in card.get("leading_variables") or ["-"]],
                    "- confirmed_variables:",
                    *[f"  - {item}" for item in card.get("confirmed_variables") or ["-"]],
                    "- unverified_variables:",
                    *[f"  - {item}" for item in card.get("unverified_variables") or ["-"]],
                    "- risk_variables:",
                    *[f"  - {item}" for item in card.get("risk_variables") or ["-"]],
                    "- invalid_conditions:",
                    *[f"  - {item}" for item in card.get("invalid_conditions") or ["-"]],
                    "- watchlist:",
                    *[f"  - {item}" for item in card.get("watchlist") or ["-"]],
                    "",
                ]
            )
        return lines

    def _render_policy(self, overview: Dict[str, Any]) -> List[str]:
        panel = overview.get("policy", {})
        lines = ["## Policy / Politics / Events"]
        events = panel.get("events") or []
        if not events:
            return lines + ["- 无事件", ""]
        for item in events[:12]:
            lines.append(
                f"- **{_pick_text(item.get('title'))}** | {_pick_text(item.get('bucket'))} | "
                f"{_pick_text(item.get('bias'))} | {_fmt_date(item.get('event_time'))}"
            )
        lines.append("")
        return lines

    def _render_pizza(self, overview: Dict[str, Any]) -> List[str]:
        panel = overview.get("pizza", {})
        latest = panel.get("latest") or {}
        return [
            "## Pentagon Pizza",
            f"- band: {_pick_text(panel.get('band'))}",
            f"- latest_level: {_fmt_num(latest.get('level'))}",
            f"- latest_headline: {_pick_text(latest.get('headline'))}",
            f"- percentile_90d: {_fmt_num(panel.get('percentile_90d'))}",
            f"- trend_7d: {_fmt_num(panel.get('trend_7d'))}",
            f"- interpretation: {_pick_text(panel.get('interpretation'))}",
            "",
        ]

    def _render_gaps(self, overview: Dict[str, Any]) -> List[str]:
        panel = overview.get("gaps", {})
        summary = panel.get("summary") or {}
        lines = [
            "## Data Gaps",
            f"- required_total: {_pick_text(summary.get('required_total'))}",
            f"- ready_total: {_pick_text(summary.get('ready_total'))}",
            f"- missing_total: {_pick_text(summary.get('missing_total'))}",
            "",
        ]
        for item in (panel.get("items") or [])[:15]:
            lines.append(
                f"- **{_pick_text(item.get('name'))}** | {_pick_text(item.get('panel'))} | "
                f"{_pick_text(item.get('status'))} | {_pick_text(item.get('source'))}"
            )
        lines.append("")
        return lines

    def _render_pipeline(self, overview: Dict[str, Any]) -> List[str]:
        pipeline = overview.get("pipeline", {})
        lines = [
            "## Pipeline Health",
            f"- last_sync_at: {_fmt_date(pipeline.get('last_sync_at'))}",
            f"- failure_24h: {_pick_text(pipeline.get('failure_24h'))}",
            "",
        ]
        for item in (pipeline.get("recent_runs") or [])[:10]:
            lines.append(
                f"- **{_pick_text(item.get('source_name') or item.get('source_key'))}** | "
                f"{_pick_text(item.get('status'))} | "
                f"added={_pick_text(item.get('records_added'))} | "
                f"{_fmt_date(item.get('run_at'))}"
            )
        lines.append("")
        return lines

    def _render_memory(self, overview: Dict[str, Any]) -> List[str]:
        memory = overview.get("memory", {})
        lines = [
            "## Research Memory",
            f"- status: {_pick_text(memory.get('status'))}",
            f"- note_count: {_pick_text(memory.get('note_count'))}",
            "",
        ]
        rows = memory.get("theme_matches") or memory.get("recent_notes") or []
        for item in rows[:10]:
            lines.append(
                f"- **{_pick_text(item.get('title'))}** | {_fmt_date(item.get('modified_at'))} | "
                f"{', '.join(item.get('tracked_tags') or item.get('tags') or [])}"
            )
        lines.append("")
        return lines

    def generate_report_markdown(self, overview: Dict[str, Any], report_type: str) -> str:
        generated_at = _pick_text(overview.get("generated_at"))
        title = f"Investment Radar {report_type.title()} Report"
        lines: List[str] = []
        lines.extend(self._frontmatter(title=title, report_type=report_type, generated_at=generated_at))
        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"_Generated: {generated_at}_")
        lines.append("")
        lines.extend(self._render_summary(overview))
        lines.extend(
            self._render_panel(
                "Macro Regime",
                overview.get("macro", {}),
                [
                    f"- macro_regime: {_pick_text((overview.get('macro') or {}).get('macro_regime'))}",
                    f"- inflation_state: {_pick_text((overview.get('macro') or {}).get('inflation_state'))}",
                ],
            )
        )
        lines.extend(
            self._render_panel(
                "External Risk",
                overview.get("external", {}),
                [
                    f"- risk_state: {_pick_text((overview.get('external') or {}).get('risk_state'))}",
                    f"- external_risk_score: {_fmt_num((overview.get('external') or {}).get('external_risk_score'))}",
                ],
            )
        )
        lines.extend(
            self._render_panel(
                "Hong Kong Liquidity",
                overview.get("hk", {}),
                [
                    f"- risk_appetite: {_pick_text((overview.get('hk') or {}).get('risk_appetite'))}",
                    f"- hk_liquidity_score: {_fmt_num((overview.get('hk') or {}).get('hk_liquidity_score'))}",
                ],
            )
        )
        lines.extend(self._render_sectors(overview))
        lines.extend(self._render_policy(overview))
        lines.extend(self._render_pizza(overview))
        lines.extend(self._render_gaps(overview))
        lines.extend(self._render_pipeline(overview))
        lines.extend(self._render_memory(overview))
        return "\n".join(lines).rstrip() + "\n"

    def generate_thesis_markdown(self, overview: Dict[str, Any], card: Dict[str, Any]) -> str:
        generated_at = _pick_text(overview.get("generated_at"))
        title = f"Investment Radar Thesis - {card.get('name')}"
        memory_rows = overview.get("memory", {}).get("theme_matches") or []
        related_memory = [
            item for item in memory_rows
            if card.get("name", "") in _pick_text(item.get("title"), item.get("content_preview"), fallback="")
            or card.get("name", "") in " ".join(item.get("tracked_tags") or item.get("tags") or [])
        ]
        policy_rows = overview.get("policy", {}).get("events") or []
        related_policy = [
            item for item in policy_rows
            if any(token.lower() in (_pick_text(item.get("title"), fallback="").lower()) for token in [card.get("name", ""), *card.get("leading_variables", [])[:2]])
        ]

        lines: List[str] = []
        lines.extend(self._frontmatter(title=title, report_type="thesis", generated_at=generated_at))
        lines.extend(
            [
                f"# {title}",
                "",
                f"_Generated: {generated_at}_",
                "",
                f"- score: {_fmt_num(card.get('score'))}",
                f"- confidence: {_fmt_num(card.get('confidence'))}",
                f"- review_cycle: {_pick_text(card.get('review_cycle'))}",
                "",
                "## Leading Variables",
                *[f"- {item}" for item in card.get("leading_variables") or ["-"]],
                "",
                "## Confirmed Variables",
                *[f"- {item}" for item in card.get("confirmed_variables") or ["-"]],
                "",
                "## Unverified Variables",
                *[f"- {item}" for item in card.get("unverified_variables") or ["-"]],
                "",
                "## Risk Variables",
                *[f"- {item}" for item in card.get("risk_variables") or ["-"]],
                "",
                "## Invalid Conditions",
                *[f"- {item}" for item in card.get("invalid_conditions") or ["-"]],
                "",
                "## Watchlist",
                *[f"- {item}" for item in card.get("watchlist") or ["-"]],
                "",
                "## Evidence",
                f"- event_matches: {_pick_text((card.get('evidence') or {}).get('events'))}",
                f"- research_matches: {_pick_text((card.get('evidence') or {}).get('research'))}",
                "",
                "## Related Policy Events",
            ]
        )
        if related_policy:
            for item in related_policy[:8]:
                lines.append(
                    f"- **{_pick_text(item.get('title'))}** | {_pick_text(item.get('bucket'))} | "
                    f"{_pick_text(item.get('bias'))} | {_fmt_date(item.get('event_time'))}"
                )
        else:
            lines.append("- 无直接匹配事件")
        lines.extend(["", "## Related Memory"])
        if related_memory:
            for item in related_memory[:8]:
                lines.append(
                    f"- **{_pick_text(item.get('title'))}** | {_fmt_date(item.get('modified_at'))} | "
                    f"{', '.join(item.get('tracked_tags') or item.get('tags') or [])}"
                )
        else:
            lines.append("- 无直接匹配笔记")
        lines.append("")
        return "\n".join(lines)

    def _write_text(self, root: Path, relative_path: str, content: str) -> Path:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def export(
        self,
        report_type: str,
        vault_path: str | None = None,
        output_dir: str | None = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        root = _resolve_output_root(vault_path=vault_path, output_dir=output_dir)
        overview = self.get_overview(force_refresh=force_refresh)
        now = datetime.now()

        if report_type == "due":
            modes: List[str] = ["daily", "thesis"]
            if now.weekday() == 0:
                modes.append("weekly")
            if now.day == 1:
                modes.append("monthly")
        elif report_type == "all":
            modes = ["daily", "weekly", "monthly", "thesis"]
        else:
            modes = [report_type]

        written: List[Path] = []
        for mode in modes:
            if mode == "daily":
                written.append(
                    self._write_text(
                        root,
                        f"daily/radar-daily-{now:%Y%m%d}.md",
                        self.generate_report_markdown(overview, "daily"),
                    )
                )
            elif mode == "weekly":
                written.append(
                    self._write_text(
                        root,
                        f"weekly/radar-weekly-{now:%G}-W{now:%V}.md",
                        self.generate_report_markdown(overview, "weekly"),
                    )
                )
            elif mode == "monthly":
                written.append(
                    self._write_text(
                        root,
                        f"monthly/radar-monthly-{now:%Y%m}.md",
                        self.generate_report_markdown(overview, "monthly"),
                    )
                )
            elif mode == "thesis":
                for card in (overview.get("sectors", {}).get("cards") or []):
                    written.append(
                        self._write_text(
                            root,
                            f"thesis/{_safe_slug(card.get('name', 'thesis'))}-{now:%Y%m%d}.md",
                            self.generate_thesis_markdown(overview, card),
                        )
                    )

        return {
            "success": True,
            "root": str(root),
            "written": [str(path) for path in written],
            "summary": overview.get("summary", {}),
            "generated_at": overview.get("generated_at"),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Investment Radar markdown reports.")
    parser.add_argument(
        "--report",
        choices=["daily", "weekly", "monthly", "thesis", "due", "all"],
        default="due",
        help="Which report set to write. Default writes due reports.",
    )
    parser.add_argument("--vault-path", default=None, help="Obsidian vault root. Writes into a separate subdirectory.")
    parser.add_argument("--output-dir", default=None, help="Explicit local output directory.")
    parser.add_argument("--db-path", default=None, help="Override radar DuckDB path.")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass RadarService in-memory cache.")
    args = parser.parse_args()

    exporter = RadarReportExporter(db_path=args.db_path)
    result = exporter.export(
        report_type=args.report,
        vault_path=args.vault_path,
        output_dir=args.output_dir,
        force_refresh=args.force_refresh,
    )

    print("=" * 60)
    print(f"Radar Report Export - {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)
    print(f"Root: {result['root']}")
    print(f"Generated at: {result['generated_at']}")
    print(f"Files written: {len(result['written'])}")
    for path in result["written"][:10]:
        print(f" - {path}")
    summary = result.get("summary", {})
    print(
        "Summary:",
        f"macro_regime={summary.get('macro_regime')}",
        f"external_risk_score={summary.get('external_risk_score')}",
        f"hk_liquidity_score={summary.get('hk_liquidity_score')}",
        f"sector_preposition_score={summary.get('sector_preposition_score')}",
        f"coverage={summary.get('data_coverage')}",
    )
    print("[OK] Export completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
