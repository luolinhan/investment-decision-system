from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.lead_lag_briefs import (  # noqa: E402
    BRIEF_SLOTS,
    LeadLagBriefGenerator,
    brief_filename,
    default_obsidian_output_dir,
    render_brief_markdown,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export fixed-time Lead-Lag V2 briefs.")
    parser.add_argument("--slot", required=True, choices=BRIEF_SLOTS, help="Brief slot to generate.")
    parser.add_argument("--as-of", default=None, help="Optional ISO datetime for the brief payload.")
    parser.add_argument("--output-dir", default=None, help="Directory for exported files. Overrides Obsidian default.")
    parser.add_argument("--obsidian", dest="obsidian", action="store_true", default=True, help="Use default Obsidian Lead-Lag Ops output directory.")
    parser.add_argument("--no-obsidian", dest="obsidian", action="store_false", help="Do not use the default Obsidian output directory.")
    parser.add_argument("--json", dest="write_json", action="store_true", help="Write JSON payload.")
    parser.add_argument("--markdown", dest="write_markdown", action="store_true", help="Write Markdown brief.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned output and do not write files.")
    return parser


def _resolve_output_dir(output_dir: Optional[str], obsidian: bool, as_of: Optional[str]) -> Path:
    if output_dir:
        return Path(output_dir)
    if obsidian:
        return default_obsidian_output_dir(as_of)
    return REPO_ROOT / "reports" / "lead_lag" / "briefs"


def export_brief(
    slot: str,
    *,
    as_of: Optional[str] = None,
    output_dir: Optional[str] = None,
    obsidian: bool = True,
    write_json: bool = False,
    write_markdown: bool = False,
    dry_run: bool = False,
    generator: Optional[LeadLagBriefGenerator] = None,
) -> List[Path]:
    if not write_json and not write_markdown:
        write_markdown = True

    brief_generator = generator or LeadLagBriefGenerator()
    payload = brief_generator.generate(slot, as_of=as_of)
    target_dir = _resolve_output_dir(output_dir, obsidian, as_of or payload.get("as_of"))

    planned: List[tuple[Path, str]] = []
    if write_markdown:
        planned.append((target_dir / brief_filename(slot, "md"), render_brief_markdown(payload)))
    if write_json:
        planned.append((target_dir / brief_filename(slot, "json"), json.dumps(payload, ensure_ascii=False, indent=2) + "\n"))

    paths = [path for path, _ in planned]
    if dry_run:
        for path in paths:
            print(f"DRY-RUN {path}")
        return paths

    target_dir.mkdir(parents=True, exist_ok=True)
    for path, content in planned:
        path.write_text(content, encoding="utf-8")
        print(path)
    return paths


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    export_brief(
        args.slot,
        as_of=args.as_of,
        output_dir=args.output_dir,
        obsidian=args.obsidian,
        write_json=args.write_json,
        write_markdown=args.write_markdown,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
