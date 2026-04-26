from __future__ import annotations

import json
from pathlib import Path

from app.services.lead_lag_briefs import BRIEF_SLOTS, LeadLagBriefGenerator, default_obsidian_output_dir, render_brief_markdown
from app.services.lead_lag_service import LeadLagService
from scripts.export_lead_lag_brief import export_brief


def _sample_generator(tmp_path: Path) -> LeadLagBriefGenerator:
    repo_root = Path(__file__).resolve().parents[1]
    service = LeadLagService(
        data_dir=repo_root / "sample_data" / "lead_lag",
        obsidian_vault=tmp_path / "missing_vault",
        live_enabled=False,
    )
    return LeadLagBriefGenerator(service=service)


def test_all_fixed_time_slots_generate_payload_and_markdown(tmp_path: Path):
    generator = _sample_generator(tmp_path)

    for slot in BRIEF_SLOTS:
        payload = generator.generate(slot, as_of="2026-04-26T08:20:00")
        markdown = render_brief_markdown(payload)
        encoded = json.dumps(payload, ensure_ascii=False)

        assert payload["slot"] == slot
        assert set(payload) >= {
            "today_focus",
            "new_catalysts",
            "invalidation_alerts",
            "next_checkpoints",
            "top_opportunities",
            "do_not_chase",
            "macro_external_hk_context",
            "source_summary",
        }
        assert payload["today_focus"]
        assert payload["new_catalysts"]
        assert payload["top_opportunities"]
        assert "Lead-Lag Brief" in markdown
        assert "Today Focus" in markdown
        assert slot in encoded


def test_export_dry_run_does_not_write_files(tmp_path: Path):
    generator = _sample_generator(tmp_path)

    planned = export_brief(
        "pre_open_playbook",
        as_of="2026-04-26T08:20:00",
        output_dir=str(tmp_path),
        obsidian=False,
        write_json=True,
        write_markdown=True,
        dry_run=True,
        generator=generator,
    )

    assert len(planned) == 2
    assert all(path.parent == tmp_path for path in planned)
    assert not any(path.exists() for path in planned)


def test_export_writes_markdown_and_json_when_not_dry_run(tmp_path: Path):
    generator = _sample_generator(tmp_path)

    written = export_brief(
        "close_review",
        as_of="2026-04-26T15:15:00",
        output_dir=str(tmp_path),
        obsidian=False,
        write_json=True,
        write_markdown=True,
        dry_run=False,
        generator=generator,
    )

    assert len(written) == 2
    assert all(path.exists() for path in written)
    json_path = next(path for path in written if path.suffix == ".json")
    md_path = next(path for path in written if path.suffix == ".md")
    assert json.loads(json_path.read_text(encoding="utf-8"))["slot"] == "close_review"
    assert "Close Review" in md_path.read_text(encoding="utf-8")


def test_default_obsidian_output_dir_uses_env(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("INVESTMENT_OBSIDIAN_VAULT", str(tmp_path))
    target = default_obsidian_output_dir("2026-04-26T08:20:00")
    assert target == tmp_path / "40-任务" / "Lead-Lag Ops" / "2026-04-26"
