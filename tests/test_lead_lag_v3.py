from __future__ import annotations

from pathlib import Path

from app.services.lead_lag_service import LeadLagService


def _sample_service(tmp_path: Path) -> LeadLagService:
    repo_root = Path(__file__).resolve().parents[1]
    return LeadLagService(
        data_dir=repo_root / "sample_data" / "lead_lag",
        obsidian_vault=tmp_path / "missing_vault",
        live_enabled=False,
    )


def test_sample_cards_are_hidden_from_default_queue_and_not_executable(tmp_path: Path):
    service = _sample_service(tmp_path)

    payload = service.opportunity_queue(limit=20)

    assert payload["cards"] == []
    assert payload["parent_thesis_cards"] == []
    assert payload["blocked_cards"]
    assert all(card["data_source_class"] in {"sample_demo", "fallback_placeholder"} for card in payload["blocked_cards"])
    assert all(card["is_executable"] is False for card in payload["blocked_cards"])
    assert all(card["generation_status"] != "actionable" for card in payload["blocked_cards"])


def test_include_sample_keeps_v3_governance_fields_and_parent_thesis(tmp_path: Path):
    service = _sample_service(tmp_path)

    payload = service.opportunity_queue(limit=20, include_sample=True)

    assert payload["cards"]
    card = payload["cards"][0]
    assert card["data_source_class"] in {"sample_demo", "fallback_placeholder"}
    assert card["evidence_panel"]
    assert card["evidence_checklist"]
    assert card["live_source_count"] == 0
    assert card["sample_source_count"] >= 1
    assert card["execution_blockers"]
    assert card["next_review_time"]
    assert card["checkpoint_status"] in {"scheduled", "stale_rolled_forward"}

    parents = payload["parent_thesis_cards"]
    assert parents
    assert parents[0]["child_variants"]
    assert parents[0]["variant_count"] >= 1
    assert parents[0]["source_card_ids"]


def test_sample_events_are_hidden_from_default_frontline(tmp_path: Path):
    service = _sample_service(tmp_path)

    assert service.event_frontline(limit=20)["events"] == []

    payload = service.event_frontline(limit=20, include_sample=True, include_research_facing=True)
    assert payload["events"]
    assert all(event["data_source_class"] in {"sample_demo", "fallback_placeholder"} for event in payload["events"])
    assert all(event["tradability_class"] in {"research-facing", "archive-only"} for event in payload["events"])
