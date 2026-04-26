from __future__ import annotations

from pathlib import Path

from app.services.lead_lag_schema import validate_opportunity_card
from app.services.lead_lag_service import LeadLagService


def _sample_service(tmp_path: Path) -> LeadLagService:
    repo_root = Path(__file__).resolve().parents[1]
    return LeadLagService(
        data_dir=repo_root / "sample_data" / "lead_lag",
        obsidian_vault=tmp_path / "missing_vault",
        live_enabled=False,
    )


def test_opportunity_card_v2_required_fields_are_populated(tmp_path: Path):
    service = _sample_service(tmp_path)

    payload = service.opportunity_queue(limit=20)

    assert payload["count"] >= 7
    assert payload["scoring_config"]["opportunity_weights"]["actionability_score"] > 0
    for card in payload["cards"]:
        assert validate_opportunity_card(card) == []
        assert card["decision_priority_score"] >= 0
        assert card["leader_asset"]["code"]
        assert card["bridge_asset"]["code"]
        assert card["local_asset"]["code"]
        assert card["decision_chain"]["result"]
        assert card["stock_pool"]
        assert card["model_discoveries"]


def test_missing_confirmations_are_explicit_for_weak_opportunities(tmp_path: Path):
    service = _sample_service(tmp_path)

    cards = service.opportunity_queue(limit=20)["cards"]
    hog_card = next(card for card in cards if card["id"] == "hog_latent")

    assert hog_card["generation_status"] == "insufficient_evidence"
    assert hog_card["missing_confirmations"]
    assert "证据" in hog_card["missing_evidence_reason"]


def test_operator_payload_methods_are_available(tmp_path: Path):
    service = _sample_service(tmp_path)

    decision_center = service.decision_center()
    avoid_board = service.avoid_board()
    what_changed = service.what_changed()

    assert decision_center["headline"] == "领先-传导决策中心"
    assert decision_center["top_directions"]
    assert isinstance(avoid_board["items"], list)
    assert set(what_changed) >= {
        "new_signals",
        "upgraded_opportunities",
        "downgraded_or_invalidated",
        "crowding_up",
    }
