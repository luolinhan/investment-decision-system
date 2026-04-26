from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from app.services.lead_lag_service import LeadLagService


def _sample_service(tmp_path: Path) -> LeadLagService:
    repo_root = Path(__file__).resolve().parents[1]
    return LeadLagService(
        data_dir=repo_root / "sample_data" / "lead_lag",
        obsidian_vault=tmp_path / "missing_vault",
        live_enabled=False,
    )


def test_macro_bridge_payload_keeps_provider_metadata_and_sample_fallback(tmp_path: Path):
    service = _sample_service(tmp_path)

    payload = service.macro_bridge()

    assert payload["cache_status"] == "sample_fallback"
    assert set(payload) >= {"macro_regime", "external_risk", "hk_liquidity", "decision_impact"}
    assert "cpi" in payload["macro_regime"]["fields"]
    assert "dxy" in payload["external_risk"]["fields"]
    assert "southbound_flow" in payload["hk_liquidity"]["fields"]
    assert "hk_visitor_activity" in payload["hk_liquidity"]["fields"]
    assert payload["hk_liquidity"]["fields"]["southbound_flow"]["provider_metadata"]["cache_status"] == "sample_fallback"
    assert payload["hk_liquidity"]["missing_fields"]
    assert payload["source_summary"]["cache_status"] == "sample_fallback"


def test_bridge_state_changes_decision_center_risk_budget(tmp_path: Path):
    service = _sample_service(tmp_path)
    baseline = service.decision_center()

    service.live_evidence["radar"] = {
        "liquidity": {
            "macro_regime": "stress_test_tight_external_hk_bridge",
            "external_risk_score": 25,
            "hk_liquidity_score": 30,
            "last_data_sync": "2026-04-25T09:30:00",
        },
        "source_health": {"source": "test_radar_snapshot"},
    }
    stressed = service.decision_center()

    assert baseline["risk_budget"]["label"] in {"balanced", "aggressive", "conservative", "no_new_risk"}
    assert stressed["risk_budget"]["label"] == "no_new_risk"
    assert "external=25.0" in stressed["risk_budget"]["reason"]
    assert stressed["macro_bridge_summary"]["bridge_state"] == "risk_off"


def test_opportunity_card_contains_bridge_score_impact(tmp_path: Path):
    service = _sample_service(tmp_path)
    default_config = deepcopy(service.v2_config)
    zero_bridge_config = deepcopy(default_config)
    zero_bridge_config["scoring"]["bridge_impact"] = {
        "actionability_weight": 0,
        "tradability_weight": 0,
        "decision_weight": 0,
        "high_beta_penalty_threshold": 45,
        "high_beta_sectors": ["ai", "semis", "innovative_pharma", "solar"],
    }
    zero_bridge_config["scoring"]["opportunity_weights"]["bridge_adjustment_score"] = 0

    service.v2_config = zero_bridge_config
    baseline = service.opportunity_queue(limit=20)["cards"][0]

    service.v2_config = default_config
    influenced = service.opportunity_queue(limit=20)["cards"][0]

    assert influenced["bridge_impact"]["explanation"]
    assert influenced["bridge_impact"]["cache_status"] == "sample_fallback"
    assert influenced["tradability_score"] != baseline["tradability_score"]
    assert influenced["decision_priority_score"] != baseline["decision_priority_score"]
