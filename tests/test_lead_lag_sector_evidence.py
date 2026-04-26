from __future__ import annotations

import json
from pathlib import Path

from app.services.lead_lag_sector_evidence import build_sector_deep_evidence


def _bundle() -> dict:
    repo_root = Path(__file__).resolve().parents[1]
    sample_path = repo_root / "sample_data" / "lead_lag" / "lead_lag_v1.json"
    return json.loads(sample_path.read_text(encoding="utf-8"))


def _event_relevance_sample() -> list[dict]:
    return [
        {
            "event_id": "ai_capex_event",
            "sector_key": "ai",
            "title": "US capex order demand and model cost update with China mapping",
            "summary": "capex order demand model cost china mapping",
            "source": {"url": "sample_data/lead_lag/lead_lag_v1.json"},
        },
        {
            "event_id": "pharma_chain_event",
            "sector_key": "innovative_pharma",
            "title": "BD licensing clinical readout IND NDA approval commercialization update",
            "summary": "BD readout IND NDA approval CXO commercial",
            "source": {"url": "sample_data/lead_lag/lead_lag_v1.json"},
        },
        {
            "event_id": "semis_chain_event",
            "sector_key": "semis",
            "title": "Overseas earnings inventory order capex advanced packaging equipment material foundry design",
            "summary": "earnings inventory order capex packaging equipment material design",
            "source": {"url": "sample_data/lead_lag/lead_lag_v1.json"},
        },
        {
            "event_id": "solar_chain_event",
            "sector_key": "solar",
            "title": "inventory destock decapacity price chain capex shrink merger",
            "summary": "去库存 去产能 价格 并购 资本开支 收缩",
            "source": {"url": "sample_data/lead_lag/lead_lag_v1.json"},
        },
        {
            "event_id": "hog_chain_event",
            "sector_key": "hog_cycle",
            "title": "sow inventory hog spot futures margin profit feed cost",
            "summary": "能繁 存栏 猪价 期货 现货 利润 饲料成本",
            "source": {"url": "sample_data/lead_lag/lead_lag_v1.json"},
        },
    ]


def test_sector_deep_evidence_covers_five_sectors_and_required_fields():
    bundle = _bundle()
    result = build_sector_deep_evidence(
        bundle=bundle,
        opportunity_cards=bundle.get("opportunities", []),
        event_relevance=_event_relevance_sample(),
        macro_bridge={"bridge_state": "risk_neutral"},
    )

    assert result["count"] == 5
    keys = {item["sector_key"] for item in result["sectors"]}
    assert keys == {"ai", "innovative_pharma", "semis", "solar", "hog_cycle"}

    required = {
        "sector_key",
        "sector_name",
        "mode",
        "leader_assets",
        "bridge_assets",
        "local_assets",
        "evidence_layers",
        "first_baton",
        "second_baton",
        "validation_baton",
        "missing_validation",
        "invalidation_rules",
        "provider_gaps",
        "action_readiness",
        "evidence_completeness",
        "last_update",
        "source_count",
        "source",
        "cache_status",
    }
    for sector in result["sectors"]:
        assert required.issubset(sector.keys())
        assert isinstance(sector["evidence_layers"], list) and sector["evidence_layers"]
        assert 0.0 <= float(sector["evidence_completeness"]) <= 100.0


def test_sector_filter_only_returns_target_sector():
    bundle = _bundle()
    result = build_sector_deep_evidence(
        bundle=bundle,
        opportunity_cards={"cards": bundle.get("opportunities", [])},
        event_relevance={"items": _event_relevance_sample()},
        macro_bridge={"bridge_state": "risk_on"},
        sector="ai",
    )
    assert result["count"] == 1
    assert result["sectors"][0]["sector_key"] == "ai"


def test_evidence_completeness_and_missing_validation_behave_as_expected():
    bundle = _bundle()
    rich_ai = {
        "opportunity_id": "ai_extra_confirmed",
        "sector_key": "ai",
        "symbol": "NVDA",
        "market": "US",
        "name": "AI capex order demand validation",
        "notes": "capex order demand model cost china mapping",
        "validation_status": "validated",
    }
    thin_hog = {
        "opportunity_id": "hog_watch_only",
        "sector_key": "hog_cycle",
        "symbol": "002714.SZ",
        "market": "A",
        "name": "hog cycle watch",
        "notes": "waiting",
        "validation_status": "watching",
    }
    opportunities = [rich_ai, thin_hog]
    events = [item for item in _event_relevance_sample() if item["sector_key"] == "ai"]

    result = build_sector_deep_evidence(
        bundle=bundle,
        opportunity_cards=opportunities,
        event_relevance=events,
        macro_bridge={"bridge_state": "risk_on"},
    )
    by_sector = {item["sector_key"]: item for item in result["sectors"]}
    assert by_sector["ai"]["evidence_completeness"] > by_sector["hog_cycle"]["evidence_completeness"]
    assert by_sector["hog_cycle"]["missing_validation"]


def test_missing_provider_gap_is_reported():
    bundle = _bundle()
    bundle["source_health"] = {
        "sec": {"status": "degraded"},
        "company_ir": {"status": "healthy"},
    }
    result = build_sector_deep_evidence(
        bundle=bundle,
        opportunity_cards=bundle.get("opportunities", []),
        event_relevance=_event_relevance_sample(),
        macro_bridge={"bridge_state": "risk_neutral"},
        sector="semis",
    )
    semis = result["sectors"][0]
    assert semis["provider_gaps"]
    providers = {item["provider"] for item in semis["provider_gaps"]}
    assert "equipment_supply_chain_feed" in providers or "price_feeds" in providers
