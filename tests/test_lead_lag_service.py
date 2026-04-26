from __future__ import annotations

from pathlib import Path

from app.services.lead_lag_service import LeadLagService


def _service(tmp_path: Path) -> LeadLagService:
    repo_root = Path(__file__).resolve().parents[1]
    sample_dir = repo_root / "sample_data" / "lead_lag"
    return LeadLagService(data_dir=sample_dir, obsidian_vault=tmp_path / "missing_vault")


def _sample_service(tmp_path: Path) -> LeadLagService:
    repo_root = Path(__file__).resolve().parents[1]
    sample_dir = repo_root / "sample_data" / "lead_lag"
    return LeadLagService(data_dir=sample_dir, obsidian_vault=tmp_path / "missing_vault", live_enabled=False)


def test_lead_lag_overview_and_model_families(tmp_path: Path):
    service = _service(tmp_path)

    overview = service.overview()
    assert overview["model_family_count"] == 10
    assert overview["sector_thesis_count"] == 5
    assert overview["opportunity_count"] >= 7
    assert set(overview["stage_counts"]) == {
        "latent",
        "pre_trigger",
        "triggered",
        "validating",
        "crowded",
        "decaying",
        "invalidated",
    }

    models = service.models()
    assert models["count"] == 10
    assert len(models["families"]) == 10
    assert models["top_families"][0] == "leadership_breadth"
    required_model_fields = {
        "model_id",
        "model_family",
        "thesis",
        "description",
        "applicable_assets",
        "applicable_sectors",
        "lead_signals",
        "follower_tiers",
        "lag_windows",
        "validation_signals",
        "crowding_signals",
        "invalidation_rules",
        "evidence_sources",
        "refresh_frequency",
        "confidence_formula",
        "stage_machine",
        "notes_links",
    }
    assert required_model_fields.issubset(models["families"][0].keys())


def test_lead_lag_opportunity_stage_machine_and_batons(tmp_path: Path):
    service = _service(tmp_path)

    opportunities = service.opportunities(limit=10)
    batons = opportunities["baton_buckets"]

    assert opportunities["stage_counts"]["invalidated"] == 1
    assert opportunities["stage_counts"]["triggered"] >= 1
    assert batons["first_baton"]
    assert batons["first_baton"][0]["opportunity_id"] == "ai_compute_first"
    assert batons["invalidated"][0]["stage"] == "invalidated"
    assert any(item["baton"] == "second-baton" for item in batons["second_baton"])
    assert any(item["baton"] == "next-baton" for item in batons["next_baton"])


def test_lead_lag_cross_market_views_and_memory_fallback(tmp_path: Path):
    service = _sample_service(tmp_path)

    cross_market = service.cross_market_map()
    transmission = service.industry_transmission()
    liquidity = service.liquidity()
    theses = service.sector_thesis()
    events = service.events_calendar()
    replay = service.replay_validation()
    memory = service.obsidian_memory()

    assert len(cross_market["nodes"]) >= 10
    assert len(cross_market["edges"]) >= 10
    required_edge_fields = {
        "from_id",
        "to_id",
        "relation",
        "sign",
        "strength",
        "lag_min_days",
        "lag_max_days",
        "evidence_type",
        "confidence",
        "last_verified_at",
    }
    assert required_edge_fields.issubset(cross_market["edges"][0].keys())
    assert "ai" in transmission["sector_paths"]
    assert "semis" in transmission["sector_paths"]
    assert liquidity["source_health"]["sec"]["status"] == "healthy"
    assert theses["count"] == 5
    assert events["count"] == 7
    assert replay["cases"] == 64
    assert replay["aggregate_hit_rate"] > 0.6
    assert memory["status"] == "missing"
    assert memory["source"] == "sample_data"


def test_lead_lag_live_fusion_adds_reliable_assets(tmp_path: Path):
    service = _service(tmp_path)

    overview = service.overview()
    opportunities = service.opportunities(limit=30)
    events = service.events_calendar(limit=30)

    assert overview["live_enabled"] is True
    assert overview["live_source_health"]["radar"]["status"] in {"healthy", "empty", "missing"}
    assert overview["live_event_count"] >= 0
    assert opportunities["count"] >= 7
    assert events["count"] >= 7

    rows = opportunities["all"]
    assert any(item.get("evidence_sources") for item in rows)
    for item in rows[:10]:
        assert item.get("asset_code")
        assert item.get("asset_name")
        assert item.get("market") is not None
