from __future__ import annotations

from pathlib import Path

from app.services.opportunity_universe import OpportunityUniverseRegistry


def test_opportunity_universe_seeds_expandable_templates(tmp_path: Path):
    registry = OpportunityUniverseRegistry(db_path=tmp_path / "registry.db")

    seeded = registry.seed_defaults()
    summary = registry.registry_summary()

    assert seeded["sectors"] >= 12
    assert summary["counts"]["sector_registry"] >= 12
    assert summary["counts"]["theme_registry"] == summary["counts"]["sector_registry"]
    assert summary["counts"]["thesis_registry"] == summary["counts"]["sector_registry"]
    assert summary["counts"]["event_template_registry"] == summary["counts"]["sector_registry"]

    sectors = registry.list_sectors()
    ai = next(item for item in sectors if item["sector_id"] == "ai_compute_infra")
    assert "NVDA" in ai["lead_assets"]
    assert "至少 2 个独立来源" in ai["source_requirements"]
