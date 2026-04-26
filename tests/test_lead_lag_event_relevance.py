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


def test_event_frontline_defaults_to_market_facing(tmp_path: Path):
    service = _sample_service(tmp_path)

    payload = service.event_frontline(limit=20)

    assert payload["events"]
    assert payload["default_filter"] == "market-facing"
    assert all(event["event_class"] == "market-facing" for event in payload["events"])
    for event in payload["events"]:
        assert event["expected_path"]
        assert isinstance(event["expected_path"], list)
        assert event["expected_path"][0]["expected_lag_days"]["min"] >= 0
        assert event["invalidation"]
        assert isinstance(event["invalidation"], list)
        assert event["china_mapping_score"] >= service.v2_config["thresholds"]["event_market_china_mapping_min"]


def test_low_mapping_developer_noise_is_not_in_default_frontline(tmp_path: Path):
    service = _sample_service(tmp_path)
    service.bundle["events"].append(
        {
            "event_id": "developer_sdk_noise",
            "event_date": "2026-04-25",
            "title": "GitHub SDK release and benchmark leaderboard update",
            "sector_key": "developer_tools",
            "market": "US",
            "importance": "high",
            "priority_score": 99,
            "related_symbols": [],
            "source_url": "https://github.com/example/sdk",
        }
    )

    default_events = service.event_frontline(limit=50)["events"]
    all_events = service.event_frontline(limit=50, include_research_facing=True)["events"]
    developer_event = next(event for event in all_events if event["event_id"] == "developer_sdk_noise")

    assert developer_event["event_class"] == "research-facing"
    assert developer_event["noise_reason"] == "developer_ecosystem_without_china_mapping"
    assert all(event["event_id"] != "developer_sdk_noise" for event in default_events)
