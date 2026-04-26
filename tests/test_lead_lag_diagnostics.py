from __future__ import annotations

from app.services.lead_lag_diagnostics import build_replay_diagnostics, build_transmission_workspace


def _sample_bundle() -> dict:
    return {
        "as_of": "2026-04-26T09:00:00",
        "transmission_graph": {
            "nodes": [
                {"node_id": "nvda", "label": "NVIDIA", "node_type": "asset", "market": "US", "sector": "ai"},
                {"node_id": "a_share_optical", "label": "A Optical", "node_type": "sector", "market": "A", "sector": "ai"},
                {"node_id": "semi_anchor", "label": "Semi Anchor", "node_type": "asset", "market": "A", "sector": "semis"},
            ],
            "edges": [
                {"source": "nvda", "target": "a_share_optical", "relation": "supplier", "strength": 0.92, "lag_days": 1, "sector": "ai"},
                {"source": "semi_anchor", "target": "nvda", "relation": "invalidates", "strength": 0.31, "lag_min_days": 2, "lag_max_days": 6, "sector": "semis"},
            ],
        },
        "replay_stats": [
            {
                "scenario_key": "ai_compute_spillover",
                "sector_key": "ai",
                "regime_key": "risk_on",
                "cases": 12,
                "hit_rate": 0.75,
                "win_rate": 0.66,
                "avg_lead_days": 2.0,
                "net_alpha_bps": 120,
                "failure_mode": "validation_lag",
            },
            {
                "scenario_key": "ai_crowded_reversal",
                "sector_key": "ai",
                "regime_key": "risk_off",
                "cases": 7,
                "hit_rate": 0.42,
                "win_rate": 0.38,
                "avg_lead_days": 5.0,
                "net_alpha_bps": -40,
                "failure_mode": "crowding_spike",
            },
            {
                "scenario_key": "semi_false_breakout",
                "sector_key": "semis",
                "regime_key": "risk_off",
                "cases": 4,
                "hit_rate": 0.35,
                "win_rate": 0.31,
                "avg_lead_days": 10.0,
                "net_alpha_bps": -80,
                "failure_mode": "crowding_spike",
            },
        ],
    }


def _sample_cards() -> list[dict]:
    return [
        {
            "id": "ai_first",
            "sector": "ai",
            "baton_stage": "first_baton",
            "generation_status": "actionable",
            "decision_priority_score": 88,
            "expected_lag_days": {"min": 1, "max": 3},
            "historical_replay_summary": {"hit_rate": 0.74, "worst_failure_mode": "validation_lag"},
            "crowding_state": {"label": "medium"},
            "local_asset": {"code": "300502.SZ", "name": "新易盛", "market": "A"},
            "cache_status": "sample_fallback",
            "last_update": "2026-04-26T08:00:00",
        },
        {
            "id": "ai_second",
            "sector": "ai",
            "baton_stage": "second_baton",
            "generation_status": "watch_only",
            "decision_priority_score": 77,
            "expected_lag_days": {"min": 3, "max": 5},
            "historical_replay_summary": {"hit_rate": 0.63, "worst_failure_mode": "crowding_spike"},
            "crowding_state": {"label": "crowded"},
            "local_asset": {"code": "002463.SZ", "name": "沪电股份", "market": "A"},
            "cache_status": "sample_fallback",
            "last_update": "2026-04-26T08:05:00",
        },
        {
            "id": "semi_invalid",
            "sector": "semis",
            "baton_stage": "third_baton",
            "generation_status": "invalidated",
            "decision_priority_score": 54,
            "expected_lag_days": {"min": 5, "max": 10},
            "historical_replay_summary": {"hit_rate": 0.32, "worst_failure_mode": "crowding_spike"},
            "crowding_state": {"label": "high"},
            "local_asset": {"code": "688981.SH", "name": "中芯国际", "market": "A"},
            "missing_confirmations": ["缺少订单确认"],
            "missing_evidence_reason": "缺少订单确认",
            "cache_status": "sample_fallback",
            "last_update": "2026-04-26T08:10:00",
        },
    ]


def test_transmission_workspace_edge_normalization_and_core_fields():
    payload = build_transmission_workspace(
        _sample_bundle(),
        _sample_cards(),
    )
    assert payload["edges"]
    edge = payload["edges"][0]
    required = {
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
        "status",
        "current_blocker",
    }
    assert required.issubset(edge.keys())
    assert payload["first_baton"][0]["opportunity_id"] == "ai_first"
    assert payload["second_baton"][0]["opportunity_id"] == "ai_second"
    assert payload["third_baton"][0]["opportunity_id"] == "semi_invalid"
    assert "sample_derived" in payload and payload["sample_derived"] is True


def test_transmission_workspace_sector_filter():
    payload = build_transmission_workspace(
        _sample_bundle(),
        _sample_cards(),
        sector="ai",
    )
    assert payload["first_baton"]
    assert all(row["sector"] == "ai" for row in payload["first_baton"])
    assert all(row["sector"] == "ai" for row in payload["second_baton"])
    assert all("semi" not in str(row.get("opportunity_id")) for row in payload["third_baton"])
    assert all(edge["from_id"] != "semi_anchor" for edge in payload["edges"])


def test_replay_diagnostics_missing_data_fallback_is_useful():
    payload = build_replay_diagnostics(
        {"as_of": "2026-04-26T09:00:00", "transmission_graph": {"nodes": [], "edges": []}, "replay_stats": []},
        _sample_cards(),
    )
    assert len(payload["horizon_distribution"]) == 5
    assert {item["horizon_days"] for item in payload["horizon_distribution"]} == {1, 3, 5, 10, 20}
    assert payload["sample_size"] == len(payload["opportunity_replay_map"])
    assert payload["failure_mode_ranking"]
    assert payload["source"] == "sample_data"
    assert payload["sample_derived"] is True


def test_replay_diagnostics_failure_mode_ranking_and_sector_filter():
    payload = build_replay_diagnostics(
        _sample_bundle(),
        _sample_cards(),
        sector="semis",
    )
    assert payload["sample_size"] == 4
    assert payload["failure_mode_ranking"][0]["failure_mode"] == "crowding_spike"
    assert all(item["sector"] == "semis" for item in payload["opportunity_replay_map"])
