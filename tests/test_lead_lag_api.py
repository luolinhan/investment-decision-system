from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_lead_lag_page_and_section_endpoints():
    page = client.get("/investment/lead-lag")
    assert page.status_code == 200
    assert "领先-传导 Alpha 引擎" in page.text
    assert "决策中心" in page.text
    assert "机会队列" in page.text
    assert "不要追高" in page.text
    assert "模型发现分组" in page.text

    overview = client.get("/investment/api/lead-lag/overview")
    assert overview.status_code == 200
    overview_payload = overview.json()
    assert overview_payload["headline"] == "领先-传导 Alpha 引擎 V1"
    assert overview_payload["source"] in {"live_fusion", "sample_data", "fallback"}
    if overview_payload["source"] == "live_fusion":
        assert "live_source_health" in overview_payload

    section_paths = {
        "models": "name",
        "opportunities": "title",
        "cross-market-map": "name",
        "industry-transmission": "name",
        "liquidity": "name",
        "sector-thesis": "name",
        "events-calendar": "title",
        "replay-validation": "title",
        "obsidian-memory": "title",
    }

    for path, key in section_paths.items():
        response = client.get(f"/investment/api/lead-lag/{path}")
        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload, list)
        assert payload
        assert key in payload[0]

    opportunities = client.get("/investment/api/lead-lag/opportunities").json()
    assert any(item.get("asset_code") and item.get("asset_name") for item in opportunities)


def test_lead_lag_v2_operator_endpoints():
    decision = client.get("/investment/api/lead-lag/decision-center")
    assert decision.status_code == 200
    decision_payload = decision.json()
    assert "main_conclusion" in decision_payload
    assert "top_directions" in decision_payload

    queue = client.get("/investment/api/lead-lag/opportunity-queue")
    assert queue.status_code == 200
    queue_payload = queue.json()
    assert queue_payload["cards"]
    first_card = queue_payload["cards"][0]
    assert first_card["why_now"]
    assert first_card["baton_stage"]
    assert first_card["invalidation_rules"]
    assert first_card["historical_replay_summary"]
    assert first_card["decision_chain"]["result"]
    assert first_card["stock_pool"][0]["name"]
    assert queue_payload["model_groups"]

    frontline = client.get("/investment/api/lead-lag/event-frontline")
    assert frontline.status_code == 200
    frontline_payload = frontline.json()
    assert frontline_payload["events"]
    assert all(item["event_class"] == "market-facing" for item in frontline_payload["events"])

    avoid = client.get("/investment/api/lead-lag/avoid-board")
    assert avoid.status_code == 200
    assert isinstance(avoid.json()["items"], list)

    changed = client.get("/investment/api/lead-lag/what-changed")
    assert changed.status_code == 200
    assert "new_signals" in changed.json()

    graph = client.get("/investment/api/lead-lag/transmission-workspace")
    assert graph.status_code == 200
    graph_payload = graph.json()
    assert "nodes" in graph_payload
    assert "edges" in graph_payload
    assert "current_bottlenecks" in graph_payload

    replay = client.get("/investment/api/lead-lag/replay-diagnostics")
    assert replay.status_code == 200
    replay_payload = replay.json()
    assert {item["horizon_days"] for item in replay_payload["horizon_distribution"]} == {1, 3, 5, 10, 20}
    assert "failure_mode_ranking" in replay_payload

    memory = client.get("/investment/api/lead-lag/research-memory/actions")
    assert memory.status_code == 200
    memory_payload = memory.json()
    assert "thesis_summary" in memory_payload
    assert "missing_memory" in memory_payload

    sector = client.get("/investment/api/lead-lag/sector-evidence")
    assert sector.status_code == 200
    sector_payload = sector.json()
    assert sector_payload["count"] >= 5
    assert all(item["evidence_layers"] for item in sector_payload["sectors"])
