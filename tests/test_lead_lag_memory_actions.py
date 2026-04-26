from __future__ import annotations

from pathlib import Path

from app.services.lead_lag_memory_actions import build_research_memory_actions


def test_build_research_memory_actions_classifies_and_maps_opportunities():
    payload = {
        "status": "ready",
        "source": "obsidian_vault",
        "cache_status": "live",
        "recent_notes": [
            {
                "title": "AI 策略主线与观察池",
                "path": "20-项目/AI/ai_thesis.md",
                "tracked_tags": ["策略", "观察池"],
                "modified_at": "2026-04-26T08:00:00",
            },
            {
                "title": "半导体复盘：命中成功案例",
                "path": "30-研究/半导体/win.md",
                "tracked_tags": ["复盘"],
                "modified_at": "2026-04-25T20:00:00",
            },
            {
                "title": "光伏复盘：失败回撤复盘",
                "path": "30-研究/光伏/failure.md",
                "tracked_tags": ["复盘"],
                "modified_at": "2026-04-25T18:30:00",
            },
            {
                "title": "猪周期风控陷阱：追高后失效",
                "path": "10-事实/猪周期/trap.md",
                "tracked_tags": ["风控"],
                "modified_at": "2026-04-24T21:00:00",
            },
            {
                "title": "创新药对标案例资源库",
                "path": "10-事实/创新药/similar.md",
                "tracked_tags": ["资源"],
                "modified_at": "2026-04-24T10:00:00",
            },
            {
                "title": "半导体午盘复盘纪要",
                "path": "30-研究/半导体/review.md",
                "tracked_tags": ["复盘"],
                "modified_at": "2026-04-24T09:00:00",
            },
        ],
    }
    cards = [
        {"opportunity_id": "opp-semi", "thesis": "半导体设备国产替代", "sector_key": "半导体"},
        {"opportunity_id": "opp-hog", "thesis": "猪周期反转", "sector_key": "猪周期"},
    ]

    result = build_research_memory_actions(payload, opportunity_cards=cards)

    assert result["source"] == "obsidian_vault"
    assert result["cache_status"] == "live"
    assert any("策略主线" in row["title"] for row in result["thesis_summary"])
    assert any("命中成功" in row["title"] for row in result["prior_wins"])
    assert any("失败回撤" in row["title"] for row in result["prior_failures"])
    assert any("陷阱" in row["title"] for row in result["typical_trap"])
    assert any("对标案例" in row["title"] for row in result["similar_cases"])
    assert any("复盘" in row["title"] for row in result["review_notes"])
    assert any(row["opportunity_id"] == "opp-semi" for row in result["mapped_opportunities"])
    assert result["last_update"] == "2026-04-26T08:00:00"


def test_build_research_memory_actions_missing_obsidian_returns_sample_fallback():
    payload = {
        "status": "missing",
        "source": "sample_data",
        "themes": ["AI", "创新药", "半导体"],
        "signals": [
            "AI 先看硬件和互联，不追最拥挤应用层",
            "创新药只保留官方事件链",
        ],
    }

    result = build_research_memory_actions(payload, opportunity_cards=None)

    assert result["source"] == "sample_data"
    assert result["cache_status"] == "sample_fallback"
    assert result["thesis_summary"]
    assert "prior_wins" in result["missing_memory"]
    assert "prior_failures" in result["missing_memory"]
    assert result["mapped_opportunities"] == []


def test_build_research_memory_actions_sector_filter_works():
    payload = {
        "status": "ready",
        "source": "obsidian_vault",
        "theme_matches": [
            {
                "title": "AI 观察池：推理算力",
                "path": "20-项目/AI/watch.md",
                "tracked_tags": ["观察池"],
                "modified_at": "2026-04-26T07:00:00",
            },
            {
                "title": "半导体策略：设备与封测验证",
                "path": "20-项目/半导体/thesis.md",
                "tracked_tags": ["策略"],
                "modified_at": "2026-04-26T07:30:00",
            },
        ],
    }
    cards = [
        {"opportunity_id": "ai-1", "thesis": "AI 链条", "sector": "AI"},
        {"opportunity_id": "semi-1", "thesis": "半导体设备", "sector": "半导体"},
    ]

    result = build_research_memory_actions(payload, opportunity_cards=cards, sector="半导体")

    titles = [row["title"] for row in result["thesis_summary"]]
    assert titles == ["半导体策略：设备与封测验证"]
    assert [row["opportunity_id"] for row in result["mapped_opportunities"]] == ["semi-1"]


def test_setup_script_contains_required_task_names_and_slots():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "setup_lead_lag_brief_tasks.ps1"
    text = script_path.read_text(encoding="utf-8")

    for task_name in ("LeadLagBrief0600", "LeadLagBrief0820", "LeadLagBrief1140", "LeadLagBrief1515", "LeadLagBrief2130"):
        assert task_name in text
    for slot in ("overnight_digest", "pre_open_playbook", "morning_review", "close_review", "us_watch_mapping"):
        assert slot in text
    assert "run_lead_lag_brief_task.ps1" in text
    assert "scripts\\$taskName.cmd" in text
    assert "Create brief task wrapper" in text
    assert "[string]$RepoRoot" in text
    assert "[string]$PythonExe" in text
    assert "[switch]$Force" in text
    assert "venv\\Scripts\\python.exe" in text
    assert "$command.Source" in text

    runner_text = (repo_root / "scripts" / "run_lead_lag_brief_task.ps1").read_text(encoding="utf-8")
    assert "--slot $Slot --markdown --json --obsidian" in runner_text
    assert "logs\\lead_lag_briefs" in runner_text
    assert "venv\\Scripts\\python.exe" in runner_text
    assert "$command.Source" in runner_text
    assert "0x77E5" in runner_text
    assert "INVESTMENT_OBSIDIAN_VAULT" in runner_text
