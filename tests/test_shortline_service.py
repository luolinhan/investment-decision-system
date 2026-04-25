from __future__ import annotations

from pathlib import Path

from app.services.shortline_service import ShortlineService


def test_shortline_tables_and_seed_data(tmp_path: Path):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    overview = service.get_overview()
    assert overview["metrics"]["mapping_total"] >= 30
    assert len(service.list_playbooks()) >= 8


def test_shortline_candidate_generation(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    def fake_fetch(symbol: str, range_value: str = "5d", interval: str = "1d"):
        return {
            "symbol": symbol,
            "regular_market_price": 110.0,
            "prev_close": 100.0,
            "quote_time": "2026-04-25 06:30:00",
            "change_pct": 10.0,
            "volume_ratio": 2.1,
            "history": [
                {"date": "2026-04-21", "close": 96.0, "volume": 90},
                {"date": "2026-04-22", "close": 98.0, "volume": 95},
                {"date": "2026-04-23", "close": 101.0, "volume": 100},
                {"date": "2026-04-24", "close": 100.0, "volume": 100},
                {"date": "2026-04-25", "close": 110.0, "volume": 210},
            ],
        }

    monkeypatch.setattr(service, "_fetch_quote_series", fake_fetch)
    monkeypatch.setattr(service.workbench, "get_market_regime", lambda: {"label": "risk_on", "score": 24, "reasons": []})

    sync_result = service.sync_us_market_events(symbols=["NVDA", "TSLA"], max_items=2)
    assert sync_result["ok"] is True
    assert sync_result["created"] >= 2

    build_result = service.build_candidates()
    assert build_result["ok"] is True
    candidates = service.list_candidates(limit=20)
    assert candidates
    assert any(item["source_symbol"] == "NVDA" for item in candidates)
    assert any(item["priority"] in {"P0", "P1"} for item in candidates)


def test_sync_sec_filings(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    monkeypatch.setattr(
        service,
        "_fetch_sec_ticker_map",
        lambda: {"AMD": {"cik": "0000002488", "title": "Advanced Micro Devices Inc"}},
    )
    monkeypatch.setattr(
        service,
        "_fetch_sec_submissions",
        lambda cik: {
            "filings": {
                "recent": {
                    "form": ["8-K"],
                    "filingDate": ["2026-04-24"],
                    "accessionNumber": ["0000002488-26-000001"],
                    "primaryDocument": ["amd8k.htm"],
                    "primaryDocDescription": ["Current report, earnings results for the quarter"],
                    "items": ["2.02"],
                    "acceptanceDateTime": ["20260424123045"],
                }
            }
        },
    )

    result = service.sync_sec_filings(symbols=["AMD"], days=14, max_companies=1)
    assert result["ok"] is True
    assert result["created"] == 1

    events = service.list_events(limit=10)
    assert any(item["source_symbol"] == "AMD" and item["source_tier"] == "T0" for item in events)
