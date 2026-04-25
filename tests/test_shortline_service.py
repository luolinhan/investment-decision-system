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


def test_sync_fda_events(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    monkeypatch.setattr(
        service,
        "_fetch_openfda_recent_results",
        lambda days=30, limit=100: [
            {
                "application_number": "123456",
                "sponsor_name": "ELI LILLY AND COMPANY",
                "products": [{"brand_name": "Zepbound"}],
                "submissions": [
                    {
                        "submission_type": "ORIG",
                        "submission_number": "1",
                        "submission_status": "AP",
                        "submission_status_date": "20260420",
                    }
                ],
            }
        ],
    )

    result = service.sync_fda_events(days=30)
    assert result["ok"] is True
    assert result["created"] == 1

    events = service.list_events(limit=10)
    assert any(item["source_symbol"] == "LLY" and item["event_type"] == "fda_approval" for item in events)


def test_sync_clinical_trials_events(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    monkeypatch.setattr(
        service,
        "_fetch_clinical_trials_studies",
        lambda sponsor_alias, page_size=20: [
            {
                "protocolSection": {
                    "identificationModule": {
                        "nctId": "NCT12345678",
                        "briefTitle": "Moderna Phase 3 Flu Vaccine Study",
                        "organization": {"fullName": "ModernaTX, Inc."},
                    },
                    "sponsorCollaboratorsModule": {
                        "leadSponsor": {"name": "ModernaTX, Inc."},
                    },
                    "statusModule": {
                        "overallStatus": "RECRUITING",
                        "studyFirstPostDateStruct": {"date": "2026-04-20"},
                        "lastUpdatePostDateStruct": {"date": "2026-04-22"},
                    },
                    "designModule": {"phases": ["PHASE3"]},
                }
            }
        ],
    )

    result = service.sync_clinical_trials_events(days=30)
    assert result["ok"] is True
    assert result["created"] >= 1

    events = service.list_events(limit=10)
    assert any(item["source_symbol"] == "MRNA" and item["event_type"].startswith("clinical_trial_") for item in events)


def test_sync_company_ir_events(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "shortline.db"
    service = ShortlineService(db_path)

    rss_payload = b"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
    <rss version=\"2.0\">
      <channel>
        <item>
          <title>AMD Reports Fiscal First Quarter 2026 Results</title>
          <link>https://ir.amd.com/news-events/press-releases/detail/1288/amd-reports-fiscal-first-quarter-2026-results</link>
          <pubDate>Wed, 22 Apr 2026 16:15:00 -0400</pubDate>
          <description>AMD reported quarterly results and updated guidance for AI data center products.</description>
        </item>
      </channel>
    </rss>"""

    monkeypatch.setattr(service, "_fetch_company_ir_feed", lambda url: rss_payload)

    result = service.sync_company_ir_events(lookback_hours=9999, max_items_per_source=2)
    assert result["ok"] is True
    assert result["created"] >= 1

    events = service.list_events(limit=20)
    assert any(item["source_symbol"] == "AMD" and item["event_type"] == "company_ir_earnings" for item in events)
