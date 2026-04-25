"""Tests for shortline_service and sync script."""
import json
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from app.services.shortline_service import (
    ShortlineService,
    SEED_MAPPINGS,
    ensure_tables,
    seed_mappings,
)


@pytest.fixture()
def tmp_db():
    """Provide a clean temp SQLite DB, auto-cleaned."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture()
def svc(tmp_db):
    """ShortlineService backed by a temp database."""
    return ShortlineService(db_path=tmp_db)


# ===================================================================
# ensure_tables
# ===================================================================
class TestEnsureTables:
    def test_creates_three_tables(self, tmp_db):
        conn = ensure_tables(tmp_db)
        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        conn.close()
        assert "cross_market_mapping_master" in tables
        assert "cross_market_signal_events" in tables
        assert "cross_market_signal_candidates" in tables

    def test_idempotent(self, tmp_db):
        """Calling ensure_tables twice should not raise."""
        ensure_tables(tmp_db)
        ensure_tables(tmp_db)

    def test_has_indexes(self, tmp_db):
        conn = ensure_tables(tmp_db)
        indexes = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
            ).fetchall()
        ]
        conn.close()
        assert len(indexes) >= 3


# ===================================================================
# Seed mappings
# ===================================================================
class TestSeedMappings:
    def test_seed_count(self, svc):
        n = seed_mappings(svc.conn)
        assert n >= 30

    def test_seed_distinct_us_tickers(self, svc):
        seed_mappings(svc.conn)
        tickers = [
            r[0] for r in svc.conn.execute(
                "SELECT DISTINCT us_ticker FROM cross_market_mapping_master"
            ).fetchall()
        ]
        assert len(tickers) >= 15

    def test_seed_sectors_covered(self, svc):
        seed_mappings(svc.conn)
        sectors = {
            r[0] for r in svc.conn.execute(
                "SELECT DISTINCT sector FROM cross_market_mapping_master"
            ).fetchall()
        }
        required = {"AI", "半导体", "机器人", "创新药", "光伏", "核电", "养猪", "红利"}
        assert required.issubset(sectors)

    def test_seed_idempotent(self, svc):
        n1 = seed_mappings(svc.conn)
        n2 = seed_mappings(svc.conn)
        assert n1 == n2  # second call inserts 0 because of OR IGNORE

    def test_mapping_has_rationale(self, svc):
        seed_mappings(svc.conn)
        null_count = svc.conn.execute(
            "SELECT COUNT(*) FROM cross_market_mapping_master WHERE rationale IS NULL OR rationale=''"
        ).fetchone()[0]
        assert null_count == 0


# ===================================================================
# Service: get_overview
# ===================================================================
class TestGetOverview:
    def test_empty_overview(self, svc):
        ov = svc.get_overview()
        assert ov["mapping_count"] == 0
        assert ov["event_count"] == 0
        assert ov["candidate_count"] == 0

    def test_overview_after_seed(self, svc):
        seed_mappings(svc.conn)
        ov = svc.get_overview()
        assert ov["mapping_count"] >= 30
        assert len(ov["sectors"]) >= 5
        assert len(ov["us_tickers"]) >= 10


# ===================================================================
# Service: list_playbooks
# ===================================================================
class TestListPlaybooks:
    def test_playbooks_exist(self, svc):
        pbs = svc.list_playbooks()
        assert len(pbs) >= 4
        for pb in pbs:
            assert "signal_type" in pb
            assert "event_types" in pb
            assert "action" in pb
            assert "hold_days" in pb


# ===================================================================
# Service: list_events
# ===================================================================
class TestListEvents:
    def test_empty(self, svc):
        assert svc.list_events() == []

    def test_after_manual_event(self, svc):
        svc.conn.execute(
            """INSERT OR IGNORE INTO cross_market_signal_events
               (us_ticker, event_type, event_date, severity, detail_json)
               VALUES ('NVDA', 'price_breakout', '2026-04-24', 0.8, '{}')"""
        )
        svc.conn.commit()
        events = svc.list_events()
        assert len(events) == 1
        assert events[0]["us_ticker"] == "NVDA"

    def test_filter_by_type(self, svc):
        svc.conn.execute(
            """INSERT OR IGNORE INTO cross_market_signal_events
               (us_ticker, event_type, event_date, severity, detail_json)
               VALUES (?, ?, '2026-04-24', 0.7, '{}')""",
            ("MSFT", "sector_rotation"),
        )
        svc.conn.commit()
        events = svc.list_events(event_type="sector_rotation")
        assert len(events) == 1
        assert svc.list_events(event_type="price_breakout") == []


# ===================================================================
# Service: sync_us_market_events (offline simulation)
# ===================================================================
class TestSyncUsMarketEvents:
    def test_sync_with_yfinance_skip_when_unavailable(self, svc, monkeypatch):
        """If yfinance is None, sync should fail gracefully."""
        import scripts.sync_shortline_us_events as sync_mod
        monkeypatch.setattr(sync_mod, "yf", None)
        with pytest.raises(ImportError):
            svc.sync_us_market_events(tickers=["NVDA"])

    def test_detect_signals_empty_df(self):
        """Empty DataFrame yields no events."""
        import pandas as pd
        from scripts.sync_shortline_us_events import detect_signals
        df = pd.DataFrame(columns=["ticker", "date", "open", "high", "low", "close", "volume"])
        events = detect_signals(df, ["NVDA"])
        assert events == []


# ===================================================================
# Service: build_candidates
# ===================================================================
class TestBuildCandidates:
    def _setup_mapping_and_event(self, svc):
        svc.conn.execute(
            """INSERT OR IGNORE INTO cross_market_mapping_master
               (us_ticker, cn_code, cn_name, signal_type, sector, confidence, rationale)
               VALUES ('NVDA', 'sh688981', '中芯国际', 'GPU算力', '半导体', 0.95, 'test')"""
        )
        svc.conn.execute(
            """INSERT OR IGNORE INTO cross_market_signal_events
               (us_ticker, event_type, event_date, severity, detail_json)
               VALUES ('NVDA', 'price_breakout', '2026-04-24', 0.8, '{}')"""
        )
        svc.conn.commit()

    def test_generates_candidates(self, svc):
        self._setup_mapping_and_event(svc)
        result = svc.build_candidates()
        assert result["rows_joined"] >= 1
        assert result["candidates_inserted"] >= 1

    def test_candidate_has_priority(self, svc):
        self._setup_mapping_and_event(svc)
        svc.build_candidates()
        cands = svc.list_candidates()
        assert len(cands) >= 1
        for c in cands:
            assert c["execution_priority"] > 0
            assert "cn_code" in c
            assert "cn_name" in c
            assert "sector" in c

    def test_candidate_priority_formula(self, svc):
        """Verify priority = confidence*0.5 + severity*0.3 + style*0.2."""
        self._setup_mapping_and_event(svc)
        svc.build_candidates()
        cands = svc.list_candidates()
        c = cands[0]
        # confidence=0.95, severity=0.8, style(price_breakout)=1.0
        expected = round(0.95 * 0.5 + 0.8 * 0.3 + 1.0 * 0.2, 4)
        assert c["execution_priority"] == expected

    def test_filter_by_sector(self, svc):
        self._setup_mapping_and_event(svc)
        svc.build_candidates()
        semi = svc.list_candidates(sector="半导体")
        assert len(semi) >= 1
        ai = svc.list_candidates(sector="AI")
        assert ai == []


# ===================================================================
# Integration: full flow
# ===================================================================
class TestFullFlow:
    def test_seed_then_build(self, svc):
        """Seed mappings, inject events, build candidates, verify output."""
        seed_mappings(svc.conn)

        # Inject a few events for tickers we know exist in seed data
        for ticker, etype, severity in [
            ("NVDA", "price_breakout", 0.9),
            ("MSFT", "sector_rotation", 0.6),
            ("LLY", "earnings_spillover", 0.75),
        ]:
            svc.conn.execute(
                """INSERT OR IGNORE INTO cross_market_signal_events
                   (us_ticker, event_type, event_date, severity, detail_json)
                   VALUES (?, ?, '2026-04-24', ?, '{}')""",
                (ticker, etype, severity),
            )
        svc.conn.commit()

        result = svc.build_candidates()
        assert result["rows_joined"] >= 3

        cands = svc.list_candidates(min_priority=0.5)
        assert len(cands) >= 1
        # highest priority should be NVDA+price_breakout
        top = cands[0]
        assert top["us_ticker"] == "NVDA"
        assert top["execution_priority"] >= 0.8
