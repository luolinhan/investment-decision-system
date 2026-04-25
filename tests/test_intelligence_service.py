"""Tests for intelligence_service sorting and filtering behavior."""
import os
import sqlite3
import tempfile

import pytest

from app.services.intelligence_service import IntelligenceService
from app.services.research_workbench_service import ResearchWorkbenchService


@pytest.fixture()
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture()
def svc(tmp_db):
    return IntelligenceService(db_path=tmp_db)


def insert_event(conn, event_key, title, category, priority, event_time, last_seen_at):
    conn.execute(
        """
        INSERT INTO intelligence_events (
            event_key, title, category, priority, event_time, last_seen_at,
            first_seen_at, status, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 0.5)
        """,
        (event_key, title, category, priority, event_time, last_seen_at, last_seen_at),
    )


def insert_report(conn, report_key, title, published_at, fetched_at=None):
    conn.execute(
        """
        INSERT INTO research_reports (
            report_key, title, url, source_key, source_name, published_at, fetched_at, status
        ) VALUES (?, ?, ?, 'test_source', 'Test Source', ?, ?, 'active')
        """,
        (report_key, title, f"https://example.com/{report_key}", published_at, fetched_at or published_at),
    )


class TestListEventsSorting:
    """Verify events are sorted strictly by time descending."""

    def test_sorts_by_event_time_desc_not_priority(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn,
            "evt_new_p2",
            "New P2 Event",
            "macro",
            "P2",
            "2026-04-25T10:00:00",
            "2026-04-25T10:00:00",
        )
        insert_event(
            conn,
            "evt_old_p0",
            "Old P0 Event",
            "macro",
            "P0",
            "2026-04-24T10:00:00",
            "2026-04-24T10:00:00",
        )
        conn.commit()
        conn.close()

        events = svc.list_events()
        assert len(events) == 2
        assert events[0]["event_key"] == "evt_new_p2"
        assert events[1]["event_key"] == "evt_old_p0"

    def test_fallback_to_last_seen_at_when_no_event_time(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn,
            "evt_no_time_new",
            "No Event Time New",
            "policy",
            "P1",
            None,
            "2026-04-25T08:00:00",
        )
        insert_event(
            conn,
            "evt_with_time_old",
            "With Event Time Old",
            "policy",
            "P1",
            "2026-04-24T08:00:00",
            "2026-04-24T08:00:00",
        )
        conn.commit()
        conn.close()

        events = svc.list_events()
        assert events[0]["event_key"] == "evt_no_time_new"

    def test_sorts_mixed_time_formats_by_actual_timestamp(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn,
            "evt_apr_22",
            "Apr 22 Event",
            "macro",
            "P1",
            "Wed, 22 Apr 2026 12:00:00 +0000",
            "2026-04-22T12:00:00+00:00",
        )
        insert_event(
            conn,
            "evt_mar_25",
            "Mar 25 Event",
            "macro",
            "P1",
            "Wed, 25 Mar 2026 11:24:47 EDT",
            "2026-03-25T15:24:47+00:00",
        )
        conn.commit()
        conn.close()

        events = svc.list_events()
        assert [item["event_key"] for item in events[:2]] == ["evt_apr_22", "evt_mar_25"]


class TestListEventsFiltering:
    """Verify priority and category filters work correctly."""

    def test_filters_by_priority(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn, "evt_p0", "P0 Event", "macro", "P0", "2026-04-25T09:00:00", "2026-04-25T09:00:00"
        )
        insert_event(
            conn, "evt_p1", "P1 Event", "macro", "P1", "2026-04-25T08:00:00", "2026-04-25T08:00:00"
        )
        insert_event(
            conn, "evt_p2", "P2 Event", "macro", "P2", "2026-04-25T07:00:00", "2026-04-25T07:00:00"
        )
        conn.commit()
        conn.close()

        p0_events = svc.list_events(priority="P0")
        assert len(p0_events) == 1
        assert p0_events[0]["priority"] == "P0"

        p1_events = svc.list_events(priority="p1")
        assert len(p1_events) == 1
        assert p1_events[0]["priority"] == "P1"

    def test_filters_by_category(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn, "evt_macro", "Macro Event", "macro", "P2", "2026-04-25T09:00:00", "2026-04-25T09:00:00"
        )
        insert_event(
            conn, "evt_policy", "Policy Event", "policy", "P2", "2026-04-25T08:00:00", "2026-04-25T08:00:00"
        )
        conn.commit()
        conn.close()

        macro_events = svc.list_events(category="macro")
        assert len(macro_events) == 1
        assert macro_events[0]["category"] == "macro"

    def test_combined_filters(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_event(
            conn, "evt1", "Event1", "macro", "P0", "2026-04-25T06:00:00", "2026-04-25T06:00:00"
        )
        insert_event(
            conn, "evt2", "Event2", "macro", "P1", "2026-04-25T07:00:00", "2026-04-25T07:00:00"
        )
        insert_event(
            conn, "evt3", "Event3", "policy", "P1", "2026-04-25T08:00:00", "2026-04-25T08:00:00"
        )
        conn.commit()
        conn.close()

        events = svc.list_events(priority="P1", category="macro")
        assert len(events) == 1
        assert events[0]["event_key"] == "evt2"


class TestListEventsLimits:
    """Verify limit parameter behavior."""

    def test_limit_is_respected(self, svc):
        conn = sqlite3.connect(svc.db_path)
        for i in range(10):
            insert_event(
                conn,
                f"evt_{i}",
                f"Event {i}",
                "macro",
                "P2",
                f"2026-04-25T{i:02d}:00:00",
                f"2026-04-25T{i:02d}:00:00",
            )
        conn.commit()
        conn.close()

        events = svc.list_events(limit=5)
        assert len(events) == 5


class TestResearchSorting:
    """Verify research surfaces sort mixed date formats chronologically."""

    def test_intelligence_research_list_sorts_mixed_formats(self, svc):
        conn = sqlite3.connect(svc.db_path)
        insert_report(conn, "report_apr_22", "April Report", "Wed, 22 Apr 2026 12:00:00 +0000")
        insert_report(conn, "report_mar_25", "March Report", "Wed, 25 Mar 2026 11:24:47 EDT")
        conn.commit()
        conn.close()

        reports = svc.list_research(limit=2)
        assert [item["report_key"] for item in reports] == ["report_apr_22", "report_mar_25"]

    def test_research_workbench_sorts_mixed_formats(self, tmp_db):
        svc = IntelligenceService(db_path=tmp_db)
        conn = sqlite3.connect(tmp_db)
        insert_report(conn, "workbench_apr_22", "April Report", "Wed, 22 Apr 2026 12:00:00 +0000")
        insert_report(conn, "workbench_mar_25", "March Report", "Wed, 25 Mar 2026 11:24:47 EDT")
        conn.commit()
        conn.close()

        workbench = ResearchWorkbenchService(db_path=tmp_db)
        reports = workbench.list_reports(limit=2)
        assert [item["report_key"] for item in reports] == ["workbench_apr_22", "workbench_mar_25"]
