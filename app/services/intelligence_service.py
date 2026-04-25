"""Persistent intelligence event and research library service."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from app.db import get_sqlite_connection

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "investment.db")


class IntelligenceService:
    """Read-only service for the intelligence hub.

    The collector scripts own writes. The web app reads persisted data only so
    slow foreign sites never block page rendering.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = get_sqlite_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _json_loads(value: Any, default: Any = None) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(value)
        except Exception:
            return default

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return {key: row[key] for key in row.keys()}

    @classmethod
    def _rows_to_dicts(cls, rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
        return [cls._row_to_dict(row) for row in rows]

    def _normalize_research_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        for field in ("focus_areas_json", "tags_json", "tickers_json", "key_points_json"):
            if field in item:
                item[field.replace("_json", "")] = self._json_loads(item.get(field), [])
        return item

    def ensure_tables(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with get_sqlite_connection(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS source_registry (
                    source_key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    category TEXT,
                    priority INTEGER DEFAULT 2,
                    credibility TEXT DEFAULT 'primary',
                    collection_method TEXT DEFAULT 'http',
                    enabled INTEGER DEFAULT 1,
                    cadence_minutes INTEGER DEFAULT 30,
                    last_checked_at TEXT,
                    last_success_at TEXT,
                    last_error TEXT,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS collection_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT,
                    records_found INTEGER DEFAULT 0,
                    records_added INTEGER DEFAULT 0,
                    records_updated INTEGER DEFAULT 0,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS raw_documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    canonical_url TEXT,
                    title TEXT,
                    title_zh TEXT,
                    published_at TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    language TEXT DEFAULT 'en',
                    content_hash TEXT,
                    summary TEXT,
                    summary_zh TEXT,
                    raw_text TEXT,
                    metadata_json TEXT,
                    status TEXT DEFAULT 'active'
                );

                CREATE TABLE IF NOT EXISTS intelligence_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    title_zh TEXT,
                    category TEXT NOT NULL,
                    priority TEXT DEFAULT 'P2',
                    status TEXT DEFAULT 'active',
                    confidence REAL DEFAULT 0.5,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    event_time TEXT,
                    summary TEXT,
                    summary_zh TEXT,
                    impact_summary TEXT,
                    impact_summary_zh TEXT,
                    impact_score REAL DEFAULT 0,
                    verification_status TEXT DEFAULT 'watching',
                    source_count INTEGER DEFAULT 0,
                    primary_source_url TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS event_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    fact_type TEXT,
                    label TEXT NOT NULL,
                    label_zh TEXT,
                    value TEXT NOT NULL,
                    value_zh TEXT,
                    unit TEXT,
                    source_url TEXT,
                    confidence REAL DEFAULT 0.7,
                    sort_order INTEGER DEFAULT 100,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(event_id, label, value, source_url)
                );

                CREATE TABLE IF NOT EXISTS event_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    entity_type TEXT,
                    name TEXT NOT NULL,
                    name_zh TEXT,
                    ticker TEXT,
                    market TEXT,
                    role TEXT,
                    role_zh TEXT,
                    relevance_score REAL DEFAULT 0.5,
                    UNIQUE(event_id, entity_type, name, role)
                );

                CREATE TABLE IF NOT EXISTS event_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    raw_document_id INTEGER,
                    source_key TEXT,
                    update_type TEXT DEFAULT 'source_seen',
                    title TEXT,
                    summary TEXT,
                    published_at TEXT,
                    url TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(event_id, url)
                );

                CREATE TABLE IF NOT EXISTS research_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_key TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    title_zh TEXT,
                    source_key TEXT,
                    source_name TEXT,
                    url TEXT NOT NULL,
                    report_type TEXT DEFAULT 'research',
                    publisher_region TEXT DEFAULT 'overseas',
                    source_tier TEXT DEFAULT 'research',
                    target_scope TEXT DEFAULT 'industry',
                    published_at TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    language TEXT DEFAULT 'en',
                    summary TEXT,
                    summary_zh TEXT,
                    thesis TEXT,
                    thesis_zh TEXT,
                    relevance TEXT,
                    relevance_zh TEXT,
                    focus_areas_json TEXT,
                    tags_json TEXT,
                    tickers_json TEXT,
                    key_points_json TEXT,
                    original_url TEXT,
                    original_asset_path TEXT,
                    original_asset_type TEXT,
                    original_asset_status TEXT DEFAULT 'pending',
                    original_downloaded_at TEXT,
                    status TEXT DEFAULT 'active'
                );

                CREATE TABLE IF NOT EXISTS research_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id INTEGER NOT NULL,
                    label TEXT,
                    value TEXT,
                    source_url TEXT,
                    sort_order INTEGER DEFAULT 100,
                    UNIQUE(report_id, label, value)
                );

                CREATE INDEX IF NOT EXISTS idx_intel_events_priority ON intelligence_events(priority, last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_intel_events_category ON intelligence_events(category, last_seen_at);
                CREATE INDEX IF NOT EXISTS idx_event_facts_event ON event_facts(event_id, sort_order);
                CREATE INDEX IF NOT EXISTS idx_event_entities_event ON event_entities(event_id, relevance_score);
                CREATE INDEX IF NOT EXISTS idx_raw_documents_source ON raw_documents(source_key, fetched_at);
                CREATE INDEX IF NOT EXISTS idx_collection_runs_source ON collection_runs(source_key, started_at);
                CREATE INDEX IF NOT EXISTS idx_research_reports_published ON research_reports(published_at);
                """
            )
            self._ensure_column(conn, "raw_documents", "title_zh", "TEXT")
            self._ensure_column(conn, "raw_documents", "summary_zh", "TEXT")
            self._ensure_column(conn, "intelligence_events", "impact_summary_zh", "TEXT")
            self._ensure_column(conn, "event_facts", "label_zh", "TEXT")
            self._ensure_column(conn, "event_facts", "value_zh", "TEXT")
            self._ensure_column(conn, "event_entities", "name_zh", "TEXT")
            self._ensure_column(conn, "event_entities", "role_zh", "TEXT")
            self._ensure_column(conn, "research_reports", "summary_zh", "TEXT")
            self._ensure_column(conn, "research_reports", "thesis_zh", "TEXT")
            self._ensure_column(conn, "research_reports", "relevance_zh", "TEXT")
            self._ensure_column(conn, "research_reports", "publisher_region", "TEXT")
            self._ensure_column(conn, "research_reports", "source_tier", "TEXT")
            self._ensure_column(conn, "research_reports", "target_scope", "TEXT")
            self._ensure_column(conn, "research_reports", "focus_areas_json", "TEXT")
            self._ensure_column(conn, "research_reports", "tags_json", "TEXT")
            self._ensure_column(conn, "research_reports", "tickers_json", "TEXT")
            self._ensure_column(conn, "research_reports", "key_points_json", "TEXT")
            self._ensure_column(conn, "research_reports", "original_url", "TEXT")
            self._ensure_column(conn, "research_reports", "original_asset_path", "TEXT")
            self._ensure_column(conn, "research_reports", "original_asset_type", "TEXT")
            self._ensure_column(conn, "research_reports", "original_asset_status", "TEXT")
            self._ensure_column(conn, "research_reports", "original_downloaded_at", "TEXT")
            conn.commit()

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def get_overview(self) -> Dict[str, Any]:
        with self._connect() as conn:
            event_counts = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT priority, COUNT(*) AS count
                    FROM intelligence_events
                    WHERE status = 'active'
                    GROUP BY priority
                    """
                ).fetchall()
            )
            category_counts = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT category, COUNT(*) AS count
                    FROM intelligence_events
                    WHERE status = 'active'
                    GROUP BY category
                    ORDER BY count DESC
                    """
                ).fetchall()
            )
            latest_events = self.list_events(limit=8)
            research = self.list_research(limit=6)
            research_count_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM research_reports
                WHERE status = 'active'
                """
            ).fetchone()
            source_health = self.list_sources()
            last_run_row = conn.execute(
                """
                SELECT started_at, finished_at, status, records_found, records_added, error
                FROM collection_runs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            last_run = self._row_to_dict(last_run_row) if last_run_row else None

        p0 = next((item["count"] for item in event_counts if item["priority"] == "P0"), 0)
        p1 = next((item["count"] for item in event_counts if item["priority"] == "P1"), 0)
        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "summary": {
                "active_events": sum(item["count"] for item in event_counts),
                "p0_events": p0,
                "p1_events": p1,
                "research_reports": int(research_count_row["count"] if research_count_row else 0),
                "enabled_sources": sum(1 for item in source_health if item.get("enabled")),
            },
            "event_counts": event_counts,
            "category_counts": category_counts,
            "latest_events": latest_events,
            "research": research,
            "source_health": source_health,
            "last_run": last_run,
        }

    def list_events(
        self,
        limit: int = 50,
        priority: Optional[str] = None,
        category: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 200))
        clauses = ["status = 'active'"]
        params: List[Any] = []
        if priority:
            clauses.append("priority = ?")
            params.append(priority.upper())
        if category:
            clauses.append("category = ?")
            params.append(category)
        params.append(limit)
        sql = f"""
            SELECT event_key, title, title_zh, category, priority, confidence,
                   first_seen_at, last_seen_at, event_time, summary, summary_zh,
                   impact_summary, impact_summary_zh, impact_score, verification_status, source_count,
                   primary_source_url
            FROM intelligence_events
            WHERE {' AND '.join(clauses)}
            ORDER BY
                COALESCE(event_time, last_seen_at, first_seen_at) DESC,
                id DESC
            LIMIT ?
        """
        with self._connect() as conn:
            return self._rows_to_dicts(conn.execute(sql, params).fetchall())

    def get_event(self, event_key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM intelligence_events
                WHERE event_key = ?
                """,
                (event_key,),
            ).fetchone()
            if not row:
                return None
            event = self._row_to_dict(row)
            event_id = event["id"]
            fact_rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT fact_type, label, label_zh, value, value_zh, unit, source_url, confidence, sort_order
                    FROM event_facts
                    WHERE event_id = ?
                    ORDER BY sort_order ASC, id ASC
                    """,
                    (event_id,),
                ).fetchall()
            )
            deduped_facts: List[Dict[str, Any]] = []
            fact_seen: Dict[tuple, Dict[str, Any]] = {}
            for fact in fact_rows:
                key = (fact.get("label"), fact.get("value"))
                if key not in fact_seen:
                    fact["source_count"] = 1
                    fact["source_urls"] = [fact.get("source_url")] if fact.get("source_url") else []
                    fact_seen[key] = fact
                    deduped_facts.append(fact)
                    continue
                existing = fact_seen[key]
                existing["source_count"] = int(existing.get("source_count") or 1) + 1
                if fact.get("source_url") and fact.get("source_url") not in existing.get("source_urls", []):
                    existing.setdefault("source_urls", []).append(fact.get("source_url"))
                if (fact.get("confidence") or 0) > (existing.get("confidence") or 0):
                    existing["confidence"] = fact.get("confidence")
            event["facts"] = deduped_facts
            event["entities"] = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT entity_type, name, name_zh, ticker, market, role, role_zh, relevance_score
                    FROM event_entities
                    WHERE event_id = ?
                    ORDER BY relevance_score DESC, id ASC
                    """,
                    (event_id,),
                ).fetchall()
            )
            event["updates"] = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT source_key, update_type, title, summary, published_at, url, created_at
                    FROM event_updates
                    WHERE event_id = ?
                    ORDER BY COALESCE(published_at, created_at) DESC, id DESC
                    """,
                    (event_id,),
                ).fetchall()
            )
            event["research"] = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT report_key, title, title_zh, source_name, url, report_type,
                           publisher_region, source_tier, target_scope,
                           published_at, summary, summary_zh, thesis, thesis_zh, relevance, relevance_zh,
                           focus_areas_json, tags_json, tickers_json, key_points_json,
                           original_url, original_asset_path, original_asset_type, original_asset_status, original_downloaded_at
                    FROM research_reports
                    WHERE status = 'active'
                      AND (
                        title LIKE '%' || ? || '%'
                        OR summary LIKE '%' || ? || '%'
                        OR relevance LIKE '%' || ? || '%'
                      )
                    ORDER BY COALESCE(published_at, fetched_at) DESC
                    LIMIT 10
                    """,
                    (event.get("title", "")[:24], event.get("title", "")[:24], event_key.split("_")[0]),
                ).fetchall()
            )
            event["research"] = [self._normalize_research_item(item) for item in event["research"]]
            return event

    def list_research(self, limit: int = 50) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 50), 200))
        with self._connect() as conn:
            rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT report_key, title, title_zh, source_name, source_key, url,
                           report_type, publisher_region, source_tier, target_scope,
                           published_at, fetched_at, language, summary,
                           summary_zh, thesis, thesis_zh, relevance, relevance_zh,
                           focus_areas_json, tags_json, tickers_json, key_points_json,
                           original_url, original_asset_path, original_asset_type, original_asset_status, original_downloaded_at
                    FROM research_reports
                    WHERE status = 'active'
                    ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            )
        return [self._normalize_research_item(item) for item in rows]

    def list_sources(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT source_key, name, source_type, url, category, priority,
                           credibility, collection_method, enabled, cadence_minutes,
                           last_checked_at, last_success_at, last_error, notes,
                           updated_at
                    FROM source_registry
                    ORDER BY enabled DESC, priority ASC, source_key ASC
                    """
                ).fetchall()
            )
        for row in rows:
            row["enabled"] = bool(row.get("enabled"))
        return rows
