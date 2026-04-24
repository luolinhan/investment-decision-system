"""Radar data store built on DuckDB with Parquet export.

Provides schema management, upsert helpers, source-run tracking, and
one-click Parquet export for downstream analytics.

Default paths (relative to project root):
    data/radar/radar.duckdb
    data/radar/parquet/
"""
import json
import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import duckdb

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "data", "radar", "radar.duckdb")
DEFAULT_PARQUET_DIR = os.path.join(BASE_DIR, "data", "radar", "parquet")

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS indicator_catalog (
        indicator_code   TEXT PRIMARY KEY,
        category         TEXT,
        indicator_type   TEXT,
        frequency        TEXT,        -- monthly / quarterly / weekly / daily
        direction        TEXT,        -- leading / coincident / lagging
        half_life_days   REAL,
        affected_assets  TEXT,        -- JSON array
        affected_sectors TEXT,        -- JSON array
        source           TEXT,
        confidence       REAL,        -- 0.0 – 1.0
        last_update      TEXT,
        status           TEXT,        -- active / planned / disabled
        notes            TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS indicator_observations (
        indicator_code  TEXT NOT NULL REFERENCES indicator_catalog(indicator_code),
        obs_date        TEXT NOT NULL,  -- ISO date (YYYY-MM-DD or YYYY-MM)
        source          TEXT NOT NULL DEFAULT '',
        value           REAL,
        unit            TEXT,
        fetch_ts        TEXT,
        quality_flag    TEXT,           -- good / estimated / stale / missing
        notes           TEXT,
        PRIMARY KEY (indicator_code, obs_date, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS source_runs (
        id              BIGINT PRIMARY KEY,
        source_name     TEXT NOT NULL,
        target_table    TEXT NOT NULL,
        started_at      TEXT NOT NULL,
        finished_at     TEXT,
        status          TEXT,           -- success / partial / failed
        rows_read       INTEGER,
        rows_upserted   INTEGER,
        error_message   TEXT,
        notes           TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_obs_code_date
    ON indicator_observations(indicator_code, obs_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_source_runs_table
    ON source_runs(target_table)
    """,
    """
    CREATE SEQUENCE IF NOT EXISTS source_run_seq START WITH 1
    """,
]


class RadarStore:
    """Thin wrapper around DuckDB for the radar subsystem."""

    def __init__(self, db_path: Optional[str] = None, parquet_dir: Optional[str] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.parquet_dir = parquet_dir or DEFAULT_PARQUET_DIR

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return a DuckDB connection, creating parent dirs if needed."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = duckdb.connect(self.db_path)
        # Allow string-parsed JSON for JSON columns
        conn.execute("SET TimeZone='UTC'")
        return conn

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create all radar tables and indexes if they don't exist."""
        conn = self.get_connection()
        try:
            for ddl in DDL_STATEMENTS:
                conn.execute(ddl.strip())
            logger.info("Schema ensured at %s", self.db_path)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Upsert helpers
    # ------------------------------------------------------------------

    def upsert_indicator_catalog(self, rows: Iterable[Dict[str, Any]]) -> int:
        """Insert or replace rows into indicator_catalog.

        Returns the number of rows upserted.
        """
        conn = self.get_connection()
        count = 0
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute("BEGIN")
            for row in rows:
                affected_assets = row.get("affected_assets")
                if isinstance(affected_assets, list):
                    affected_assets = json.dumps(affected_assets, ensure_ascii=False)
                affected_sectors = row.get("affected_sectors")
                if isinstance(affected_sectors, list):
                    affected_sectors = json.dumps(affected_sectors, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO indicator_catalog
                    (indicator_code, category, indicator_type, frequency,
                     direction, half_life_days, affected_assets, affected_sectors,
                     source, confidence, last_update, status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["indicator_code"],
                        row.get("category"),
                        row.get("indicator_type"),
                        row.get("frequency"),
                        row.get("direction"),
                        row.get("half_life_days"),
                        affected_assets,
                        affected_sectors,
                        row.get("source"),
                        row.get("confidence"),
                        row.get("last_update", now),
                        row.get("status", "active"),
                        row.get("notes"),
                    ),
                )
                count += 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        logger.info("Upserted %d rows into indicator_catalog", count)
        return count

    def upsert_indicator_observations(self, rows: Iterable[Dict[str, Any]]) -> int:
        """Insert or replace observation rows.

        Uses composite PK (indicator_code, obs_date, source) for idempotency.
        Returns the number of rows upserted.
        """
        conn = self.get_connection()
        count = 0
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn.execute("BEGIN")
            for row in rows:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO indicator_observations
                    (indicator_code, obs_date, source, value, unit, fetch_ts, quality_flag, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["indicator_code"],
                        row["obs_date"],
                        row.get("source", ""),
                        row.get("value"),
                        row.get("unit"),
                        row.get("fetch_ts", now),
                        row.get("quality_flag", "good"),
                        row.get("notes"),
                    ),
                )
                count += 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        logger.info("Upserted %d rows into indicator_observations", count)
        return count

    # ------------------------------------------------------------------
    # Source-run tracking
    # ------------------------------------------------------------------

    def record_source_run(
        self,
        source_name: str,
        target_table: str,
        started_at: str,
        finished_at: Optional[str] = None,
        status: str = "success",
        rows_read: Optional[int] = None,
        rows_upserted: Optional[int] = None,
        error_message: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Log a source-run record. Returns the inserted id."""
        conn = self.get_connection()
        try:
            next_id = conn.execute("SELECT NEXTVAL('source_run_seq')").fetchone()[0]
            conn.execute(
                """
                INSERT INTO source_runs
                (id, source_name, target_table, started_at, finished_at, status,
                 rows_read, rows_upserted, error_message, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_id,
                    source_name,
                    target_table,
                    started_at,
                    finished_at,
                    status,
                    rows_read,
                    rows_upserted,
                    error_message,
                    notes,
                ),
            )
            run_id = next_id
            logger.info("Recorded source run #%s for %s -> %s (%s)", run_id, source_name, target_table, status)
            return run_id  # type: ignore[return-value]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Parquet export
    # ------------------------------------------------------------------

    def export_table_to_parquet(self, table_name: str) -> str:
        """Export a radar table to a Parquet file. Returns the file path."""
        conn = self.get_connection()
        os.makedirs(self.parquet_dir, exist_ok=True)
        dest = os.path.join(self.parquet_dir, f"{table_name}.parquet")
        try:
            conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{dest}' (FORMAT 'parquet')")
            logger.info("Exported %s -> %s", table_name, dest)
        finally:
            conn.close()
        return dest
