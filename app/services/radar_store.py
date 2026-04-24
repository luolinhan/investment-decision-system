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

EXPECTED_COLUMNS = {
    "indicator_catalog": {"indicator_code", "category", "indicator_type", "frequency", "direction"},
    "indicator_observations": {"indicator_code", "obs_date", "source", "value"},
    "source_runs": {"id", "source_name", "target_table", "started_at", "status"},
}


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
            legacy_tables = self._prepare_legacy_tables(conn)
            for ddl in DDL_STATEMENTS:
                conn.execute(ddl.strip())
            self._migrate_legacy_tables(conn, legacy_tables)
            logger.info("Schema ensured at %s", self.db_path)
        finally:
            conn.close()

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])

    def _table_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
        if not self._table_exists(conn, table_name):
            return set()
        rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {row[1] for row in rows}

    def _prepare_legacy_tables(self, conn: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        legacy_tables: Dict[str, str] = {}
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        for table_name, expected in EXPECTED_COLUMNS.items():
            columns = self._table_columns(conn, table_name)
            if not columns:
                continue
            if expected.issubset(columns):
                continue
            backup_name = f"{table_name}_legacy_{timestamp}"
            conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_name}")
            legacy_tables[table_name] = backup_name
            logger.warning("Renamed legacy table %s -> %s for schema repair", table_name, backup_name)
        return legacy_tables

    def _migrate_legacy_tables(self, conn: duckdb.DuckDBPyConnection, legacy_tables: Dict[str, str]) -> None:
        legacy_catalog = legacy_tables.get("indicator_catalog")
        if legacy_catalog:
            cols = self._table_columns(conn, legacy_catalog)
            key_col = "indicator_key" if "indicator_key" in cols else "indicator_code"
            type_col = "type" if "type" in cols else "indicator_type"
            conn.execute(
                f"""
                INSERT OR REPLACE INTO indicator_catalog
                (indicator_code, category, indicator_type, frequency, direction,
                 half_life_days, affected_assets, affected_sectors, source, confidence,
                 last_update, status, notes)
                SELECT {key_col}, category, {type_col}, frequency, direction,
                       half_life_days, affected_assets, affected_sectors, source, confidence,
                       COALESCE(last_update, ?), COALESCE(status, 'planned'), notes
                FROM {legacy_catalog}
                WHERE {key_col} IS NOT NULL
                """,
                [datetime.now(timezone.utc).isoformat()],
            )
            logger.info("Migrated legacy indicator_catalog from %s", legacy_catalog)

        legacy_obs = legacy_tables.get("indicator_observations")
        if legacy_obs:
            cols = self._table_columns(conn, legacy_obs)
            key_col = "indicator_key" if "indicator_key" in cols else "indicator_code"
            date_col = "observation_date" if "observation_date" in cols else "obs_date"
            notes_expr = "notes" if "notes" in cols else "value_text" if "value_text" in cols else "metadata" if "metadata" in cols else "NULL"
            fetch_expr = "fetch_ts" if "fetch_ts" in cols else "updated_at" if "updated_at" in cols else "CURRENT_TIMESTAMP"
            quality_expr = "quality_flag" if "quality_flag" in cols else "'estimated'"
            conn.execute(
                f"""
                INSERT OR REPLACE INTO indicator_observations
                (indicator_code, obs_date, source, value, unit, fetch_ts, quality_flag, notes)
                SELECT {key_col}, {date_col}, COALESCE(source, ''), value, unit,
                       {fetch_expr}, {quality_expr}, {notes_expr}
                FROM {legacy_obs}
                WHERE {key_col} IS NOT NULL AND {date_col} IS NOT NULL
                """
            )
            logger.info("Migrated legacy indicator_observations from %s", legacy_obs)

        legacy_runs = legacy_tables.get("source_runs")
        if legacy_runs:
            cols = self._table_columns(conn, legacy_runs)
            run_at_col = "run_at" if "run_at" in cols else "started_at"
            source_name_col = "source_name" if "source_name" in cols else "source_key"
            notes_col = "notes" if "notes" in cols else "error_message"
            rows_read_col = "records_found" if "records_found" in cols else "rows_read"
            rows_upserted_col = "records_added" if "records_added" in cols else "rows_upserted"
            target_col = "target_table" if "target_table" in cols else "'unknown'"
            conn.execute(
                f"""
                INSERT INTO source_runs
                (id, source_name, target_table, started_at, finished_at, status,
                 rows_read, rows_upserted, error_message, notes)
                SELECT NEXTVAL('source_run_seq'), {source_name_col}, {target_col},
                       {run_at_col}, {run_at_col}, COALESCE(status, 'success'),
                       {rows_read_col}, {rows_upserted_col}, NULL, {notes_col}
                FROM {legacy_runs}
                """
            )
            logger.info("Migrated legacy source_runs from %s", legacy_runs)

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
