"""Research workbench service for filtering, aggregating, and viewing research reports."""
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional

from app.db import get_sqlite_connection

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "investment.db")


def _detect_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names for *table* via PRAGMA table_info."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _has_col(columns: set[str], name: str) -> bool:
    return name in columns


def _safe_div(num: float, den: float, fallback: float = 0.0) -> float:
    return num / den if den != 0 else fallback


_EMPTY_OVERVIEW = {
    "total": 0,
    "active": 0,
    "overseas": 0,
    "domestic": 0,
    "bilingual_coverage": {"title_zh": 0, "title_zh_pct": 0.0, "summary_zh": 0, "summary_zh_pct": 0.0},
    "archive_coverage": {"total": 0, "active": 0, "archived": 0, "archive_pct": 0.0},
    "focus_distribution": [],
    "source_distribution": [],
    "latest_updated_at": None,
}


class ResearchWorkbenchService:
    """Read-only aggregator for research_reports / research_evidence."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = get_sqlite_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return (row["cnt"] if row else 0) > 0

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return {key: row[key] for key in row.keys()}

    @classmethod
    def _rows_to_dicts(cls, rows) -> List[Dict[str, Any]]:
        return [cls._row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------

    def get_overview(self) -> Dict[str, Any]:
        """Aggregate summary: totals, bilingual coverage, source distribution, etc."""
        with self._connect() as conn:
            if not self._table_exists(conn, "research_reports"):
                return dict(_EMPTY_OVERVIEW)
            columns = _detect_columns(conn, "research_reports")

            # Basic counts
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM research_reports"
            ).fetchone()
            total = row["total"] if row else 0

            # Active vs archived
            if _has_col(columns, "status"):
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM research_reports WHERE status = 'active'"
                ).fetchone()
                active = row["cnt"] if row else 0
                archived = total - active
            else:
                active = total
                archived = 0

            # Region split (publisher_region / language heuristic)
            overseas_expr = self._overseas_filter(columns)
            if overseas_expr:
                row = conn.execute(
                    f"SELECT COUNT(*) AS cnt FROM research_reports WHERE {overseas_expr}"
                ).fetchone()
                overseas = row["cnt"] if row else 0
            else:
                overseas = 0
            domestic = total - overseas

            # Bilingual coverage
            if _has_col(columns, "title_zh"):
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM research_reports WHERE title_zh IS NOT NULL AND title_zh != ''"
                ).fetchone()
                zh_title = row["cnt"] if row else 0
            else:
                zh_title = 0

            if _has_col(columns, "summary_zh"):
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM research_reports WHERE summary_zh IS NOT NULL AND summary_zh != ''"
                ).fetchone()
                zh_summary = row["cnt"] if row else 0
            else:
                zh_summary = 0

            # Source distribution
            source_rows = conn.execute(
                """
                SELECT source_name, source_key, COUNT(*) AS cnt
                FROM research_reports
                GROUP BY COALESCE(source_name, source_key)
                ORDER BY cnt DESC
                LIMIT 20
                """
            ).fetchall()
            source_distribution = [
                {"name": r["source_name"] or r["source_key"], "count": r["cnt"]}
                for r in source_rows
            ]

            # Focus area distribution (fallback to report_type)
            if _has_col(columns, "focus_area"):
                fa_rows = conn.execute(
                    """
                    SELECT focus_area, COUNT(*) AS cnt
                    FROM research_reports
                    WHERE focus_area IS NOT NULL AND focus_area != ''
                    GROUP BY focus_area
                    ORDER BY cnt DESC
                    LIMIT 20
                    """
                ).fetchall()
                focus_distribution = [
                    {"area": r["focus_area"], "count": r["cnt"]} for r in fa_rows
                ]
            else:
                rt_rows = conn.execute(
                    """
                    SELECT report_type, COUNT(*) AS cnt
                    FROM research_reports
                    WHERE report_type IS NOT NULL
                    GROUP BY report_type
                    ORDER BY cnt DESC
                    LIMIT 20
                    """
                ).fetchall()
                focus_distribution = [
                    {"area": r["report_type"], "count": r["cnt"]} for r in rt_rows
                ]

            # Latest update time
            row = conn.execute(
                "SELECT MAX(COALESCE(published_at, fetched_at)) AS latest FROM research_reports"
            ).fetchone()
            latest = row["latest"] if row else None

        bilingual_coverage = {
            "title_zh": zh_title,
            "title_zh_pct": round(_safe_div(zh_title * 100, max(1, total)), 1),
            "summary_zh": zh_summary,
            "summary_zh_pct": round(_safe_div(zh_summary * 100, max(1, total)), 1),
        }
        archive_coverage = {
            "total": total,
            "active": active,
            "archived": archived,
            "archive_pct": round(_safe_div(archived * 100, max(1, total)), 1),
        }

        return {
            "total": total,
            "active": active,
            "overseas": overseas,
            "domestic": domestic,
            "bilingual_coverage": bilingual_coverage,
            "archive_coverage": archive_coverage,
            "focus_distribution": focus_distribution,
            "source_distribution": source_distribution,
            "latest_updated_at": latest,
        }

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_reports(
        self,
        limit: int = 50,
        focus_area: Optional[str] = None,
        publisher_region: Optional[str] = None,
        target_scope: Optional[str] = None,
        report_type: Optional[str] = None,
        query: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return a filtered list of research reports."""
        limit = max(1, min(int(limit or 50), 500))
        with self._connect() as conn:
            if not self._table_exists(conn, "research_reports"):
                return []
            columns = _detect_columns(conn, "research_reports")

            clauses = ["1=1"]
            params: list = []

            if focus_area and _has_col(columns, "focus_area"):
                clauses.append("focus_area = ?")
                params.append(focus_area)

            if publisher_region:
                if _has_col(columns, "publisher_region"):
                    clauses.append("publisher_region = ?")
                    params.append(publisher_region)
                else:
                    clauses.append(self._region_filter_sql(publisher_region, columns))

            if target_scope and _has_col(columns, "target_scope"):
                clauses.append("target_scope = ?")
                params.append(target_scope)

            if report_type:
                if _has_col(columns, "report_type"):
                    clauses.append("report_type = ?")
                    params.append(report_type)

            if query:
                parts = [
                    "title LIKE ?",
                    "COALESCE(summary, '') LIKE ?",
                ]
                if _has_col(columns, "title_zh"):
                    parts.append("title_zh LIKE ?")
                if _has_col(columns, "summary_zh"):
                    parts.append("summary_zh LIKE ?")
                if _has_col(columns, "thesis"):
                    parts.append("thesis LIKE ?")
                like = f"%{query}%"
                for _ in parts:
                    params.append(like)
                clauses.append(f"({' OR '.join(parts)})")

            where = " AND ".join(clauses)

            select_cols = [
                "id", "report_key", "title", "title_zh",
                "source_key", "source_name", "url", "report_type",
                "published_at", "fetched_at", "language",
                "summary", "summary_zh",
            ]
            for extra in ("thesis", "thesis_zh", "relevance", "relevance_zh",
                          "focus_area", "publisher_region", "target_scope", "status"):
                if _has_col(columns, extra):
                    select_cols.append(extra)

            sql = f"""
                SELECT {', '.join(select_cols)}
                FROM research_reports
                WHERE {where}
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                LIMIT ?
            """
            params.append(limit)
            return self._rows_to_dicts(conn.execute(sql, params).fetchall())

    # ------------------------------------------------------------------
    # Detail
    # ------------------------------------------------------------------

    def get_report_detail(self, report_key: str) -> Optional[Dict[str, Any]]:
        """Return full report + evidence list by report_key."""
        with self._connect() as conn:
            if not self._table_exists(conn, "research_reports"):
                return None

            row = conn.execute(
                "SELECT * FROM research_reports WHERE report_key = ?",
                (report_key,),
            ).fetchone()
            if not row:
                return None

            report = self._row_to_dict(row)
            report_id = report["id"]

            # Evidence
            ev_columns = _detect_columns(conn, "research_evidence") if self._table_exists(conn, "research_evidence") else set()
            ev_select = ["id", "report_id", "label", "value", "source_url", "sort_order"]
            for extra in ("evidence_type", "confidence", "metadata_json"):
                if _has_col(ev_columns, extra):
                    ev_select.append(extra)

            if self._table_exists(conn, "research_evidence"):
                evidence_rows = conn.execute(
                    f"""
                    SELECT {', '.join(ev_select)}
                    FROM research_evidence
                    WHERE report_id = ?
                    ORDER BY COALESCE(sort_order, 100) ASC, id ASC
                    """,
                    (report_id,),
                ).fetchall()
            else:
                evidence_rows = []
            report["evidence"] = self._rows_to_dicts(evidence_rows)
            return report

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _overseas_filter(columns: set[str]) -> Optional[str]:
        """Return a SQL WHERE fragment for overseas reports, or None."""
        if _has_col(columns, "publisher_region"):
            return "publisher_region = 'overseas'"
        if _has_col(columns, "language"):
            return "language = 'en'"
        return None

    @staticmethod
    def _region_filter_sql(region: str, columns: set[str]) -> str:
        """Best-effort region filter when publisher_region column is absent."""
        r = region.lower()
        if r in ("overseas", "海外", "international", "global"):
            if _has_col(columns, "language"):
                return "language = 'en'"
            return "(COALESCE(source_key, '') IN ('sec', 'reuters', 'bloomberg', 'wsj', 'ft', 'economist', 'ap', 'afp', 'nytimes', 'techcrunch', 'venturebeat') OR source_key LIKE '%global%')"
        if r in ("domestic", "国内", "cn", "china"):
            if _has_col(columns, "language"):
                return "COALESCE(language, 'zh') != 'en'"
            return "(COALESCE(source_key, '') NOT IN ('sec', 'reuters', 'bloomberg', 'wsj', 'ft', 'economist', 'ap', 'afp', 'nytimes', 'techcrunch', 'venturebeat'))"
        return "1=1"


_research_workbench_service: Optional[ResearchWorkbenchService] = None


def get_research_workbench_service(db_path: str = DB_PATH) -> ResearchWorkbenchService:
    global _research_workbench_service
    if _research_workbench_service is None:
        _research_workbench_service = ResearchWorkbenchService(db_path=db_path)
    return _research_workbench_service
