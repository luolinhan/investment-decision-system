"""Research workbench aggregator for research_reports / research_evidence."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from app.db import get_sqlite_connection

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data",
    "investment.db",
)


FOCUS_AREAS = (
    "芯片半导体",
    "AI",
    "机器人",
    "创新药",
    "光伏",
    "核电",
    "养猪",
)


class ResearchWorkbenchService:
    """Read-only workbench over the persistent research library."""

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
        return bool(row and row["cnt"])

    @staticmethod
    def _detect_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    @staticmethod
    def _json_loads(value: Any, default: Any) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(value)
        except Exception:
            return default

    @staticmethod
    def _safe_pct(numerator: float, denominator: float) -> float:
        if not denominator:
            return 0.0
        return round((numerator / denominator) * 100.0, 1)

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return {key: row[key] for key in row.keys()}

    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        focus_areas = self._json_loads(item.get("focus_areas_json"), [])
        tags = self._json_loads(item.get("tags_json"), [])
        tickers = self._json_loads(item.get("tickers_json"), [])
        key_points = self._json_loads(item.get("key_points_json"), [])

        item["focus_areas"] = [str(value) for value in focus_areas if str(value).strip()]
        item["tags"] = [str(value) for value in tags if str(value).strip()]
        item["tickers"] = [str(value) for value in tickers if str(value).strip()]
        item["key_points"] = [
            {
                "zh": str(point.get("zh") or "").strip(),
                "en": str(point.get("en") or "").strip(),
            }
            for point in key_points
            if isinstance(point, dict) and (point.get("zh") or point.get("en"))
        ]
        item["has_translation"] = bool(
            (item.get("title_zh") and item.get("title_zh") != item.get("title"))
            or (item.get("summary_zh") and item.get("summary_zh") != item.get("summary"))
            or any(point.get("zh") for point in item["key_points"])
        )
        item["archived"] = str(item.get("original_asset_status") or "").startswith("archived")
        item["display_title"] = item.get("title_zh") or item.get("title") or "-"
        item["display_summary"] = item.get("summary_zh") or item.get("summary") or item.get("thesis_zh") or item.get("thesis") or ""
        item["detail_intro"] = item.get("thesis_zh") or item.get("thesis") or ""
        item["sort_time"] = item.get("published_at") or item.get("fetched_at") or ""
        item["primary_focus"] = item["focus_areas"][0] if item["focus_areas"] else ""
        return item

    def _fetch_reports(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            if not self._table_exists(conn, "research_reports"):
                return []
            columns = self._detect_columns(conn, "research_reports")
            selected = [
                "id",
                "report_key",
                "title",
                "title_zh",
                "source_key",
                "source_name",
                "url",
                "report_type",
                "publisher_region",
                "source_tier",
                "target_scope",
                "published_at",
                "fetched_at",
                "language",
                "summary",
                "summary_zh",
                "thesis",
                "thesis_zh",
                "relevance",
                "relevance_zh",
                "focus_areas_json",
                "tags_json",
                "tickers_json",
                "key_points_json",
                "original_url",
                "original_asset_path",
                "original_asset_type",
                "original_asset_status",
                "original_downloaded_at",
                "status",
            ]
            selected = [col for col in selected if col in columns]
            rows = conn.execute(
                f"""
                SELECT {', '.join(selected)}
                FROM research_reports
                WHERE COALESCE(status, 'active') = 'active'
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                """
            ).fetchall()
        return [self._normalize_item(self._row_to_dict(row)) for row in rows]

    @staticmethod
    def _matches_query(item: Dict[str, Any], query: str) -> bool:
        needle = query.strip().lower()
        if not needle:
            return True
        haystack = " ".join(
            [
                str(item.get("title") or ""),
                str(item.get("title_zh") or ""),
                str(item.get("summary") or ""),
                str(item.get("summary_zh") or ""),
                str(item.get("thesis") or ""),
                str(item.get("thesis_zh") or ""),
                " ".join(item.get("focus_areas") or []),
                " ".join(item.get("tags") or []),
                " ".join(item.get("tickers") or []),
                str(item.get("source_name") or ""),
            ]
        ).lower()
        return needle in haystack

    def get_overview(self) -> Dict[str, Any]:
        reports = self._fetch_reports()
        total = len(reports)
        domestic = sum(1 for item in reports if item.get("publisher_region") == "domestic")
        overseas = sum(1 for item in reports if item.get("publisher_region") == "overseas")
        archived = sum(1 for item in reports if item.get("archived"))
        bilingual = sum(1 for item in reports if item.get("has_translation"))

        focus_distribution: List[Dict[str, Any]] = []
        for focus in FOCUS_AREAS:
            count = sum(1 for item in reports if focus in (item.get("focus_areas") or []))
            focus_distribution.append({"focus_area": focus, "count": count})
        focus_distribution = [item for item in focus_distribution if item["count"] > 0]

        source_counts: Dict[str, int] = {}
        for item in reports:
            key = item.get("source_name") or item.get("source_key") or "unknown"
            source_counts[key] = source_counts.get(key, 0) + 1
        source_distribution = [
            {"name": name, "count": count}
            for name, count in sorted(source_counts.items(), key=lambda pair: pair[1], reverse=True)[:12]
        ]

        latest_updated_at = None
        if reports:
            latest_updated_at = max(item.get("sort_time") or "" for item in reports) or None

        return {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "summary": {
                "total_reports": total,
                "domestic_reports": domestic,
                "overseas_reports": overseas,
                "bilingual_reports": bilingual,
                "bilingual_pct": self._safe_pct(bilingual, total),
                "archived_reports": archived,
                "archived_pct": self._safe_pct(archived, total),
                "latest_updated_at": latest_updated_at,
            },
            "focus_distribution": focus_distribution,
            "source_distribution": source_distribution,
            "recent_reports": reports[:8],
        }

    def list_reports(
        self,
        limit: int = 50,
        focus_area: Optional[str] = None,
        publisher_region: Optional[str] = None,
        target_scope: Optional[str] = None,
        report_type: Optional[str] = None,
        query: Optional[str] = None,
        bilingual_only: bool = False,
        archived_only: bool = False,
    ) -> List[Dict[str, Any]]:
        items = self._fetch_reports()
        if focus_area:
            items = [item for item in items if focus_area in (item.get("focus_areas") or [])]
        if publisher_region:
            items = [item for item in items if item.get("publisher_region") == publisher_region]
        if target_scope:
            items = [item for item in items if item.get("target_scope") == target_scope]
        if report_type:
            items = [item for item in items if item.get("report_type") == report_type]
        if bilingual_only:
            items = [item for item in items if item.get("has_translation")]
        if archived_only:
            items = [item for item in items if item.get("archived")]
        if query:
            items = [item for item in items if self._matches_query(item, query)]
        return items[: max(1, min(int(limit or 50), 400))]

    def get_report_detail(self, report_key: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            if not self._table_exists(conn, "research_reports"):
                return None
            row = conn.execute(
                "SELECT * FROM research_reports WHERE report_key = ?",
                (report_key,),
            ).fetchone()
            if not row:
                return None
            report = self._normalize_item(self._row_to_dict(row))
            report_id = report["id"]
            evidence_rows: Iterable[sqlite3.Row] = []
            if self._table_exists(conn, "research_evidence"):
                evidence_rows = conn.execute(
                    """
                    SELECT label, value, source_url, sort_order
                    FROM research_evidence
                    WHERE report_id = ?
                    ORDER BY COALESCE(sort_order, 100) ASC, id ASC
                    """,
                    (report_id,),
                ).fetchall()
            report["evidence"] = [self._row_to_dict(row) for row in evidence_rows]
            return report


_service: Optional[ResearchWorkbenchService] = None


def get_research_workbench_service(db_path: str = DB_PATH) -> ResearchWorkbenchService:
    global _service
    if _service is None:
        _service = ResearchWorkbenchService(db_path=db_path)
    return _service
