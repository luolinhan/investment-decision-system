"""Evidence Vault storage and local source archive support.

The service owns the V3 research archive tables. It is intentionally local and
network-free: collectors can hand it text, URLs, PDF metadata, or existing V2
rows, and the service persists both database metadata and local text archives.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from app.db import get_sqlite_connection


SCHEMA_VERSION = "v3.0.0"
PARSER_VERSION = "evidence_vault.v1"
DEFAULT_DB_PATH = Path("data/investment.db")
DEFAULT_ARCHIVE_ROOT = Path("data/archive")
ARCHIVE_TEXT_DIR = "text"
ARCHIVE_REPORT_DIR = "reports"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def slugify(value: Any, fallback: str = "unknown") -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"https?://", "", text)
    text = re.sub(r"[^a-z0-9_\-.]+", "_", text)
    text = text.strip("._-")
    return text[:80] or fallback


def parse_json_list(value: Any) -> List[str]:
    if value in (None, "", "null"):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item or "").strip()]
    return []


class EvidenceVaultService:
    """Create, migrate, and query the local V3 Evidence Vault."""

    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
    ) -> None:
        self.db_path = Path(db_path)
        self.archive_root = Path(archive_root)

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = get_sqlite_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> Dict[str, Any]:
        self.archive_root.mkdir(parents=True, exist_ok=True)
        for child in ("html", "pdf", ARCHIVE_TEXT_DIR, ARCHIVE_REPORT_DIR, "screenshots"):
            (self.archive_root / child).mkdir(parents=True, exist_ok=True)

        with self.connect() as conn:
            self._create_tables(conn)
            self._create_indexes(conn)
            fts_available = self._create_fts(conn)
            conn.commit()
        return {
            "schema_version": SCHEMA_VERSION,
            "db_path": self.db_path.as_posix(),
            "archive_root": self.archive_root.as_posix(),
            "fts_available": fts_available,
        }

    @staticmethod
    def _create_tables(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_catalog (
                source_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'news',
                domain TEXT,
                reliability_tier TEXT NOT NULL DEFAULT 'T3',
                default_weight REAL NOT NULL DEFAULT 0.5,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_documents (
                document_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                title TEXT,
                canonical_url TEXT,
                original_url TEXT,
                local_archive_path TEXT,
                content_type TEXT,
                language TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL,
                author_org TEXT,
                checksum TEXT,
                content_hash TEXT,
                parser_version TEXT,
                parse_status TEXT NOT NULL DEFAULT 'parsed',
                extraction_quality REAL NOT NULL DEFAULT 0.0,
                data_source_class TEXT NOT NULL DEFAULT 'live_public',
                summary TEXT,
                raw_text TEXT,
                markdown_text TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_id) REFERENCES source_catalog(source_id)
            );

            CREATE TABLE IF NOT EXISTS source_chunks (
                chunk_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                text TEXT NOT NULL,
                token_estimate INTEGER NOT NULL DEFAULT 0,
                entity_tags TEXT,
                sector_tags TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES source_documents(document_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS citations (
                citation_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                quote_text TEXT NOT NULL,
                normalized_fact TEXT,
                page_or_section TEXT,
                used_by_object_type TEXT,
                used_by_object_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES source_documents(document_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS extracted_facts (
                fact_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                fact_type TEXT NOT NULL,
                entity_id TEXT,
                metric_name TEXT,
                metric_value TEXT,
                metric_unit TEXT,
                metric_date TEXT,
                confidence REAL NOT NULL DEFAULT 0.0,
                extraction_method TEXT NOT NULL DEFAULT 'manual_or_rule',
                created_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES source_documents(document_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS archived_links (
                link_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                url TEXT NOT NULL,
                link_type TEXT NOT NULL DEFAULT 'outbound',
                anchor_text TEXT,
                archived_status TEXT NOT NULL DEFAULT 'pending',
                last_checked_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES source_documents(document_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                report_type TEXT NOT NULL,
                title TEXT NOT NULL,
                local_path TEXT,
                generated_at TEXT NOT NULL,
                as_of_date TEXT,
                related_entities TEXT,
                related_sectors TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                parent_report_id TEXT,
                content_hash TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS report_sections (
                section_id TEXT PRIMARY KEY,
                report_id TEXT NOT NULL,
                section_index INTEGER NOT NULL DEFAULT 0,
                section_title TEXT NOT NULL,
                body_markdown TEXT,
                linked_citations TEXT,
                linked_opportunities TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (report_id) REFERENCES reports(report_id) ON DELETE CASCADE
            );
            """
        )

    @staticmethod
    def _create_indexes(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_source_documents_source ON source_documents(source_id);
            CREATE INDEX IF NOT EXISTS idx_source_documents_url ON source_documents(canonical_url);
            CREATE INDEX IF NOT EXISTS idx_source_documents_fetched ON source_documents(fetched_at);
            CREATE INDEX IF NOT EXISTS idx_source_documents_class ON source_documents(data_source_class);
            CREATE INDEX IF NOT EXISTS idx_source_chunks_document ON source_chunks(document_id, chunk_index);
            CREATE INDEX IF NOT EXISTS idx_citations_object ON citations(used_by_object_type, used_by_object_id);
            CREATE INDEX IF NOT EXISTS idx_facts_entity_metric ON extracted_facts(entity_id, metric_name);
            CREATE INDEX IF NOT EXISTS idx_archived_links_document ON archived_links(document_id);
            CREATE INDEX IF NOT EXISTS idx_reports_type_asof ON reports(report_type, as_of_date);
            CREATE INDEX IF NOT EXISTS idx_report_sections_report ON report_sections(report_id, section_index);
            """
        )

    @staticmethod
    def _create_fts(conn: sqlite3.Connection) -> bool:
        try:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS source_documents_fts
                USING fts5(document_id UNINDEXED, title, summary, raw_text, markdown_text)
                """
            )
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS reports_fts
                USING fts5(report_id UNINDEXED, title, body_markdown)
                """
            )
            return True
        except sqlite3.OperationalError:
            return False

    def upsert_source(
        self,
        conn: sqlite3.Connection,
        *,
        source_id: Optional[str] = None,
        source_name: str,
        source_type: str = "news",
        domain: Optional[str] = None,
        reliability_tier: str = "T3",
        default_weight: float = 0.5,
        enabled: bool = True,
    ) -> str:
        now = utc_now_iso()
        domain = domain or self._domain_from_url(source_name)
        source_id = source_id or slugify(domain or source_name, fallback="source")
        conn.execute(
            """
            INSERT INTO source_catalog (
                source_id, source_name, source_type, domain, reliability_tier,
                default_weight, enabled, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                source_name=excluded.source_name,
                source_type=excluded.source_type,
                domain=excluded.domain,
                reliability_tier=excluded.reliability_tier,
                default_weight=excluded.default_weight,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at
            """,
            (
                source_id,
                source_name,
                source_type,
                domain,
                reliability_tier,
                float(default_weight),
                1 if enabled else 0,
                now,
                now,
            ),
        )
        return source_id

    def archive_text_document(
        self,
        *,
        title: str,
        text: str,
        source_name: str,
        source_type: str = "news",
        original_url: Optional[str] = None,
        canonical_url: Optional[str] = None,
        published_at: Optional[str] = None,
        fetched_at: Optional[str] = None,
        language: Optional[str] = None,
        author_org: Optional[str] = None,
        summary: Optional[str] = None,
        markdown_text: Optional[str] = None,
        content_type: str = "text/plain",
        reliability_tier: str = "T3",
        data_source_class: str = "live_public",
        metadata: Optional[Dict[str, Any]] = None,
        document_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.ensure_schema()
        now = utc_now_iso()
        fetched = fetched_at or now
        canonical = canonical_url or original_url or ""
        checksum = sha256_text(text or "")
        stable_key = "|".join([canonical, title or "", checksum])
        document_id = document_id or f"doc_{sha256_text(stable_key)[:24]}"
        local_path = self._write_archive_text(document_id, text or "", fetched)
        domain = self._domain_from_url(canonical or original_url or source_name)

        with self.connect() as conn:
            source_id = self.upsert_source(
                conn,
                source_name=source_name or domain or "unknown",
                source_type=source_type,
                domain=domain,
                reliability_tier=reliability_tier,
            )
            self._upsert_document(
                conn,
                document_id=document_id,
                source_id=source_id,
                title=title,
                canonical_url=canonical,
                original_url=original_url,
                local_archive_path=local_path.as_posix(),
                content_type=content_type,
                language=language,
                published_at=published_at,
                fetched_at=fetched,
                author_org=author_org,
                checksum=checksum,
                content_hash=checksum,
                parse_status="parsed" if text else "empty",
                extraction_quality=self._quality_score(text, summary),
                data_source_class=data_source_class,
                summary=summary,
                raw_text=text,
                markdown_text=markdown_text,
                metadata_json=json.dumps(metadata or {}, ensure_ascii=False),
            )
            self._replace_chunks(conn, document_id, self.chunk_text(text or ""))
            self._refresh_document_fts(conn, document_id, title, summary or "", text or "", markdown_text or "")
            conn.commit()
        return {
            "document_id": document_id,
            "source_id": source_id,
            "local_archive_path": local_path.as_posix(),
            "checksum": checksum,
            "chunk_count": len(self.chunk_text(text or "")),
        }

    def backfill_from_existing(self, limit: Optional[int] = None) -> Dict[str, Any]:
        self.ensure_schema()
        metrics = {"raw_documents": 0, "research_reports": 0, "sources": 0, "errors": []}
        if not self.db_path.exists():
            return metrics
        with self.connect() as conn:
            tables = self._tables(conn)
            if "source_registry" in tables:
                metrics["sources"] += self._backfill_sources(conn)
            if "raw_documents" in tables:
                metrics["raw_documents"] += self._backfill_raw_documents(conn, limit=limit)
            if "research_reports" in tables:
                metrics["research_reports"] += self._backfill_research_reports(conn, limit=limit)
            conn.commit()
        return metrics

    def search_documents(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        if not query.strip():
            return []
        limit_value = max(1, min(int(limit), 100))
        with self.connect() as conn:
            rows = []
            if "source_documents_fts" in self._tables(conn):
                try:
                    rows = conn.execute(
                        """
                        SELECT d.document_id, d.title, d.canonical_url, d.local_archive_path,
                               d.summary, d.data_source_class, c.source_name, c.reliability_tier
                        FROM source_documents_fts f
                        JOIN source_documents d ON d.document_id = f.document_id
                        JOIN source_catalog c ON c.source_id = d.source_id
                        WHERE source_documents_fts MATCH ?
                        LIMIT ?
                        """,
                        (query, limit_value),
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = []
            if not rows:
                like = f"%{query}%"
                rows = conn.execute(
                    """
                    SELECT d.document_id, d.title, d.canonical_url, d.local_archive_path,
                           d.summary, d.data_source_class, c.source_name, c.reliability_tier
                    FROM source_documents d
                    JOIN source_catalog c ON c.source_id = d.source_id
                    WHERE d.title LIKE ? OR d.summary LIKE ? OR d.raw_text LIKE ? OR d.markdown_text LIKE ?
                    ORDER BY d.fetched_at DESC
                    LIMIT ?
                    """,
                    (like, like, like, like, limit_value),
                ).fetchall()
            return [dict(row) for row in rows]

    def search_reports(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        if not query.strip():
            return []
        limit_value = max(1, min(int(limit), 100))
        with self.connect() as conn:
            rows = []
            if "reports_fts" in self._tables(conn):
                try:
                    rows = conn.execute(
                        """
                        SELECT r.report_id, r.report_type, r.title, r.local_path, r.generated_at, r.as_of_date
                        FROM reports_fts f
                        JOIN reports r ON r.report_id = f.report_id
                        WHERE reports_fts MATCH ?
                        LIMIT ?
                        """,
                        (query, limit_value),
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = []
            if not rows:
                like = f"%{query}%"
                rows = conn.execute(
                    """
                    SELECT DISTINCT r.report_id, r.report_type, r.title, r.local_path, r.generated_at, r.as_of_date
                    FROM reports r
                    LEFT JOIN report_sections s ON s.report_id = r.report_id
                    WHERE r.title LIKE ? OR s.body_markdown LIKE ?
                    ORDER BY r.generated_at DESC
                    LIMIT ?
                    """,
                    (like, like, limit_value),
                ).fetchall()
            return [dict(row) for row in rows]

    def source_quality_summary(self) -> Dict[str, Any]:
        self.ensure_schema()
        with self.connect() as conn:
            document_count = conn.execute("SELECT COUNT(*) FROM source_documents").fetchone()[0]
            archived_count = conn.execute(
                "SELECT COUNT(*) FROM source_documents WHERE local_archive_path IS NOT NULL AND local_archive_path != ''"
            ).fetchone()[0]
            parse_failures = conn.execute(
                "SELECT COUNT(*) FROM source_documents WHERE parse_status NOT IN ('parsed', 'partial')"
            ).fetchone()[0]
            tier_rows = conn.execute(
                "SELECT reliability_tier, COUNT(*) AS count FROM source_catalog GROUP BY reliability_tier"
            ).fetchall()
            class_rows = conn.execute(
                "SELECT data_source_class, COUNT(*) AS count FROM source_documents GROUP BY data_source_class"
            ).fetchall()
        return {
            "schema_version": SCHEMA_VERSION,
            "document_count": int(document_count),
            "archived_document_count": int(archived_count),
            "parse_failure_count": int(parse_failures),
            "parse_failure_rate": round(parse_failures / max(1, document_count), 4),
            "reliability_tiers": {row["reliability_tier"] or "unknown": row["count"] for row in tier_rows},
            "data_source_classes": {row["data_source_class"] or "unknown": row["count"] for row in class_rows},
        }

    def evidence_panel_for_urls(self, urls: Iterable[Any], limit: int = 5) -> List[Dict[str, Any]]:
        normalized = [str(url or "").strip() for url in urls if str(url or "").strip()]
        if not normalized:
            return []
        placeholders = ",".join(["?"] * len(normalized))
        with self.connect() as conn:
            tables = self._tables(conn)
            if "source_documents" not in tables:
                return []
            rows = conn.execute(
                f"""
                SELECT d.document_id, d.title, d.canonical_url, d.original_url, d.local_archive_path,
                       d.content_type, d.language, d.published_at, d.fetched_at,
                       d.summary, d.raw_text, d.data_source_class, c.source_name,
                       c.source_type, c.reliability_tier
                FROM source_documents d
                JOIN source_catalog c ON c.source_id = d.source_id
                WHERE d.canonical_url IN ({placeholders}) OR d.original_url IN ({placeholders})
                ORDER BY d.fetched_at DESC
                LIMIT ?
                """,
                (*normalized, *normalized, max(1, min(int(limit), 20))),
            ).fetchall()
        panels = []
        for row in rows:
            raw_text = str(row["raw_text"] or "")
            panels.append(
                {
                    "document_id": row["document_id"],
                    "title": row["title"],
                    "source_name": row["source_name"],
                    "source_type": row["source_type"],
                    "reliability_tier": row["reliability_tier"],
                    "original_link": row["original_url"] or row["canonical_url"],
                    "local_archive_path": row["local_archive_path"],
                    "published_at": row["published_at"],
                    "fetched_at": row["fetched_at"],
                    "content_type": row["content_type"],
                    "data_source_class": row["data_source_class"],
                    "summary": row["summary"],
                    "quote_text": raw_text[:260],
                }
            )
        return panels

    def _upsert_document(self, conn: sqlite3.Connection, **values: Any) -> None:
        now = utc_now_iso()
        values.setdefault("parser_version", PARSER_VERSION)
        values.setdefault("created_at", now)
        values.setdefault("updated_at", now)
        conn.execute(
            """
            INSERT INTO source_documents (
                document_id, source_id, title, canonical_url, original_url,
                local_archive_path, content_type, language, published_at,
                fetched_at, author_org, checksum, content_hash, parser_version,
                parse_status, extraction_quality, data_source_class, summary,
                raw_text, markdown_text, metadata_json, created_at, updated_at
            )
            VALUES (
                :document_id, :source_id, :title, :canonical_url, :original_url,
                :local_archive_path, :content_type, :language, :published_at,
                :fetched_at, :author_org, :checksum, :content_hash, :parser_version,
                :parse_status, :extraction_quality, :data_source_class, :summary,
                :raw_text, :markdown_text, :metadata_json, :created_at, :updated_at
            )
            ON CONFLICT(document_id) DO UPDATE SET
                source_id=excluded.source_id,
                title=excluded.title,
                canonical_url=excluded.canonical_url,
                original_url=excluded.original_url,
                local_archive_path=excluded.local_archive_path,
                content_type=excluded.content_type,
                language=excluded.language,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                author_org=excluded.author_org,
                checksum=excluded.checksum,
                content_hash=excluded.content_hash,
                parser_version=excluded.parser_version,
                parse_status=excluded.parse_status,
                extraction_quality=excluded.extraction_quality,
                data_source_class=excluded.data_source_class,
                summary=excluded.summary,
                raw_text=excluded.raw_text,
                markdown_text=excluded.markdown_text,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            values,
        )

    def _backfill_sources(self, conn: sqlite3.Connection) -> int:
        rows = conn.execute(
            """
            SELECT source_key, name, source_type, url, credibility, enabled
            FROM source_registry
            """
        ).fetchall()
        for row in rows:
            domain = self._domain_from_url(row["url"])
            tier = self._tier_from_source(row["source_type"], row["credibility"])
            self.upsert_source(
                conn,
                source_id=slugify(row["source_key"], fallback="source"),
                source_name=row["name"] or row["source_key"],
                source_type=self._normalize_source_type(row["source_type"]),
                domain=domain,
                reliability_tier=tier,
                default_weight=self._default_weight(tier),
                enabled=bool(row["enabled"]),
            )
        return len(rows)

    def _backfill_raw_documents(self, conn: sqlite3.Connection, limit: Optional[int]) -> int:
        sql = """
            SELECT id, source_key, url, canonical_url, title, title_zh, published_at,
                   fetched_at, language, content_hash, summary, summary_zh, raw_text,
                   metadata_json, status
            FROM raw_documents
            ORDER BY id ASC
        """
        if limit:
            sql += " LIMIT ?"
            rows = conn.execute(sql, (int(limit),)).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        count = 0
        for row in rows:
            text = row["raw_text"] or row["summary"] or row["summary_zh"] or ""
            source_id = slugify(row["source_key"], fallback="source")
            self._ensure_source_from_document(conn, source_id, row["url"])
            document_id = f"rawdoc_{row['id']}"
            local_path = self._write_archive_text(document_id, text, row["fetched_at"] or utc_now_iso())
            checksum = row["content_hash"] or sha256_text(text)
            self._upsert_document(
                conn,
                document_id=document_id,
                source_id=source_id,
                title=row["title_zh"] or row["title"] or row["canonical_url"] or row["url"] or document_id,
                canonical_url=row["canonical_url"] or row["url"],
                original_url=row["url"],
                local_archive_path=local_path.as_posix(),
                content_type="text/html",
                language=row["language"],
                published_at=row["published_at"],
                fetched_at=row["fetched_at"] or utc_now_iso(),
                author_org=None,
                checksum=checksum,
                content_hash=checksum,
                parse_status="parsed" if row["status"] in (None, "", "ok", "success", "active") else str(row["status"]),
                extraction_quality=self._quality_score(text, row["summary"] or row["summary_zh"]),
                data_source_class="live_public",
                summary=row["summary_zh"] or row["summary"],
                raw_text=text,
                markdown_text=None,
                metadata_json=row["metadata_json"] or "{}",
            )
            self._replace_chunks(conn, document_id, self.chunk_text(text))
            self._refresh_document_fts(conn, document_id, row["title_zh"] or row["title"] or "", row["summary_zh"] or row["summary"] or "", text, "")
            count += 1
        return count

    def _backfill_research_reports(self, conn: sqlite3.Connection, limit: Optional[int]) -> int:
        sql = """
            SELECT id, report_key, title, title_zh, source_key, source_name, url,
                   report_type, published_at, fetched_at, language, summary, summary_zh,
                   thesis, thesis_zh, source_tier, focus_areas_json, tags_json,
                   tickers_json, key_points_json, original_url, original_asset_path,
                   original_asset_type, original_asset_status
            FROM research_reports
            ORDER BY id ASC
        """
        if limit:
            sql += " LIMIT ?"
            rows = conn.execute(sql, (int(limit),)).fetchall()
        else:
            rows = conn.execute(sql).fetchall()
        count = 0
        for row in rows:
            source_id = slugify(row["source_key"] or row["source_name"], fallback="source")
            self._ensure_source_from_document(conn, source_id, row["url"] or row["original_url"], source_name=row["source_name"])
            title = row["title_zh"] or row["title"] or row["report_key"] or f"research_report_{row['id']}"
            body_parts = [
                row["summary_zh"] or row["summary"] or "",
                row["thesis_zh"] or row["thesis"] or "",
                "\n".join(parse_json_list(row["key_points_json"])),
            ]
            text = "\n\n".join(part for part in body_parts if part)
            document_id = f"reportdoc_{row['id']}"
            local_path = row["original_asset_path"] or self._write_archive_text(document_id, text, row["fetched_at"] or utc_now_iso()).as_posix()
            checksum = sha256_text(text + (row["original_url"] or row["url"] or ""))
            self._upsert_document(
                conn,
                document_id=document_id,
                source_id=source_id,
                title=title,
                canonical_url=row["original_url"] or row["url"],
                original_url=row["original_url"] or row["url"],
                local_archive_path=local_path,
                content_type=row["original_asset_type"] or "text/report",
                language=row["language"],
                published_at=row["published_at"],
                fetched_at=row["fetched_at"] or utc_now_iso(),
                author_org=row["source_name"],
                checksum=checksum,
                content_hash=checksum,
                parse_status=row["original_asset_status"] or "parsed",
                extraction_quality=self._quality_score(text, row["summary_zh"] or row["summary"]),
                data_source_class="live_public",
                summary=row["summary_zh"] or row["summary"],
                raw_text=text,
                markdown_text=text,
                metadata_json=json.dumps(
                    {
                        "legacy_report_id": row["id"],
                        "focus_areas": parse_json_list(row["focus_areas_json"]),
                        "tags": parse_json_list(row["tags_json"]),
                        "tickers": parse_json_list(row["tickers_json"]),
                    },
                    ensure_ascii=False,
                ),
            )
            self._replace_chunks(conn, document_id, self.chunk_text(text))
            self._refresh_document_fts(conn, document_id, title, row["summary_zh"] or row["summary"] or "", text, text)
            self._upsert_report_record(conn, row, title, text, local_path)
            count += 1
        return count

    def _upsert_report_record(self, conn: sqlite3.Connection, row: sqlite3.Row, title: str, text: str, local_path: str) -> None:
        now = utc_now_iso()
        report_id = f"legacy_report_{row['id']}"
        related_sectors = json.dumps(parse_json_list(row["focus_areas_json"]), ensure_ascii=False)
        related_entities = json.dumps(parse_json_list(row["tickers_json"]), ensure_ascii=False)
        content_hash = sha256_text(text)
        conn.execute(
            """
            INSERT INTO reports (
                report_id, report_type, title, local_path, generated_at, as_of_date,
                related_entities, related_sectors, version, parent_report_id,
                content_hash, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?)
            ON CONFLICT(report_id) DO UPDATE SET
                report_type=excluded.report_type,
                title=excluded.title,
                local_path=excluded.local_path,
                generated_at=excluded.generated_at,
                as_of_date=excluded.as_of_date,
                related_entities=excluded.related_entities,
                related_sectors=excluded.related_sectors,
                content_hash=excluded.content_hash,
                updated_at=excluded.updated_at
            """,
            (
                report_id,
                row["report_type"] or "research",
                title,
                local_path,
                row["fetched_at"] or now,
                (row["published_at"] or row["fetched_at"] or now)[:10],
                related_entities,
                related_sectors,
                content_hash,
                now,
                now,
            ),
        )
        section_id = f"{report_id}_summary"
        conn.execute(
            """
            INSERT INTO report_sections (
                section_id, report_id, section_index, section_title, body_markdown,
                linked_citations, linked_opportunities, created_at
            )
            VALUES (?, ?, 0, ?, ?, '[]', '[]', ?)
            ON CONFLICT(section_id) DO UPDATE SET
                section_title=excluded.section_title,
                body_markdown=excluded.body_markdown
            """,
            (section_id, report_id, "摘要", text, now),
        )
        self._refresh_report_fts(conn, report_id, title, text)

    def _ensure_source_from_document(
        self,
        conn: sqlite3.Connection,
        source_id: str,
        url: Any,
        source_name: Optional[str] = None,
    ) -> None:
        existing = conn.execute("SELECT source_id FROM source_catalog WHERE source_id = ?", (source_id,)).fetchone()
        if existing:
            return
        domain = self._domain_from_url(url)
        self.upsert_source(
            conn,
            source_id=source_id,
            source_name=source_name or domain or source_id,
            source_type="news",
            domain=domain,
            reliability_tier="T3",
            default_weight=0.5,
        )

    def _write_archive_text(self, document_id: str, text: str, fetched_at: str) -> Path:
        try:
            parsed = datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))
        except Exception:
            parsed = datetime.now(timezone.utc)
        folder = self.archive_root / ARCHIVE_TEXT_DIR / f"{parsed.year:04d}" / f"{parsed.month:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{slugify(document_id, fallback='document')}.txt"
        path.write_text(text or "", encoding="utf-8")
        return path

    @staticmethod
    def chunk_text(text: str, max_chars: int = 1800) -> List[str]:
        text = str(text or "").strip()
        if not text:
            return []
        chunks: List[str] = []
        current: List[str] = []
        size = 0
        for paragraph in re.split(r"\n{2,}", text):
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            if current and size + len(paragraph) > max_chars:
                chunks.append("\n\n".join(current))
                current = []
                size = 0
            if len(paragraph) > max_chars:
                for start in range(0, len(paragraph), max_chars):
                    chunks.append(paragraph[start : start + max_chars])
                continue
            current.append(paragraph)
            size += len(paragraph)
        if current:
            chunks.append("\n\n".join(current))
        return chunks

    @staticmethod
    def _replace_chunks(conn: sqlite3.Connection, document_id: str, chunks: List[str]) -> None:
        now = utc_now_iso()
        conn.execute("DELETE FROM source_chunks WHERE document_id = ?", (document_id,))
        for index, chunk in enumerate(chunks):
            chunk_id = f"{document_id}_chunk_{index:04d}"
            token_estimate = max(1, int(len(chunk) / 4))
            conn.execute(
                """
                INSERT INTO source_chunks (
                    chunk_id, document_id, chunk_index, text, token_estimate,
                    entity_tags, sector_tags, created_at
                )
                VALUES (?, ?, ?, ?, ?, '[]', '[]', ?)
                """,
                (chunk_id, document_id, index, chunk, token_estimate, now),
            )

    @staticmethod
    def _refresh_document_fts(
        conn: sqlite3.Connection,
        document_id: str,
        title: str,
        summary: str,
        raw_text: str,
        markdown_text: str,
    ) -> None:
        if "source_documents_fts" not in EvidenceVaultService._tables(conn):
            return
        conn.execute("DELETE FROM source_documents_fts WHERE document_id = ?", (document_id,))
        conn.execute(
            "INSERT INTO source_documents_fts(document_id, title, summary, raw_text, markdown_text) VALUES (?, ?, ?, ?, ?)",
            (document_id, title or "", summary or "", raw_text or "", markdown_text or ""),
        )

    @staticmethod
    def _refresh_report_fts(conn: sqlite3.Connection, report_id: str, title: str, body_markdown: str) -> None:
        if "reports_fts" not in EvidenceVaultService._tables(conn):
            return
        conn.execute("DELETE FROM reports_fts WHERE report_id = ?", (report_id,))
        conn.execute(
            "INSERT INTO reports_fts(report_id, title, body_markdown) VALUES (?, ?, ?)",
            (report_id, title or "", body_markdown or ""),
        )

    @staticmethod
    def _tables(conn: sqlite3.Connection) -> set[str]:
        return {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual')").fetchall()
        }

    @staticmethod
    def _domain_from_url(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            parsed = urlparse(text if "://" in text else f"https://{text}")
        except Exception:
            return ""
        return parsed.netloc.lower().removeprefix("www.")

    @staticmethod
    def _normalize_source_type(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"official", "exchange", "filing", "company_ir", "news", "report", "user_note", "generated"}:
            return text
        if text in {"sec", "announcement", "regulatory"}:
            return "filing"
        if text in {"official_docs", "official_doc"}:
            return "official"
        return "news"

    @staticmethod
    def _tier_from_source(source_type: Any, credibility: Any) -> str:
        text = f"{source_type or ''} {credibility or ''}".lower()
        if any(token in text for token in ("official", "exchange", "filing", "regulatory")):
            return "T1"
        if any(token in text for token in ("research", "report", "verified", "public")):
            return "T2"
        return "T3"

    @staticmethod
    def _default_weight(tier: str) -> float:
        return {"T1": 1.0, "T2": 0.75, "T3": 0.45, "T4": 0.2}.get(str(tier), 0.5)

    @staticmethod
    def _quality_score(text: Any, summary: Any) -> float:
        text_len = len(str(text or "").strip())
        summary_len = len(str(summary or "").strip())
        if text_len >= 1200 and summary_len >= 40:
            return 0.9
        if text_len >= 300:
            return 0.7
        if summary_len >= 40:
            return 0.45
        if text_len:
            return 0.25
        return 0.0
