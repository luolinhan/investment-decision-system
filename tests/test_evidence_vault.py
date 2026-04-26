from __future__ import annotations

import sqlite3
from pathlib import Path

from app.services.evidence_vault import EvidenceVaultService


def test_evidence_vault_archives_text_and_searches_chinese(tmp_path: Path):
    service = EvidenceVaultService(db_path=tmp_path / "vault.db", archive_root=tmp_path / "archive")

    result = service.archive_text_document(
        title="交易所公告",
        text="第一段证据。\n\n第二段证据说明订单和价格。",
        source_name="交易所",
        source_type="exchange",
        original_url="https://example.com/notice",
        reliability_tier="T1",
    )

    assert Path(result["local_archive_path"]).exists()
    assert result["chunk_count"] == 1
    summary = service.source_quality_summary()
    assert summary["document_count"] == 1
    assert summary["archived_document_count"] == 1
    assert summary["reliability_tiers"]["T1"] == 1

    rows = service.search_documents("证据")
    assert rows
    assert rows[0]["title"] == "交易所公告"


def test_evidence_vault_backfills_existing_raw_documents(tmp_path: Path):
    db_path = tmp_path / "investment.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE raw_documents (
            id INTEGER PRIMARY KEY,
            source_key TEXT,
            url TEXT,
            canonical_url TEXT,
            title TEXT,
            title_zh TEXT,
            published_at TEXT,
            fetched_at TEXT,
            language TEXT,
            content_hash TEXT,
            summary TEXT,
            summary_zh TEXT,
            raw_text TEXT,
            metadata_json TEXT,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO raw_documents (
            id, source_key, url, canonical_url, title, title_zh, published_at,
            fetched_at, language, content_hash, summary, summary_zh, raw_text,
            metadata_json, status
        )
        VALUES (
            1, 'exchange_demo', 'https://example.com/a', 'https://example.com/a',
            'Notice', '公告', '2026-04-25', '2026-04-26T01:00:00+00:00',
            'zh', '', 'summary', '摘要', '原始公告文本', '{}', 'success'
        )
        """
    )
    conn.commit()
    conn.close()

    service = EvidenceVaultService(db_path=db_path, archive_root=tmp_path / "archive")
    metrics = service.backfill_from_existing()

    assert metrics["raw_documents"] == 1
    panels = service.evidence_panel_for_urls(["https://example.com/a"])
    assert panels
    assert panels[0]["local_archive_path"]
