# -*- coding: utf-8 -*-
"""Fill Chinese fields for Intelligence Hub using Bailian Coding Plan.

The script only translates persisted records with missing Chinese fields. It is
safe to run often: completed rows are skipped.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import get_sqlite_connection  # noqa: E402
from app.services.intelligence_service import IntelligenceService  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db"))


class BailianTranslator:
    def __init__(self) -> None:
        load_dotenv(os.path.join(BASE_DIR, ".env.local"), override=False)
        load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)
        self.api_key = os.getenv("BAILIAN_API_KEY", "").strip()
        self.base_url = os.getenv("BAILIAN_BASE_URL", "https://coding.dashscope.aliyuncs.com/v1").rstrip("/")
        self.model = os.getenv("BAILIAN_MODEL", "qwen3-coder-plus").strip()
        self.timeout = int(os.getenv("BAILIAN_TIMEOUT_SECONDS", "25"))

    def enabled(self) -> bool:
        return bool(self.api_key)

    def translate_payload(self, payload: Dict[str, Any]) -> Dict[str, str]:
        if not self.enabled():
            return {}
        prompt = (
            "你是面向A股和港股投资研究的信息翻译助手。"
            "请把输入英文翻译成简洁、准确、保留专有名词的中文。"
            "只返回严格JSON，字段名保持和输入一致；不要添加解释。"
        )
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={
                "model": self.model,
                "temperature": 0.1,
                "max_tokens": 1200,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        content = (((resp.json().get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        if content.startswith("```"):
            content = content.strip("`")
            content = content.replace("json\n", "", 1).strip()
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            content = content[start : end + 1]
        parsed = json.loads(content)
        return {str(k): str(v) for k, v in parsed.items() if v is not None}


def fallback_translate_label(text: str) -> str:
    mapping = {
        "Model": "模型",
        "Model family": "模型族",
        "API model": "API 模型",
        "Core capability": "核心能力",
        "Context window": "上下文窗口",
        "Availability": "可用性",
        "GPT-5.5 pricing": "GPT-5.5 定价",
        "A/H watch chain": "A/H 观察链条",
        "Old model deprecation": "旧模型弃用",
        "Source signal": "来源信号",
    }
    return mapping.get(text, text)


def translate_event_rows(conn: sqlite3.Connection, translator: BailianTranslator, limit: int) -> int:
    rows = conn.execute(
        """
        SELECT id, title, summary, impact_summary
        FROM intelligence_events
        WHERE status = 'active'
          AND (title_zh IS NULL OR title_zh = ''
               OR summary_zh IS NULL OR summary_zh = ''
               OR impact_summary_zh IS NULL OR impact_summary_zh = '')
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    updated = 0
    for row in rows:
        translated = translator.translate_payload(
            {
                "title_zh": row["title"] or "",
                "summary_zh": row["summary"] or "",
                "impact_summary_zh": row["impact_summary"] or "",
            }
        ) if translator.enabled() else {}
        conn.execute(
            """
            UPDATE intelligence_events
            SET title_zh=COALESCE(NULLIF(title_zh, ''), ?),
                summary_zh=COALESCE(NULLIF(summary_zh, ''), ?),
                impact_summary_zh=COALESCE(NULLIF(impact_summary_zh, ''), ?)
            WHERE id=?
            """,
            (
                translated.get("title_zh") or row["title"],
                translated.get("summary_zh") or row["summary"],
                translated.get("impact_summary_zh") or row["impact_summary"],
                row["id"],
            ),
        )
        updated += 1
    return updated


def translate_fact_rows(conn: sqlite3.Connection, translator: BailianTranslator, limit: int) -> int:
    rows = conn.execute(
        """
        SELECT id, label, value
        FROM event_facts
        WHERE label_zh IS NULL OR label_zh = '' OR value_zh IS NULL OR value_zh = ''
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    updated = 0
    for row in rows:
        translated = {}
        if translator.enabled():
            translated = translator.translate_payload({"label_zh": row["label"] or "", "value_zh": row["value"] or ""})
        conn.execute(
            """
            UPDATE event_facts
            SET label_zh=COALESCE(NULLIF(label_zh, ''), ?),
                value_zh=COALESCE(NULLIF(value_zh, ''), ?)
            WHERE id=?
            """,
            (
                translated.get("label_zh") or fallback_translate_label(row["label"] or ""),
                translated.get("value_zh") or row["value"],
                row["id"],
            ),
        )
        updated += 1
    return updated


def translate_research_rows(conn: sqlite3.Connection, translator: BailianTranslator, limit: int) -> int:
    rows = conn.execute(
        """
        SELECT id, title, summary, thesis, relevance
        FROM research_reports
        WHERE status = 'active'
          AND (title_zh IS NULL OR title_zh = ''
               OR summary_zh IS NULL OR summary_zh = ''
               OR thesis_zh IS NULL OR thesis_zh = ''
               OR relevance_zh IS NULL OR relevance_zh = '')
        ORDER BY fetched_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    updated = 0
    for row in rows:
        translated = translator.translate_payload(
            {
                "title_zh": row["title"] or "",
                "summary_zh": row["summary"] or "",
                "thesis_zh": row["thesis"] or "",
                "relevance_zh": row["relevance"] or "",
            }
        ) if translator.enabled() else {}
        conn.execute(
            """
            UPDATE research_reports
            SET title_zh=COALESCE(NULLIF(title_zh, ''), ?),
                summary_zh=COALESCE(NULLIF(summary_zh, ''), ?),
                thesis_zh=COALESCE(NULLIF(thesis_zh, ''), ?),
                relevance_zh=COALESCE(NULLIF(relevance_zh, ''), ?)
            WHERE id=?
            """,
            (
                translated.get("title_zh") or row["title"],
                translated.get("summary_zh") or row["summary"],
                translated.get("thesis_zh") or row["thesis"],
                translated.get("relevance_zh") or row["relevance"],
                row["id"],
            ),
        )
        updated += 1
    return updated


def main() -> int:
    IntelligenceService(DB_PATH).ensure_tables()
    limit = int(os.getenv("INTELLIGENCE_TRANSLATION_LIMIT", "20"))
    translator = BailianTranslator()
    with get_sqlite_connection(DB_PATH, timeout=60, busy_timeout=15000) as conn:
        conn.row_factory = sqlite3.Row
        event_count = translate_event_rows(conn, translator, limit)
        fact_count = translate_fact_rows(conn, translator, limit * 3)
        research_count = translate_research_rows(conn, translator, limit)
        conn.commit()
    print("ETL_METRICS_JSON=" + json.dumps({
        "records_processed": event_count + fact_count + research_count,
        "records_failed": 0,
        "records_skipped": 0,
        "events_translated": event_count,
        "facts_translated": fact_count,
        "research_translated": research_count,
        "translator": "bailian" if translator.enabled() else "fallback",
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
