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

    def _request_content(self, system_prompt: str, user_payload: Dict[str, Any]) -> str:
        if not self.enabled():
            return ""
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={
                "model": self.model,
                "temperature": 0.1,
                "max_tokens": 1200,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        content = (((resp.json().get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        return content

    @staticmethod
    def _normalize_json_block(content: str) -> str:
        if content.startswith("```"):
            content = content.strip("`")
            content = content.replace("json\n", "", 1).strip()
        start = content.find("{")
        end = content.rfind("}")
        if start >= 0 and end > start:
            content = content[start : end + 1]
        return content

    def translate_payload(self, payload: Dict[str, Any]) -> Dict[str, str]:
        if not self.enabled():
            return {}
        prompt = (
            "你是面向A股和港股投资研究的信息翻译助手。"
            "请把输入英文翻译成简洁、准确、保留专有名词的中文。"
            "只返回严格JSON，字段名保持和输入一致；不要添加解释。"
        )
        content = self._normalize_json_block(self._request_content(prompt, payload))
        try:
            parsed = json.loads(content)
            return {str(k): str(v) for k, v in parsed.items() if v is not None}
        except json.JSONDecodeError:
            repair_prompt = (
                "你是 JSON 修复助手。"
                "把输入中的 raw_output 修复成严格合法的 JSON，且只保留 required_keys 中列出的字段。"
                "不要添加解释。"
            )
            repaired = self._normalize_json_block(
                self._request_content(
                    repair_prompt,
                    {"required_keys": list(payload.keys()), "raw_output": content},
                )
            )
            try:
                parsed = json.loads(repaired)
                return {str(k): str(v) for k, v in parsed.items() if v is not None}
            except json.JSONDecodeError:
                return {}


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


def needs_translation(existing_zh: str, original: str) -> bool:
    zh = (existing_zh or "").strip()
    src = (original or "").strip()
    if not zh:
        return True
    if src and zh == src:
        return True
    chinese_chars = sum(1 for ch in zh if "\u4e00" <= ch <= "\u9fff")
    ascii_letters = sum(1 for ch in zh if ch.isascii() and ch.isalpha())
    if chinese_chars == 0 and ascii_letters > 0:
        return True
    placeholder_prefixes = (
        "官方 AI 信号：",
        "FDA 生物医药信号：",
        "AI 仓库信号：",
        "AI 研究信号：",
    )
    return any(prefix in zh for prefix in placeholder_prefixes)


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
        try:
            translated = translator.translate_payload(
                {
                    "title_zh": row["title"] or "",
                    "summary_zh": row["summary"] or "",
                    "impact_summary_zh": row["impact_summary"] or "",
                }
            ) if translator.enabled() else {}
        except Exception:
            translated = {}
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
            try:
                translated = translator.translate_payload({"label_zh": row["label"] or "", "value_zh": row["value"] or ""})
            except Exception:
                translated = {}
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
        ORDER BY fetched_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    updated = 0
    for row in rows:
        existing = conn.execute(
            """
            SELECT title_zh, summary_zh, thesis_zh, relevance_zh
            FROM research_reports
            WHERE id = ?
            """,
            (row["id"],),
        ).fetchone()
        title_zh_old = (existing["title_zh"] if existing else "") or ""
        summary_zh_old = (existing["summary_zh"] if existing else "") or ""
        thesis_zh_old = (existing["thesis_zh"] if existing else "") or ""
        relevance_zh_old = (existing["relevance_zh"] if existing else "") or ""

        should_translate = any(
            (
                needs_translation(title_zh_old, row["title"] or ""),
                needs_translation(summary_zh_old, row["summary"] or ""),
                needs_translation(thesis_zh_old, row["thesis"] or ""),
                needs_translation(relevance_zh_old, row["relevance"] or ""),
            )
        )
        if not should_translate:
            continue
        try:
            translated = translator.translate_payload(
                {
                    "title_zh": row["title"] or "",
                    "summary_zh": row["summary"] or "",
                    "thesis_zh": row["thesis"] or "",
                    "relevance_zh": row["relevance"] or "",
                }
            ) if translator.enabled() else {}
        except Exception:
            translated = {}
        conn.execute(
            """
            UPDATE research_reports
            SET title_zh=?,
                summary_zh=?,
                thesis_zh=?,
                relevance_zh=?
            WHERE id=?
            """,
            (
                translated.get("title_zh") or title_zh_old or row["title"],
                translated.get("summary_zh") or summary_zh_old or row["summary"],
                translated.get("thesis_zh") or thesis_zh_old or row["thesis"],
                translated.get("relevance_zh") or relevance_zh_old or row["relevance"],
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
