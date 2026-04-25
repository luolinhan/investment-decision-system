# -*- coding: utf-8 -*-
"""Fill Chinese fields for Intelligence Hub using Bailian Coding Plan.

The script only translates persisted records with missing Chinese fields. It is
safe to run often: completed rows are skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import get_sqlite_connection  # noqa: E402
from app.services.intelligence_service import IntelligenceService  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db"))


def _normalize_openai_base_url(value: str, default: str) -> str:
    base_url = (value or default).strip().rstrip("/")
    if base_url.endswith("/chat/completions"):
        base_url = base_url[: -len("/chat/completions")]
    return base_url


class BailianTranslator:
    def __init__(self) -> None:
        load_dotenv(os.path.join(BASE_DIR, ".env.local"), override=False)
        load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)
        self.api_key = (
            os.getenv("BAILIAN_API_KEY", "").strip()
            or os.getenv("DASHSCOPE_API_KEY", "").strip()
        )
        self.base_url = _normalize_openai_base_url(
            os.getenv("BAILIAN_BASE_URL", "").strip()
            or os.getenv("DASHSCOPE_BASE_URL", "").strip(),
            "https://coding.dashscope.aliyuncs.com/v1",
        )
        self.model = (
            os.getenv("BAILIAN_MODEL", "").strip()
            or os.getenv("DASHSCOPE_MODEL", "").strip()
            or "qwen3.6-plus"
        )
        self.timeout = int(os.getenv("BAILIAN_TIMEOUT_SECONDS", "60"))

    def enabled(self) -> bool:
        return bool(self.api_key)

    def config_snapshot(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled(),
            "base_url": self.base_url,
            "model": self.model,
            "timeout_seconds": self.timeout,
        }

    def _request_content(self, system_prompt: str, user_payload: Dict[str, Any]) -> str:
        if not self.enabled():
            return ""
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={
                "model": self.model,
                "temperature": 0.1,
                "max_tokens": 800,
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

    def enrich_research_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled():
            return {}
        prompt = (
            "你是A股和港股投研场景的双语研报助手。"
            "请把输入中的英文研报信息翻译为专业、简洁的中文，并补充有深度的投研洞察。"
            "严格返回 JSON，不要输出额外解释。"
            "字段必须包含: title_zh, summary_zh, thesis_zh, relevance_zh, key_points。"
            "其中 key_points 是数组，每项是对象，包含 zh 和 en 两个字段。"
            "key_points 要有 4 到 6 条，优先覆盖：核心结论、产业链传导、催化剂/验证点、主要风险。"
            "如果输入里提供 existing_key_points，需要尽量保留其 en 文本并补齐对应 zh。"
        )
        content = self._normalize_json_block(self._request_content(prompt, payload))
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return {}
        if not isinstance(parsed, dict):
            return {}
        key_points = parsed.get("key_points")
        normalized_points = []
        if isinstance(key_points, list):
            for item in key_points[:6]:
                if isinstance(item, dict):
                    zh = str(item.get("zh") or "").strip()
                    en = str(item.get("en") or "").strip()
                else:
                    zh = str(item or "").strip()
                    en = ""
                if zh or en:
                    normalized_points.append({"zh": zh, "en": en})
        parsed["key_points"] = normalized_points
        return parsed


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


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-").split()).strip()


def normalize_key_points(raw: Any, max_points: int = 6) -> List[Dict[str, str]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        return []
    normalized: List[Dict[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            zh = normalize_text(item.get("zh") or "")
            en = normalize_text(item.get("en") or "")
        else:
            zh = ""
            en = normalize_text(item)
        if zh or en:
            normalized.append({"zh": zh, "en": en})
        if len(normalized) >= max_points:
            break
    return normalized


def merge_key_points(existing: Any, translated: Any, fallback_en_parts: Tuple[Any, ...], max_points: int = 6) -> List[Dict[str, str]]:
    old_points = normalize_key_points(existing, max_points=max_points)
    new_points = normalize_key_points(translated, max_points=max_points)
    merged: List[Dict[str, str]] = []
    seen = set()

    anchor_count = max(len(old_points), len(new_points))
    for idx in range(anchor_count):
        old_point = old_points[idx] if idx < len(old_points) else {}
        new_point = new_points[idx] if idx < len(new_points) else {}
        zh = old_point.get("zh") or new_point.get("zh") or ""
        en = old_point.get("en") or new_point.get("en") or ""
        if not zh and not en:
            continue
        key = f"{zh.lower()}|{en.lower()}"
        if key in seen:
            continue
        seen.add(key)
        merged.append({"zh": zh, "en": en})
        if len(merged) >= max_points:
            return merged

    if not merged:
        for part in fallback_en_parts:
            text = normalize_text(part)
            if not text:
                continue
            key = f"|{text.lower()}"
            if key in seen:
                continue
            seen.add(key)
            merged.append({"zh": "", "en": text[:220]})
            if len(merged) >= max_points:
                break
    return merged


def needs_key_points_translation(raw_points: Any) -> bool:
    points = normalize_key_points(raw_points)
    if not points:
        return True
    for point in points:
        zh = point.get("zh") or ""
        en = point.get("en") or ""
        if en and not zh:
            return True
    return False


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


def translate_research_rows(
    conn: sqlite3.Connection,
    translator: BailianTranslator,
    limit: int,
    scan_limit: int,
    order: str = "asc",
) -> Tuple[int, int, int]:
    order_sql = "ASC" if order.lower() == "asc" else "DESC"
    rows = conn.execute(
        f"""
        SELECT id, title, summary, thesis, relevance, key_points_json,
               title_zh, summary_zh, thesis_zh, relevance_zh, published_at, fetched_at
        FROM research_reports
        WHERE status = 'active'
        ORDER BY COALESCE(published_at, fetched_at) {order_sql}, id {order_sql}
        LIMIT ?
        """,
        (max(scan_limit, limit),),
    ).fetchall()
    scanned = 0
    updated = 0
    skipped = 0
    for row in rows:
        if updated >= limit:
            break
        scanned += 1
        title_zh_old = row["title_zh"] or ""
        summary_zh_old = row["summary_zh"] or ""
        thesis_zh_old = row["thesis_zh"] or ""
        relevance_zh_old = row["relevance_zh"] or ""
        key_points_old = row["key_points_json"] or ""
        should_translate = any(
            (
                needs_translation(title_zh_old, row["title"] or ""),
                needs_translation(summary_zh_old, row["summary"] or ""),
                needs_translation(thesis_zh_old, row["thesis"] or ""),
                needs_translation(relevance_zh_old, row["relevance"] or ""),
                needs_key_points_translation(key_points_old),
            )
        )
        if not should_translate:
            skipped += 1
            continue
        payload = {
            "title": row["title"] or "",
            "summary": row["summary"] or "",
            "thesis": row["thesis"] or "",
            "relevance": row["relevance"] or "",
            "existing_key_points": normalize_key_points(key_points_old),
        }
        try:
            translated = translator.enrich_research_payload(payload) if translator.enabled() else {}
        except Exception:
            translated = {}
        merged_points = merge_key_points(
            key_points_old,
            translated.get("key_points"),
            (row["summary"], row["thesis"], row["relevance"]),
        )
        conn.execute(
            """
            UPDATE research_reports
            SET title_zh=?,
                summary_zh=?,
                thesis_zh=?,
                relevance_zh=?,
                key_points_json=?
            WHERE id=?
            """,
            (
                translated.get("title_zh") or title_zh_old or row["title"],
                translated.get("summary_zh") or summary_zh_old or row["summary"],
                translated.get("thesis_zh") or thesis_zh_old or row["thesis"],
                translated.get("relevance_zh") or relevance_zh_old or row["relevance"],
                json.dumps(merged_points, ensure_ascii=False),
                row["id"],
            ),
        )
        updated += 1
    return updated, scanned, skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Translate Intelligence Hub records with Bailian.")
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("INTELLIGENCE_TRANSLATION_LIMIT", "20")),
        help="Base per-run translation limit for events and research.",
    )
    parser.add_argument(
        "--research-limit",
        type=int,
        default=int(os.getenv("INTELLIGENCE_RESEARCH_TRANSLATION_LIMIT", "0")),
        help="Optional explicit per-run limit for research_reports; 0 means inherit --limit.",
    )
    parser.add_argument(
        "--research-scan-limit",
        type=int,
        default=int(os.getenv("INTELLIGENCE_RESEARCH_SCAN_LIMIT", "160")),
        help="How many active research rows to scan for missing zh fields each run.",
    )
    parser.add_argument(
        "--research-order",
        choices=("asc", "desc"),
        default=os.getenv("INTELLIGENCE_RESEARCH_TRANSLATION_ORDER", "asc").lower(),
        help="Research scan order by COALESCE(published_at,fetched_at).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    IntelligenceService(DB_PATH).ensure_tables()
    limit = max(1, int(args.limit))
    research_limit = max(1, int(args.research_limit)) if int(args.research_limit) > 0 else limit
    research_scan_limit = max(research_limit, int(args.research_scan_limit))
    translator = BailianTranslator()
    with get_sqlite_connection(DB_PATH, timeout=60, busy_timeout=15000) as conn:
        conn.row_factory = sqlite3.Row
        event_count = translate_event_rows(conn, translator, limit)
        fact_count = translate_fact_rows(conn, translator, limit * 3)
        research_count, research_scanned, research_skipped = translate_research_rows(
            conn,
            translator,
            research_limit,
            research_scan_limit,
            order=args.research_order,
        )
        conn.commit()
    print("ETL_METRICS_JSON=" + json.dumps({
        "records_processed": event_count + fact_count + research_count,
        "records_failed": 0,
        "records_skipped": research_skipped,
        "events_translated": event_count,
        "facts_translated": fact_count,
        "research_translated": research_count,
        "research_scanned": research_scanned,
        "research_scan_limit": research_scan_limit,
        "research_order": args.research_order,
        "research_limit": research_limit,
        "translator": "bailian" if translator.enabled() else "fallback",
        "bailian_config": translator.config_snapshot(),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
