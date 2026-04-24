# -*- coding: utf-8 -*-
"""Continuously collect high-impact AI and market intelligence into SQLite.

The website reads these tables only. This script may run on Windows or on an
overseas collector; the storage target remains the Windows SQLite database.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import get_sqlite_connection  # noqa: E402
from app.services.intelligence_service import IntelligenceService  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db"))

USER_AGENT = os.getenv(
    "INTELLIGENCE_USER_AGENT",
    "InvestmentHubIntelligence/1.0 (+https://github.com/luolinhan/investment-decision-system)",
)
REQUEST_TIMEOUT = int(os.getenv("INTELLIGENCE_TIMEOUT_SECONDS", "25"))
BOOTSTRAP_SEED = os.getenv("INTELLIGENCE_BOOTSTRAP_SEED", "1") != "0"


SOURCES: List[Dict[str, Any]] = [
    {
        "source_key": "openai_gpt55_release",
        "name": "OpenAI GPT-5.5 Release",
        "source_type": "official",
        "url": "https://openai.com/index/introducing-gpt-5-5/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "cadence_minutes": 15,
        "notes": "Official launch page for GPT-5.5.",
    },
    {
        "source_key": "openai_gpt55_system_card",
        "name": "OpenAI GPT-5.5 System Card",
        "source_type": "official",
        "url": "https://openai.com/index/gpt-5-5-system-card/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "cadence_minutes": 30,
        "notes": "Official safety and capability system card.",
    },
    {
        "source_key": "deepseek_api_docs",
        "name": "DeepSeek API Docs",
        "source_type": "official",
        "url": "https://api-docs.deepseek.com/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "cadence_minutes": 15,
        "notes": "DeepSeek API documentation and deprecation notices.",
    },
    {
        "source_key": "deepseek_hf_flash",
        "name": "DeepSeek V4 Flash on Hugging Face",
        "source_type": "model_repo",
        "url": "https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash/tree/main",
        "category": "ai_model",
        "priority": 0,
        "credibility": "primary_repo",
        "collection_method": "fixed_html",
        "cadence_minutes": 30,
        "notes": "Model repository for DeepSeek-V4-Flash.",
    },
    {
        "source_key": "huggingface_deepseek_models",
        "name": "Hugging Face DeepSeek Models API",
        "source_type": "model_repo",
        "url": "https://huggingface.co/api/models?author=deepseek-ai&sort=lastModified&direction=-1&limit=20",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "cadence_minutes": 20,
        "notes": "Detect new DeepSeek model repositories.",
    },
    {
        "source_key": "github_deepseek_repos",
        "name": "DeepSeek GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/deepseek-ai/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "cadence_minutes": 30,
        "notes": "Detect official DeepSeek repository updates.",
    },
    {
        "source_key": "ap_deepseek_v4",
        "name": "AP DeepSeek V4 Coverage",
        "source_type": "credible_media",
        "url": "https://apnews.com/article/deepseek-ai-china-gpt-v4-d2ed33f2521917193616e061674d5f92",
        "category": "ai_model",
        "priority": 1,
        "credibility": "credible_media",
        "collection_method": "fixed_html",
        "cadence_minutes": 60,
        "notes": "Credible media confirmation for market context.",
    },
    {
        "source_key": "bootstrap_verified_events",
        "name": "Verified Bootstrap Events",
        "source_type": "manual_seed",
        "url": "local://verified-intelligence-bootstrap",
        "category": "ai_model",
        "priority": 0,
        "credibility": "verified_seed",
        "collection_method": "bootstrap",
        "cadence_minutes": 1440,
        "notes": "Prevents empty pages when foreign network collection is temporarily unavailable.",
    },
]


BOOTSTRAP_DOCUMENTS: List[Dict[str, Any]] = [
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://openai.com/index/introducing-gpt-5-5/",
        "title": "Introducing GPT-5.5",
        "published_at": "2026-04-24",
        "summary": "OpenAI released GPT-5.5 for complex coding, research, document/spreadsheet work, tool use and real-world workflows.",
        "raw_text": (
            "GPT-5.5 is OpenAI's frontier model for complex real-world work. "
            "It is available in ChatGPT and Codex, with API availability expected soon. "
            "Official materials describe stronger coding, research, document, spreadsheet, tool-use and scientific workflow capabilities. "
            "The release page and pricing page indicate GPT-5.5 pricing of $5 per million input tokens and $30 per million output tokens, "
            "with GPT-5.5 Pro at higher pricing. The context window is described as 1M tokens."
        ),
        "metadata": {"language": "en", "bootstrap": True},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://openai.com/index/gpt-5-5-system-card/",
        "title": "GPT-5.5 System Card",
        "published_at": "2026-04-24",
        "summary": "OpenAI published the GPT-5.5 system card as a primary safety and capability document.",
        "raw_text": (
            "OpenAI published the GPT-5.5 system card. This document is a primary source for capability, safety and deployment notes. "
            "For investors it should be linked to AI agent commercialization, cloud workloads, enterprise productivity and data-center demand."
        ),
        "metadata": {"language": "en", "bootstrap": True, "report_type": "system_card"},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://api-docs.deepseek.com/",
        "title": "DeepSeek API Docs list DeepSeek V4 models",
        "published_at": "2026-04-24",
        "summary": "DeepSeek API documentation lists deepseek-v4-flash and deepseek-v4-pro, with old chat/reasoner model deprecation on 2026/07/24.",
        "raw_text": (
            "DeepSeek API docs list deepseek-v4-flash and deepseek-v4-pro. "
            "The docs state that old deepseek-chat and deepseek-reasoner models will be deprecated on 2026/07/24. "
            "DeepSeek V4 materials reference 1M context and agentic capabilities."
        ),
        "metadata": {"language": "en", "bootstrap": True},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash/tree/main",
        "title": "DeepSeek-V4-Flash model repository",
        "published_at": "2026-04-24",
        "summary": "DeepSeek-V4-Flash appears on Hugging Face under the deepseek-ai namespace.",
        "raw_text": (
            "Hugging Face hosts DeepSeek-V4-Flash under deepseek-ai. "
            "The model repository is a primary distribution signal and should be tied to open model availability, inference cost and China AI competitiveness."
        ),
        "metadata": {"language": "en", "bootstrap": True, "report_type": "model_card"},
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def get_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en,zh-CN;q=0.8,zh;q=0.7"})
    proxy = (os.getenv("INTELLIGENCE_PROXY") or "").strip()
    if proxy and proxy.lower() not in {"direct", "none", "0", "false"}:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def ensure_sources(conn: sqlite3.Connection) -> None:
    for source in SOURCES:
        conn.execute(
            """
            INSERT INTO source_registry
            (source_key, name, source_type, url, category, priority, credibility,
             collection_method, enabled, cadence_minutes, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                name=excluded.name,
                source_type=excluded.source_type,
                url=excluded.url,
                category=excluded.category,
                priority=excluded.priority,
                credibility=excluded.credibility,
                collection_method=excluded.collection_method,
                cadence_minutes=excluded.cadence_minutes,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            (
                source["source_key"],
                source["name"],
                source["source_type"],
                source["url"],
                source.get("category"),
                source.get("priority", 2),
                source.get("credibility", "primary"),
                source.get("collection_method", "http"),
                source.get("cadence_minutes", 30),
                source.get("notes"),
                now_iso(),
            ),
        )


def html_to_document(source: Dict[str, Any], url: str, html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = ""
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title = og_title["content"]
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else source["name"]

    description = ""
    for selector in ({"name": "description"}, {"property": "og:description"}):
        meta = soup.find("meta", attrs=selector)
        if meta and meta.get("content"):
            description = meta["content"]
            break

    published_at = ""
    for selector in ({"property": "article:published_time"}, {"name": "date"}, {"name": "pubdate"}):
        meta = soup.find("meta", attrs=selector)
        if meta and meta.get("content"):
            published_at = meta["content"]
            break

    text = soup.get_text(" ", strip=True)
    return {
        "source_key": source["source_key"],
        "url": url,
        "title": normalize_text(title),
        "published_at": published_at,
        "summary": normalize_text(description or text[:500]),
        "raw_text": normalize_text(text)[:30000],
        "metadata": {"language": "en", "content_type": "html"},
    }


def fetch_fixed_html(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return [html_to_document(source, source["url"], resp.text)]


def fetch_json_list(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    docs: List[Dict[str, Any]] = []
    if isinstance(data, list):
        items = data
    else:
        items = data.get("items") or data.get("models") or []

    for item in items[:30]:
        if not isinstance(item, dict):
            continue
        model_id = item.get("modelId") or item.get("full_name") or item.get("name") or item.get("id") or ""
        html_url = item.get("html_url") or item.get("url") or ""
        if source["source_key"].startswith("huggingface") and model_id:
            html_url = f"https://huggingface.co/{model_id}"
        title = model_id or item.get("name") or html_url or source["name"]
        raw_text = json.dumps(item, ensure_ascii=False, sort_keys=True)
        docs.append(
            {
                "source_key": source["source_key"],
                "url": html_url or f"{source['url']}#{content_hash(raw_text)[:12]}",
                "title": title,
                "published_at": item.get("createdAt") or item.get("created_at") or item.get("pushedAt") or item.get("updated_at") or "",
                "summary": normalize_text(item.get("description") or item.get("pipeline_tag") or title),
                "raw_text": raw_text[:30000],
                "metadata": {"language": "en", "content_type": "json", "raw": item},
            }
        )
    return docs


def upsert_raw_document(conn: sqlite3.Connection, doc: Dict[str, Any]) -> Tuple[int, bool, bool]:
    doc_hash = content_hash((doc.get("title") or "") + "\n" + (doc.get("raw_text") or ""))
    existing = conn.execute("SELECT id, content_hash FROM raw_documents WHERE url = ?", (doc["url"],)).fetchone()
    fetched_at = now_iso()
    if existing:
        changed = existing[1] != doc_hash
        conn.execute(
            """
            UPDATE raw_documents
            SET source_key=?, title=?, published_at=?, fetched_at=?, language=?,
                content_hash=?, summary=?, raw_text=?, metadata_json=?, status='active'
            WHERE id=?
            """,
            (
                doc["source_key"],
                doc.get("title"),
                doc.get("published_at"),
                fetched_at,
                (doc.get("metadata") or {}).get("language", "en"),
                doc_hash,
                doc.get("summary"),
                doc.get("raw_text"),
                json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
                existing[0],
            ),
        )
        return int(existing[0]), False, changed

    cursor = conn.execute(
        """
        INSERT INTO raw_documents
        (source_key, url, canonical_url, title, published_at, fetched_at, language,
         content_hash, summary, raw_text, metadata_json, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        """,
        (
            doc["source_key"],
            doc["url"],
            doc.get("canonical_url") or doc["url"],
            doc.get("title"),
            doc.get("published_at"),
            fetched_at,
            (doc.get("metadata") or {}).get("language", "en"),
            doc_hash,
            doc.get("summary"),
            doc.get("raw_text"),
            json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
        ),
    )
    return int(cursor.lastrowid), True, False


def event_payload_for_document(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = normalize_text(doc.get("title") or "")
    text = normalize_text(f"{title} {doc.get('url') or ''} {doc.get('summary') or ''} {doc.get('raw_text') or ''}")
    lower = text.lower()

    if re.search(r"gpt[- ]?5\.5", lower):
        return {
            "event_key": "openai_gpt_5_5_release_2026_04_24",
            "title": "OpenAI releases GPT-5.5",
            "title_zh": "OpenAI 发布 GPT-5.5",
            "category": "ai_model",
            "priority": "P0",
            "confidence": 0.92,
            "event_time": doc.get("published_at") or "2026-04-24",
            "summary": "OpenAI released GPT-5.5 for complex coding, research, document/spreadsheet work, tool use and real-world workflows.",
            "summary_zh": "OpenAI 发布 GPT-5.5，重点指向复杂代码、研究、文档表格、工具协作和现实工作流。",
            "impact_summary": "Raises the bar for AI agent capability and sustains demand expectations for cloud, data-center capex, inference infrastructure and enterprise AI software.",
            "impact_score": 88,
            "verification_status": "official_confirmed",
            "facts": [
                ("model", "Model", "GPT-5.5", "", 1, 0.95),
                ("capability", "Core capability", "complex coding, research, documents, spreadsheets, tool use", "", 2, 0.85),
                ("context", "Context window", "1M", "tokens", 3, 0.8),
                ("availability", "Availability", "ChatGPT and Codex; API availability expected soon", "", 4, 0.82),
                ("pricing", "GPT-5.5 pricing", "$5/M input, $30/M output", "USD tokens", 5, 0.78),
                ("market", "A/H watch chain", "cloud, data center, servers, optical modules, PCB, storage, AI applications", "", 6, 0.76),
            ],
            "entities": [
                ("company", "OpenAI", "", "", "issuer", 0.98),
                ("product", "GPT-5.5", "", "", "model", 0.98),
                ("product", "Codex", "", "", "developer workflow", 0.76),
                ("sector", "AI infrastructure", "", "global", "impact_chain", 0.86),
                ("sector", "AI applications", "", "global", "impact_chain", 0.78),
            ],
            "research": {
                "report_type": "system_card" if "system card" in lower else "official_release",
                "thesis": "GPT-5.5 strengthens the AI agent and enterprise workflow cycle; track inference cost, cloud capex and software adoption.",
                "relevance": "AI, cloud, semiconductors, server supply chain, enterprise software",
            },
        }

    if "deepseek-v4" in lower or "deepseek v4" in lower or "deepseek_v4" in lower or (
        "deepseek" in lower and re.search(r"\bgpt[- ]?v4\b|\bv4\b", lower)
    ):
        return {
            "event_key": "deepseek_v4_release_2026_04_24",
            "title": "DeepSeek V4 models appear across official docs and model repositories",
            "title_zh": "DeepSeek V4 模型在官方文档和模型仓库出现",
            "category": "ai_model",
            "priority": "P0",
            "confidence": 0.9,
            "event_time": doc.get("published_at") or "2026-04-24",
            "summary": "DeepSeek V4 Pro and Flash signals point to a new China model cycle, with API docs, Hugging Face distribution and deprecation notices for older models.",
            "summary_zh": "DeepSeek V4 Pro/Flash 指向新一轮国产大模型周期，官方 API 文档、Hugging Face 仓库和旧模型弃用节奏构成关键证据。",
            "impact_summary": "Strengthens China AI competitiveness narrative and should be tied to domestic inference stack, model application, cloud and compute supply-chain validation.",
            "impact_score": 86,
            "verification_status": "primary_sources_seen",
            "facts": [
                ("model", "Model family", "DeepSeek V4", "", 1, 0.95),
                ("model", "API model", "deepseek-v4-flash", "", 2, 0.92),
                ("model", "API model", "deepseek-v4-pro", "", 3, 0.9),
                ("lifecycle", "Old model deprecation", "deepseek-chat and deepseek-reasoner deprecated on 2026/07/24", "", 4, 0.85),
                ("context", "Context window", "1M", "tokens", 5, 0.78),
                ("market", "A/H watch chain", "domestic AI apps, inference hardware, servers, cloud, optical modules, memory", "", 6, 0.76),
            ],
            "entities": [
                ("company", "DeepSeek", "", "China", "issuer", 0.98),
                ("product", "DeepSeek V4", "", "", "model", 0.98),
                ("product", "DeepSeek-V4-Flash", "", "", "model", 0.9),
                ("platform", "Hugging Face", "", "global", "distribution", 0.68),
                ("sector", "China AI infrastructure", "", "A/H", "impact_chain", 0.86),
            ],
            "research": {
                "report_type": "model_card" if "huggingface" in lower else "official_docs",
                "thesis": "DeepSeek V4 improves the China AI prepositioning case; verify model usage, API pricing, open-weight adoption and downstream order signals.",
                "relevance": "China AI, cloud, semiconductors, inference, software",
            },
        }

    return None


def upsert_event_bundle(conn: sqlite3.Connection, doc_id: int, doc: Dict[str, Any], bundle: Dict[str, Any]) -> bool:
    first_seen = now_iso()
    existing = conn.execute("SELECT id, first_seen_at FROM intelligence_events WHERE event_key = ?", (bundle["event_key"],)).fetchone()
    if existing:
        event_id = int(existing[0])
        first_seen = existing[1] or first_seen
        conn.execute(
            """
            UPDATE intelligence_events
            SET title=?, title_zh=?, category=?, priority=?, confidence=?,
                first_seen_at=?, last_seen_at=?, event_time=?, summary=?, summary_zh=?,
                impact_summary=?, impact_score=?, verification_status=?,
                primary_source_url=COALESCE(primary_source_url, ?), updated_at=?
            WHERE id=?
            """,
            (
                bundle["title"],
                bundle.get("title_zh"),
                bundle["category"],
                bundle["priority"],
                bundle.get("confidence", 0.5),
                first_seen,
                now_iso(),
                bundle.get("event_time"),
                bundle.get("summary"),
                bundle.get("summary_zh"),
                bundle.get("impact_summary"),
                bundle.get("impact_score", 0),
                bundle.get("verification_status", "watching"),
                doc["url"],
                now_iso(),
                event_id,
            ),
        )
        added = False
    else:
        cursor = conn.execute(
            """
            INSERT INTO intelligence_events
            (event_key, title, title_zh, category, priority, status, confidence,
             first_seen_at, last_seen_at, event_time, summary, summary_zh,
             impact_summary, impact_score, verification_status, primary_source_url, updated_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle["event_key"],
                bundle["title"],
                bundle.get("title_zh"),
                bundle["category"],
                bundle["priority"],
                bundle.get("confidence", 0.5),
                first_seen,
                first_seen,
                bundle.get("event_time"),
                bundle.get("summary"),
                bundle.get("summary_zh"),
                bundle.get("impact_summary"),
                bundle.get("impact_score", 0),
                bundle.get("verification_status", "watching"),
                doc["url"],
                now_iso(),
            ),
        )
        event_id = int(cursor.lastrowid)
        added = True

    for fact_type, label, value, unit, sort_order, confidence in bundle.get("facts") or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO event_facts
            (event_id, fact_type, label, value, unit, source_url, confidence, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, fact_type, label, value, unit, doc["url"], confidence, sort_order),
        )

    for entity_type, name, ticker, market, role, relevance_score in bundle.get("entities") or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO event_entities
            (event_id, entity_type, name, ticker, market, role, relevance_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, entity_type, name, ticker, market, role, relevance_score),
        )

    conn.execute(
        """
        INSERT OR IGNORE INTO event_updates
        (event_id, raw_document_id, source_key, update_type, title, summary, published_at, url, created_at)
        VALUES (?, ?, ?, 'source_seen', ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            doc_id,
            doc["source_key"],
            doc.get("title"),
            doc.get("summary"),
            doc.get("published_at"),
            doc["url"],
            now_iso(),
        ),
    )

    conn.execute(
        """
        UPDATE intelligence_events
        SET source_count = (
            SELECT COUNT(DISTINCT source_key)
            FROM event_updates
            WHERE event_id = ?
        )
        WHERE id = ?
        """,
        (event_id, event_id),
    )

    research = bundle.get("research")
    if research:
        report_key = content_hash(f"{bundle['event_key']}|{doc['url']}")[:24]
        conn.execute(
            """
            INSERT INTO research_reports
            (report_key, title, title_zh, source_key, source_name, url, report_type,
             published_at, fetched_at, language, summary, thesis, relevance, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'en', ?, ?, ?, 'active')
            ON CONFLICT(report_key) DO UPDATE SET
                title=excluded.title,
                title_zh=excluded.title_zh,
                source_key=excluded.source_key,
                source_name=excluded.source_name,
                report_type=excluded.report_type,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                summary=excluded.summary,
                thesis=excluded.thesis,
                relevance=excluded.relevance,
                status='active'
            """,
            (
                report_key,
                doc.get("title") or bundle["title"],
                bundle.get("title_zh"),
                doc["source_key"],
                doc["source_key"],
                doc["url"],
                research.get("report_type", "research"),
                doc.get("published_at"),
                now_iso(),
                doc.get("summary") or bundle.get("summary"),
                research.get("thesis"),
                research.get("relevance"),
            ),
        )
    return added


def source_by_key() -> Dict[str, Dict[str, Any]]:
    return {source["source_key"]: source for source in SOURCES}


def update_source_status(conn: sqlite3.Connection, source_key: str, ok: bool, error: str = "") -> None:
    conn.execute(
        """
        UPDATE source_registry
        SET last_checked_at=?, last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END,
            last_error=?, updated_at=?
        WHERE source_key=?
        """,
        (now_iso(), 1 if ok else 0, now_iso(), error[:500] if error else None, now_iso(), source_key),
    )


def run_collection() -> Dict[str, int]:
    service = IntelligenceService(DB_PATH)
    session = get_session()
    total_found = 0
    total_added_docs = 0
    total_updated_docs = 0
    total_events_added = 0
    failures = 0

    with get_sqlite_connection(DB_PATH, timeout=60, busy_timeout=15000) as conn:
        ensure_sources(conn)
        conn.commit()

        documents_by_source: Dict[str, List[Dict[str, Any]]] = {}
        if BOOTSTRAP_SEED:
            for doc in BOOTSTRAP_DOCUMENTS:
                documents_by_source.setdefault(doc["source_key"], []).append(doc)

        for source in SOURCES:
            if source["collection_method"] == "bootstrap":
                continue
            started_at = now_iso()
            run_status = "success"
            found = 0
            added_docs = 0
            updated_docs = 0
            error = ""
            try:
                if source["collection_method"] == "fixed_html":
                    docs = fetch_fixed_html(session, source)
                elif source["collection_method"] == "json_list":
                    docs = fetch_json_list(session, source)
                else:
                    docs = []
                documents_by_source.setdefault(source["source_key"], []).extend(docs)
                found = len(docs)
                update_source_status(conn, source["source_key"], ok=True)
            except Exception as exc:
                failures += 1
                run_status = "failed"
                error = str(exc)[:500]
                update_source_status(conn, source["source_key"], ok=False, error=error)
                print(f"  {source['source_key']} FAIL: {error}")

            conn.execute(
                """
                INSERT INTO collection_runs
                (source_key, started_at, finished_at, status, records_found, records_added, records_updated, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (source["source_key"], started_at, now_iso(), run_status, found, added_docs, updated_docs, error),
            )
            conn.commit()

        source_map = source_by_key()
        for source_key, docs in documents_by_source.items():
            for doc in docs:
                doc.setdefault("source_key", source_key)
                total_found += 1
                doc_id, added_doc, updated_doc = upsert_raw_document(conn, doc)
                if added_doc:
                    total_added_docs += 1
                if updated_doc:
                    total_updated_docs += 1
                bundle = event_payload_for_document(doc)
                if bundle:
                    if upsert_event_bundle(conn, doc_id, doc, bundle):
                        total_events_added += 1
            update_source_status(conn, source_key, ok=True)

        conn.commit()

    _service = service  # keep ensure_tables side effect obvious for linters
    return {
        "records_found": total_found,
        "records_added": total_added_docs,
        "records_updated": total_updated_docs,
        "events_added": total_events_added,
        "records_failed": failures,
    }


def main() -> int:
    print("=" * 60)
    print(f"Intelligence Sync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    metrics = run_collection()
    print(
        f"[OK] Intelligence sync: found={metrics['records_found']}, "
        f"added_docs={metrics['records_added']}, updated_docs={metrics['records_updated']}, "
        f"new_events={metrics['events_added']}, failed_sources={metrics['records_failed']}"
    )
    print("ETL_METRICS_JSON=" + json.dumps(metrics, ensure_ascii=False))
    # Partial source failures are acceptable because pages read persisted data.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
