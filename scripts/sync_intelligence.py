# -*- coding: utf-8 -*-
"""Continuously collect high-impact AI and market intelligence into SQLite.

The website reads these tables only. This script may run on Windows or on an
overseas collector; the storage target remains the Windows SQLite database.
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.db import get_sqlite_connection  # noqa: E402
    from app.services.intelligence_service import IntelligenceService  # noqa: E402
except Exception:
    get_sqlite_connection = None
    IntelligenceService = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db"))

USER_AGENT = os.getenv(
    "INTELLIGENCE_USER_AGENT",
    "InvestmentHubIntelligence/1.0 (+https://github.com/luolinhan/investment-decision-system)",
)
REQUEST_TIMEOUT = int(os.getenv("INTELLIGENCE_TIMEOUT_SECONDS", "25"))
BOOTSTRAP_SEED = os.getenv("INTELLIGENCE_BOOTSTRAP_SEED", "1") != "0"
COLLECTOR_MODE = os.getenv("INTELLIGENCE_COLLECTOR_MODE", "windows").strip().lower()
SEARCH_PROXY_URL = os.getenv("INTELLIGENCE_SEARCH_PROXY_URL", "").rstrip("/")


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
        "enabled": 0,
        "cadence_minutes": 15,
        "notes": "Official launch page for GPT-5.5. Direct fetch is blocked; use search signal and bootstrap facts.",
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
        "enabled": 0,
        "cadence_minutes": 30,
        "notes": "Official safety and capability system card. Direct fetch is blocked; use search signal and bootstrap facts.",
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
        "collector": "aliyun",
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
        "collector": "aliyun",
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
        "source_key": "openai_gpt55_search",
        "name": "OpenAI GPT-5.5 Search Signal",
        "source_type": "official_search",
        "url": "search://site:openai.com/index/introducing-gpt-5-5 GPT-5.5",
        "query": "site:openai.com/index/introducing-gpt-5-5 GPT-5.5",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official_search",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "OpenAI blocks direct fetch; search-proxy captures official page title/snippet.",
    },
    {
        "source_key": "anthropic_news",
        "name": "Anthropic News",
        "source_type": "official",
        "url": "https://www.anthropic.com/news",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "Official Anthropic model and product news.",
    },
    {
        "source_key": "google_ai_blog_rss",
        "name": "Google AI Blog RSS",
        "source_type": "official_feed",
        "url": "https://blog.google/technology/ai/rss/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "Official Google AI announcements.",
    },
    {
        "source_key": "google_deepmind_blog",
        "name": "Google DeepMind Blog",
        "source_type": "official",
        "url": "https://deepmind.google/discover/blog/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Google DeepMind research and model announcements.",
    },
    {
        "source_key": "meta_ai_blog",
        "name": "Meta AI Blog",
        "source_type": "official",
        "url": "https://ai.meta.com/blog/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official Meta AI research and product announcements.",
    },
    {
        "source_key": "mistral_news",
        "name": "Mistral AI News",
        "source_type": "official",
        "url": "https://mistral.ai/news/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official Mistral AI announcements.",
    },
    {
        "source_key": "xai_news",
        "name": "xAI News",
        "source_type": "official",
        "url": "https://x.ai/news",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "enabled": 0,
        "cadence_minutes": 45,
        "notes": "Official xAI announcements. Direct fetch currently returns 403; disabled until a stable feed is available.",
    },
    {
        "source_key": "huggingface_trending_models",
        "name": "Hugging Face Most Liked Models",
        "source_type": "model_repo",
        "url": "https://huggingface.co/api/models?sort=likes&direction=-1&limit=25",
        "category": "ai_model",
        "priority": 1,
        "credibility": "model_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "High-attention model repositories for model-cycle detection.",
    },
    {
        "source_key": "github_openai_repos",
        "name": "OpenAI GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/openai/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official OpenAI repository updates.",
    },
    {
        "source_key": "github_modelcontextprotocol_repos",
        "name": "Model Context Protocol GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/modelcontextprotocol/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Agent/tooling ecosystem signal.",
    },
    {
        "source_key": "arxiv_ai_recent",
        "name": "arXiv AI Recent",
        "source_type": "research_feed",
        "url": "https://export.arxiv.org/api/query?search_query=cat:cs.AI+OR+cat:cs.LG&sortBy=submittedDate&sortOrder=descending&max_results=25",
        "category": "ai_research",
        "priority": 2,
        "credibility": "research_preprint",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 120,
        "notes": "Recent AI/ML papers; P2 research signal only.",
    },
    {
        "source_key": "sec_current_8k",
        "name": "SEC Current 8-K Feed",
        "source_type": "regulatory_feed",
        "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&owner=include&count=40&output=atom",
        "category": "company_filing",
        "priority": 2,
        "credibility": "regulatory",
        "collection_method": "feed",
        "collector": "aliyun",
        "enabled": 0,
        "cadence_minutes": 60,
        "notes": "US company current reports for cross-market shocks. Disabled until SEC user-agent/access policy is handled.",
    },
    {
        "source_key": "fda_press_rss",
        "name": "FDA Press Announcements",
        "source_type": "regulatory_feed",
        "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
        "category": "biotech",
        "priority": 2,
        "credibility": "regulatory",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 120,
        "notes": "FDA press releases for global biotech and approval signals.",
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                name=excluded.name,
                source_type=excluded.source_type,
                url=excluded.url,
                category=excluded.category,
                priority=excluded.priority,
                credibility=excluded.credibility,
                collection_method=excluded.collection_method,
                enabled=excluded.enabled,
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
                int(source.get("enabled", 1)),
                source.get("cadence_minutes", 30),
                source.get("notes"),
                now_iso(),
            ),
        )


def html_to_document(source: Dict[str, Any], url: str, html: str) -> Dict[str, Any]:
    if BeautifulSoup is None:
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html or "", flags=re.IGNORECASE | re.DOTALL)
        title = normalize_text(re.sub(r"<[^>]+>", " ", title_match.group(1))) if title_match else source["name"]
        desc_match = re.search(
            r"<meta[^>]+(?:name|property)=[\"'](?:description|og:description)[\"'][^>]+content=[\"'](.*?)[\"']",
            html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )
        description = normalize_text(desc_match.group(1)) if desc_match else ""
        text = normalize_text(re.sub(r"<[^>]+>", " ", html or ""))
        return {
            "source_key": source["source_key"],
            "url": url,
            "title": title,
            "published_at": "",
            "summary": description or text[:500],
            "raw_text": text[:30000],
            "metadata": {"language": "en", "content_type": "html"},
        }

    soup = BeautifulSoup(html, "html.parser")
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
                "published_at": (
                    item.get("lastModified")
                    or item.get("last_modified")
                    or item.get("pushedAt")
                    or item.get("pushed_at")
                    or item.get("updated_at")
                    or item.get("createdAt")
                    or item.get("created_at")
                    or ""
                ),
                "summary": normalize_text(item.get("description") or item.get("pipeline_tag") or title),
                "raw_text": raw_text[:30000],
                "metadata": {"language": "en", "content_type": "json", "raw": item},
            }
        )
    return docs


def fetch_feed(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    docs: List[Dict[str, Any]] = []

    channel_items = root.findall(".//item")
    atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    entries = channel_items or atom_entries

    def child_text(node: ET.Element, names: List[str]) -> str:
        for name in names:
            child = node.find(name)
            if child is not None and child.text:
                return normalize_text(child.text)
            child = node.find("{http://www.w3.org/2005/Atom}" + name)
            if child is not None and child.text:
                return normalize_text(child.text)
        return ""

    for entry in entries[:40]:
        title = child_text(entry, ["title"]) or source["name"]
        link = child_text(entry, ["link"])
        if not link:
            atom_link = entry.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href", "")
        published = child_text(entry, ["pubDate", "published", "updated"])
        summary = child_text(entry, ["description", "summary", "content"])
        raw_text = normalize_text(" ".join(part for part in (title, summary, published, link) if part))
        docs.append(
            {
                "source_key": source["source_key"],
                "url": link or f"{source['url']}#{content_hash(raw_text)[:12]}",
                "title": title,
                "published_at": published,
                "summary": summary[:1200],
                "raw_text": raw_text[:30000],
                "metadata": {"language": "en", "content_type": "feed"},
            }
        )
    return docs


def fetch_search_proxy(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not SEARCH_PROXY_URL:
        raise RuntimeError("INTELLIGENCE_SEARCH_PROXY_URL is not configured")
    query = source.get("query") or source["url"].replace("search://", "")
    resp = session.post(
        f"{SEARCH_PROXY_URL}/search",
        json={"q": query, "num_results": 6, "nocache": 1},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    docs: List[Dict[str, Any]] = []
    for item in (payload.get("results") or [])[:6]:
        if not isinstance(item, dict):
            continue
        title = normalize_text(item.get("title") or source["name"])
        url = item.get("url") or f"{source['url']}#{content_hash(title)[:12]}"
        snippet = normalize_text(item.get("snippet") or "")
        docs.append(
            {
                "source_key": source["source_key"],
                "url": url,
                "title": title,
                "published_at": "",
                "summary": snippet,
                "raw_text": normalize_text(f"{title} {snippet} {url}")[:30000],
                "metadata": {
                    "language": "en",
                    "content_type": "search_result",
                    "query": query,
                    "engine": item.get("engine"),
                },
            }
        )
    return docs


def source_runs_on_current_collector(source: Dict[str, Any]) -> bool:
    if not int(source.get("enabled", 1)):
        return False
    collector = (source.get("collector") or "all").lower()
    return collector in {"all", "both", COLLECTOR_MODE}


def upsert_raw_document(conn: sqlite3.Connection, doc: Dict[str, Any]) -> Tuple[int, bool, bool]:
    doc_hash = content_hash((doc.get("title") or "") + "\n" + (doc.get("raw_text") or ""))
    existing = conn.execute("SELECT id, content_hash FROM raw_documents WHERE url = ?", (doc["url"],)).fetchone()
    fetched_at = now_iso()
    if existing:
        changed = existing[1] != doc_hash
        conn.execute(
            """
            UPDATE raw_documents
            SET source_key=?, title=?, title_zh=?, published_at=?, fetched_at=?, language=?,
                content_hash=?, summary=?, summary_zh=?, raw_text=?, metadata_json=?, status='active'
            WHERE id=?
            """,
            (
                doc["source_key"],
                doc.get("title"),
                doc.get("title_zh"),
                doc.get("published_at"),
                fetched_at,
                (doc.get("metadata") or {}).get("language", "en"),
                doc_hash,
                doc.get("summary"),
                doc.get("summary_zh"),
                doc.get("raw_text"),
                json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
                existing[0],
            ),
        )
        return int(existing[0]), False, changed

    cursor = conn.execute(
        """
        INSERT INTO raw_documents
        (source_key, url, canonical_url, title, title_zh, published_at, fetched_at, language,
         content_hash, summary, summary_zh, raw_text, metadata_json, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        """,
        (
            doc["source_key"],
            doc["url"],
            doc.get("canonical_url") or doc["url"],
            doc.get("title"),
            doc.get("title_zh"),
            doc.get("published_at"),
            fetched_at,
            (doc.get("metadata") or {}).get("language", "en"),
            doc_hash,
            doc.get("summary"),
            doc.get("summary_zh"),
            doc.get("raw_text"),
            json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
        ),
    )
    return int(cursor.lastrowid), True, False


OFFICIAL_AI_SIGNAL_SOURCES = {
    "openai_gpt55_search",
    "anthropic_news",
    "google_ai_blog_rss",
    "google_deepmind_blog",
    "meta_ai_blog",
    "mistral_news",
}

MODEL_REPO_SIGNAL_SOURCES = {
    "deepseek_hf_flash",
    "huggingface_deepseek_models",
    "huggingface_trending_models",
    "github_deepseek_repos",
    "github_openai_repos",
    "github_modelcontextprotocol_repos",
}

EVENT_PROMOTION_LIMITS = {
    "huggingface_deepseek_models": 8,
    "github_deepseek_repos": 6,
    "huggingface_trending_models": 6,
    "github_openai_repos": 6,
    "github_modelcontextprotocol_repos": 6,
    "google_ai_blog_rss": 8,
    "arxiv_ai_recent": 8,
    "fda_press_rss": 8,
}

AI_SOURCE_ENTITIES = {
    "openai_gpt55_search": "OpenAI",
    "anthropic_news": "Anthropic",
    "google_ai_blog_rss": "Google",
    "google_deepmind_blog": "Google DeepMind",
    "meta_ai_blog": "Meta AI",
    "mistral_news": "Mistral AI",
    "deepseek_hf_flash": "DeepSeek",
    "huggingface_deepseek_models": "DeepSeek",
    "github_deepseek_repos": "DeepSeek",
    "github_openai_repos": "OpenAI",
    "github_modelcontextprotocol_repos": "Model Context Protocol",
}

OFFICIAL_AI_TERMS = (
    "introducing",
    "announce",
    "announcing",
    "launch",
    "release",
    "available",
    "general availability",
    "new model",
    "frontier model",
    "reasoning model",
    "agent",
    "agents",
    "inference",
    "benchmark",
    "claude",
    "gemini",
    "llama",
    "mistral",
    "gpt",
    "deepseek",
    "grok",
    "alphafold",
    "alphago",
    "vertex ai",
    "cloud tpu",
    "data center",
)

REPO_SIGNAL_TERMS = (
    "deepseek",
    "gpt",
    "openai",
    "qwen",
    "llama",
    "mistral",
    "gemma",
    "phi",
    "claude",
    "gemini",
    "reasoning",
    "agent",
    "agents",
    "inference",
    "multimodal",
    "vision",
    "audio",
    "vl",
    "v4",
    "r1",
    "moe",
    "diffusion",
    "embedding",
    "reranker",
    "mcp",
    "codex",
    "eval",
    "sdk",
    "server",
)

RESEARCH_SIGNAL_TERMS = (
    "large language model",
    "llm",
    "reasoning",
    "agent",
    "agents",
    "inference",
    "benchmark",
    "multimodal",
    "diffusion",
    "reinforcement learning",
    "transformer",
    "mixture of experts",
    "gpu",
)

FDA_SIGNAL_TERMS = (
    "fda approves",
    "approval",
    "approves",
    "authorizes",
    "clearance",
    "clinical",
    "drug",
    "therapy",
    "biologics",
    "vaccine",
)

GENERIC_NOISE_TERMS = (
    "privacy policy",
    "terms of use",
    "careers",
    "cookie",
    "press kit",
    "newsletter",
    "podcast",
)


def source_meta_for_key(source_key: str) -> Dict[str, Any]:
    for source in SOURCES:
        if source["source_key"] == source_key:
            return source
    return {"source_key": source_key, "name": source_key, "category": "intelligence", "source_type": "unknown"}


def compact_event_slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")
    slug = slug[:max_len].strip("_")
    return slug or content_hash(value)[:12]


def contains_any(text: str, terms: Iterable[str]) -> bool:
    return any(term in text for term in terms)


def generic_event_key(category: str, source_key: str, title: str, url: str) -> str:
    return f"{category}_{source_key}_{compact_event_slug(title)}_{content_hash(url or title)[:8]}"


def clean_doc_summary(doc: Dict[str, Any], title: str, max_len: int = 520) -> str:
    summary = normalize_text(doc.get("summary") or "")
    if not summary or summary == title:
        summary = normalize_text(doc.get("raw_text") or "")[:max_len]
    return summary[:max_len] if summary else title


def metadata_raw(doc: Dict[str, Any]) -> Dict[str, Any]:
    raw = (doc.get("metadata") or {}).get("raw") or {}
    return raw if isinstance(raw, dict) else {}


def repository_metric_facts(doc: Dict[str, Any], start_order: int) -> List[Tuple[str, str, str, str, str, str, int, float]]:
    raw = metadata_raw(doc)
    facts: List[Tuple[str, str, str, str, str, str, int, float]] = []
    metric_candidates = [
        ("stars", "Stars", "星标数", raw.get("stargazers_count")),
        ("forks", "Forks", "Fork 数", raw.get("forks_count")),
        ("likes", "Likes", "点赞数", raw.get("likes")),
        ("downloads", "Downloads", "下载量", raw.get("downloads")),
        ("pipeline", "Pipeline", "模型任务", raw.get("pipeline_tag")),
    ]
    order = start_order
    for fact_type, label, label_zh, value in metric_candidates:
        if value is None or value == "":
            continue
        facts.append((fact_type, label, label_zh, str(value), str(value), "", order, 0.58))
        order += 1
    return facts


def material_repo_signal(source_key: str, lower: str, title: str) -> bool:
    title_lower = title.lower()
    if source_key == "huggingface_deepseek_models":
        return contains_any(title_lower, ("v4", "v3.2", "r1", "ocr", "prover", "math", "vl", "coder", "moe"))
    if source_key == "github_deepseek_repos":
        return contains_any(
            title_lower,
            ("v4", "v3", "r1", "ocr", "vl", "coder", "moe", "deepgemm", "deepep", "flashmla", "tilekernels", "3fs"),
        )
    if source_key == "huggingface_trending_models":
        return contains_any(
            title_lower,
            ("deepseek", "qwen", "llama", "mistral", "gemma", "phi", "whisper", "stable-diffusion", "flux", "gpt", "vl"),
        )
    if source_key == "github_openai_repos":
        return contains_any(
            title_lower,
            ("codex", "agents", "gpt", "eval", "openai-python", "openai-node", "realtime", "responses", "skills"),
        )
    if source_key == "github_modelcontextprotocol_repos":
        return contains_any(
            title_lower,
            ("sdk", "servers", "inspector", "registry", "specification", "conformance", "experimental", "auth"),
        )
    return contains_any(lower, REPO_SIGNAL_TERMS)


def generic_signal_event_payload(doc: Dict[str, Any], title: str, text: str, lower: str) -> Optional[Dict[str, Any]]:
    source_key = doc.get("source_key") or ""
    source = source_meta_for_key(source_key)
    source_name = source.get("name") or source_key
    url = doc.get("url") or ""
    event_time = doc.get("published_at") or now_iso()
    summary = clean_doc_summary(doc, title)

    has_specific_signal = contains_any(lower, OFFICIAL_AI_TERMS + REPO_SIGNAL_TERMS + RESEARCH_SIGNAL_TERMS)
    if contains_any(lower, GENERIC_NOISE_TERMS) and not has_specific_signal:
        return None

    if source_key in OFFICIAL_AI_SIGNAL_SOURCES and contains_any(lower, OFFICIAL_AI_TERMS):
        company = AI_SOURCE_ENTITIES.get(source_key, source_name)
        return {
            "event_key": generic_event_key("ai_official", source_key, title, url),
            "title": f"{company} official AI signal: {title}",
            "title_zh": f"{company} 官方 AI 信号：{title}",
            "category": "ai_model",
            "priority": "P1",
            "confidence": 0.68,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"官方源出现 AI 模型、产品或基础设施相关信号：{summary}",
            "impact_summary": (
                "Treat as a pre-confirmation signal for the AI cycle. Verify model capability, availability, pricing, "
                "developer adoption and A/H supply-chain read-through before raising exposure."
            ),
            "impact_summary_zh": "作为 AI 周期预确认信号处理，后续验证模型能力、可用性、定价、开发者采用和 A/H 供应链映射。",
            "impact_score": 68,
            "verification_status": "official_signal",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.85),
                ("signal", "Signal title", "信号标题", title, title, "", 2, 0.72),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.56),
                (
                    "market",
                    "A/H watch chain",
                    "A/H 观察链条",
                    "AI applications, cloud, servers, optical modules, PCB, memory, edge devices",
                    "AI 应用、云、服务器、光模块、PCB、存储、端侧设备",
                    "",
                    4,
                    0.64,
                ),
            ],
            "entities": [
                ("company", company, "", "", "issuer", 0.82),
                ("sector", "AI infrastructure", "", "A/H", "impact_chain", 0.72),
                ("sector", "AI applications", "", "A/H", "impact_chain", 0.66),
            ],
            "research": {
                "report_type": "official_signal",
                "thesis": "Official AI updates can reprice model-cycle expectations; track follow-on benchmark, API, pricing and capex evidence.",
                "thesis_zh": "官方 AI 更新可能重估模型周期预期，后续跟踪 benchmark、API、定价和资本开支证据。",
                "relevance": "AI models, cloud, semiconductors, server supply chain, software applications",
                "relevance_zh": "AI 模型、云、半导体、服务器供应链、软件应用",
            },
        }

    if source_key in MODEL_REPO_SIGNAL_SOURCES and material_repo_signal(source_key, lower, title):
        company = AI_SOURCE_ENTITIES.get(source_key, "AI ecosystem")
        facts = [
            ("source", "Source", "来源", source_name, source_name, "", 1, 0.72),
            ("repo", "Repository/model", "仓库或模型", title, title, "", 2, 0.7),
            ("time", "Created or updated", "创建或更新时间", event_time, event_time, "", 3, 0.52),
            (
                "market",
                "Validation path",
                "验证路径",
                "watch downloads, stars, API references, inference cost and downstream app integration",
                "跟踪下载、星标、API 引用、推理成本和下游应用集成",
                "",
                4,
                0.58,
            ),
        ]
        facts.extend(repository_metric_facts(doc, 5))
        return {
            "event_key": generic_event_key("ai_repo", source_key, title, url),
            "title": f"AI repository signal: {title}",
            "title_zh": f"AI 仓库信号：{title}",
            "category": "ai_tooling" if "mcp" in lower or "sdk" in lower or "codex" in lower else "ai_model",
            "priority": "P2",
            "confidence": 0.52,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"模型或代码仓库出现更新信号：{summary}",
            "impact_summary": (
                "Repository activity is an early but noisy adoption signal. Promote only after official announcement, benchmark, "
                "download, API or downstream order evidence improves."
            ),
            "impact_summary_zh": "仓库活跃度是早期但噪声较高的采用信号，需结合官方公告、benchmark、下载、API 或下游订单证据再升级。",
            "impact_score": 52,
            "verification_status": "repo_signal",
            "facts": facts,
            "entities": [
                ("company", company, "", "", "maintainer", 0.7),
                ("platform", "GitHub" if "github" in source_key else "Hugging Face", "", "global", "distribution", 0.66),
                ("sector", "AI developer ecosystem", "", "global", "impact_chain", 0.62),
            ],
            "research": {
                "report_type": "repo_signal",
                "thesis": "Repository updates help detect early AI tooling and model adoption, but need external validation before becoming a trade thesis.",
                "thesis_zh": "仓库更新可帮助发现 AI 工具和模型采用早期变化，但成为交易线索前需要外部验证。",
                "relevance": "AI developer tools, model distribution, inference stack",
                "relevance_zh": "AI 开发工具、模型分发、推理栈",
            },
        }

    if source_key == "arxiv_ai_recent" and contains_any(lower, RESEARCH_SIGNAL_TERMS):
        return {
            "event_key": generic_event_key("ai_research", source_key, title, url),
            "title": f"AI research signal: {title}",
            "title_zh": f"AI 研究信号：{title}",
            "category": "ai_research",
            "priority": "P2",
            "confidence": 0.42,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"arXiv 出现 AI 研究前沿信号：{summary}",
            "impact_summary": "Use as a technology radar input, not a standalone trading signal; verify whether it enters product releases, benchmarks or capex plans.",
            "impact_summary_zh": "作为技术雷达输入，而非独立交易信号；需验证是否进入产品发布、评测榜单或资本开支计划。",
            "impact_score": 42,
            "verification_status": "research_preprint",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.66),
                ("paper", "Paper title", "论文标题", title, title, "", 2, 0.66),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.5),
                (
                    "market",
                    "Follow-up validation",
                    "后续验证",
                    "model release, benchmark adoption, cloud/GPU demand, enterprise productization",
                    "模型发布、评测采用、云/GPU 需求、企业产品化",
                    "",
                    4,
                    0.48,
                ),
            ],
            "entities": [
                ("sector", "AI research", "", "global", "technology_signal", 0.7),
                ("sector", "AI infrastructure", "", "A/H", "impact_chain", 0.5),
            ],
            "research": {
                "report_type": "preprint",
                "thesis": "Recent AI papers can foreshadow model capability shifts; keep them in the radar until product or benchmark confirmation appears.",
                "thesis_zh": "近期 AI 论文可能提前反映模型能力变化，在产品或 benchmark 确认前作为雷达信号保留。",
                "relevance": "AI research, model capability, compute demand",
                "relevance_zh": "AI 研究、模型能力、算力需求",
            },
        }

    if source_key == "fda_press_rss" and contains_any(lower, FDA_SIGNAL_TERMS):
        is_approval = "approves" in lower or "approval" in lower or "fda approves" in lower
        return {
            "event_key": generic_event_key("biotech_regulatory", source_key, title, url),
            "title": f"FDA biotech signal: {title}",
            "title_zh": f"FDA 生物医药信号：{title}",
            "category": "biotech",
            "priority": "P1" if is_approval else "P2",
            "confidence": 0.66 if is_approval else 0.48,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"FDA 监管源出现药品、疗法或临床相关信号：{summary}",
            "impact_summary": "Map to HK/A biotech peers by indication, modality and partner pipeline; verify licensing, milestones and clinical read-through.",
            "impact_summary_zh": "按适应症、技术路线和合作管线映射港股/A股创新药标的，后续验证 BD、里程碑和临床读出传导。",
            "impact_score": 64 if is_approval else 48,
            "verification_status": "regulatory_release",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.82),
                ("signal", "Regulatory signal", "监管信号", title, title, "", 2, 0.72),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.54),
                (
                    "market",
                    "A/H watch chain",
                    "A/H 观察链条",
                    "innovative drugs, CRO/CDMO, licensing, clinical catalysts",
                    "创新药、CRO/CDMO、授权交易、临床催化",
                    "",
                    4,
                    0.58,
                ),
            ],
            "entities": [
                ("regulator", "FDA", "", "US", "source", 0.92),
                ("sector", "Biotech", "", "A/H", "impact_chain", 0.68),
            ],
            "research": {
                "report_type": "regulatory_release",
                "thesis": "FDA regulatory releases can validate modalities and indications relevant to HK/A biotech pipelines.",
                "thesis_zh": "FDA 监管公告可验证与港股/A股创新药管线相关的技术路线和适应症。",
                "relevance": "Biotech, innovative drugs, CRO/CDMO, licensing",
                "relevance_zh": "生物医药、创新药、CRO/CDMO、授权交易",
            },
        }

    return None


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
            "impact_summary_zh": "提高 AI Agent 能力门槛，并继续支撑云、数据中心资本开支、推理基础设施和企业 AI 软件的需求预期。",
            "impact_score": 88,
            "verification_status": "official_confirmed",
            "facts": [
                ("model", "Model", "模型", "GPT-5.5", "GPT-5.5", "", 1, 0.95),
                ("capability", "Core capability", "核心能力", "complex coding, research, documents, spreadsheets, tool use", "复杂代码、研究、文档、表格和工具调用", "", 2, 0.85),
                ("context", "Context window", "上下文窗口", "1M", "100 万", "tokens", 3, 0.8),
                ("availability", "Availability", "可用性", "ChatGPT and Codex; API availability expected soon", "ChatGPT 与 Codex 已可用；API 预计随后开放", "", 4, 0.82),
                ("pricing", "GPT-5.5 pricing", "GPT-5.5 定价", "$5/M input, $30/M output", "输入 5 美元/百万 token，输出 30 美元/百万 token", "USD tokens", 5, 0.78),
                ("market", "A/H watch chain", "A/H 观察链条", "cloud, data center, servers, optical modules, PCB, storage, AI applications", "云、数据中心、服务器、光模块、PCB、存储、AI 应用", "", 6, 0.76),
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
                "thesis_zh": "GPT-5.5 强化 AI Agent 与企业工作流周期，后续重点跟踪推理成本、云资本开支和软件采用率。",
                "relevance": "AI, cloud, semiconductors, server supply chain, enterprise software",
                "relevance_zh": "AI、云、半导体、服务器供应链、企业软件",
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
            "impact_summary_zh": "强化中国 AI 竞争力叙事，需联动国产推理栈、模型应用、云和算力供应链订单验证。",
            "impact_score": 86,
            "verification_status": "primary_sources_seen",
            "facts": [
                ("model", "Model family", "模型族", "DeepSeek V4", "DeepSeek V4", "", 1, 0.95),
                ("model", "API model", "API 模型", "deepseek-v4-flash", "deepseek-v4-flash", "", 2, 0.92),
                ("model", "API model", "API 模型", "deepseek-v4-pro", "deepseek-v4-pro", "", 3, 0.9),
                ("lifecycle", "Old model deprecation", "旧模型弃用", "deepseek-chat and deepseek-reasoner deprecated on 2026/07/24", "deepseek-chat 与 deepseek-reasoner 将于 2026/07/24 弃用", "", 4, 0.85),
                ("context", "Context window", "上下文窗口", "1M", "100 万", "tokens", 5, 0.78),
                ("market", "A/H watch chain", "A/H 观察链条", "domestic AI apps, inference hardware, servers, cloud, optical modules, memory", "国产 AI 应用、推理硬件、服务器、云、光模块、存储", "", 6, 0.76),
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
                "thesis_zh": "DeepSeek V4 强化中国 AI 提前布局逻辑，需验证模型调用、API 定价、开源权重采用和下游订单信号。",
                "relevance": "China AI, cloud, semiconductors, inference, software",
                "relevance_zh": "中国 AI、云、半导体、推理、软件",
            },
        }

    return generic_signal_event_payload(doc, title, text, lower)


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
                impact_summary=?, impact_summary_zh=?, impact_score=?, verification_status=?,
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
                bundle.get("impact_summary_zh"),
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
             impact_summary, impact_summary_zh, impact_score, verification_status, primary_source_url, updated_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                bundle.get("impact_summary_zh"),
                bundle.get("impact_score", 0),
                bundle.get("verification_status", "watching"),
                doc["url"],
                now_iso(),
            ),
        )
        event_id = int(cursor.lastrowid)
        added = True

    for fact in bundle.get("facts") or []:
        if len(fact) == 6:
            fact_type, label, value, unit, sort_order, confidence = fact
            label_zh = None
            value_zh = None
        else:
            fact_type, label, label_zh, value, value_zh, unit, sort_order, confidence = fact
        conn.execute(
            """
            INSERT OR IGNORE INTO event_facts
            (event_id, fact_type, label, label_zh, value, value_zh, unit, source_url, confidence, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, fact_type, label, label_zh, value, value_zh, unit, doc["url"], confidence, sort_order),
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
             published_at, fetched_at, language, summary, summary_zh, thesis, thesis_zh, relevance, relevance_zh, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'en', ?, ?, ?, ?, ?, ?, 'active')
            ON CONFLICT(report_key) DO UPDATE SET
                title=excluded.title,
                title_zh=excluded.title_zh,
                source_key=excluded.source_key,
                source_name=excluded.source_name,
                report_type=excluded.report_type,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                summary=excluded.summary,
                summary_zh=excluded.summary_zh,
                thesis=excluded.thesis,
                thesis_zh=excluded.thesis_zh,
                relevance=excluded.relevance,
                relevance_zh=excluded.relevance_zh,
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
                doc.get("summary_zh") or bundle.get("summary_zh"),
                research.get("thesis"),
                research.get("thesis_zh"),
                research.get("relevance"),
                research.get("relevance_zh"),
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


def require_db_helpers() -> None:
    if get_sqlite_connection is None or IntelligenceService is None:
        raise RuntimeError("database helpers are unavailable; use --collect-only on lightweight collectors")


def collect_documents(include_bootstrap: bool = BOOTSTRAP_SEED) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], int]:
    session = get_session()
    documents_by_source: Dict[str, List[Dict[str, Any]]] = {}
    source_runs: List[Dict[str, Any]] = []
    failures = 0

    if include_bootstrap:
        for doc in BOOTSTRAP_DOCUMENTS:
            documents_by_source.setdefault(doc["source_key"], []).append(doc)

    for source in SOURCES:
        if source["collection_method"] == "bootstrap":
            continue
        started_at = now_iso()
        run_status = "success"
        found = 0
        error = ""
        if not source_runs_on_current_collector(source):
            reason = "disabled" if not int(source.get("enabled", 1)) else f"collector={source.get('collector')} skipped on {COLLECTOR_MODE}"
            source_runs.append(
                {
                    "source_key": source["source_key"],
                    "started_at": started_at,
                    "finished_at": now_iso(),
                    "status": "skipped",
                    "records_found": 0,
                    "records_added": 0,
                    "records_updated": 0,
                    "error": reason,
                }
            )
            continue
        try:
            if source["collection_method"] == "fixed_html":
                docs = fetch_fixed_html(session, source)
            elif source["collection_method"] == "json_list":
                docs = fetch_json_list(session, source)
            elif source["collection_method"] == "feed":
                docs = fetch_feed(session, source)
            elif source["collection_method"] == "search_proxy":
                docs = fetch_search_proxy(session, source)
            else:
                docs = []
            documents_by_source.setdefault(source["source_key"], []).extend(docs)
            found = len(docs)
        except Exception as exc:
            failures += 1
            run_status = "failed"
            error = str(exc)[:500]
            print(f"  {source['source_key']} FAIL: {error}")

        source_runs.append(
            {
                "source_key": source["source_key"],
                "started_at": started_at,
                "finished_at": now_iso(),
                "status": run_status,
                "records_found": found,
                "records_added": 0,
                "records_updated": 0,
                "error": error,
            }
        )
    return documents_by_source, source_runs, failures


def apply_documents_to_db(
    documents_by_source: Dict[str, List[Dict[str, Any]]],
    source_runs: List[Dict[str, Any]],
    failures: int,
) -> Dict[str, int]:
    require_db_helpers()
    service = IntelligenceService(DB_PATH)
    total_found = 0
    total_added_docs = 0
    total_updated_docs = 0
    total_events_added = 0
    promoted_by_source: Dict[str, int] = {}

    with get_sqlite_connection(DB_PATH, timeout=60, busy_timeout=15000) as conn:
        ensure_sources(conn)
        conn.commit()

        for run in source_runs:
            status = run.get("status") or "unknown"
            if status == "success":
                update_source_status(conn, run["source_key"], ok=True)
            elif status == "failed":
                update_source_status(conn, run["source_key"], ok=False, error=run.get("error") or "")
            conn.execute(
                """
                INSERT INTO collection_runs
                (source_key, started_at, finished_at, status, records_found, records_added, records_updated, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.get("source_key"),
                    run.get("started_at"),
                    run.get("finished_at"),
                    status,
                    int(run.get("records_found") or 0),
                    int(run.get("records_added") or 0),
                    int(run.get("records_updated") or 0),
                    run.get("error") or "",
                ),
            )

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
                    priority = bundle.get("priority") or "P2"
                    limit = EVENT_PROMOTION_LIMITS.get(source_key)
                    if priority != "P0" and limit is not None:
                        promoted = promoted_by_source.get(source_key, 0)
                        if promoted >= limit:
                            continue
                        promoted_by_source[source_key] = promoted + 1
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


def run_collection() -> Dict[str, int]:
    documents_by_source, source_runs, failures = collect_documents(include_bootstrap=BOOTSTRAP_SEED)
    return apply_documents_to_db(documents_by_source, source_runs, failures)


def export_bundle(output_path: str, include_bootstrap: bool = False) -> Dict[str, int]:
    documents_by_source, source_runs, failures = collect_documents(include_bootstrap=include_bootstrap)
    documents = []
    for _source_key, docs in documents_by_source.items():
        documents.extend(docs)
    bundle = {
        "generated_at": now_iso(),
        "collector_mode": COLLECTOR_MODE,
        "sources": SOURCES,
        "source_runs": source_runs,
        "documents": documents,
        "metrics": {
            "records_found": len(documents),
            "records_failed": failures,
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, ensure_ascii=False, indent=2)
    return {
        "records_found": len(documents),
        "records_added": 0,
        "records_updated": 0,
        "events_added": 0,
        "records_failed": failures,
    }


def import_bundle(input_path: str) -> Dict[str, int]:
    with open(input_path, "r", encoding="utf-8") as fh:
        bundle = json.load(fh)
    documents_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for doc in bundle.get("documents") or []:
        source_key = doc.get("source_key") or "unknown"
        documents_by_source.setdefault(source_key, []).append(doc)
    source_runs = bundle.get("source_runs") or []
    failures = int((bundle.get("metrics") or {}).get("records_failed") or 0)
    return apply_documents_to_db(documents_by_source, source_runs, failures)


def main() -> int:
    parser = argparse.ArgumentParser(description="Investment intelligence collector")
    parser.add_argument("--collect-only", action="store_true", help="collect sources and write a JSON bundle without touching SQLite")
    parser.add_argument("--output", default="", help="bundle output path for --collect-only")
    parser.add_argument("--import-json", default="", help="import a collector JSON bundle into SQLite")
    parser.add_argument("--no-bootstrap", action="store_true", help="skip verified bootstrap documents")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Intelligence Sync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    if args.collect_only:
        output = args.output or os.path.join(BASE_DIR, "data", "intelligence_bundle.json")
        metrics = export_bundle(output, include_bootstrap=not args.no_bootstrap)
        print(f"[OK] Collector bundle written: {output}")
    elif args.import_json:
        metrics = import_bundle(args.import_json)
        print(f"[OK] Collector bundle imported: {args.import_json}")
    else:
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
