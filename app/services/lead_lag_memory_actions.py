from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


TRACKED_TAGS = ("策略", "复盘", "主题", "观察池", "风控", "资源")

SECTOR_ALIASES: Dict[str, str] = {
    "ai": "ai",
    "人工智能": "ai",
    "算力": "ai",
    "创新药": "innovative_pharma",
    "innovative_pharma": "innovative_pharma",
    "半导体": "semis",
    "芯片": "semis",
    "semiconductor": "semis",
    "semis": "semis",
    "光伏": "solar",
    "solar": "solar",
    "猪周期": "hog_cycle",
    "养猪": "hog_cycle",
    "hog_cycle": "hog_cycle",
}

SECTOR_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "ai": ("ai", "人工智能", "算力", "大模型"),
    "innovative_pharma": ("创新药", "biotech", "pharma", "医药"),
    "semis": ("半导体", "芯片", "semiconductor", "semis"),
    "solar": ("光伏", "solar"),
    "hog_cycle": ("猪周期", "养猪", "hog"),
}

WIN_KEYWORDS = ("命中", "成功", "兑现", "抓住", "胜率", "赚")
FAILURE_KEYWORDS = ("失败", "失效", "止损", "回撤", "踩雷", "亏损")
TRAP_KEYWORDS = ("陷阱", "拥挤", "追高", "误判", "假突破", "噪音")
SIMILAR_CASE_KEYWORDS = ("类似", "同类", "对标", "映射", "案例", "case")
REVIEW_KEYWORDS = ("复盘", "回顾", "review", "总结")


def _normalize_sector(sector: Optional[str]) -> str:
    token = str(sector or "").strip().lower()
    if not token:
        return ""
    return SECTOR_ALIASES.get(token, token)


def _split_tags(note: Dict[str, Any]) -> List[str]:
    rows = note.get("tracked_tags") or note.get("tags") or []
    tags: List[str] = []
    for row in rows:
        tag = str(row or "").strip().lstrip("#")
        if tag:
            tags.append(tag)
    return tags


def _iter_notes(memory_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    for key in ("theme_matches", "recent_notes"):
        for item in memory_payload.get(key, []) or []:
            if not isinstance(item, dict):
                continue
            note_key = str(item.get("path") or item.get("relative_path") or item.get("title") or "")
            if not note_key or note_key in seen:
                continue
            seen.add(note_key)
            merged.append(item)
    return merged


def _detect_note_sectors(title: str, path: str, tags: Iterable[str]) -> Set[str]:
    haystack = " ".join([title, path, " ".join(tags)]).lower()
    sectors: Set[str] = set()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(keyword.lower() in haystack for keyword in keywords):
            sectors.add(sector)
    return sectors


def _classify_note(title: str, path: str, tags: Iterable[str]) -> str:
    haystack = " ".join([title, path, " ".join(tags)]).lower()
    tags_set = {str(tag).strip("#") for tag in tags}

    if "风控" in tags_set or any(token in haystack for token in TRAP_KEYWORDS):
        return "typical_trap"
    if any(token in haystack for token in FAILURE_KEYWORDS):
        return "prior_failures"
    if any(token in haystack for token in WIN_KEYWORDS):
        return "prior_wins"
    if "资源" in tags_set or any(token in haystack for token in SIMILAR_CASE_KEYWORDS):
        return "similar_cases"
    if "复盘" in tags_set or any(token in haystack for token in REVIEW_KEYWORDS):
        return "review_notes"
    return "thesis_summary"


def _cache_status(memory_payload: Dict[str, Any], source: str) -> str:
    status = str(memory_payload.get("status") or "").strip().lower()
    explicit = str(memory_payload.get("cache_status") or "").strip()
    if explicit:
        return explicit
    if source == "obsidian_vault" and status == "ready":
        return "live"
    if source == "sample_data" and status in {"missing", "sample", "fallback"}:
        return "sample_fallback"
    if status == "ready":
        return "cached"
    return "missing"


def _sort_key(note: Dict[str, Any]) -> str:
    return str(note.get("modified_at") or note.get("last_update") or "")


def _collect_last_update(rows: Iterable[Dict[str, Any]], payload: Dict[str, Any]) -> Optional[str]:
    candidates = [str(row.get("modified_at") or row.get("last_update") or "") for row in rows]
    candidates.append(str(payload.get("last_update") or ""))
    normalized = [text for text in candidates if text]
    if not normalized:
        return None
    try:
        return max(normalized, key=lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")))
    except Exception:
        return max(normalized)


def _opportunity_sector(card: Dict[str, Any]) -> str:
    return _normalize_sector(card.get("sector") or card.get("sector_key"))


def build_research_memory_actions(
    memory_payload: Dict[str, Any],
    opportunity_cards: Optional[List[Dict[str, Any]]] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    payload = memory_payload if isinstance(memory_payload, dict) else {}
    source = str(payload.get("source") or ("obsidian_vault" if str(payload.get("status")) == "ready" else "sample_data"))
    sector_filter = _normalize_sector(sector)

    result: Dict[str, Any] = {
        "thesis_summary": [],
        "prior_wins": [],
        "prior_failures": [],
        "typical_trap": [],
        "similar_cases": [],
        "review_notes": [],
        "mapped_opportunities": [],
        "missing_memory": [],
        "source": source,
        "cache_status": _cache_status(payload, source),
        "last_update": None,
    }

    notes = _iter_notes(payload)
    enriched: List[Dict[str, Any]] = []
    for note in notes:
        title = str(note.get("title") or "").strip()
        path = str(note.get("path") or note.get("relative_path") or "").strip()
        tags = _split_tags(note)
        sectors = _detect_note_sectors(title, path, tags)
        if sector_filter and sector_filter not in sectors:
            continue
        memory_type = _classify_note(title, path, tags)
        normalized_note = {
            "title": title,
            "path": path,
            "tags": tags,
            "sectors": sorted(sectors),
            "memory_type": memory_type,
            "modified_at": note.get("modified_at"),
        }
        enriched.append(normalized_note)
        result[memory_type].append({k: normalized_note[k] for k in ("title", "path", "tags", "modified_at")})

    if not enriched:
        for signal in payload.get("signals", []) or []:
            text = str(signal or "").strip()
            if text:
                result["thesis_summary"].append({"title": text, "path": "", "tags": [], "modified_at": None})
        if not result["thesis_summary"]:
            themes = [str(item).strip() for item in payload.get("themes", []) or [] if str(item or "").strip()]
            if themes:
                result["thesis_summary"].append(
                    {"title": f"Sample themes: {', '.join(themes[:5])}", "path": "", "tags": list(TRACKED_TAGS), "modified_at": None}
                )

    cards = [card for card in (opportunity_cards or []) if isinstance(card, dict)]
    for card in cards:
        card_sector = _opportunity_sector(card)
        if sector_filter and card_sector and card_sector != sector_filter:
            continue
        card_text = " ".join(
            str(card.get(key) or "").strip().lower()
            for key in ("thesis", "opportunity_id", "symbol", "asset_name")
        )
        hits = []
        for note in enriched:
            sector_match = bool(card_sector) and card_sector in set(note.get("sectors") or [])
            text_match = card_text and str(note.get("title") or "").lower() in card_text
            if sector_match or text_match:
                hits.append(
                    {
                        "title": note.get("title"),
                        "path": note.get("path"),
                        "memory_type": note.get("memory_type"),
                        "modified_at": note.get("modified_at"),
                    }
                )
        if hits:
            result["mapped_opportunities"].append(
                {
                    "opportunity_id": card.get("opportunity_id") or card.get("id") or card.get("symbol"),
                    "thesis": card.get("thesis"),
                    "sector": card_sector or "",
                    "memory_hits": sorted(hits, key=_sort_key, reverse=True)[:5],
                }
            )

    required = ("thesis_summary", "prior_wins", "prior_failures", "typical_trap", "similar_cases", "review_notes")
    result["missing_memory"] = [field for field in required if not result[field]]
    result["last_update"] = _collect_last_update(enriched, payload)
    return result
