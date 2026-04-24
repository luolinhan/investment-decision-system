"""
Obsidian 研究记忆读取服务。

只读索引指定 vault，不修改源笔记。
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


TRACKED_TAGS = ("策略", "复盘", "主题", "观察池", "风控", "资源")
TAG_PATTERN = re.compile(r"(?<!\w)#([\u4e00-\u9fffA-Za-z0-9_\-/]+)")


class ObsidianMemoryService:
    def __init__(self, vault_path: Optional[str] = None):
        self.vault_path = self._resolve_vault_path(vault_path)
        self._cache: Dict[str, Dict[str, object]] = {}
        self._cache_at: Dict[str, datetime] = {}

    def _resolve_vault_path(self, vault_path: Optional[str]) -> Optional[Path]:
        candidates = [
            vault_path,
            os.getenv("INVESTMENT_OBSIDIAN_VAULT"),
            os.getenv("OBSIDIAN_VAULT_PATH"),
            r"C:\Users\Administrator\Documents\Obsidian\知识库",
            "/Users/lhluo/Documents/Obsidian/知识库",
        ]
        for candidate in candidates:
            if not candidate:
                continue
            path = Path(candidate).expanduser()
            if path.exists():
                return path
        return None

    def _iter_markdown_files(self) -> Iterable[Path]:
        if not self.vault_path:
            return []
        return self.vault_path.rglob("*.md")

    def _read_text(self, path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""

    def _extract_tags(self, text: str) -> List[str]:
        found = []
        seen = set()
        for tag in TAG_PATTERN.findall(text or ""):
            normalized = tag.strip()
            if not normalized or normalized in seen:
                continue
            found.append(normalized)
            seen.add(normalized)
        return found

    def _score_note(self, text: str, themes: Optional[List[str]]) -> int:
        if not themes:
            return 0
        haystack = (text or "").lower()
        score = 0
        for theme in themes:
            token = str(theme or "").strip().lower()
            if token and token in haystack:
                score += 1
        return score

    def index_notes(self, themes: Optional[List[str]] = None, limit: int = 12) -> Dict[str, object]:
        cache_key = "|".join(sorted(themes or [])) + f":{int(limit or 12)}"
        cached_at = self._cache_at.get(cache_key)
        if cached_at and (datetime.now() - cached_at).total_seconds() < 300:
            return self._cache[cache_key]
        if not self.vault_path:
            payload = {
                "status": "missing",
                "vault_path": None,
                "tracked_tags": list(TRACKED_TAGS),
                "tag_counts": {},
                "recent_notes": [],
                "theme_matches": [],
                "note_count": 0,
            }
            self._cache[cache_key] = payload
            self._cache_at[cache_key] = datetime.now()
            return payload

        tag_counts: Dict[str, int] = {tag: 0 for tag in TRACKED_TAGS}
        recent_notes: List[Dict[str, object]] = []
        theme_matches: List[Dict[str, object]] = []
        note_count = 0

        for path in self._iter_markdown_files():
            text = self._read_text(path)
            tags = self._extract_tags(text)
            matched_tags = [tag for tag in TRACKED_TAGS if tag in tags]
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            note = {
                "title": path.stem,
                "path": str(path),
                "relative_path": str(path.relative_to(self.vault_path)),
                "modified_at": mtime.replace(microsecond=0).isoformat(),
                "tags": tags,
                "tracked_tags": matched_tags,
            }
            note_count += 1
            for tag in matched_tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
            recent_notes.append(note)
            theme_score = self._score_note(text, themes)
            if theme_score:
                theme_matches.append({**note, "theme_score": theme_score})

        recent_notes.sort(key=lambda item: item.get("modified_at") or "", reverse=True)
        theme_matches.sort(
            key=lambda item: (int(item.get("theme_score") or 0), item.get("modified_at") or ""),
            reverse=True,
        )
        payload = {
            "status": "ready",
            "vault_path": str(self.vault_path),
            "tracked_tags": list(TRACKED_TAGS),
            "tag_counts": tag_counts,
            "recent_notes": recent_notes[: max(1, min(int(limit or 12), 50))],
            "theme_matches": theme_matches[: max(1, min(int(limit or 12), 50))],
            "note_count": note_count,
        }
        self._cache[cache_key] = payload
        self._cache_at[cache_key] = datetime.now()
        return payload
