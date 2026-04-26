"""Shared Bailian-compatible chat client helpers.

This module keeps API-key handling and JSON repair behavior in one place so
collection and translation scripts do not grow their own slightly different
clients. It intentionally logs no secrets and treats missing credentials as a
normal fallback mode.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv


DEFAULT_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"
DEFAULT_MODEL = "qwen3-coder-plus"
DEFAULT_TRANSLATION_PROMPT = (
    "你是面向A股和港股投资研究的信息翻译助手。"
    "请把输入英文翻译成简洁、准确、保留专有名词的中文。"
    "只返回严格JSON，字段名保持和输入一致；不要添加解释。"
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _normalize_base_url(value: str, default: str = DEFAULT_BASE_URL) -> str:
    text = (value or default).strip() or default
    return text.rstrip("/")


def _load_env_files(repo_root: Optional[Path] = None) -> None:
    root = repo_root or _repo_root()
    load_dotenv(root / ".env.local", override=False)
    load_dotenv(root / ".env", override=False)


def _read_timeout_seconds(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def normalize_json_block(content: str) -> str:
    text = (content or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:].strip()
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1]
    return text


class BailianJsonTranslator:
    """Small OpenAI-compatible JSON translator for Bailian/DashScope endpoints."""

    def __init__(
        self,
        *,
        repo_root: Optional[Path] = None,
        default_model: str = DEFAULT_MODEL,
        timeout_default: int = 25,
        session: Optional[requests.Session] = None,
    ) -> None:
        _load_env_files(repo_root)
        self.api_key = (
            os.getenv("BAILIAN_API_KEY", "").strip()
            or os.getenv("DASHSCOPE_API_KEY", "").strip()
        )
        self.base_url = _normalize_base_url(
            os.getenv("BAILIAN_BASE_URL", "").strip()
            or os.getenv("DASHSCOPE_BASE_URL", "").strip(),
            DEFAULT_BASE_URL,
        )
        self.model = (
            os.getenv("BAILIAN_MODEL", "").strip()
            or os.getenv("DASHSCOPE_MODEL", "").strip()
            or default_model
        )
        self.timeout = _read_timeout_seconds(os.getenv("BAILIAN_TIMEOUT_SECONDS", ""), timeout_default)
        self.session = session or requests.Session()

    def enabled(self) -> bool:
        return bool(self.api_key)

    def _request_content(self, system_prompt: str, user_payload: Dict[str, Any], max_tokens: int) -> str:
        if not self.enabled():
            return ""
        response = self.session.post(
            f"{self.base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return (((response.json().get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()

    def translate_payload(
        self,
        payload: Dict[str, Any],
        *,
        system_prompt: str = DEFAULT_TRANSLATION_PROMPT,
        max_tokens: int = 1200,
    ) -> Dict[str, str]:
        if not self.enabled():
            return {}
        content = normalize_json_block(self._request_content(system_prompt, payload, max_tokens))
        try:
            parsed = json.loads(content)
            return {str(key): str(value) for key, value in parsed.items() if value is not None}
        except json.JSONDecodeError:
            return self._repair_json(payload, content, max_tokens=max_tokens)

    def _repair_json(self, payload: Dict[str, Any], raw_output: str, *, max_tokens: int) -> Dict[str, str]:
        repair_prompt = (
            "你是 JSON 修复助手。"
            "把输入中的 raw_output 修复成严格合法的 JSON，且只保留 required_keys 中列出的字段。"
            "不要添加解释。"
        )
        try:
            repaired = normalize_json_block(
                self._request_content(
                    repair_prompt,
                    {"required_keys": list(payload.keys()), "raw_output": raw_output},
                    max_tokens,
                )
            )
            parsed = json.loads(repaired)
            return {str(key): str(value) for key, value in parsed.items() if value is not None}
        except Exception:
            return {}
