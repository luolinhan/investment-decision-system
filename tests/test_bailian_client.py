from __future__ import annotations

import json

from app.services.bailian_client import BailianJsonTranslator, normalize_json_block


class FakeResponse:
    def __init__(self, content: str):
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"content": self.content}}]}


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        return FakeResponse(self.responses.pop(0))


def test_normalize_json_block_extracts_fenced_json():
    raw = "```json\n{\"title_zh\":\"中文\"}\n```"
    assert normalize_json_block(raw) == "{\"title_zh\":\"中文\"}"


def test_translator_repairs_bad_json(monkeypatch):
    monkeypatch.setenv("BAILIAN_API_KEY", "test-key")
    monkeypatch.setenv("BAILIAN_MODEL", "test-model")
    session = FakeSession(
        [
            "{\"title_zh\":\"中文\"",  # invalid first response
            json.dumps({"title_zh": "中文"}, ensure_ascii=False),
        ]
    )
    translator = BailianJsonTranslator(session=session)

    result = translator.translate_payload({"title_zh": "English title"})

    assert result == {"title_zh": "中文"}
    assert len(session.requests) == 2
    assert session.requests[0]["headers"]["Authorization"] == "Bearer test-key"


def test_translator_disabled_without_key(tmp_path, monkeypatch):
    monkeypatch.delenv("BAILIAN_API_KEY", raising=False)
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.setenv("BAILIAN_TIMEOUT_SECONDS", "invalid")

    translator = BailianJsonTranslator(repo_root=tmp_path, timeout_default=17)

    assert translator.enabled() is False
    assert translator.timeout == 17
    assert translator.translate_payload({"title_zh": "English"}) == {}
