# -*- coding: utf-8 -*-
"""HTTP smoke tests for the Investment Hub service."""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List


BASE_URL = os.getenv("INVESTMENT_BASE_URL", "http://100.64.93.19:8080").rstrip("/")
TIMEOUT_SECONDS = int(os.getenv("SMOKE_TIMEOUT_SECONDS", "45"))


TESTS = [
    {
        "name": "runtime profile",
        "path": "/investment/api/runtime/profile",
        "assert": lambda p: bool(p.get("db_exists")) and p.get("node_role") is not None,
    },
    {
        "name": "data health",
        "path": "/investment/api/data-health/overview",
        "assert": lambda p: (p.get("summary") or {}).get("total", 0) >= 1,
    },
    {
        "name": "news",
        "path": "/investment/api/news",
        "assert": lambda p: sum(len(v or []) for v in (p.get("news") or {}).values()) > 0,
    },
    {
        "name": "global risk",
        "path": "/investment/api/global-risk?days=5",
        "assert": lambda p: bool(p.get("us10y") or p.get("vix") or p.get("composite")),
    },
    {
        "name": "opportunity pool",
        "path": "/investment/api/opportunity-pools/overview?limit=5",
        "assert": lambda p: len(p.get("leaderboard") or p.get("opportunities") or []) > 0,
    },
    {
        "name": "intelligence brief",
        "path": "/investment/api/intelligence/brief/latest",
        "assert": lambda p: bool(p.get("summary")) or p.get("status") == "building",
    },
    {
        "name": "decision center",
        "path": "/investment/api/decision-center",
        "assert": lambda p: bool(p.get("regime") or p.get("decision_matrix") or p.get("macro_regime")),
    },
    {
        "name": "etl status",
        "path": "/investment/api/etl/status",
        "assert": lambda p: isinstance(p.get("recent_logs"), list),
    },
]


def fetch_json(path: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"User-Agent": "investment-hub-smoke-test/1.0"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {body[:200]}")
        return json.loads(body)


def run() -> int:
    print("=" * 60)
    print(f"Investment Hub API Smoke Test - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 60)
    failures: List[Dict[str, str]] = []
    for item in TESTS:
        try:
            payload = fetch_json(item["path"])
            if not item["assert"](payload):
                raise AssertionError("response assertion failed")
            print(f"[PASS] {item['name']}")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, Exception) as exc:
            print(f"[FAIL] {item['name']}: {exc}")
            failures.append({"name": item["name"], "error": str(exc)})
    print("=" * 60)
    print(f"Passed: {len(TESTS) - len(failures)} / {len(TESTS)}")
    if failures:
        print("Failures:")
        for failure in failures:
            print(f"  - {failure['name']}: {failure['error']}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run())
