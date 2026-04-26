"""Config-driven V2 scoring for Lead-Lag OpportunityCards and events."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict


DEFAULT_LEAD_LAG_V2_CONFIG: Dict[str, Any] = {
    "scoring": {
        "opportunity_weights": {
            "actionability_score": 0.30,
            "tradability_score": 0.22,
            "evidence_completeness": 0.20,
            "freshness_score": 0.16,
            "historical_replay_score": 0.12,
            "noise_penalty": -0.18,
        },
        "event_weights": {
            "china_mapping_score": 0.30,
            "tradability_score": 0.25,
            "evidence_quality": 0.25,
            "time_decay": 0.20,
            "noise_penalty": -1.0,
        },
    },
    "thresholds": {
        "event_market_china_mapping_min": 55.0,
        "event_market_relevance_min": 45.0,
        "opportunity_actionable_min": 70.0,
        "opportunity_watch_min": 45.0,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _minimal_yaml_mapping(text: str) -> Dict[str, Any]:
    """Parse the small YAML subset used by the example config when PyYAML is absent."""
    root: Dict[str, Any] = {}
    stack: list[tuple[int, Dict[str, Any]]] = [(-1, root)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value:
            child: Dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
            continue
        if value.lower() in {"true", "false"}:
            parsed: Any = value.lower() == "true"
        else:
            try:
                parsed = float(value)
            except ValueError:
                parsed = value.strip("'\"")
        parent[key] = parsed
    return root


def load_lead_lag_v2_config(repo_root: Path, config_path: Path | None = None) -> Dict[str, Any]:
    path = config_path or repo_root / "config" / "lead_lag_v2.example.yaml"
    if not path.exists():
        return deepcopy(DEFAULT_LEAD_LAG_V2_CONFIG)
    text = path.read_text(encoding="utf-8")
    parsed: Dict[str, Any] = {}
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            parsed = payload
    except Exception:
        try:
            import yaml  # type: ignore

            payload = yaml.safe_load(text)
            if isinstance(payload, dict):
                parsed = payload
        except Exception:
            parsed = _minimal_yaml_mapping(text)
    return _deep_merge(DEFAULT_LEAD_LAG_V2_CONFIG, parsed)


def clamp_score(value: Any, minimum: float = 0.0, maximum: float = 100.0) -> float:
    try:
        number = float(value)
    except Exception:
        number = 0.0
    return round(max(minimum, min(maximum, number)), 2)


def weighted_score(inputs: Dict[str, Any], weights: Dict[str, Any]) -> float:
    total = 0.0
    for key, weight in weights.items():
        total += clamp_score(inputs.get(key)) * float(weight)
    return clamp_score(total)


def opportunity_decision_score(inputs: Dict[str, Any], config: Dict[str, Any]) -> float:
    weights = ((config.get("scoring") or {}).get("opportunity_weights") or {})
    return weighted_score(inputs, weights)


def event_relevance_score(inputs: Dict[str, Any], config: Dict[str, Any]) -> float:
    weights = ((config.get("scoring") or {}).get("event_weights") or {})
    return weighted_score(inputs, weights)
