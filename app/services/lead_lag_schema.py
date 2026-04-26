"""V2 schema guards for the Lead-Lag operator payloads."""
from __future__ import annotations

from typing import Any, Dict, List


OPPORTUNITY_REQUIRED_FIELDS = {
    "id",
    "generation_status",
    "thesis",
    "region",
    "sector",
    "model_family",
    "baton_stage",
    "why_now",
    "driver",
    "risk",
    "invalidation_rules",
    "expected_lag_days",
    "expected_review_times",
    "crowding_state",
    "liquidity_state",
    "actionability_score",
    "tradability_score",
    "evidence_completeness",
    "freshness_score",
    "noise_penalty",
    "decision_priority_score",
    "historical_replay_summary",
    "source_count",
    "source_quality",
    "confidence",
    "cache_status",
    "last_update",
}

CRITICAL_ASSET_FIELDS = ("leader_asset", "bridge_asset", "local_asset")


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in {"", "-", "N/A", "n/a"}
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def compact_strings(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if not is_blank(value)]


def asset_is_present(asset: Any) -> bool:
    return isinstance(asset, dict) and not is_blank(asset.get("code")) and not is_blank(asset.get("name"))


def validate_opportunity_card(card: Dict[str, Any]) -> List[str]:
    """Return missing critical field names for an OpportunityCard V2 payload."""
    missing = [field for field in sorted(OPPORTUNITY_REQUIRED_FIELDS) if is_blank(card.get(field))]
    if is_blank(card.get("confirmations")) and is_blank(card.get("missing_confirmations")):
        missing.append("confirmations_or_missing_confirmations")
    for field in CRITICAL_ASSET_FIELDS:
        if not asset_is_present(card.get(field)):
            missing.append(field)
    return missing


def apply_generation_status(card: Dict[str, Any]) -> Dict[str, Any]:
    """Make missing evidence explicit instead of allowing silent blank critical fields."""
    missing = validate_opportunity_card(card)
    if missing:
        confirmations = compact_strings(card.get("missing_confirmations"))
        for field in missing:
            confirmations.append(f"缺少 {field} 的可验证证据")
        card["missing_confirmations"] = list(dict.fromkeys(confirmations))
        card["missing_evidence_reason"] = card.get("missing_evidence_reason") or "关键字段证据不完整，不能升级为可执行机会。"
        card["generation_status"] = "insufficient_evidence"
    elif card.get("generation_status") == "insufficient_evidence" and not card.get("missing_confirmations"):
        card["generation_status"] = "watch_only"
    return card
