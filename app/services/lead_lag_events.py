"""Event Relevance Engine for Lead-Lag V2."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List

from app.services.lead_lag_scoring import clamp_score, event_relevance_score


MARKET_SECTORS = {"ai", "semis", "innovative_pharma", "solar", "hog_cycle", "cross_market"}
CHINA_MARKETS = {"A", "HK", "ETF", "CN"}
DEVELOPER_KEYWORDS = {
    "sdk",
    "github",
    "framework",
    "benchmark",
    "leaderboard",
    "open-source",
    "opensource",
    "api",
    "developer",
    "tooling",
}
EVENT_TYPE_KEYWORDS = {
    "policy": ("policy", "regulation", "official", "approval", "监管", "政策", "审批"),
    "macro": ("macro", "cpi", "ppi", "pboC", "fed", "yield", "vix", "宏观", "利率"),
    "earnings": ("earnings", "10-q", "8-k", "results", "业绩", "财报"),
    "guidance": ("guidance", "outlook", "指引"),
    "capex": ("capex", "capacity", "supply chain", "spending", "产能", "供应链"),
    "clinical": ("clinical", "pipeline", "trial", "fda", "nmpa", "临床"),
    "approval": ("approval", "approved", "批准", "获批"),
    "price_spread": ("price", "spread", "spot", "pricing", "现货", "价格"),
    "inventory": ("inventory", "utilization", "库存", "开工"),
    "liquidity": ("liquidity", "southbound", "flow", "short selling", "流动性", "资金"),
    "developer_ecosystem": tuple(DEVELOPER_KEYWORDS),
    "research_update": ("research", "report", "paper", "arxiv", "研报", "论文"),
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d")
    except Exception:
        return None


def infer_event_type(event: Dict[str, Any]) -> str:
    text = " ".join(
        str(event.get(key) or "")
        for key in ("event_type", "category", "title", "title_zh", "summary", "summary_zh", "impact_summary")
    ).lower()
    for event_type, keywords in EVENT_TYPE_KEYWORDS.items():
        if any(keyword.lower() in text for keyword in keywords):
            return event_type
    return "other"


def _event_assets(event: Dict[str, Any], asset_resolver: Callable[..., Dict[str, Any]]) -> List[Dict[str, Any]]:
    raw_assets = event.get("related_assets") or event.get("assets") or []
    assets: List[Dict[str, Any]] = []
    if isinstance(raw_assets, list):
        for raw in raw_assets:
            if isinstance(raw, dict):
                code = raw.get("code") or raw.get("symbol") or raw.get("ticker")
                asset = asset_resolver(code, raw.get("name"), raw.get("market"), event.get("sector_key") or event.get("category"))
                if asset.get("code"):
                    assets.append(asset)
    for code in event.get("related_symbols") or []:
        asset = asset_resolver(code, sector=event.get("sector_key") or event.get("category"))
        if asset.get("code") and all(existing.get("code") != asset.get("code") for existing in assets):
            assets.append(asset)
    return assets


def build_event_relevance(
    event: Dict[str, Any],
    *,
    asset_resolver: Callable[..., Dict[str, Any]],
    sector_normalizer: Callable[[Any], str],
    config: Dict[str, Any],
    now: datetime | None = None,
) -> Dict[str, Any]:
    now = now or datetime.now()
    event_id = str(event.get("event_id") or event.get("event_key") or event.get("id") or "")
    title = str(event.get("title_zh") or event.get("title") or event_id or "未命名事件")
    sector = sector_normalizer(event.get("sector_key") or event.get("category"))
    event_type = infer_event_type(event)
    assets = _event_assets(event, asset_resolver)
    china_assets = [asset for asset in assets if str(asset.get("market")) in CHINA_MARKETS]
    has_market_sector = sector in MARKET_SECTORS and sector != "cross_market"
    text = f"{title} {event.get('summary') or ''} {event.get('impact_summary') or ''}".lower()
    is_developer_noise = event_type == "developer_ecosystem" or any(keyword in text for keyword in DEVELOPER_KEYWORDS)

    china_mapping_score = 20.0
    if china_assets:
        china_mapping_score = 78.0
    elif has_market_sector and assets:
        china_mapping_score = 62.0
    elif has_market_sector:
        china_mapping_score = 48.0
    if sector == "cross_market" and china_assets:
        china_mapping_score = max(china_mapping_score, 58.0)
    if is_developer_noise and not china_assets:
        china_mapping_score = min(china_mapping_score, 30.0)

    tradability_score = 25.0 + min(len(assets), 5) * 8.0 + min(len(china_assets), 3) * 12.0
    if event.get("stage") in {"triggered", "validating"} or event.get("priority") in {"P0", "P1"}:
        tradability_score += 10.0
    if is_developer_noise and not china_assets:
        tradability_score -= 20.0

    source_url = event.get("source_url") or event.get("primary_source_url") or "sample_data/lead_lag/lead_lag_v1.json"
    source_tier = "sample_fallback" if str(source_url).startswith("sample_data/") else "T1"
    source_count = int(_as_float(event.get("source_count"), 1.0))
    evidence_quality = 45.0 + min(source_count, 4) * 8.0 + _as_float(event.get("confidence"), 0.5) * 20.0
    if source_tier == "sample_fallback":
        evidence_quality = max(evidence_quality, 58.0)

    effective_time = event.get("event_date") or event.get("event_time") or event.get("last_seen_at") or event.get("first_seen_at")
    parsed_time = _parse_datetime(effective_time)
    age_days = max((now - parsed_time).days, 0) if parsed_time else 3
    time_decay = clamp_score(100.0 - min(age_days, 30) * 3.0)
    noise_penalty = 22.0 if is_developer_noise and not china_assets else 0.0

    relevance_inputs = {
        "china_mapping_score": china_mapping_score,
        "tradability_score": tradability_score,
        "evidence_quality": evidence_quality,
        "time_decay": time_decay,
        "noise_penalty": noise_penalty,
    }
    relevance_score = event_relevance_score(relevance_inputs, config)
    thresholds = config.get("thresholds") or {}
    mapping_min = float(thresholds.get("event_market_china_mapping_min", 55.0))
    relevance_min = float(thresholds.get("event_market_relevance_min", 45.0))
    market_facing = (
        china_mapping_score >= mapping_min
        and relevance_score >= relevance_min
        and bool(assets or has_market_sector)
        and not (is_developer_noise and not china_assets)
    )

    mapped_asset_codes = "/".join(asset.get("code") for asset in assets[:3] if asset.get("code")) or "待映射资产"
    expected_path = [
        {
            "from": title,
            "to": mapped_asset_codes,
            "relation": f"{sector} 事件 -> {mapped_asset_codes} -> 1-5 个交易日验证",
            "expected_lag_days": {"min": 1, "max": 5},
        }
    ]
    invalidation = ["若本地映射资产无成交/价差/基本面确认，降级为 research-facing。"]
    if is_developer_noise and not market_facing:
        invalidation = ["仅为开发者生态噪音，缺少中国可交易资产映射。"]

    return {
        "event_id": event_id,
        "title": title,
        "event_type": event_type,
        "event_class": "market-facing" if market_facing else "research-facing",
        "sector_mapping": [{"sector": sector, "confidence": clamp_score(china_mapping_score)}] if sector else [],
        "asset_mapping": assets,
        "china_mapping_score": clamp_score(china_mapping_score),
        "tradability_score": clamp_score(tradability_score),
        "evidence_quality": clamp_score(evidence_quality),
        "time_decay": time_decay,
        "relevance_score": relevance_score,
        "owner": "operator" if market_facing else "builder",
        "watch_items": [
            f"跟踪 {asset.get('code')} {asset.get('name')} 的成交和相对强弱" for asset in assets[:3]
        ] or ["等待明确的可交易资产映射"],
        "base_case": f"{title} 对 {sector} 形成可验证催化。",
        "bull_case": "映射资产和二级传导同时确认，机会卡上调。",
        "bear_case": "事件不改变订单、价格、审批、流动性或盈利预期。",
        "expected_path": expected_path,
        "invalidation": invalidation,
        "source": {"url": source_url, "tier": source_tier, "count": source_count},
        "ingest_time": event.get("last_seen_at") or event.get("first_seen_at") or event.get("event_date") or now.isoformat(),
        "effective_time": effective_time or now.isoformat(),
        "freshness": "fresh" if age_days <= 3 else "aging" if age_days <= 14 else "stale",
        "staleness_reason": "" if age_days <= 14 else f"事件已超过 {age_days} 天",
        "noise_reason": "developer_ecosystem_without_china_mapping" if is_developer_noise and not china_assets else "",
    }


def filter_event_relevance(
    events: List[Dict[str, Any]],
    *,
    event_class: str = "market-facing",
    include_research_facing: bool = False,
) -> List[Dict[str, Any]]:
    if include_research_facing:
        return events
    normalized = re.sub(r"_", "-", str(event_class or "market-facing"))
    return [event for event in events if event.get("event_class") == normalized]
