"""Deep Evidence Engine for Lead-Lag V2 key sectors."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set


SECTOR_KEYS = ("ai", "innovative_pharma", "semis", "solar", "hog_cycle")
SECTOR_ALIASES = {
    "ai": "ai",
    "人工智能": "ai",
    "算力": "ai",
    "pharma": "innovative_pharma",
    "biotech": "innovative_pharma",
    "innovative_pharma": "innovative_pharma",
    "创新药": "innovative_pharma",
    "semis": "semis",
    "semi": "semis",
    "semiconductor": "semis",
    "半导体": "semis",
    "solar": "solar",
    "pv": "solar",
    "光伏": "solar",
    "hog_cycle": "hog_cycle",
    "hog": "hog_cycle",
    "猪周期": "hog_cycle",
}
SECTOR_NAME_MAP = {
    "ai": "AI",
    "innovative_pharma": "创新药",
    "semis": "半导体",
    "solar": "光伏",
    "hog_cycle": "猪周期",
}
MARKET_ALIAS = {
    "A": "A",
    "SH": "A",
    "SZ": "A",
    "HK": "HK",
    "US": "US",
    "ETF": "ETF",
}

SECTOR_LAYER_BLUEPRINT: Dict[str, List[Dict[str, Any]]] = {
    "ai": [
        {
            "layer_key": "capex_order",
            "layer_name": "Capex / Order Chain",
            "keywords": ("capex", "order", "supply chain", "订单", "资本开支"),
            "description": "先看海外龙头资本开支与订单确认。",
        },
        {
            "layer_key": "demand_utilization",
            "layer_name": "Demand / Utilization",
            "keywords": ("demand", "utilization", "server", "gpu", "需求", "开工"),
            "description": "验证真实需求与利用率，而非单纯叙事。",
        },
        {
            "layer_key": "model_cost",
            "layer_name": "Model Cost Curve",
            "keywords": ("cost", "token", "training", "inference", "成本", "推理"),
            "description": "跟踪模型训练和推理成本曲线变化。",
        },
        {
            "layer_key": "china_mapping",
            "layer_name": "China Mapping",
            "keywords": ("china", "a-share", "hk", "映射", "国产替代"),
            "description": "要求中国可交易映射资产形成确认。",
        },
    ],
    "innovative_pharma": [
        {
            "layer_key": "bd_licensing",
            "layer_name": "BD / Licensing",
            "keywords": ("bd", "licensing", "partner", "合作", "里程碑"),
            "description": "关注 BD 合作条款、首付款与里程碑兑现。",
        },
        {
            "layer_key": "clinical_readout",
            "layer_name": "Clinical Readout",
            "keywords": ("readout", "phase", "trial", "临床", "读出"),
            "description": "临床读出强于估值修复叙事。",
        },
        {
            "layer_key": "ind_nda_approval",
            "layer_name": "IND / NDA / Approval",
            "keywords": ("ind", "nda", "approval", "fda", "nmpa", "获批", "审批"),
            "description": "跟踪申报、受理、批准链条。",
        },
        {
            "layer_key": "cxo_commercialization",
            "layer_name": "CXO / Commercialization",
            "keywords": ("cxo", "cro", "commercial", "收入", "放量"),
            "description": "验证 CXO 与商业化兑现是否形成二棒。",
        },
    ],
    "semis": [
        {
            "layer_key": "overseas_earnings_inventory",
            "layer_name": "Overseas Earnings / Inventory",
            "keywords": ("earnings", "inventory", "guidance", "财报", "库存"),
            "description": "海外财报、库存与指引是先行证据。",
        },
        {
            "layer_key": "order_capex",
            "layer_name": "Order / Capex",
            "keywords": ("order", "capex", "订单", "资本开支"),
            "description": "订单与 capex 是传导强度核心。",
        },
        {
            "layer_key": "advanced_packaging",
            "layer_name": "Advanced Packaging",
            "keywords": ("packaging", "hbm", "先进封装", "封测"),
            "description": "先进封装和 HBM 供需验证持续性。",
        },
        {
            "layer_key": "equipment_material_atd",
            "layer_name": "Equipment / Material / Assembly-Test-Design",
            "keywords": ("equipment", "material", "foundry", "design", "设备", "材料", "代工"),
            "description": "设备材料封测设计链条需要同步确认。",
        },
    ],
    "solar": [
        {
            "layer_key": "destocking",
            "layer_name": "Destocking",
            "keywords": ("inventory", "destock", "去库存", "库存"),
            "description": "先看去库存，而非单日弹性。",
        },
        {
            "layer_key": "decapacity",
            "layer_name": "Decapacity",
            "keywords": ("capacity", "shutdown", "utilization", "去产能", "停产"),
            "description": "去产能和开工率变化决定持续性。",
        },
        {
            "layer_key": "price_chain",
            "layer_name": "Price Chain",
            "keywords": ("price", "spread", "asp", "价格", "价差"),
            "description": "价格链企稳是关键验证。",
        },
        {
            "layer_key": "capex_shrink_mna",
            "layer_name": "Capex Shrink / M&A",
            "keywords": ("capex", "m&a", "merger", "并购", "资本开支"),
            "description": "减停产并购与 capex 收缩才是盈利拐点证据。",
        },
    ],
    "hog_cycle": [
        {
            "layer_key": "sow_inventory",
            "layer_name": "Sow / Inventory",
            "keywords": ("sow", "inventory", "存栏", "能繁"),
            "description": "能繁母猪和存栏去化是周期起点。",
        },
        {
            "layer_key": "hog_price_spot_future",
            "layer_name": "Hog Price Spot/Futures",
            "keywords": ("hog", "spot", "futures", "猪价", "期货", "现货"),
            "description": "期现共振优于单边价格波动。",
        },
        {
            "layer_key": "margin_profit",
            "layer_name": "Profit / Margin",
            "keywords": ("margin", "profit", "盈利", "利润"),
            "description": "利润修复确认二棒机会。",
        },
        {
            "layer_key": "feed_cost",
            "layer_name": "Feed Cost",
            "keywords": ("feed", "corn", "soy", "饲料", "豆粕", "玉米"),
            "description": "饲料成本下行是验证条件之一。",
        },
    ],
}

SECTOR_PROVIDER_REQUIREMENTS: Dict[str, List[str]] = {
    "ai": ["sec", "company_ir", "price_feeds", "china_mapping_feed"],
    "innovative_pharma": ["fda", "nmpa", "clinicaltrials", "company_ir"],
    "semis": ["sec", "company_ir", "price_feeds", "equipment_supply_chain_feed"],
    "solar": ["price_feeds", "company_ir", "industry_capacity_feed"],
    "hog_cycle": ["agri_stats", "futures_exchange", "price_feeds", "feed_cost_feed"],
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _normalize_sector(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return SECTOR_ALIASES.get(text, SECTOR_ALIASES.get(text.lower(), text.lower()))


def _guess_market(code: str, market_hint: Any = None) -> str:
    market_text = str(market_hint or "").upper().strip()
    if market_text in MARKET_ALIAS:
        return MARKET_ALIAS[market_text]
    code_upper = str(code or "").upper()
    if code_upper.endswith(".SH") or code_upper.endswith(".SZ"):
        return "A"
    if code_upper.endswith(".HK"):
        return "HK"
    if code_upper.isalpha() and len(code_upper) <= 5:
        return "US"
    return "UNKNOWN"


def _extract_cards(opportunity_cards: Any) -> List[Dict[str, Any]]:
    if isinstance(opportunity_cards, list):
        return [row for row in opportunity_cards if isinstance(row, dict)]
    if isinstance(opportunity_cards, dict):
        for key in ("cards", "items", "all", "opportunities"):
            if isinstance(opportunity_cards.get(key), list):
                return [row for row in opportunity_cards.get(key) if isinstance(row, dict)]
    return []


def _extract_events(event_relevance: Any) -> List[Dict[str, Any]]:
    if isinstance(event_relevance, list):
        return [row for row in event_relevance if isinstance(row, dict)]
    if isinstance(event_relevance, dict):
        for key in ("events", "items", "cards"):
            if isinstance(event_relevance.get(key), list):
                return [row for row in event_relevance.get(key) if isinstance(row, dict)]
    return []


def _latest_time(candidates: Iterable[Any], fallback: str) -> str:
    parsed: List[datetime] = []
    for value in candidates:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            parsed.append(datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None))
            continue
        except Exception:
            pass
        try:
            parsed.append(datetime.strptime(text[:10], "%Y-%m-%d"))
        except Exception:
            continue
    if not parsed:
        return fallback
    return max(parsed).replace(microsecond=0).isoformat()


def _collect_thesis(bundle: Dict[str, Any], sector_key: str) -> Dict[str, Any]:
    theses = bundle.get("sector_theses") if isinstance(bundle, dict) else []
    if not isinstance(theses, list):
        return {}
    for item in theses:
        if isinstance(item, dict) and _normalize_sector(item.get("sector_key")) == sector_key:
            return item
    return {}


def _collect_sector_assets(
    sector_key: str,
    thesis: Dict[str, Any],
    cards: Sequence[Dict[str, Any]],
    events: Sequence[Dict[str, Any]],
    watchlists: Dict[str, Any],
) -> List[Dict[str, Any]]:
    assets: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def add_asset(code: Any, name: Any = None, market: Any = None) -> None:
        code_text = str(code or "").strip()
        if not code_text:
            return
        if code_text in seen:
            return
        seen.add(code_text)
        assets.append(
            {
                "asset_code": code_text,
                "asset_name": str(name or code_text),
                "market": _guess_market(code_text, market),
            }
        )

    baton = thesis.get("baton_map") if isinstance(thesis.get("baton_map"), dict) else {}
    for key in ("first_baton", "second_baton", "next_baton"):
        for code in baton.get(key) or []:
            add_asset(code)

    for row in cards:
        if _normalize_sector(row.get("sector") or row.get("sector_key")) != sector_key:
            continue
        add_asset(row.get("asset_code") or row.get("symbol"), row.get("asset_name") or row.get("name"), row.get("market"))

    for row in events:
        event_sector = _normalize_sector(row.get("sector_key") or row.get("sector"))
        if not event_sector and isinstance(row.get("sector_mapping"), list):
            for mapping in row.get("sector_mapping"):
                if isinstance(mapping, dict):
                    event_sector = _normalize_sector(mapping.get("sector"))
                    if event_sector:
                        break
        if event_sector != sector_key:
            continue
        for code in row.get("related_symbols") or []:
            add_asset(code)
        for mapped in row.get("asset_mapping") or []:
            if isinstance(mapped, dict):
                add_asset(mapped.get("code") or mapped.get("asset_code"), mapped.get("name") or mapped.get("asset_name"), mapped.get("market"))

    for market_key in ("us", "hk", "a_share", "etf"):
        rows = watchlists.get(market_key) if isinstance(watchlists, dict) else []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if _normalize_sector(row.get("sector")) != sector_key:
                continue
            add_asset(row.get("symbol"), row.get("name"))
    return assets


def _layer_signal_count(layer_keywords: Sequence[str], cards: Sequence[Dict[str, Any]], events: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = tuple(keyword.lower() for keyword in layer_keywords)
    matches: List[str] = []
    source_tags: Set[str] = set()

    for row in cards:
        text = " ".join(
            str(row.get(key) or "")
            for key in ("name", "notes", "driver", "thesis", "validation_signal", "validation_status")
        ).lower()
        if any(keyword in text for keyword in keywords):
            matches.append(str(row.get("name") or row.get("opportunity_id") or "opportunity"))
            source_tags.add(str(row.get("source") or row.get("source_url") or "sample_data"))

    for row in events:
        text = " ".join(
            str(row.get(key) or "")
            for key in ("title", "title_zh", "summary", "base_case", "event_type", "watch_items")
        ).lower()
        if any(keyword in text for keyword in keywords):
            matches.append(str(row.get("title") or row.get("event_id") or "event"))
            source_value = row.get("source")
            if isinstance(source_value, dict):
                source_tags.add(str(source_value.get("url") or source_value.get("tier") or "sample_data"))
            else:
                source_tags.add(str(source_value or row.get("source_url") or "sample_data"))

    signal_count = len(matches)
    if signal_count >= 2:
        status = "confirmed"
    elif signal_count == 1:
        status = "partial"
    else:
        status = "missing"
    if not source_tags:
        source_tags.add("sample_data")

    return {
        "signal_count": signal_count,
        "samples": matches[:2],
        "status": status,
        "sources": sorted(source_tags),
    }

def _detect_source_cache(source_values: Iterable[str]) -> Dict[str, str]:
    normalized = [str(value or "").lower() for value in source_values if str(value or "").strip()]
    if not normalized:
        return {"source": "sample_data", "cache_status": "sample_fallback"}
    if any("live" in value for value in normalized):
        return {"source": "live_evidence", "cache_status": "live"}
    if any("http" in value for value in normalized):
        return {"source": "live_evidence", "cache_status": "live"}
    if any("sample" in value for value in normalized):
        return {"source": "sample_data", "cache_status": "sample_fallback"}
    return {"source": "mixed", "cache_status": "mixed"}


def _provider_gaps(bundle: Dict[str, Any], sector_key: str, layer_statuses: Sequence[str]) -> List[Dict[str, Any]]:
    source_health = bundle.get("source_health") if isinstance(bundle, dict) else {}
    source_health = source_health if isinstance(source_health, dict) else {}
    required = SECTOR_PROVIDER_REQUIREMENTS.get(sector_key, [])
    gaps: List[Dict[str, Any]] = []
    for provider in required:
        provider_row = source_health.get(provider)
        if not isinstance(provider_row, dict):
            gaps.append(
                {
                    "provider": provider,
                    "reason": "missing_provider_health",
                    "required_for": sector_key,
                }
            )
            continue
        status = str(provider_row.get("status") or "missing").lower()
        if status not in {"healthy", "ok"}:
            gaps.append(
                {
                    "provider": provider,
                    "reason": f"provider_status_{status}",
                    "required_for": sector_key,
                }
            )
    if any(status == "missing" for status in layer_statuses):
        gaps.append(
            {
                "provider": "cross_validation",
                "reason": "missing_sector_layer_confirmation",
                "required_for": sector_key,
            }
        )
    return gaps


def _action_readiness(score: float, missing_validation: Sequence[str], provider_gaps: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if score >= 75 and not missing_validation and len(provider_gaps) <= 1:
        label = "actionable"
    elif score >= 55:
        label = "watch"
    else:
        label = "monitor"
    return {
        "label": label,
        "score": round(score, 2),
        "reason": "insufficient_validation" if missing_validation else "evidence_ready",
    }


def _build_sector_payload(
    bundle: Dict[str, Any],
    cards: Sequence[Dict[str, Any]],
    events: Sequence[Dict[str, Any]],
    macro_bridge: Optional[Dict[str, Any]],
    sector_key: str,
) -> Dict[str, Any]:
    thesis = _collect_thesis(bundle, sector_key)
    sector_name = str(thesis.get("sector_name") or SECTOR_NAME_MAP.get(sector_key, sector_key))
    watchlists = bundle.get("watchlists") if isinstance(bundle.get("watchlists"), dict) else {}
    assets = _collect_sector_assets(sector_key, thesis, cards, events, watchlists)
    leader_assets = [asset for asset in assets if asset.get("market") == "US"][:4]
    bridge_assets = [asset for asset in assets if asset.get("market") in {"HK", "ETF"}][:4]
    local_assets = [asset for asset in assets if asset.get("market") in {"A", "HK", "ETF"}][:6]

    baton_map = thesis.get("baton_map") if isinstance(thesis.get("baton_map"), dict) else {}
    first_baton = [code for code in baton_map.get("first_baton", []) if str(code).strip()]
    second_baton = [code for code in baton_map.get("second_baton", []) if str(code).strip()]

    validation_baton: List[str] = []
    for row in cards:
        if _normalize_sector(row.get("sector") or row.get("sector_key")) != sector_key:
            continue
        validation_text = str(row.get("validation_status") or row.get("stage") or "").lower()
        if validation_text in {"validated", "validating", "triggered"}:
            code = str(row.get("asset_code") or row.get("symbol") or "").strip()
            if code and code not in validation_baton:
                validation_baton.append(code)

    sector_cards = [row for row in cards if _normalize_sector(row.get("sector") or row.get("sector_key")) == sector_key]
    sector_events: List[Dict[str, Any]] = []
    for row in events:
        direct = _normalize_sector(row.get("sector_key") or row.get("sector"))
        mapped = ""
        if isinstance(row.get("sector_mapping"), list):
            for mapping in row.get("sector_mapping"):
                if isinstance(mapping, dict):
                    mapped = _normalize_sector(mapping.get("sector"))
                    if mapped:
                        break
        if direct == sector_key or mapped == sector_key:
            sector_events.append(row)

    evidence_layers: List[Dict[str, Any]] = []
    layer_statuses: List[str] = []
    source_values: Set[str] = set()
    for layer in SECTOR_LAYER_BLUEPRINT.get(sector_key, []):
        signal = _layer_signal_count(layer.get("keywords") or (), sector_cards, sector_events)
        layer_statuses.append(signal["status"])
        source_values.update(signal.get("sources") or [])
        evidence_layers.append(
            {
                "layer_key": layer["layer_key"],
                "layer_name": layer["layer_name"],
                "description": layer["description"],
                "status": signal["status"],
                "signal_count": signal["signal_count"],
                "samples": signal["samples"],
                "source": "live_evidence" if any("http" in source.lower() or "live" in source.lower() for source in signal["sources"]) else "sample_data",
            }
        )

    missing_validation: List[str] = []
    if not validation_baton:
        missing_validation.append("缺少验证棒资产确认")
    for layer in evidence_layers:
        if layer.get("status") == "missing":
            missing_validation.append(f"{layer.get('layer_name')} 缺失关键证据")
    if not local_assets:
        missing_validation.append("缺少本地可交易资产映射")

    provider_gaps = _provider_gaps(bundle, sector_key, layer_statuses)

    full_hits = sum(1 for status in layer_statuses if status == "confirmed")
    partial_hits = sum(1 for status in layer_statuses if status == "partial")
    total_layers = max(len(layer_statuses), 1)
    layer_ratio = (full_hits + 0.5 * partial_hits) / total_layers

    required_providers = SECTOR_PROVIDER_REQUIREMENTS.get(sector_key, [])
    provider_ratio = (len(required_providers) - len([gap for gap in provider_gaps if gap.get("provider") != "cross_validation"])) / max(
        len(required_providers), 1
    )
    provider_ratio = max(0.0, provider_ratio)
    validation_ratio = min(len(validation_baton), 2) / 2.0
    evidence_completeness = max(0.0, min(100.0, round((layer_ratio * 0.55 + provider_ratio * 0.30 + validation_ratio * 0.15) * 100.0, 2)))

    readiness = _action_readiness(evidence_completeness, missing_validation, provider_gaps)
    bridge_state = str((macro_bridge or {}).get("bridge_state") or "neutral")
    mode = "defensive" if bridge_state in {"risk_off", "blocked"} else "aggressive" if bridge_state == "risk_on" and evidence_completeness >= 65 else "balanced"

    invalidation_rules = [str(rule) for rule in (thesis.get("bear_case") or []) if str(rule).strip()]
    if not invalidation_rules:
        invalidation_rules = [str(rule) for rule in (thesis.get("risks") or []) if str(rule).strip()]
    if not invalidation_rules:
        invalidation_rules = ["领先资产失效或验证证据撤销时失效"]

    last_update = _latest_time(
        [bundle.get("as_of")]
        + [row.get("last_update") for row in sector_cards]
        + [row.get("updated_at") for row in sector_cards]
        + [row.get("effective_time") for row in sector_events]
        + [row.get("event_date") for row in sector_events],
        fallback=_now_iso(),
    )

    source_cache = _detect_source_cache(source_values)
    source_count = len(source_values) if source_values else 1

    return {
        "sector_key": sector_key,
        "sector_name": sector_name,
        "mode": mode,
        "leader_assets": leader_assets,
        "bridge_assets": bridge_assets,
        "local_assets": local_assets,
        "evidence_layers": evidence_layers,
        "first_baton": first_baton,
        "second_baton": second_baton,
        "validation_baton": validation_baton,
        "missing_validation": missing_validation,
        "invalidation_rules": invalidation_rules,
        "provider_gaps": provider_gaps,
        "action_readiness": readiness,
        "evidence_completeness": evidence_completeness,
        "last_update": last_update,
        "source_count": source_count,
        "source": source_cache["source"],
        "cache_status": source_cache["cache_status"],
    }


def build_sector_deep_evidence(
    bundle: Dict[str, Any],
    opportunity_cards: Any,
    event_relevance: Any = None,
    macro_bridge: Any = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """Build deep-evidence payload for Lead-Lag V2 sectors.

    The function is pure and side-effect free: inputs are read-only dictionaries/lists.
    """

    cards = _extract_cards(opportunity_cards)
    events = _extract_events(event_relevance)
    macro_payload = macro_bridge if isinstance(macro_bridge, dict) else {}

    target_sectors = list(SECTOR_KEYS)
    if sector and str(sector).strip().lower() not in {"all", "*"}:
        normalized = _normalize_sector(sector)
        target_sectors = [normalized] if normalized in SECTOR_KEYS else []

    sector_payloads = [
        _build_sector_payload(bundle=bundle if isinstance(bundle, dict) else {}, cards=cards, events=events, macro_bridge=macro_payload, sector_key=sector_key)
        for sector_key in target_sectors
    ]
    overall_cache_status = "live" if any(item.get("cache_status") == "live" for item in sector_payloads) else "sample_fallback"
    return {
        "as_of": str((bundle or {}).get("as_of") or _now_iso()),
        "count": len(sector_payloads),
        "sectors": sector_payloads,
        "source": "lead_lag_sector_deep_evidence_v2",
        "cache_status": overall_cache_status,
    }
