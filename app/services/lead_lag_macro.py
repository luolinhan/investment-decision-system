"""Macro / External / HK bridge builder for Lead-Lag V2."""
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


REQUIRED_MACRO_FIELDS = {
    "cpi": ("CPI", ("cpi", "cpi同比")),
    "core_cpi": ("Core CPI", ("core cpi", "核心cpi")),
    "ppi": ("PPI", ("ppi", "ppi同比")),
    "social_financing": ("Social financing", ("social", "社融", "tsf")),
    "m1": ("M1", ("m1", "m1同比")),
    "m2": ("M2", ("m2", "m2同比")),
    "fixed_asset_investment": ("Fixed asset investment", ("fixed asset", "固定资产")),
    "property_investment": ("Property investment", ("property", "real estate", "地产", "国房")),
}

REQUIRED_EXTERNAL_FIELDS = {
    "dxy": ("DXY", ("dxy",)),
    "us_treasury_2y": ("US Treasury 2Y", ("2y", "美国2y")),
    "us_treasury_10y": ("US Treasury 10Y", ("10y", "美国10y")),
    "yield_curve_change": ("Yield curve change", ("2s10s", "yield curve")),
    "gold": ("Gold", ("gold", "黄金")),
    "vix": ("VIX", ("vix",)),
    "cnh_risk_proxy": ("CNH / RMB risk proxy", ("usd/cnh", "usdcnh", "cnh")),
    "ftse_china_or_adr_proxy": ("FTSE China / ADR risk proxy", ("ftse", "adr", "yang", "a50")),
}

REQUIRED_HK_FIELDS = {
    "southbound_flow": ("Southbound flow", ("southbound", "南向")),
    "northbound_linkage": ("Northbound linkage", ("northbound", "北向")),
    "hk_short_selling": ("HK short selling", ("short", "卖空")),
    "ah_premium": ("A/H premium", ("a/h", "ah", "溢价")),
    "stock_connect_holdings_change": ("Stock Connect holdings change", ("holdings", "持股")),
    "hk_visitor_activity": ("HK visitor activity", ("visitor", "旅客", "访港")),
}

DEFAULT_BRIDGE_CONFIG = {
    "macro_score": 0.34,
    "external_score": 0.33,
    "hk_score": 0.33,
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: Any, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return round(max(minimum, min(maximum, _as_float(value))), 2)


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _signal_map(signals: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        label = str(signal.get("label") or "").strip()
        if label:
            rows[label.lower()] = signal
    return rows


def _find_signal(signals: Dict[str, Dict[str, Any]], aliases: Iterable[str]) -> Optional[Dict[str, Any]]:
    normalized_aliases = [alias.lower() for alias in aliases]
    for label, signal in signals.items():
        if any(alias in label for alias in normalized_aliases):
            return signal
    return None


def _freshness(panel: Dict[str, Any], effective_time: Any, cache_status: str) -> Dict[str, Any]:
    panel_freshness = panel.get("freshness") if isinstance(panel, dict) else {}
    if isinstance(panel_freshness, dict):
        return {
            "last_update": panel_freshness.get("last_update") or effective_time,
            "age_days": panel_freshness.get("age_days"),
            "is_stale": bool(panel_freshness.get("is_stale", False)),
            "status": "stale" if panel_freshness.get("is_stale") else cache_status,
            "staleness_reason": "radar_panel_marked_stale" if panel_freshness.get("is_stale") else "",
        }
    return {
        "last_update": effective_time,
        "age_days": None,
        "is_stale": False if effective_time else True,
        "status": cache_status if effective_time else "missing",
        "staleness_reason": "" if effective_time else "no_effective_time",
    }


def _content_hash(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _provider_metadata(
    provider_key: str,
    source_url: str,
    effective_time: Any,
    unit: str,
    cache_status: str,
    transformation: str,
    confidence: float,
    value: Any,
    parser_version: str = "lead_lag_macro_v1",
) -> Dict[str, Any]:
    freshness = "current" if cache_status == "live" and effective_time else cache_status
    if value in (None, ""):
        freshness = "missing"
    return {
        "provider_key": provider_key,
        "source": provider_key,
        "source_url": source_url,
        "ingest_time": _now_iso(),
        "effective_time": effective_time,
        "frequency": "daily",
        "unit": unit,
        "confidence": confidence,
        "transformation": transformation,
        "freshness": freshness,
        "staleness_reason": "" if value not in (None, "") else "missing_from_current_source",
        "cache_status": cache_status,
        "content_hash": _content_hash(provider_key, effective_time, value, cache_status),
        "parser_version": parser_version,
        "license_note": "public_or_local_snapshot_metadata_only",
    }


def _metric(
    key: str,
    label: str,
    signal: Optional[Dict[str, Any]],
    provider_key: str,
    source_url: str,
    effective_time: Any,
    cache_status: str,
    unit: str = "",
) -> Dict[str, Any]:
    value = signal.get("value") if isinstance(signal, dict) else None
    score = signal.get("score") if isinstance(signal, dict) else None
    return {
        "key": key,
        "label": label,
        "value": value,
        "display": signal.get("display") if isinstance(signal, dict) else None,
        "unit": unit,
        "score": _clamp(score, 0.0, 100.0) if score is not None else None,
        "signal": signal.get("signal") if isinstance(signal, dict) else "missing",
        "hint": signal.get("hint") if isinstance(signal, dict) else "",
        "provider_metadata": _provider_metadata(
            provider_key=provider_key,
            source_url=source_url,
            effective_time=effective_time,
            unit=unit,
            cache_status=cache_status,
            transformation="radar_signal_passthrough" if cache_status == "live" else "sample_bundle_fallback",
            confidence=0.82 if cache_status == "live" and value not in (None, "") else 0.35,
            value=value,
        ),
    }


def _provider_health(provider_key: str, fields: Dict[str, Dict[str, Any]], cache_status: str) -> Dict[str, Any]:
    loaded = [field for field in fields.values() if field.get("value") not in (None, "")]
    metadata = [field.get("provider_metadata") or {} for field in fields.values()]
    effective_times = [meta.get("effective_time") for meta in metadata if meta.get("effective_time")]
    return {
        "provider_key": provider_key,
        "last_success_at": max(effective_times) if effective_times and loaded else None,
        "last_attempt_at": _now_iso(),
        "records_loaded": len(loaded),
        "freshness_status": cache_status if loaded else "missing",
        "error": None if loaded else "required_fields_missing",
        "fallback_used": cache_status == "sample_fallback",
        "cache_status": cache_status,
        "parser_version": "lead_lag_macro_v1",
    }


def _label_from_score(score: float, good: str, neutral: str, poor: str) -> str:
    if score >= 65:
        return good
    if score >= 45:
        return neutral
    return poor


def _style_mapping(macro: Dict[str, Any], external: Dict[str, Any], hk: Dict[str, Any]) -> Dict[str, str]:
    macro_score = _as_float(macro.get("score"), 50.0)
    external_score = _as_float(external.get("score"), 50.0)
    hk_score = _as_float(hk.get("score"), 50.0)
    high_beta = "allowed" if min(external_score, hk_score) >= 55 else "reduced"
    return {
        "growth": "positive" if macro_score >= 55 and high_beta == "allowed" else "selective",
        "cyclicals": "positive" if macro_score >= 60 else "neutral",
        "dividend": "neutral" if external_score >= 50 else "preferred_defense",
        "hk_beta": high_beta,
    }


def build_macro_bridge(
    bundle: Dict[str, Any],
    live_evidence: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
) -> Dict[str, Any]:
    radar = live_evidence.get("radar") if isinstance(live_evidence, dict) else {}
    radar = radar if isinstance(radar, dict) else {}
    macro_panel = radar.get("macro") if isinstance(radar.get("macro"), dict) else {}
    external_panel = radar.get("external") if isinstance(radar.get("external"), dict) else {}
    hk_panel = radar.get("hk") if isinstance(radar.get("hk"), dict) else {}
    radar_liquidity = radar.get("liquidity") if isinstance(radar.get("liquidity"), dict) else {}
    live_available = bool(macro_panel or external_panel or hk_panel or radar_liquidity)
    cache_status = "live" if live_available else "sample_fallback"
    if live_available:
        source_url = (radar.get("source_health") or {}).get("source") or "data/radar/cache/overview.json"
    else:
        source_url = "sample_data/lead_lag/lead_lag_v1.json"

    sample_liquidity = bundle.get("liquidity") if isinstance(bundle, dict) else {}
    sample_liquidity = sample_liquidity if isinstance(sample_liquidity, dict) else {}
    sample_markets = sample_liquidity.get("markets") if isinstance(sample_liquidity.get("markets"), list) else []
    sample_hk = next((row for row in sample_markets if str(row.get("market")).upper() == "HK"), {})
    sample_us = next((row for row in sample_markets if str(row.get("market")).upper() == "US"), {})

    macro_effective = (macro_panel.get("freshness") or {}).get("last_update") or radar_liquidity.get("last_data_sync")
    external_effective = (external_panel.get("freshness") or {}).get("last_update") or macro_effective
    hk_effective = (hk_panel.get("freshness") or {}).get("last_update") or macro_effective

    macro_signals = _signal_map(macro_panel.get("signals") or [])
    external_signals = _signal_map(external_panel.get("signals") or [])
    hk_signals = _signal_map(hk_panel.get("signals") or [])

    macro_fields = {
        key: _metric(key, label, _find_signal(macro_signals, aliases), "radar_macro" if live_available else "sample_bundle", source_url, macro_effective, cache_status, "pct")
        for key, (label, aliases) in REQUIRED_MACRO_FIELDS.items()
    }
    external_fields = {
        key: _metric(key, label, _find_signal(external_signals, aliases), "radar_external" if live_available else "sample_bundle", source_url, external_effective, cache_status, "index")
        for key, (label, aliases) in REQUIRED_EXTERNAL_FIELDS.items()
    }
    hk_fields = {
        key: _metric(key, label, _find_signal(hk_signals, aliases), "radar_hk" if live_available else "sample_bundle", source_url, hk_effective, cache_status, "")
        for key, (label, aliases) in REQUIRED_HK_FIELDS.items()
    }

    if not live_available and sample_hk:
        hk_fields["hk_risk_appetite_label"] = {
            "key": "hk_risk_appetite_label",
            "label": "HK risk appetite label",
            "value": sample_hk.get("state"),
            "display": sample_hk.get("state"),
            "unit": "",
            "score": _clamp(_as_float(sample_hk.get("score"), 0.5) * 100.0),
            "signal": "sample_fallback",
            "hint": sample_hk.get("comment") or "",
            "provider_metadata": _provider_metadata(
                "sample_bundle",
                "sample_data/lead_lag",
                bundle.get("as_of"),
                "",
                "sample_fallback",
                "sample_liquidity_market_row",
                0.35,
                sample_hk.get("state"),
            ),
        }

    macro_score = _clamp(macro_panel.get("score") if macro_panel.get("score") is not None else 50.0) if live_available else 50.0
    external_score = _clamp(
        external_panel.get("external_risk_score")
        if external_panel.get("external_risk_score") is not None
        else radar_liquidity.get("external_risk_score", 50.0)
    ) if live_available else _clamp(_as_float(sample_us.get("score"), 0.5) * 100.0)
    hk_score = _clamp(
        hk_panel.get("hk_liquidity_score")
        if hk_panel.get("hk_liquidity_score") is not None
        else radar_liquidity.get("hk_liquidity_score", 50.0)
    ) if live_available else _clamp(_as_float(sample_hk.get("score"), 0.5) * 100.0)

    bridge_weights = (((config or {}).get("scoring") or {}).get("bridge_weights") or DEFAULT_BRIDGE_CONFIG)
    bridge_score = _clamp(
        macro_score * _as_float(bridge_weights.get("macro_score"), DEFAULT_BRIDGE_CONFIG["macro_score"])
        + external_score * _as_float(bridge_weights.get("external_score"), DEFAULT_BRIDGE_CONFIG["external_score"])
        + hk_score * _as_float(bridge_weights.get("hk_score"), DEFAULT_BRIDGE_CONFIG["hk_score"])
    )
    bridge_state = _label_from_score(bridge_score, "risk_on", "risk_neutral", "risk_off")

    macro_layer = {
        "label": macro_panel.get("macro_regime") or radar_liquidity.get("macro_regime") or "sample_fallback_neutral",
        "score": macro_score,
        "quadrant": macro_panel.get("quadrant") or {},
        "fields": macro_fields,
        "drivers": [signal.get("label") for signal in (macro_panel.get("signals") or [])[:4] if isinstance(signal, dict)],
        "missing_fields": [key for key, value in macro_fields.items() if value.get("value") in (None, "")],
        "style_mapping": {},
        "freshness": _freshness(macro_panel, macro_effective, cache_status),
        "provider_health": _provider_health("radar_macro" if live_available else "sample_bundle", macro_fields, cache_status),
        "cache_status": cache_status,
    }
    external_layer = {
        "label": external_panel.get("risk_state") or _label_from_score(external_score, "risk_relief", "neutral", "risk_tightening"),
        "score": external_score,
        "high_beta_permission": "allowed" if external_score >= 58 else "reduced" if external_score >= 45 else "blocked",
        "fields": external_fields,
        "drivers": [signal.get("label") for signal in (external_panel.get("signals") or [])[:4] if isinstance(signal, dict)],
        "missing_fields": [key for key, value in external_fields.items() if value.get("value") in (None, "")],
        "freshness": _freshness(external_panel, external_effective, cache_status),
        "provider_health": _provider_health("radar_external" if live_available else "sample_bundle", external_fields, cache_status),
        "cache_status": cache_status,
    }
    hk_layer = {
        "label": hk_panel.get("risk_appetite") or sample_hk.get("state") or _label_from_score(hk_score, "hk_beta_open", "hk_selective", "hk_risk_off"),
        "score": hk_score,
        "hk_risk_appetite_label": hk_panel.get("risk_appetite") or sample_hk.get("state") or "unknown",
        "fields": hk_fields,
        "drivers": [signal.get("label") for signal in (hk_panel.get("signals") or [])[:4] if isinstance(signal, dict)] or ([sample_hk.get("comment")] if sample_hk.get("comment") else []),
        "missing_fields": [key for key, value in hk_fields.items() if value.get("value") in (None, "")],
        "freshness": _freshness(hk_panel, hk_effective, cache_status),
        "provider_health": _provider_health("radar_hk" if live_available else "sample_bundle", hk_fields, cache_status),
        "cache_status": cache_status,
    }
    macro_layer["style_mapping"] = _style_mapping(macro_layer, external_layer, hk_layer)

    decision_impact = {
        "bridge_state": bridge_state,
        "bridge_score": bridge_score,
        "risk_budget_bias": "increase" if bridge_score >= 68 else "reduce" if bridge_score < 45 else "neutral",
        "opportunity_score_bias": round((bridge_score - 50.0) / 10.0, 2),
        "summary": (
            f"Bridge {bridge_state}: macro={macro_score:.1f}, "
            f"external={external_score:.1f}, hk={hk_score:.1f}."
        ),
    }

    return {
        "as_of": as_of or _now_iso(),
        "region": region,
        "regime": regime,
        "macro_regime": macro_layer,
        "external_risk": external_layer,
        "hk_liquidity": hk_layer,
        "hk_liquidity_activity": hk_layer,
        "bridge_score": bridge_score,
        "bridge_state": bridge_state,
        "decision_impact": decision_impact,
        "source_summary": {
            "source_count": sum(
                layer["provider_health"]["records_loaded"]
                for layer in (macro_layer, external_layer, hk_layer)
            ),
            "freshness": "live_radar" if live_available else "sample_bundle_fallback",
            "cache_status": cache_status,
        },
        "cache_status": cache_status,
    }
