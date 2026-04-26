"""Diagnostics builders for Lead-Lag V2 transmission workspace and replay views."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


CANONICAL_RELATIONS = {
    "supplier": "spills_over_to",
    "pcb_supplier": "affects_demand_of",
    "assembly_peer": "maps_to",
    "direct_peer": "maps_to",
    "foundry_cycle": "affects_capex_of",
    "equipment_peer": "affects_capex_of",
    "inverter_peer": "maps_to",
    "cycle_peer": "validates",
}
HORIZONS = (1, 3, 5, 10, 20)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _clamp(value: Any, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, _as_float(value)))


def _normalize_sector(value: Any) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "AI": "ai",
        "artificial_intelligence": "ai",
        "semiconductor": "semis",
        "semi": "semis",
        "pharma": "innovative_pharma",
        "biotech": "innovative_pharma",
        "pv": "solar",
        "hog": "hog_cycle",
    }
    return aliases.get(text, text)


def _safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _is_live_card(card: Dict[str, Any]) -> bool:
    return str(card.get("cache_status") or "").lower() == "live"


def _cache_status(opportunity_cards: List[Dict[str, Any]], bundle: Dict[str, Any]) -> str:
    if any(_is_live_card(card) for card in opportunity_cards):
        return "live"
    bundle_status = str(bundle.get("cache_status") or "").strip().lower()
    if bundle_status:
        return bundle_status
    return "sample_fallback"


def _as_of(bundle: Dict[str, Any], opportunity_cards: List[Dict[str, Any]]) -> Optional[str]:
    if bundle.get("as_of"):
        return str(bundle.get("as_of"))
    stamps = [
        str(card.get("last_update"))
        for card in opportunity_cards
        if isinstance(card, dict) and card.get("last_update")
    ]
    return max(stamps) if stamps else None


def _normalize_relation(value: Any) -> str:
    raw = str(value or "").strip()
    if raw in {
        "leads",
        "validates",
        "crowds",
        "invalidates",
        "maps_to",
        "spills_over_to",
        "affects_margin_of",
        "affects_demand_of",
        "affects_capex_of",
    }:
        return raw
    return CANONICAL_RELATIONS.get(raw, "maps_to")


def _normalize_sign(value: Any) -> int:
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"-", "negative", "neg", "-1"}:
            return -1
        if v in {"+", "positive", "pos", "1"}:
            return 1
    return -1 if _as_float(value, 1.0) < 0 else 1


def _nearest_horizon(days: Any) -> int:
    d = max(1.0, _as_float(days, 3.0))
    for horizon in HORIZONS:
        if d <= horizon:
            return horizon
    return HORIZONS[-1]


def _card_sector(card: Dict[str, Any]) -> str:
    return _normalize_sector(card.get("sector") or card.get("sector_key"))


def _card_id(card: Dict[str, Any], fallback_index: int) -> str:
    return str(card.get("id") or card.get("opportunity_id") or f"opportunity_{fallback_index}")


def _iter_related_assets(card: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for key in ("leader_asset", "bridge_asset", "local_asset"):
        asset = card.get(key)
        if isinstance(asset, dict):
            yield asset
    for asset in _safe_list(card.get("local_proxy_assets")):
        if isinstance(asset, dict):
            yield asset


def _edge_status_and_blocker(edge: Dict[str, Any]) -> tuple[str, str]:
    blocker = str(edge.get("current_blocker") or "").strip()
    if blocker:
        return "blocked", blocker
    confidence = _as_float(edge.get("confidence"), _as_float(edge.get("strength"), 0.5))
    relation = str(edge.get("relation") or "")
    if relation == "invalidates":
        return "risk", ""
    if confidence < 0.4:
        return "weak_evidence", ""
    return "active", ""


def _normalize_edge(raw: Dict[str, Any], as_of: Optional[str]) -> Dict[str, Any]:
    from_id = raw.get("from_id") or raw.get("source") or raw.get("from") or ""
    to_id = raw.get("to_id") or raw.get("target") or raw.get("to") or ""
    lag_days = _as_int(raw.get("lag_days"), 3)
    lag_min = max(1, _as_int(raw.get("lag_min_days"), lag_days if lag_days > 0 else 1))
    lag_max = max(lag_min, _as_int(raw.get("lag_max_days"), lag_min + 2))
    strength = _clamp(raw.get("strength", 0.5), 0.0, 1.0)
    confidence = _clamp(raw.get("confidence", strength), 0.0, 1.0)
    relation_detail = raw.get("relation") or raw.get("relation_detail") or "maps_to"
    normalized = {
        "from_id": str(from_id),
        "to_id": str(to_id),
        "relation": _normalize_relation(raw.get("relation")),
        "sign": _normalize_sign(raw.get("sign")),
        "strength": round(strength, 4),
        "lag_min_days": lag_min,
        "lag_max_days": lag_max,
        "evidence_type": str(raw.get("evidence_type") or relation_detail),
        "confidence": round(confidence, 4),
        "last_verified_at": raw.get("last_verified_at") or as_of,
        "status": str(raw.get("status") or ""),
        "current_blocker": str(raw.get("current_blocker") or ""),
    }
    if not normalized["status"]:
        status, blocker = _edge_status_and_blocker(normalized)
        normalized["status"] = status
        if not normalized["current_blocker"] and blocker:
            normalized["current_blocker"] = blocker
    return normalized


def _edge_sector(edge: Dict[str, Any], node_sector: Dict[str, str], card_sector: Dict[str, str]) -> str:
    direct = _normalize_sector(edge.get("sector"))
    if direct:
        return direct
    src = _normalize_sector(node_sector.get(str(edge.get("from_id"))))
    dst = _normalize_sector(node_sector.get(str(edge.get("to_id"))))
    if src:
        return src
    if dst:
        return dst
    src_card = _normalize_sector(card_sector.get(str(edge.get("from_id"))))
    dst_card = _normalize_sector(card_sector.get(str(edge.get("to_id"))))
    return src_card or dst_card


def build_transmission_workspace(
    bundle: Dict[str, Any],
    opportunity_cards: List[Dict[str, Any]],
    event_relevance: Optional[List[Dict[str, Any]]] = None,
    macro_bridge: Optional[Dict[str, Any]] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    bundle = _safe_dict(bundle)
    cards = [card for card in _safe_list(opportunity_cards) if isinstance(card, dict)]
    graph = _safe_dict(bundle.get("transmission_graph"))
    graph_nodes = [node for node in _safe_list(graph.get("nodes")) if isinstance(node, dict)]
    graph_edges = [edge for edge in _safe_list(graph.get("edges")) if isinstance(edge, dict)]
    normalized_sector_filter = _normalize_sector(sector)
    as_of = _as_of(bundle, cards)
    cache_status = _cache_status(cards, bundle)
    sample_derived = cache_status != "live"

    nodes: List[Dict[str, Any]] = []
    node_sector: Dict[str, str] = {}
    for raw in graph_nodes:
        node_id = str(raw.get("node_id") or raw.get("id") or raw.get("symbol") or raw.get("label") or "").strip()
        if not node_id:
            continue
        normalized = {
            "node_id": node_id,
            "label": raw.get("label") or raw.get("name") or raw.get("symbol") or node_id,
            "node_type": raw.get("node_type") or ("asset" if raw.get("symbol") else "sector"),
            "market": raw.get("market"),
            "sector": _normalize_sector(raw.get("sector")),
            "score": raw.get("score"),
            "source": raw.get("source") or ("sample_data" if sample_derived else "live_fusion"),
        }
        node_sector[node_id] = str(normalized.get("sector") or "")
        nodes.append(normalized)

    card_sector: Dict[str, str] = {}
    for index, card in enumerate(cards):
        card_id = _card_id(card, index)
        s = _card_sector(card)
        card_sector[card_id] = s
        for asset in _iter_related_assets(card):
            code = str(asset.get("code") or "").strip()
            if not code:
                continue
            node_id = code
            if any(node.get("node_id") == node_id for node in nodes):
                continue
            nodes.append(
                {
                    "node_id": node_id,
                    "label": asset.get("name") or code,
                    "node_type": "asset",
                    "market": asset.get("market"),
                    "sector": s,
                    "score": card.get("decision_priority_score"),
                    "source": "opportunity_cards",
                }
            )
            node_sector[node_id] = s

    edges = [_normalize_edge(raw, as_of) for raw in graph_edges]
    edges = [edge for edge in edges if edge.get("from_id") and edge.get("to_id")]

    if normalized_sector_filter:
        keep_nodes = {node["node_id"] for node in nodes if _normalize_sector(node.get("sector")) == normalized_sector_filter}
        filtered_edges: List[Dict[str, Any]] = []
        for edge in edges:
            edge_sector = _edge_sector(edge, node_sector, card_sector)
            if edge_sector == normalized_sector_filter:
                filtered_edges.append(edge)
                keep_nodes.add(str(edge.get("from_id")))
                keep_nodes.add(str(edge.get("to_id")))
        edges = filtered_edges
        nodes = [node for node in nodes if node.get("node_id") in keep_nodes]
        cards = [card for card in cards if _card_sector(card) == normalized_sector_filter]

    first_baton: List[Dict[str, Any]] = []
    second_baton: List[Dict[str, Any]] = []
    third_baton: List[Dict[str, Any]] = []
    validation_baton: List[Dict[str, Any]] = []
    for index, card in enumerate(cards):
        card_id = _card_id(card, index)
        item = {
            "opportunity_id": card_id,
            "sector": _card_sector(card),
            "stage": card.get("baton_stage") or card.get("stage"),
            "decision_priority_score": _as_float(card.get("decision_priority_score"), 0.0),
        }
        baton_stage = str(card.get("baton_stage") or card.get("stage") or "").lower()
        if baton_stage in {"first_baton", "first-baton", "pre_trigger"}:
            first_baton.append(item)
        elif baton_stage in {"second_baton", "second-baton", "triggered"}:
            second_baton.append(item)
        elif baton_stage in {"third_baton", "third-baton", "next_baton", "next-baton", "latent"}:
            third_baton.append(item)
        if baton_stage in {"validation_baton", "validation-baton", "validating"}:
            validation_baton.append(item)
        elif str(card.get("generation_status") or "").lower() in {"actionable", "watch_only"}:
            validation_baton.append(item)

    first_baton.sort(key=lambda row: (-_as_float(row.get("decision_priority_score")), row.get("opportunity_id")))
    second_baton.sort(key=lambda row: (-_as_float(row.get("decision_priority_score")), row.get("opportunity_id")))
    third_baton.sort(key=lambda row: (-_as_float(row.get("decision_priority_score")), row.get("opportunity_id")))
    validation_baton.sort(key=lambda row: (-_as_float(row.get("decision_priority_score")), row.get("opportunity_id")))

    baton_paths: List[Dict[str, Any]] = []
    for first in first_baton[:5]:
        same_sector_second = next(
            (item for item in second_baton if item.get("sector") == first.get("sector")),
            second_baton[0] if second_baton else None,
        )
        same_sector_third = next(
            (item for item in third_baton if item.get("sector") == first.get("sector")),
            third_baton[0] if third_baton else None,
        )
        path_nodes = [first.get("opportunity_id")]
        if same_sector_second:
            path_nodes.append(same_sector_second.get("opportunity_id"))
        if same_sector_third:
            path_nodes.append(same_sector_third.get("opportunity_id"))
        baton_paths.append(
            {
                "sector": first.get("sector"),
                "path": path_nodes,
                "validation_target": next(
                    (
                        item.get("opportunity_id")
                        for item in validation_baton
                        if item.get("sector") == first.get("sector")
                    ),
                    validation_baton[0].get("opportunity_id") if validation_baton else None,
                ),
            }
        )

    avoid_or_hedge_assets: List[Dict[str, Any]] = []
    for card in cards:
        crowding_label = str(_safe_dict(card.get("crowding_state")).get("label") or "").lower()
        generation_status = str(card.get("generation_status") or "").lower()
        if generation_status in {"invalidated", "insufficient_evidence"} or crowding_label in {"high", "crowded"}:
            local_asset = _safe_dict(card.get("local_asset"))
            if local_asset.get("code"):
                avoid_or_hedge_assets.append(
                    {
                        "asset_code": local_asset.get("code"),
                        "asset_name": local_asset.get("name"),
                        "market": local_asset.get("market"),
                        "reason": card.get("missing_evidence_reason") or card.get("risk") or "crowding_or_invalidation",
                        "opportunity_id": card.get("id") or card.get("opportunity_id"),
                    }
                )
    if isinstance(macro_bridge, dict):
        bridge_state = str(macro_bridge.get("bridge_state") or "")
        if bridge_state in {"risk_off", "risk_tightening"}:
            avoid_or_hedge_assets.append(
                {
                    "asset_code": "CASH_OR_HEDGE",
                    "asset_name": "Cash / Index Hedge",
                    "market": "CrossMarket",
                    "reason": f"macro_bridge={bridge_state}",
                    "opportunity_id": None,
                }
            )

    current_bottlenecks: List[Dict[str, Any]] = []
    for edge in edges:
        if edge.get("status") in {"blocked", "weak_evidence", "risk"}:
            current_bottlenecks.append(
                {
                    "type": "edge",
                    "item_id": f"{edge.get('from_id')}->{edge.get('to_id')}",
                    "status": edge.get("status"),
                    "reason": edge.get("current_blocker") or edge.get("evidence_type"),
                }
            )
    for index, card in enumerate(cards):
        missing = _safe_list(card.get("missing_confirmations"))
        if missing:
            current_bottlenecks.append(
                {
                    "type": "opportunity",
                    "item_id": _card_id(card, index),
                    "status": "missing_confirmation",
                    "reason": "; ".join(str(item) for item in missing[:3]),
                }
            )
    relevance_rows = [row for row in _safe_list(event_relevance) if isinstance(row, dict)]
    if relevance_rows and normalized_sector_filter:
        sector_rows = [
            row
            for row in relevance_rows
            if any(_normalize_sector(_safe_dict(s).get("sector")) == normalized_sector_filter for s in _safe_list(row.get("sector_mapping")))
        ]
        if sector_rows and not any(str(row.get("event_class")) == "market-facing" for row in sector_rows):
            current_bottlenecks.append(
                {
                    "type": "event_relevance",
                    "item_id": normalized_sector_filter,
                    "status": "low_market_mapping",
                    "reason": "sector events are mostly research-facing",
                }
            )

    edge_status_summary: Dict[str, int] = {}
    for edge in edges:
        status = str(edge.get("status") or "unknown")
        edge_status_summary[status] = edge_status_summary.get(status, 0) + 1

    return {
        "nodes": nodes,
        "edges": edges,
        "baton_paths": baton_paths,
        "first_baton": first_baton,
        "second_baton": second_baton,
        "third_baton": third_baton,
        "validation_baton": validation_baton,
        "avoid_or_hedge_assets": avoid_or_hedge_assets,
        "current_bottlenecks": current_bottlenecks,
        "edge_status_summary": edge_status_summary,
        "as_of": as_of,
        "source": "sample_data" if sample_derived else "live_fusion",
        "cache_status": cache_status,
        "sample_derived": sample_derived,
    }


def build_replay_diagnostics(
    bundle: Dict[str, Any],
    opportunity_cards: List[Dict[str, Any]],
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    bundle = _safe_dict(bundle)
    cards = [card for card in _safe_list(opportunity_cards) if isinstance(card, dict)]
    replay_stats = [row for row in _safe_list(bundle.get("replay_stats")) if isinstance(row, dict)]
    normalized_sector_filter = _normalize_sector(sector)
    if normalized_sector_filter:
        replay_stats = [row for row in replay_stats if _normalize_sector(row.get("sector_key") or row.get("sector")) == normalized_sector_filter]
        cards = [card for card in cards if _card_sector(card) == normalized_sector_filter]

    cache_status = _cache_status(cards, bundle)
    sample_derived = cache_status != "live"
    as_of = _as_of(bundle, cards)

    horizon_acc = {
        horizon: {"horizon_days": horizon, "cases": 0, "hit_weighted": 0.0, "win_weighted": 0.0, "alpha_weighted": 0.0}
        for horizon in HORIZONS
    }

    for row in replay_stats:
        cases = max(1, _as_int(row.get("cases"), 1))
        horizon = _nearest_horizon(row.get("avg_lead_days"))
        bucket = horizon_acc[horizon]
        bucket["cases"] += cases
        bucket["hit_weighted"] += _as_float(row.get("hit_rate")) * cases
        bucket["win_weighted"] += _as_float(row.get("win_rate")) * cases
        bucket["alpha_weighted"] += _as_float(row.get("net_alpha_bps")) * cases

    # Card-derived fallback keeps output useful even when replay_stats is missing.
    if not replay_stats:
        for card in cards:
            expected = _safe_dict(card.get("expected_lag_days"))
            horizon = _nearest_horizon(expected.get("max") or expected.get("min") or 5)
            replay = _safe_dict(card.get("historical_replay_summary"))
            hit = _as_float(replay.get("hit_rate"), _as_float(card.get("confidence"), 50.0) / 100.0)
            cases = 1
            bucket = horizon_acc[horizon]
            bucket["cases"] += cases
            bucket["hit_weighted"] += hit * cases
            bucket["win_weighted"] += max(0.0, min(1.0, hit - 0.08)) * cases
            bucket["alpha_weighted"] += _as_float(card.get("decision_priority_score"), 0.0) - 50.0

    horizon_distribution: List[Dict[str, Any]] = []
    for horizon in HORIZONS:
        bucket = horizon_acc[horizon]
        cases = bucket["cases"]
        horizon_distribution.append(
            {
                "horizon_days": horizon,
                "cases": cases,
                "hit_rate": round(bucket["hit_weighted"] / cases, 4) if cases else None,
                "win_rate": round(bucket["win_weighted"] / cases, 4) if cases else None,
                "avg_net_alpha_bps": round(bucket["alpha_weighted"] / cases, 2) if cases else None,
                "sample_derived": sample_derived,
            }
        )

    regime_rows: Dict[str, Dict[str, Any]] = {}
    for row in replay_stats:
        regime = str(row.get("regime") or row.get("regime_key") or "unknown")
        cases = max(1, _as_int(row.get("cases"), 1))
        slot = regime_rows.setdefault(regime, {"regime": regime, "cases": 0, "hit_weighted": 0.0, "win_weighted": 0.0})
        slot["cases"] += cases
        slot["hit_weighted"] += _as_float(row.get("hit_rate")) * cases
        slot["win_weighted"] += _as_float(row.get("win_rate")) * cases
    if not regime_rows and cards:
        crowded_count = sum(
            1
            for card in cards
            if str(_safe_dict(card.get("crowding_state")).get("label") or "").lower() in {"high", "crowded"}
        )
        normal_count = max(0, len(cards) - crowded_count)
        regime_rows["crowded"] = {"regime": "crowded", "cases": crowded_count, "hit_weighted": 0.0, "win_weighted": 0.0}
        regime_rows["normal"] = {"regime": "normal", "cases": normal_count, "hit_weighted": 0.0, "win_weighted": 0.0}

    regime_split = [
        {
            "regime": row["regime"],
            "cases": row["cases"],
            "hit_rate": round(row["hit_weighted"] / row["cases"], 4) if row["cases"] else None,
            "win_rate": round(row["win_weighted"] / row["cases"], 4) if row["cases"] else None,
        }
        for row in sorted(regime_rows.values(), key=lambda item: (-item["cases"], item["regime"]))
    ]

    crowded_before = 0
    crowded_after = 0
    crowded_before_hit = 0.0
    crowded_after_hit = 0.0
    for card in cards:
        replay = _safe_dict(card.get("historical_replay_summary"))
        hit = _as_float(replay.get("hit_rate"), _as_float(card.get("confidence"), 50.0) / 100.0)
        crowding = str(_safe_dict(card.get("crowding_state")).get("label") or "").lower()
        if crowding in {"high", "crowded"}:
            crowded_after += 1
            crowded_after_hit += hit
        else:
            crowded_before += 1
            crowded_before_hit += hit
    crowded_before_after = {
        "before_crowded_cases": crowded_before,
        "after_crowded_cases": crowded_after,
        "before_hit_rate": round(crowded_before_hit / crowded_before, 4) if crowded_before else None,
        "after_hit_rate": round(crowded_after_hit / crowded_after, 4) if crowded_after else None,
        "delta_hit_rate": (
            round((crowded_after_hit / crowded_after) - (crowded_before_hit / crowded_before), 4)
            if crowded_before and crowded_after
            else None
        ),
    }

    failure_counter: Dict[str, Dict[str, Any]] = {}
    for row in replay_stats:
        name = str(row.get("failure_mode") or "unknown_failure")
        cases = max(1, _as_int(row.get("cases"), 1))
        slot = failure_counter.setdefault(name, {"failure_mode": name, "count": 0, "weighted_cases": 0})
        slot["count"] += 1
        slot["weighted_cases"] += cases
    for card in cards:
        replay = _safe_dict(card.get("historical_replay_summary"))
        name = str(replay.get("worst_failure_mode") or card.get("missing_evidence_reason") or "unknown_failure")
        slot = failure_counter.setdefault(name, {"failure_mode": name, "count": 0, "weighted_cases": 0})
        slot["count"] += 1
        slot["weighted_cases"] += 1
    failure_mode_ranking = [
        {"failure_mode": row["failure_mode"], "count": row["count"], "weighted_cases": row["weighted_cases"]}
        for row in sorted(
            failure_counter.values(),
            key=lambda item: (-item["weighted_cases"], -item["count"], item["failure_mode"]),
        )
    ]

    transitions = {
        "pre_trigger_to_triggered": {"count": 0, "score_sum": 0.0},
        "triggered_to_validating": {"count": 0, "score_sum": 0.0},
        "validating_to_crowded": {"count": 0, "score_sum": 0.0},
        "validating_to_invalidated": {"count": 0, "score_sum": 0.0},
    }
    for card in cards:
        baton_stage = str(card.get("baton_stage") or card.get("stage") or "").lower()
        status = str(card.get("generation_status") or "").lower()
        replay = _safe_dict(card.get("historical_replay_summary"))
        hit = _as_float(replay.get("hit_rate"), _as_float(card.get("confidence"), 50.0) / 100.0)
        crowding = str(_safe_dict(card.get("crowding_state")).get("label") or "").lower()
        if baton_stage in {"pre_trigger", "first_baton", "first-baton"}:
            transitions["pre_trigger_to_triggered"]["count"] += 1
            transitions["pre_trigger_to_triggered"]["score_sum"] += hit
        if baton_stage in {"triggered", "second_baton", "second-baton"}:
            transitions["triggered_to_validating"]["count"] += 1
            transitions["triggered_to_validating"]["score_sum"] += hit
        if crowding in {"high", "crowded"}:
            transitions["validating_to_crowded"]["count"] += 1
            transitions["validating_to_crowded"]["score_sum"] += hit
        if status == "invalidated":
            transitions["validating_to_invalidated"]["count"] += 1
            transitions["validating_to_invalidated"]["score_sum"] += hit
    stage_transition_performance = [
        {
            "transition": key,
            "cases": value["count"],
            "hit_rate": round(value["score_sum"] / value["count"], 4) if value["count"] else None,
        }
        for key, value in transitions.items()
    ]

    false_positive_factors = [
        {
            "factor": row["failure_mode"],
            "impact_cases": row["weighted_cases"],
            "note": "sample_derived" if sample_derived else "live_or_mixed",
        }
        for row in failure_mode_ranking[:5]
    ]

    opportunity_replay_map: List[Dict[str, Any]] = []
    for index, card in enumerate(cards):
        card_id = _card_id(card, index)
        replay = _safe_dict(card.get("historical_replay_summary"))
        lag = _safe_dict(card.get("expected_lag_days"))
        horizon = _nearest_horizon(lag.get("max") or lag.get("min") or replay.get("best_horizon") or 5)
        opportunity_replay_map.append(
            {
                "opportunity_id": card_id,
                "sector": _card_sector(card),
                "generation_status": card.get("generation_status"),
                "baton_stage": card.get("baton_stage") or card.get("stage"),
                "horizon_days": horizon,
                "replay_hit_rate": _as_float(replay.get("hit_rate"), _as_float(card.get("confidence"), 50.0) / 100.0),
                "worst_failure_mode": replay.get("worst_failure_mode") or card.get("missing_evidence_reason"),
                "sample_derived": sample_derived,
            }
        )

    sample_size = sum(_as_int(row.get("cases"), 0) for row in replay_stats)
    if sample_size <= 0:
        sample_size = len(opportunity_replay_map)

    return {
        "horizon_distribution": horizon_distribution,
        "regime_split": regime_split,
        "crowded_before_after": crowded_before_after,
        "failure_mode_ranking": failure_mode_ranking,
        "stage_transition_performance": stage_transition_performance,
        "false_positive_factors": false_positive_factors,
        "opportunity_replay_map": opportunity_replay_map,
        "sample_size": sample_size,
        "as_of": as_of,
        "source": "sample_data" if sample_derived else "live_fusion",
        "cache_status": cache_status,
        "sample_derived": sample_derived,
    }
