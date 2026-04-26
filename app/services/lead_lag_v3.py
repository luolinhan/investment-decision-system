"""V3 projection helpers for Lead-Lag opportunity and event payloads."""
from __future__ import annotations

import re
from datetime import datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


EXECUTABLE_SOURCE_CLASSES = {"live_official", "live_public", "live_media", "user_curated"}
BLOCKED_SOURCE_CLASSES = {"sample_demo", "fallback_placeholder"}
OFFICIAL_SOURCE_HINTS = {"official", "exchange", "filing", "company_ir", "regulatory_release", "company_release"}
PUBLIC_SOURCE_HINTS = {"live_public", "public", "research_reports", "intelligence_events", "radar_snapshot"}
SAMPLE_SOURCE_HINTS = {"sample", "sample_data", "sample_fallback", "mock", "demo", "fallback"}
OPPORTUNITY_FAMILY_BY_MODEL = {
    "leadership_breadth": "industry_transmission",
    "event_spillover": "event_calendar",
    "transmission_graph": "cross_market_mapping",
    "liquidity_dispersion": "external_liquidity_bridge",
    "earnings_revision": "earnings_revision",
    "policy_surprise": "policy_credit_fiscal",
    "valuation_gap": "valuation_gap",
    "replay_validation": "industry_transmission",
    "breadth_thrust": "crowding_short_squeeze",
    "memory_alignment": "entity_specific_dislocation",
}
CHECKPOINT_TIMES = (time(11, 40), time(15, 15))
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, "", "-", "--"):
            return default
        return float(value)
    except Exception:
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, "", "-", "--"):
            return default
        return int(float(value))
    except Exception:
        return default


def clamp(value: Any, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return round(max(minimum, min(maximum, as_float(value))), 2)


def compact_strings(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return []
    return [str(item).strip() for item in values if str(item or "").strip()]


def slug(value: Any, fallback: str = "item") -> str:
    text = str(value or "").lower().strip()
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", text)
    text = text.strip("_")
    return text[:80] or fallback


def parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        try:
            parsed = datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            try:
                parsed = datetime.strptime(text[:10], "%Y-%m-%d")
            except Exception:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TZ)
    return parsed.astimezone(LOCAL_TZ)


def next_legal_review_time(candidates: Iterable[Any], now: Optional[datetime] = None) -> Dict[str, Any]:
    current = now.astimezone(LOCAL_TZ) if now else datetime.now(LOCAL_TZ)
    parsed_candidates = [parsed for parsed in (parse_dt(item) for item in candidates) if parsed is not None]
    future = sorted(item for item in parsed_candidates if item > current)
    if future:
        selected = future[0]
        stale = len(parsed_candidates) > len(future)
    else:
        selected = None
        for day_offset in range(0, 4):
            day = current.date() + timedelta(days=day_offset)
            for checkpoint in CHECKPOINT_TIMES:
                candidate = datetime.combine(day, checkpoint, tzinfo=LOCAL_TZ)
                if candidate > current:
                    selected = candidate
                    break
            if selected:
                break
        stale = bool(parsed_candidates)
    return {
        "next_review_time": selected.isoformat() if selected else current.isoformat(),
        "checkpoint_status": "stale_rolled_forward" if stale else "scheduled",
        "stale_check_detected": stale,
        "timezone": "Asia/Shanghai",
    }


class LeadLagV3Projector:
    """Apply V3 data-governance and grouping rules to V2 cards."""

    def __init__(self, evidence_panels_by_url: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> None:
        self.evidence_panels_by_url = evidence_panels_by_url or {}

    def enrich_card(self, card: Dict[str, Any], raw_item: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raw_item = raw_item or {}
        payload = dict(card)
        source_class = self.classify_card_source(payload, raw_item)
        family = self.opportunity_family(payload)
        live_source_count, sample_source_count = self.source_counts(payload, raw_item, source_class)
        payload["data_source_class"] = source_class
        payload["source_classes"] = self.source_classes(payload, raw_item, source_class)
        payload["live_source_count"] = live_source_count
        payload["sample_source_count"] = sample_source_count
        payload["opportunity_family"] = family
        payload["opportunity_families"] = [family]
        payload["evidence_checklist"] = self.evidence_checklist(payload, source_class)
        payload["evidence_panel"] = self.evidence_panel(payload, raw_item, source_class)
        payload["citation_count"] = sum(as_int(item.get("citation_count"), 0) for item in payload["evidence_panel"])
        payload["archived_link_count"] = sum(1 for item in payload["evidence_panel"] if item.get("local_archive_path"))
        review = next_legal_review_time(payload.get("expected_review_times") or [payload.get("last_update")])
        payload.update(review)

        blockers = self.execution_blockers(payload, source_class)
        payload["execution_blockers"] = blockers
        payload["is_executable"] = not blockers and payload.get("generation_status") == "actionable"
        if blockers and payload.get("generation_status") == "actionable":
            payload["generation_status"] = "insufficient_evidence"
            payload["generation_status_zh"] = "证据不足"
            payload["missing_confirmations"] = list(
                dict.fromkeys(compact_strings(payload.get("missing_confirmations")) + blockers)
            )
            payload["missing_evidence_reason"] = "；".join(payload["missing_confirmations"])
            if payload.get("decision_chain"):
                payload["decision_chain"]["result"] = f"证据不足：{payload.get('thesis')}"
                payload["decision_chain"]["strategy"] = "当前只进入研究观察，等待 live 证据和本地归档补齐。"
        payload["evidence_summary"] = self.evidence_summary(payload)
        payload["data_summary"] = self.data_summary(payload)
        return payload

    @staticmethod
    def classify_card_source(card: Dict[str, Any], raw_item: Dict[str, Any]) -> str:
        text_parts = [
            card.get("cache_status"),
            raw_item.get("cache_status"),
            card.get("source_quality", {}).get("explanation") if isinstance(card.get("source_quality"), dict) else "",
            " ".join(compact_strings(raw_item.get("evidence_sources"))),
            " ".join(str(item.get("source_surface") or "") for item in raw_item.get("evidence_items") or [] if isinstance(item, dict)),
        ]
        text = " ".join(str(part or "").lower() for part in text_parts)
        if any(hint in text for hint in SAMPLE_SOURCE_HINTS):
            return "fallback_placeholder" if "fallback" in text else "sample_demo"
        if any(hint in text for hint in OFFICIAL_SOURCE_HINTS):
            return "live_official"
        if any(hint in text for hint in PUBLIC_SOURCE_HINTS):
            return "live_public"
        if str(card.get("cache_status") or "").lower() == "live" or str(raw_item.get("opportunity_id") or "").startswith("live_"):
            return "generated_inference"
        return "generated_inference"

    @staticmethod
    def source_classes(card: Dict[str, Any], raw_item: Dict[str, Any], primary: str) -> List[str]:
        classes = {primary}
        for item in raw_item.get("evidence_items") or []:
            if not isinstance(item, dict):
                continue
            surface = str(item.get("source_surface") or "").lower()
            if surface in {"intelligence", "research"}:
                classes.add("live_public")
            elif surface == "radar":
                classes.add("generated_inference")
        if card.get("cache_status") == "live" and primary not in BLOCKED_SOURCE_CLASSES:
            classes.add("live_public")
        return sorted(classes)

    @staticmethod
    def source_counts(card: Dict[str, Any], raw_item: Dict[str, Any], source_class: str) -> tuple[int, int]:
        total = max(as_int(card.get("source_count")), len(raw_item.get("evidence_sources") or []), len(raw_item.get("evidence_items") or []))
        if source_class in BLOCKED_SOURCE_CLASSES:
            return 0, total
        if source_class == "generated_inference":
            return 0, 0
        return total, 0

    @staticmethod
    def opportunity_family(card: Dict[str, Any]) -> str:
        model_id = str(card.get("model_family") or "").strip()
        if model_id in OPPORTUNITY_FAMILY_BY_MODEL:
            return OPPORTUNITY_FAMILY_BY_MODEL[model_id]
        for discovery in card.get("model_discoveries") or []:
            if isinstance(discovery, dict) and discovery.get("model_id") in OPPORTUNITY_FAMILY_BY_MODEL:
                return OPPORTUNITY_FAMILY_BY_MODEL[str(discovery.get("model_id"))]
        return "industry_transmission"

    @staticmethod
    def evidence_checklist(card: Dict[str, Any], source_class: str) -> List[Dict[str, Any]]:
        live_count = as_int(card.get("live_source_count"))
        source_count = as_int(card.get("source_count"))
        local_asset = card.get("local_asset") if isinstance(card.get("local_asset"), dict) else {}
        mapped_events = card.get("mapped_events") if isinstance(card.get("mapped_events"), list) else []
        panel = card.get("evidence_panel") if isinstance(card.get("evidence_panel"), list) else []
        items = [
            ("independent_sources", "至少 2 个独立来源", source_count >= 2),
            ("live_source", "至少 1 个 live/official/public 来源", live_count >= 1),
            ("local_instrument", "存在本地可交易标的", bool(local_asset.get("code"))),
            ("market_facing_event", "存在可交易催化或明确传导路径", bool(mapped_events) or bool(card.get("bridge_asset"))),
            ("archive_or_link", "证据可下钻到原始链接或本地归档", bool(panel) or source_class not in BLOCKED_SOURCE_CLASSES),
        ]
        checklist = []
        for key, label, passed in items:
            if source_class in BLOCKED_SOURCE_CLASSES:
                status = "sample_only"
            elif not passed:
                status = "missing"
            else:
                status = "confirmed"
            checklist.append({"key": key, "label": label, "status": status})
        return checklist

    def evidence_panel(self, card: Dict[str, Any], raw_item: Dict[str, Any], source_class: str) -> List[Dict[str, Any]]:
        panels: List[Dict[str, Any]] = []
        for index, item in enumerate(raw_item.get("evidence_items") or []):
            if not isinstance(item, dict):
                continue
            source_url = str(item.get("source_url") or raw_item.get("source_url") or "").strip()
            archived = self.evidence_panels_by_url.get(source_url, [])
            if archived:
                for archived_item in archived:
                    panels.append({**archived_item, "data_source_class": archived_item.get("data_source_class") or source_class})
                continue
            panels.append(
                {
                    "panel_id": f"{card.get('id')}_evidence_{index}",
                    "title": item.get("title") or card.get("thesis"),
                    "latest_value": item.get("confidence") or card.get("decision_priority_score"),
                    "previous_value": None,
                    "delta": None,
                    "as_of": item.get("updated_at") or card.get("last_update"),
                    "source_name": item.get("source_surface") or "lead_lag_runtime",
                    "source_type": item.get("source_surface") or "generated",
                    "original_link": source_url,
                    "local_archive_path": None,
                    "archive_status": "missing",
                    "summary": item.get("title") or card.get("why_now"),
                    "quote_text": item.get("title") or "",
                    "citation_count": 0,
                    "data_source_class": source_class,
                }
            )
        if not panels:
            data_points = ((card.get("decision_chain") or {}).get("data") or []) if isinstance(card.get("decision_chain"), dict) else []
            for index, point in enumerate(data_points[:6]):
                if not isinstance(point, dict):
                    continue
                panels.append(
                    {
                        "panel_id": f"{card.get('id')}_metric_{index}",
                        "title": point.get("label"),
                        "latest_value": point.get("value"),
                        "previous_value": None,
                        "delta": None,
                        "as_of": card.get("last_update"),
                        "source_name": "lead_lag_scoring",
                        "source_type": "generated",
                        "original_link": raw_item.get("source_url") or "",
                        "local_archive_path": None,
                        "archive_status": "missing",
                        "summary": point.get("explain"),
                        "quote_text": point.get("explain") or "",
                        "citation_count": 0,
                        "data_source_class": source_class,
                    }
                )
        return panels

    @staticmethod
    def execution_blockers(card: Dict[str, Any], source_class: str) -> List[str]:
        blockers: List[str] = []
        if source_class in BLOCKED_SOURCE_CLASSES:
            blockers.append("样例/回退数据不能进入可执行队列")
        if source_class == "generated_inference" and as_int(card.get("live_source_count")) == 0:
            blockers.append("模型推断不能单独构成高优先级机会")
        if as_int(card.get("source_count")) < 2:
            blockers.append("缺少至少 2 个独立来源")
        if as_int(card.get("live_source_count")) < 1:
            blockers.append("缺少 live_official/live_public/company filing/exchange 来源")
        if not (card.get("local_asset") or {}).get("code"):
            blockers.append("缺少本地可交易标的")
        return list(dict.fromkeys(blockers))

    @staticmethod
    def evidence_summary(card: Dict[str, Any]) -> Dict[str, Any]:
        checklist = card.get("evidence_checklist") or []
        return {
            "confirmed": [item["label"] for item in checklist if item.get("status") == "confirmed"],
            "missing": [item["label"] for item in checklist if item.get("status") in {"missing", "stale"}],
            "sample_only": [item["label"] for item in checklist if item.get("status") == "sample_only"],
            "source_count": card.get("source_count"),
            "live_source_count": card.get("live_source_count"),
            "sample_source_count": card.get("sample_source_count"),
            "citation_count": card.get("citation_count"),
            "archived_link_count": card.get("archived_link_count"),
        }

    @staticmethod
    def data_summary(card: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for point in ((card.get("decision_chain") or {}).get("data") or [])[:8]:
            if not isinstance(point, dict):
                continue
            rows.append(
                {
                    "metric_name": point.get("label"),
                    "latest_value": point.get("value"),
                    "previous_value": None,
                    "delta": None,
                    "as_of": card.get("last_update"),
                    "source_name": "lead_lag_scoring",
                    "citation_count": card.get("citation_count", 0),
                    "archived_link_count": card.get("archived_link_count", 0),
                }
            )
        return rows

    def parent_thesis_cards(self, cards: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for card in cards:
            thesis_id = self.thesis_id(card)
            groups.setdefault(thesis_id, []).append(card)
        parents = []
        for thesis_id, group_cards in groups.items():
            group_cards.sort(key=lambda row: (-as_float(row.get("decision_priority_score")), str(row.get("id"))))
            best = group_cards[0]
            parents.append(self._parent_card(thesis_id, best, group_cards))
        parents.sort(key=lambda row: (-as_float(row.get("decision_priority_score")), row.get("thesis_id", "")))
        return parents[: max(1, min(as_int(limit, 12), 50))]

    @staticmethod
    def thesis_id(card: Dict[str, Any]) -> str:
        return f"thesis_{slug(card.get('sector'))}_{slug(card.get('thesis'))}"

    def _parent_card(self, thesis_id: str, best: Dict[str, Any], cards: List[Dict[str, Any]]) -> Dict[str, Any]:
        variants = self._child_variants(cards)
        missing = list(dict.fromkeys([item for card in cards for item in compact_strings(card.get("missing_confirmations"))]))
        invalidations = list(dict.fromkeys([item for card in cards for item in compact_strings(card.get("invalidation_rules"))]))
        source_count = sum(as_int(card.get("source_count")) for card in cards)
        live_count = sum(as_int(card.get("live_source_count")) for card in cards)
        sample_count = sum(as_int(card.get("sample_source_count")) for card in cards)
        return {
            "thesis_id": thesis_id,
            "thesis_title": best.get("thesis"),
            "sector": best.get("sector"),
            "sector_zh": best.get("sector_zh") or best.get("sector"),
            "family": best.get("opportunity_family"),
            "families": sorted({family for card in cards for family in card.get("opportunity_families", [])}),
            "why_now": best.get("why_now"),
            "result": (best.get("decision_chain") or {}).get("result") or best.get("generation_status_zh"),
            "reasoning": (best.get("decision_chain") or {}).get("thinking") or best.get("driver"),
            "strategy": (best.get("decision_chain") or {}).get("strategy"),
            "evidence_summary": best.get("evidence_summary"),
            "data_summary": best.get("data_summary"),
            "missing_confirmations": missing,
            "invalidation_rules": invalidations,
            "freshness_score": clamp(max(as_float(card.get("freshness_score")) for card in cards)),
            "tradability_score": clamp(max(as_float(card.get("tradability_score")) for card in cards)),
            "actionability_score": clamp(max(as_float(card.get("actionability_score")) for card in cards)),
            "evidence_completeness": clamp(max(as_float(card.get("evidence_completeness")) for card in cards)),
            "decision_priority_score": clamp(max(as_float(card.get("decision_priority_score")) for card in cards)),
            "source_count": source_count,
            "live_source_count": live_count,
            "sample_source_count": sample_count,
            "citation_count": sum(as_int(card.get("citation_count")) for card in cards),
            "archived_link_count": sum(as_int(card.get("archived_link_count")) for card in cards),
            "next_review_time": min((card.get("next_review_time") for card in cards if card.get("next_review_time")), default=best.get("next_review_time")),
            "current_stage": best.get("baton_stage"),
            "generation_status": best.get("generation_status"),
            "generation_status_zh": best.get("generation_status_zh"),
            "is_executable": any(bool(card.get("is_executable")) for card in cards),
            "execution_blockers": list(dict.fromkeys([item for card in cards for item in compact_strings(card.get("execution_blockers"))])),
            "child_variants": variants,
            "variant_count": len(variants),
            "source_card_ids": [card.get("id") for card in cards],
        }

    @staticmethod
    def _child_variants(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: Dict[str, Dict[str, Any]] = {}
        for card in cards:
            for asset in card.get("stock_pool") or []:
                if not isinstance(asset, dict):
                    continue
                ticker = asset.get("code")
                if not ticker:
                    continue
                key = f"{asset.get('market') or ''}:{ticker}"
                factor = asset.get("factor_snapshot") or {}
                existing = rows.get(key)
                candidate = {
                    "instrument_id": key,
                    "market": asset.get("market"),
                    "ticker": ticker,
                    "name": asset.get("name"),
                    "role": asset.get("role"),
                    "role_zh": asset.get("role_zh"),
                    "liquidity_score": card.get("tradability_score"),
                    "local_factor_snapshot": factor,
                    "crowding_score": (card.get("crowding_state") or {}).get("score"),
                    "recent_relative_strength": factor.get("technical") if isinstance(factor, dict) else None,
                    "current_status": card.get("generation_status"),
                    "variant_risk": card.get("risk"),
                    "variant_notes": asset.get("reason"),
                    "opportunity_id": card.get("id"),
                    "next_review_time": card.get("next_review_time"),
                }
                if existing is None or as_float(candidate.get("liquidity_score")) > as_float(existing.get("liquidity_score")):
                    rows[key] = candidate
        ordered = sorted(rows.values(), key=lambda row: (-as_float(row.get("liquidity_score")), str(row.get("ticker") or "")))
        return ordered

    @staticmethod
    def quality_lineage_summary(cards: List[Dict[str, Any]], events: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        events = events or []
        class_counts: Dict[str, int] = {}
        for card in cards:
            source_class = str(card.get("data_source_class") or "unknown")
            class_counts[source_class] = class_counts.get(source_class, 0) + 1
        blocked = [card for card in cards if card.get("execution_blockers")]
        no_archive = [card for card in cards if as_int(card.get("archived_link_count")) == 0]
        stale = [card for card in cards if card.get("stale_check_detected")]
        return {
            "card_count": len(cards),
            "source_class_counts": class_counts,
            "live_vs_sample": {
                "live": sum(1 for card in cards if card.get("data_source_class") in EXECUTABLE_SOURCE_CLASSES),
                "sample_or_fallback": sum(1 for card in cards if card.get("data_source_class") in BLOCKED_SOURCE_CLASSES),
                "generated_inference": sum(1 for card in cards if card.get("data_source_class") == "generated_inference"),
            },
            "blocked_executable_count": len(blocked),
            "cards_without_archive_count": len(no_archive),
            "stale_next_review_count": len(stale),
            "event_class_counts": {
                label: sum(1 for event in events if event.get("tradability_class") == label or event.get("event_class") == label)
                for label in ("market-facing", "research-facing", "archive-only")
            },
            "mapping_pollution_alerts": [
                {
                    "opportunity_id": card.get("id"),
                    "reason": "样例/回退数据或缺少 live 来源，禁止进入高优先级",
                }
                for card in blocked
                if card.get("data_source_class") in BLOCKED_SOURCE_CLASSES or as_int(card.get("live_source_count")) < 1
            ][:20],
        }

    def enhance_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(event)
        source_class = self.classify_event_source(payload)
        payload["data_source_class"] = source_class
        tradability_class = payload.get("event_class") or payload.get("tradability_class") or "research-facing"
        if source_class in BLOCKED_SOURCE_CLASSES and tradability_class == "market-facing":
            tradability_class = "research-facing"
        if as_float(payload.get("relevance_score")) < 35:
            tradability_class = "archive-only"
        payload["tradability_class"] = tradability_class
        payload["event_class"] = tradability_class
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        payload["source_type"] = payload.get("source_type") or payload.get("source_tier") or source.get("tier") or source_class
        payload["china_mapping_score"] = self._mapping_score(payload.get("asset_mapping") or payload.get("assets"))
        payload["sector_mapping_score"] = self._mapping_score(payload.get("sector_mapping"))
        payload["entity_mapping_score"] = self._mapping_score(payload.get("asset_mapping") or payload.get("entities"))
        payload["time_decay"] = clamp(100 - as_float(payload.get("days_since_event"), 0) * 8)
        payload["evidence_strength"] = clamp(payload.get("relevance_score") or payload.get("confidence") or 0)
        effective = parse_dt(payload.get("effective_time") or payload.get("event_time") or payload.get("last_seen_at"))
        if effective:
            payload["catalyst_window_start"] = effective.isoformat()
            payload["catalyst_window_end"] = (effective + timedelta(days=10)).isoformat()
        else:
            payload["catalyst_window_start"] = None
            payload["catalyst_window_end"] = None
        payload["expected_validation_days"] = 5 if tradability_class == "market-facing" else 20
        payload["archival_status"] = "linked" if payload.get("linked_documents") else "not_archived"
        payload.setdefault("linked_documents", [])
        payload.setdefault("linked_reports", [])
        payload.setdefault("linked_theses", [])
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        payload["source_drawer"] = {
            "title": payload.get("title"),
            "original_link": payload.get("primary_source_url") or payload.get("source_url") or source.get("url") or "",
            "local_archive_path": None,
            "summary": payload.get("summary_zh") or payload.get("summary") or payload.get("title"),
            "tradability_mapping": tradability_class,
            "why_classified": self._event_class_reason(payload),
        }
        return payload

    @staticmethod
    def classify_event_source(event: Dict[str, Any]) -> str:
        text = " ".join(
            str(part or "").lower()
            for part in [
                event.get("cache_status"),
                event.get("source_tier"),
                event.get("source_type"),
                event.get("verification_status"),
                event.get("primary_source_url"),
                event.get("source_url"),
                (event.get("source") or {}).get("tier") if isinstance(event.get("source"), dict) else "",
                (event.get("source") or {}).get("url") if isinstance(event.get("source"), dict) else "",
            ]
        )
        if any(hint in text for hint in SAMPLE_SOURCE_HINTS):
            return "fallback_placeholder" if "fallback" in text else "sample_demo"
        if any(hint in text for hint in OFFICIAL_SOURCE_HINTS):
            return "live_official"
        if text.strip():
            return "live_public"
        return "sample_demo"

    @staticmethod
    def _mapping_score(values: Any) -> float:
        if not isinstance(values, list) or not values:
            return 0.0
        scores = []
        for item in values:
            if isinstance(item, dict):
                scores.append(as_float(item.get("relevance_score") or item.get("score") or item.get("confidence"), 50.0))
        return clamp(max(scores) if scores else min(100, len(values) * 20))

    @staticmethod
    def _event_class_reason(event: Dict[str, Any]) -> str:
        event_class = event.get("tradability_class") or event.get("event_class")
        if event.get("data_source_class") in BLOCKED_SOURCE_CLASSES:
            return "样例/回退来源只能作为研究背景，不进入可交易催化。"
        if event_class == "market-facing":
            return "事件具备可交易窗口、映射资产和足够相关性。"
        if event_class == "archive-only":
            return "事件相关性不足或缺少交易映射，仅归档。"
        return "事件可用于研究背景，但还缺少本地交易确认。"
