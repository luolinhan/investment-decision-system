"""Fixed-time Lead-Lag V2 brief generation.

This module intentionally stays outside the LeadLagService facade. It composes
existing V2 payloads into scheduled research-operation briefs without changing
API routes, UI rendering, or production scoring behavior.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.services.lead_lag_service import LeadLagService


BRIEF_SLOTS = (
    "overnight_digest",
    "pre_open_playbook",
    "morning_review",
    "close_review",
    "us_watch_mapping",
)


@dataclass(frozen=True)
class BriefSlotConfig:
    slot: str
    time_label: str
    title: str
    filename_stem: str
    emphasis: str


SLOT_CONFIGS: Dict[str, BriefSlotConfig] = {
    "overnight_digest": BriefSlotConfig(
        slot="overnight_digest",
        time_label="06:00",
        title="Overnight Digest",
        filename_stem="0600-overnight-digest",
        emphasis="隔夜全球领导资产、外部风险、港股桥接和新增事件。",
    ),
    "pre_open_playbook": BriefSlotConfig(
        slot="pre_open_playbook",
        time_label="08:20",
        title="Pre-Open Playbook",
        filename_stem="0820-pre-open-playbook",
        emphasis="开盘前 Top 3、第一/第二棒、风险预算和失效条件。",
    ),
    "morning_review": BriefSlotConfig(
        slot="morning_review",
        time_label="11:40",
        title="Morning Review",
        filename_stem="1140-morning-review",
        emphasis="上午验证、失败开盘、拥挤变化和午后检查。",
    ),
    "close_review": BriefSlotConfig(
        slot="close_review",
        time_label="15:15",
        title="Close Review",
        filename_stem="1515-close-review",
        emphasis="收盘验证、升级/降级、失败模式和明日观察。",
    ),
    "us_watch_mapping": BriefSlotConfig(
        slot="us_watch_mapping",
        time_label="21:30",
        title="US Watch Mapping",
        filename_stem="2130-us-watch-mapping",
        emphasis="美股领导资产、跨市场映射和次日预触发验证。",
    ),
}


def parse_as_of(value: Optional[str | datetime] = None) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if value:
        text = str(value).strip()
        if text:
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None, microsecond=0)
            except ValueError:
                pass
    return datetime.now().replace(microsecond=0)


def default_obsidian_output_dir(as_of: Optional[str | datetime] = None, vault: Optional[str | Path] = None) -> Path:
    configured_vault = vault or os.getenv("INVESTMENT_OBSIDIAN_VAULT") or os.getenv("OBSIDIAN_VAULT_PATH")
    if not configured_vault and os.name == "nt":
        admin_vault = Path(os.getenv("SystemDrive", "C:")) / "Users" / "Administrator" / "Documents" / "Obsidian" / "知识库"
        if admin_vault.exists():
            configured_vault = admin_vault
    base = Path(configured_vault) if configured_vault else Path.home() / "Documents" / "Obsidian" / "知识库"
    return base / "40-任务" / "Lead-Lag Ops" / parse_as_of(as_of).date().isoformat()


def brief_filename(slot: str, suffix: str) -> str:
    config = _slot_config(slot)
    return f"{config.filename_stem}.{suffix.lstrip('.')}"


def _slot_config(slot: str) -> BriefSlotConfig:
    try:
        return SLOT_CONFIGS[slot]
    except KeyError as exc:
        raise ValueError(f"Unsupported Lead-Lag brief slot: {slot}") from exc


def _compact_strings(values: Iterable[Any], limit: int = 6) -> List[str]:
    rows: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
        if len(rows) >= limit:
            break
    return rows


def _event_path_text(event: Dict[str, Any]) -> str:
    paths = event.get("expected_path") or []
    if isinstance(paths, list) and paths:
        first = paths[0] if isinstance(paths[0], dict) else {}
        source = first.get("from") or "leader"
        target = first.get("to") or "local_mapping"
        relation = first.get("relation") or "maps_to"
        lag = first.get("expected_lag_days") or {}
        if isinstance(lag, dict) and ("min" in lag or "max" in lag):
            return f"{source} -> {target} ({relation}, {lag.get('min', 0)}-{lag.get('max', 0)}d)"
        return f"{source} -> {target} ({relation})"
    return str(event.get("base_case") or "等待本地映射和价格验证")


def _asset_label(asset: Dict[str, Any]) -> str:
    code = str(asset.get("code") or "").strip()
    name = str(asset.get("name") or "").strip()
    market = str(asset.get("market") or "").strip()
    label = " ".join(part for part in (name, code) if part)
    return f"{label} ({market})" if market and label else label


def _opportunity_line(card: Dict[str, Any]) -> str:
    thesis = card.get("thesis") or card.get("id")
    stage = card.get("baton_stage") or "unknown"
    score = card.get("decision_priority_score")
    return f"{thesis} | {stage} | priority={score}"


def _macro_context(liquidity: Dict[str, Any]) -> Dict[str, str]:
    markets = [item for item in liquidity.get("markets", []) if isinstance(item, dict)]
    hk_rows = [item for item in markets if "hk" in str(item.get("name", "")).lower()]
    external_rows = [item for item in markets if "external" in str(item.get("name", "")).lower()]
    source_health = liquidity.get("live_source_health") or {}
    radar_health = source_health.get("radar") if isinstance(source_health, dict) else {}
    radar_status = radar_health.get("status") if isinstance(radar_health, dict) else None
    return {
        "macro_regime": str(radar_health.get("data_coverage") or radar_status or "sample_fallback"),
        "external_risk": str((external_rows[0].get("summary") if external_rows else "") or "等待外部风险快照确认"),
        "hk_liquidity": str((hk_rows[0].get("summary") if hk_rows else "") or "等待港股流动性快照确认"),
    }


def _macro_context_from_bridge(bridge: Dict[str, Any]) -> Dict[str, str]:
    if not bridge:
        return {}
    macro = bridge.get("macro_regime") or {}
    external = bridge.get("external_risk") or {}
    hk = bridge.get("hk_liquidity") or {}
    return {
        "macro_regime": str(macro.get("label") or bridge.get("bridge_state") or "unknown"),
        "external_risk": str(external.get("label") or "unknown"),
        "hk_liquidity": str(hk.get("label") or "unknown"),
        "bridge_state": str(bridge.get("bridge_state") or "unknown"),
        "bridge_score": str(bridge.get("bridge_score") or ""),
        "cache_status": str(bridge.get("cache_status") or ""),
    }


class LeadLagBriefGenerator:
    def __init__(self, service: Optional[LeadLagService] = None):
        self.service = service or LeadLagService()

    def generate(self, slot: str, as_of: Optional[str | datetime] = None) -> Dict[str, Any]:
        config = _slot_config(slot)
        as_of_dt = parse_as_of(as_of)
        decision = self.service.decision_center(limit=5)
        queue = self.service.opportunity_queue(limit=12)
        frontline = self.service.event_frontline(limit=12, include_research_facing=(slot == "us_watch_mapping"))
        avoid = self.service.avoid_board(limit=8)
        changed = self.service.what_changed(limit=8)
        liquidity = self.service.liquidity()
        bridge = self.service.macro_bridge() if hasattr(self.service, "macro_bridge") else {}

        cards = [card for card in queue.get("cards", []) if isinstance(card, dict)]
        events = [event for event in frontline.get("events", []) if isinstance(event, dict)]
        avoid_items = [item for item in avoid.get("items", []) if isinstance(item, dict)]
        macro_context = _macro_context_from_bridge(bridge) or _macro_context(liquidity)

        return {
            "slot": config.slot,
            "as_of": as_of_dt.isoformat(),
            "headline": self._headline(config, decision, changed),
            "today_focus": self._today_focus(config.slot, decision, changed, cards, events),
            "new_catalysts": self._new_catalysts(events),
            "invalidation_alerts": self._invalidation_alerts(cards, avoid_items),
            "next_checkpoints": self._next_checkpoints(config.slot, as_of_dt, decision, cards),
            "top_opportunities": self._top_opportunities(cards),
            "do_not_chase": self._do_not_chase(decision, avoid_items),
            "macro_external_hk_context": macro_context,
            "source_summary": self._source_summary(decision, queue, frontline, avoid, changed),
        }

    @staticmethod
    def _headline(config: BriefSlotConfig, decision: Dict[str, Any], changed: Dict[str, Any]) -> str:
        main = decision.get("main_conclusion") or ""
        changes = changed.get("new_signals") or []
        if config.slot == "overnight_digest" and changes:
            return f"{config.title}: {changes[0]}"
        if main:
            return f"{config.title}: {main}"
        return f"{config.title}: {config.emphasis}"

    @staticmethod
    def _today_focus(
        slot: str,
        decision: Dict[str, Any],
        changed: Dict[str, Any],
        cards: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
    ) -> List[str]:
        top_directions = decision.get("top_directions") or []
        if slot == "overnight_digest":
            return _compact_strings(
                list(changed.get("new_signals") or [])
                + [event.get("title") for event in events[:3]]
                + [item.get("thesis") for item in top_directions[:2]],
                limit=5,
            )
        if slot == "pre_open_playbook":
            return _compact_strings(
                [f"Top {item.get('rank')}: {item.get('thesis')} - {item.get('reason')}" for item in top_directions[:3]],
                limit=5,
            )
        if slot == "morning_review":
            return _compact_strings(
                [f"验证: {item.get('opportunity_id')} - {item.get('reason')}" for item in changed.get("upgraded_opportunities", [])]
                + [f"未确认: {item.get('opportunity_id')} - {item.get('reason')}" for item in changed.get("downgraded_or_invalidated", [])],
                limit=5,
            )
        if slot == "close_review":
            return _compact_strings(
                [_opportunity_line(card) for card in cards[:4]]
                + [f"拥挤: {item.get('thesis')} - {item.get('reason')}" for item in changed.get("crowding_up", [])],
                limit=5,
            )
        return _compact_strings(
            [
                f"US leader: {_asset_label(card.get('leader_asset') or {})} -> {_asset_label(card.get('local_asset') or {})}"
                for card in cards
                if (card.get("leader_asset") or {}).get("market") == "US"
            ]
            + list(changed.get("macro_external_policy_changes") or []),
            limit=5,
        )

    @staticmethod
    def _new_catalysts(events: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        return [
            {
                "event_id": str(event.get("event_id") or ""),
                "title": str(event.get("title") or ""),
                "expected_path": _event_path_text(event),
            }
            for event in events[:6]
            if event.get("title")
        ]

    @staticmethod
    def _invalidation_alerts(cards: List[Dict[str, Any]], avoid_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        alerts: List[Dict[str, str]] = []
        for card in cards:
            rules = card.get("invalidation_rules") or []
            if rules:
                status = "triggered" if card.get("generation_status") == "invalidated" else "watch"
                if card.get("generation_status") == "insufficient_evidence":
                    status = "near_trigger"
                alerts.append(
                    {
                        "opportunity_id": str(card.get("id") or ""),
                        "rule": str(rules[0]),
                        "status": status,
                    }
                )
        for item in avoid_items:
            alerts.append(
                {
                    "opportunity_id": str(item.get("id") or ""),
                    "rule": str(item.get("reason") or ""),
                    "status": "near_trigger",
                }
            )
        return alerts[:8]

    @staticmethod
    def _next_checkpoints(
        slot: str,
        as_of: datetime,
        decision: Dict[str, Any],
        cards: List[Dict[str, Any]],
    ) -> List[Dict[str, str]]:
        slot_times = {
            "overnight_digest": [("08:20", "开盘前确认 Top 3、第一棒和风险预算")],
            "pre_open_playbook": [("09:45", "开盘 15 分钟验证量价和跟随资产"), ("11:40", "上午复盘确认是否继续持有假设")],
            "morning_review": [("13:30", "午后检查失败开盘是否修复"), ("15:15", "收盘复盘升级/降级")],
            "close_review": [("21:30", "美股映射和外部风险检查"), ("08:20", "次日开盘前重排机会队列")],
            "us_watch_mapping": [("06:00", "隔夜简报确认美股领导资产传导"), ("08:20", "本地映射开盘前验证")],
        }
        checkpoints = [
            {"time": as_of.replace(hour=int(time[:2]), minute=int(time[3:]), second=0).isoformat(), "item": item}
            for time, item in slot_times[slot]
        ]
        if decision.get("next_check_time"):
            checkpoints.append({"time": str(decision["next_check_time"]), "item": "Decision Center next check"})
        for card in cards[:3]:
            for review_time in card.get("expected_review_times") or []:
                checkpoints.append({"time": str(review_time), "item": f"{card.get('id')} validation"})
                break
        return checkpoints[:6]

    @staticmethod
    def _top_opportunities(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "opportunity_id": str(card.get("id") or ""),
                "thesis": str(card.get("thesis") or ""),
                "actionability_score": card.get("actionability_score", 0),
                "tradability_score": card.get("tradability_score", 0),
            }
            for card in cards[:5]
        ]

    @staticmethod
    def _do_not_chase(decision: Dict[str, Any], avoid_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        rows = [
            {"thesis": str(item.get("thesis") or ""), "reason": str(item.get("reason") or item.get("reason_type") or "")}
            for item in avoid_items
            if item.get("thesis")
        ]
        rows.extend(
            {"thesis": str(text), "reason": "Decision Center do_not_do_today"}
            for text in decision.get("do_not_do_today", [])
            if text
        )
        deduped: List[Dict[str, str]] = []
        seen = set()
        for row in rows:
            marker = row["thesis"]
            if marker in seen:
                continue
            seen.add(marker)
            deduped.append(row)
        return deduped[:6]

    @staticmethod
    def _source_summary(*payloads: Dict[str, Any]) -> Dict[str, Any]:
        source_count = 0
        cache_statuses = []
        freshness = []
        for payload in payloads:
            source_count += int(payload.get("source_count") or payload.get("count") or 0)
            if payload.get("cache_status"):
                cache_statuses.append(str(payload["cache_status"]))
            if payload.get("as_of"):
                freshness.append(str(payload["as_of"]))
        cache_status = "live" if "live" in cache_statuses else "sample_fallback"
        if not cache_statuses and source_count:
            cache_status = "cached"
        return {
            "source_count": source_count,
            "freshness": max(freshness) if freshness else datetime.now().replace(microsecond=0).isoformat(),
            "cache_status": cache_status,
        }


def render_brief_markdown(payload: Dict[str, Any]) -> str:
    config = _slot_config(str(payload.get("slot")))

    def bullet(items: Iterable[Any]) -> str:
        rows = [f"- {item}" for item in items if item]
        return "\n".join(rows) if rows else "- none"

    lines = [
        f"# Lead-Lag Brief: {config.title}",
        "",
        f"- slot: `{payload.get('slot')}`",
        f"- as_of: {payload.get('as_of')}",
        f"- headline: {payload.get('headline')}",
        "",
        "## Today Focus",
        "",
        bullet(payload.get("today_focus") or []),
        "",
        "## New Catalysts",
        "",
        bullet(
            f"{item.get('event_id')}: {item.get('title')} | path={item.get('expected_path')}"
            for item in payload.get("new_catalysts", [])
        ),
        "",
        "## Invalidation Alerts",
        "",
        bullet(
            f"{item.get('opportunity_id')} | {item.get('status')} | {item.get('rule')}"
            for item in payload.get("invalidation_alerts", [])
        ),
        "",
        "## Next Checkpoints",
        "",
        bullet(f"{item.get('time')} | {item.get('item')}" for item in payload.get("next_checkpoints", [])),
        "",
        "## Top Opportunities",
        "",
        bullet(
            f"{item.get('opportunity_id')} | actionability={item.get('actionability_score')} | tradability={item.get('tradability_score')} | {item.get('thesis')}"
            for item in payload.get("top_opportunities", [])
        ),
        "",
        "## Do Not Chase",
        "",
        bullet(f"{item.get('thesis')} | {item.get('reason')}" for item in payload.get("do_not_chase", [])),
        "",
        "## Macro / External / HK Context",
        "",
    ]
    context = payload.get("macro_external_hk_context") or {}
    lines.extend(
        [
            f"- macro_regime: {context.get('macro_regime')}",
            f"- external_risk: {context.get('external_risk')}",
            f"- hk_liquidity: {context.get('hk_liquidity')}",
            "",
            "## Source Summary",
            "",
        ]
    )
    source = payload.get("source_summary") or {}
    lines.extend(
        [
            f"- source_count: {source.get('source_count')}",
            f"- freshness: {source.get('freshness')}",
            f"- cache_status: {source.get('cache_status')}",
            "",
        ]
    )
    return "\n".join(lines)
