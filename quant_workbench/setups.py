"""Setup rules for the Quant Workbench first-phase strategy engine."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List


SETUP_ORDER = [
    "quality_value_recovery",
    "earnings_revision_breakout",
    "risk_on_pullback_leader",
    "sector_catalyst_confirmation",
]

SETUP_LABELS = {
    "quality_value_recovery": "质量估值修复",
    "earnings_revision_breakout": "预期上修突破",
    "risk_on_pullback_leader": "风险偏好回踩龙头",
    "sector_catalyst_confirmation": "行业催化确认",
}


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_pct(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):.{digits}f}%"
    except (TypeError, ValueError):
        return "-"


def _risk_level(risk: float) -> str:
    if risk <= 35:
        return "low"
    if risk <= 55:
        return "medium"
    return "high"


def _derive_action(score: float, risk: float, regime_label: str) -> tuple[str, str]:
    if regime_label == "risk_off" and score < 76:
        return ("watch", "1%-2%") if score >= 60 and risk <= 48 else ("avoid", "0%")
    if score >= 74 and risk <= 42:
        return "buy", "3%-6%"
    if score >= 62 and risk <= 58:
        return "watch", "1%-3%"
    return "avoid", "0%"


def _review_at(event_summary: str, fallback_days: int) -> str:
    if event_summary and len(event_summary) >= 10 and event_summary[:4].isdigit():
        return event_summary[:10]
    return (datetime.now() + timedelta(days=fallback_days)).strftime("%Y-%m-%d")


def _candidate_sort_key(candidate: Dict[str, Any]) -> tuple[int, float]:
    action_rank = {"buy": 2, "watch": 1, "avoid": 0}
    return action_rank.get(candidate.get("action"), 0), float(candidate.get("score", 0.0))


def _backtest_summary(backtest_stats: List[Dict[str, Any]]) -> str:
    if not backtest_stats:
        return "回测样本待生成"
    base = backtest_stats[0]
    win_rate = base.get("win_rate")
    sample_size = base.get("sample_size")
    avg_return = base.get("avg_return")
    try:
        return f"{base['hold_days']}日胜率 {float(win_rate):.1f}% / 平均收益 {float(avg_return):.1f}% / 样本 {int(sample_size)}"
    except (KeyError, TypeError, ValueError):
        return "回测样本待生成"


def _build_candidate(
    setup_name: str,
    score: float,
    risk: float,
    regime_label: str,
    factors_list: List[str],
    invalid_conditions: List[str],
    event_summary: str,
    backtest_stats: List[Dict[str, Any]],
    review_days: int,
) -> Dict[str, Any]:
    clamped = round(_clamp(score), 1)
    action, position_range = _derive_action(clamped, risk, regime_label)
    clean_invalid_conditions = [item for item in invalid_conditions if item]
    return {
        "setup_name": setup_name,
        "setup_label": SETUP_LABELS[setup_name],
        "score": clamped,
        "action": action,
        "position_range": position_range,
        "risk_level": _risk_level(risk),
        "factors": factors_list[:4],
        "invalid_conditions": clean_invalid_conditions[:4] or ["无明显失效条件"],
        "review_at": _review_at(event_summary, review_days),
        "event_summary": event_summary or "事件日历未接入，按周复核",
        "backtest_stats": backtest_stats,
        "backtest_summary": _backtest_summary(backtest_stats),
    }


def build_setup_candidates(
    factors: Dict[str, float],
    model_profiles: Dict[str, Dict[str, Any]],
    trend: Dict[str, Any],
    structure: Dict[str, Any],
    sentiment: Dict[str, Any],
    latest: Dict[str, Any],
    fundamentals: Dict[str, Any],
    regime: Dict[str, Any],
    feature_context: Dict[str, Any],
    stats_by_setup: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    valuation = feature_context.get("valuation", {})
    estimate = feature_context.get("estimate", {})
    flow_ctx = feature_context.get("flow", {})
    sector_summary = feature_context.get("sector_summary", "行业先行指标未接入")
    event_summary = feature_context.get("event_summary", "事件日历未接入，按周复核")
    risk = _safe_float(factors.get("risk"))
    regime_label = regime.get("label", "neutral")

    quality_score = _safe_float(factors.get("quality"))
    growth_score = _safe_float(factors.get("growth"))
    valuation_score = _safe_float(factors.get("valuation"))
    flow_score = _safe_float(factors.get("flow"))
    technical_score = _safe_float(factors.get("technical"))

    coverage = sentiment.get("coverage", 0)
    ret_5 = _safe_float(latest.get("ret_5"))
    ret_20 = _safe_float(latest.get("ret_20"))
    roe = fundamentals.get("roe")
    pe_ttm = fundamentals.get("pe_ttm")
    pb = fundamentals.get("pb")
    pe_pctile = _safe_float(valuation.get("pe_percentile_5y"))
    pb_pctile = _safe_float(valuation.get("pb_percentile_5y"))
    target_upside = _safe_float(estimate.get("target_upside"))
    eps_rev_30d = _safe_float(estimate.get("eps_rev_30d"))
    northbound_net = _safe_float(flow_ctx.get("northbound_net"))
    main_inflow = _safe_float(flow_ctx.get("main_inflow"))

    candidates = [
        _build_candidate(
            "quality_value_recovery",
            score=(
                0.34 * quality_score
                + 0.28 * valuation_score
                + 0.20 * technical_score
                + 0.08 * flow_score
                + 0.10 * max(0.0, 60.0 - risk)
                + (6.0 if 0 < pb_pctile <= 35 else 0.0)
                + (4.0 if 0 < pe_pctile <= 35 else 0.0)
                - (5.0 if pb not in (None, "") and _safe_float(pb) > 5 else 0.0)
            ),
            risk=risk,
            regime_label=regime_label,
            factors_list=[
                f"估值分位 PE/PB: {valuation.get('pe_percentile_5y', '-')}/{valuation.get('pb_percentile_5y', '-')}",
                f"ROE {roe if roe is not None else '-'} / PB {pb if pb is not None else '-'} / PE {pe_ttm if pe_ttm is not None else '-'}",
                f"技术结构 {structure.get('label', '-')} / 趋势分 {trend.get('score', 0)}",
                "优先等待估值压缩后再由量价确认修复",
            ],
            invalid_conditions=[
                "跌回 20 日线下方且量能未放大",
                "估值分位重新抬升但业绩质量未跟随",
                event_summary if event_summary != "事件日历未接入，按周复核" else "",
            ],
            event_summary=event_summary,
            backtest_stats=stats_by_setup.get("quality_value_recovery", []),
            review_days=7,
        ),
        _build_candidate(
            "earnings_revision_breakout",
            score=(
                0.32 * growth_score
                + 0.25 * technical_score
                + 0.18 * flow_score
                + 0.15 * quality_score
                + 0.10 * max(0.0, 60.0 - risk)
                + (6.0 if coverage >= 3 else 0.0)
                + min(max(target_upside, 0.0), 15.0) * 0.4
                + min(max(eps_rev_30d, 0.0), 15.0) * 0.3
            ),
            risk=risk,
            regime_label=regime_label,
            factors_list=[
                f"近 20 日动量 {_safe_pct(ret_20)} / 近 5 日 {_safe_pct(ret_5)}",
                f"研报覆盖 {coverage} 篇 / 目标涨幅 {_safe_pct(target_upside)}",
                f"近 30 日 EPS 上修 {_safe_pct(eps_rev_30d)} / 结构 {structure.get('label', '-')}",
                "适合业绩催化与预期差共振时追踪",
            ],
            invalid_conditions=[
                "放量突破后 3 个交易日内失守 20 日线",
                "预期差数据缺失或研报覆盖迅速转弱",
                "事件催化兑现后量价未继续确认",
            ],
            event_summary=event_summary,
            backtest_stats=stats_by_setup.get("earnings_revision_breakout", []),
            review_days=3,
        ),
        _build_candidate(
            "risk_on_pullback_leader",
            score=(
                0.30 * technical_score
                + 0.25 * flow_score
                + 0.20 * growth_score
                + 0.10 * quality_score
                + 0.15 * max(0.0, 60.0 - risk)
                + (8.0 if regime_label == "risk_on" else 2.0 if regime_label == "neutral" else -10.0)
                + (4.0 if -3.0 <= ret_5 <= 4.0 else 0.0)
            ),
            risk=risk,
            regime_label=regime_label,
            factors_list=[
                f"市场环境 {regime_label} / 风险偏好分 {regime.get('score', 0)}",
                f"北向/主力流入 {northbound_net if northbound_net else '-'} / {main_inflow if main_inflow else '-'}",
                f"技术形态 {structure.get('label', '-')} / 趋势分 {trend.get('score', 0)}",
                "更适合市场转暖阶段的龙头回踩再启动",
            ],
            invalid_conditions=[
                "市场风险偏好重新转为 risk_off",
                "跌回中枢下沿或 20 日线下方",
                "成交量持续低于 20 日均量",
            ],
            event_summary=event_summary,
            backtest_stats=stats_by_setup.get("risk_on_pullback_leader", []),
            review_days=5,
        ),
        _build_candidate(
            "sector_catalyst_confirmation",
            score=(
                0.27 * growth_score
                + 0.23 * flow_score
                + 0.20 * technical_score
                + 0.15 * quality_score
                + 0.15 * max(0.0, 60.0 - risk)
                + (8.0 if sector_summary != "行业先行指标未接入" else -6.0)
                + (3.0 if event_summary != "事件日历未接入，按周复核" else 0.0)
            ),
            risk=risk,
            regime_label=regime_label,
            factors_list=[
                sector_summary,
                f"事件提醒: {event_summary}",
                f"趋势/资金确认 {structure.get('label', '-')} / Flow {flow_score:.1f}",
                "适合行业景气或单点催化已有二次确认时执行",
            ],
            invalid_conditions=[
                "行业催化被证伪或兑现后缺少量价承接",
                "事件窗口前后出现异常波动",
                "行业先行指标长时间未更新",
            ],
            event_summary=event_summary,
            backtest_stats=stats_by_setup.get("sector_catalyst_confirmation", []),
            review_days=4,
        ),
    ]

    candidates.sort(key=_candidate_sort_key, reverse=True)
    return candidates


def summarize_strategy(
    setup_candidates: List[Dict[str, Any]],
    model_profiles: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    primary = setup_candidates[0]
    preferred_model = max(
        model_profiles.items(),
        key=lambda item: float(item[1].get("total", 0.0)),
    )[0]

    return {
        "setup_name": primary["setup_name"],
        "setup_label": primary["setup_label"],
        "setup_score": primary["score"],
        "action": primary["action"],
        "position_range": primary["position_range"],
        "risk_level": primary["risk_level"],
        "factors": primary["factors"],
        "thesis": primary["factors"][:3],
        "invalid_conditions": primary["invalid_conditions"],
        "review_at": primary["review_at"],
        "event_summary": primary["event_summary"],
        "backtest_stats": primary["backtest_stats"],
        "backtest_summary": primary["backtest_summary"],
        "setup_candidates": setup_candidates,
        "preferred_model": preferred_model,
        "models": model_profiles,
    }
