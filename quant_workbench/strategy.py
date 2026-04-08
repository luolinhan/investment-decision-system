"""Expert strategy helpers for the Quant Workbench."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict

from quant_workbench.config import INVESTMENT_DB_PATH
from quant_workbench.db_views import QuantWorkbenchDBViews, ensure_strategy_tables
from quant_workbench.setups import build_setup_candidates, summarize_strategy

MODELS = {
    "conservative": {
        "quality": 0.25,
        "growth": 0.20,
        "valuation": 0.15,
        "flow": 0.15,
        "technical": 0.20,
        "risk": 0.15,
    },
    "aggressive": {
        "quality": 0.15,
        "growth": 0.25,
        "valuation": 0.10,
        "flow": 0.20,
        "technical": 0.25,
        "risk": 0.15,
    },
}

_DB_VIEWS = QuantWorkbenchDBViews()


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def compute_six_factors(
    latest: Dict[str, Any],
    trend: Dict[str, Any],
    structure: Dict[str, Any],
    fundamentals: Dict[str, Any],
    sentiment: Dict[str, Any],
    regime: Dict[str, Any],
    feature_context: Dict[str, Any] | None = None,
) -> Dict[str, float]:
    feature_context = feature_context or {}
    valuation_ctx = feature_context.get("valuation", {})
    technical_ctx = feature_context.get("technical", {})
    flow_ctx = feature_context.get("flow", {})
    estimate_ctx = feature_context.get("estimate", {})
    market_flow_ctx = feature_context.get("market_flow", {})
    events = feature_context.get("events", [])
    sector = feature_context.get("sector", {})

    roe = _safe_float(fundamentals.get("roe"))
    pb = _safe_float(fundamentals.get("pb"))
    pe = _safe_float(fundamentals.get("pe_ttm"))
    gross_margin = _safe_float(fundamentals.get("gross_margin"))
    revenue_yoy = _safe_float(fundamentals.get("revenue_yoy"))
    ocf = _safe_float(fundamentals.get("operating_cash_flow"))
    fcf = _safe_float(fundamentals.get("free_cash_flow"))
    vol_ratio = _safe_float(latest.get("vol_ratio"))
    ret_5 = _safe_float(latest.get("ret_5"))
    ret_20 = _safe_float(latest.get("ret_20"))
    trend_score = _safe_float(trend.get("score"))
    struct_conf = _safe_float(structure.get("confidence")) * 100
    structure_score = _safe_float(structure.get("score"))
    pe_pctile = _safe_float(valuation_ctx.get("pe_percentile_5y"))
    pb_pctile = _safe_float(valuation_ctx.get("pb_percentile_5y"))
    dy_pctile = _safe_float(valuation_ctx.get("dy_percentile_3y"))
    eps_rev_30d = _safe_float(estimate_ctx.get("eps_rev_30d"))
    eps_rev_90d = _safe_float(estimate_ctx.get("eps_rev_90d"))
    target_upside = _safe_float(estimate_ctx.get("target_upside"))
    turnover_rank = _safe_float(flow_ctx.get("turnover_rank"))
    northbound_net = _safe_float(flow_ctx.get("northbound_net"))
    main_inflow = _safe_float(flow_ctx.get("main_inflow"))
    market_inflow = _safe_float(market_flow_ctx.get("total_net_inflow"))
    rsi_14 = _safe_float(technical_ctx.get("rsi_14"))
    atr_pct = _safe_float(technical_ctx.get("atr_pct")) or _safe_float(latest.get("atr_pct_14"))
    volatility_30d = _safe_float(technical_ctx.get("volatility_30d"))

    sector_revenue_yoy = _safe_float(sector.get("revenue_yoy"))
    sector_member_growth = _safe_float(sector.get("member_growth_yoy"))
    sector_bonus = 0.0
    if sector_revenue_yoy > 0:
        sector_bonus += min(sector_revenue_yoy, 20.0) * 0.4
    if sector_member_growth > 0:
        sector_bonus += min(sector_member_growth, 20.0) * 0.3

    quality = _clamp(
        24
        + roe * 2.5
        + max(0.0, 5.0 - pb) * 6
        + max(0.0, gross_margin - 20.0) * 0.6
        + (4.0 if ocf > 0 else 0.0)
        + (4.0 if fcf > 0 else 0.0)
        + sector_bonus
    )
    growth = _clamp(
        18
        + trend_score * 1.2
        + structure_score * 1.0
        + struct_conf * 0.08
        + max(0.0, ret_5)
        + max(0.0, ret_20) * 0.4
        + max(0.0, revenue_yoy) * 0.3
        + max(0.0, eps_rev_30d) * 0.6
        + max(0.0, eps_rev_90d) * 0.4
        + max(0.0, target_upside) * 0.25
    )
    percentile_score = 0.0
    if pe_pctile > 0:
        percentile_score += max(0.0, 60.0 - pe_pctile) * 0.6
    if pb_pctile > 0:
        percentile_score += max(0.0, 60.0 - pb_pctile) * 0.5
    if dy_pctile > 0:
        percentile_score += max(0.0, 60.0 - dy_pctile) * 0.2
    valuation = _clamp(
        18
        + max(0.0, 45.0 - min(pe, 80.0))
        + max(0.0, 20.0 - min(pb, 20.0))
        + percentile_score
    )
    coverage = sentiment.get("coverage", 0)
    flow = _clamp(
        16
        + min(max((vol_ratio - 1.0) * 35.0, -25.0), 30.0)
        + min(max(coverage, 0), 12) * 2.5
        + (6.0 if northbound_net > 0 else -4.0 if northbound_net < 0 else 0.0)
        + (6.0 if main_inflow > 0 else -4.0 if main_inflow < 0 else 0.0)
        + (4.0 if market_inflow > 0 else -3.0 if market_inflow < 0 else 0.0)
        + min(max(turnover_rank, 0.0), 100.0) * 0.08
    )
    technical = _clamp(
        22
        + trend_score * 1.4
        + structure_score * 2.0
        + (5.0 if 45.0 <= rsi_14 <= 68.0 else 0.0)
        - max(0.0, atr_pct - 6.0) * 2.0
    )

    risk_flags = len(trend.get("risk_flags", [])) + len(structure.get("risk_flags", []))
    event_importance = max((_safe_float(item.get("importance")) for item in events), default=0.0)
    regime_penalty = 10 if regime.get("label") == "risk_off" else -5 if regime.get("label") == "risk_on" else 0
    risk = _clamp(
        18
        + risk_flags * 8
        + max(0.0, 50.0 - structure_score * 10.0)
        + regime_penalty
        + (3.0 if coverage < 3 else 0.0)
        + max(0.0, volatility_30d - 30.0) * 0.8
        + max(0.0, atr_pct - 5.0) * 2.0
        + event_importance * 7.0
    )

    return {
        "quality": round(quality, 1),
        "growth": round(growth, 1),
        "valuation": round(valuation, 1),
        "flow": round(flow, 1),
        "technical": round(technical, 1),
        "risk": round(risk, 1),
    }


def _derive_action(total: float, risk: float) -> tuple[str, str]:
    if total >= 70 and risk <= 45:
        return "buy", "3%-6%"
    if total >= 60:
        return "watch", "1%-3%"
    return "avoid", "0%"


def build_strategy_profiles(factors: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    profiles: Dict[str, Dict[str, Any]] = {}
    for name, weights in MODELS.items():
        total = 0.0
        for key, weight in weights.items():
            if key == "risk":
                continue
            total += weight * factors.get(key, 0.0)
        total -= weights.get("risk", 0.0) * factors.get("risk", 0.0)
        total = _clamp(total)
        action, position = _derive_action(total, factors.get("risk", 0.0))
        profiles[name] = {
            "total": round(total, 1),
            "action": action,
            "position_range": position,
        }
    return profiles


def ensure_snapshot_table() -> None:
    ensure_strategy_tables()


def persist_snapshots(
    code: str,
    factors: Dict[str, float],
    profiles: Dict[str, Dict[str, Any]],
    trade_date: str | None = None,
) -> None:
    ensure_snapshot_table()
    trade_date = trade_date or datetime.now().date().isoformat()
    with sqlite3.connect(INVESTMENT_DB_PATH) as conn:
        for model, profile in profiles.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO stock_factor_snapshot (
                    trade_date, code, model, quality, growth, valuation, flow, technical, risk, total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_date,
                    code,
                    model,
                    factors.get("quality", 0.0),
                    factors.get("growth", 0.0),
                    factors.get("valuation", 0.0),
                    factors.get("flow", 0.0),
                    factors.get("technical", 0.0),
                    factors.get("risk", 0.0),
                    profile.get("total", 0.0),
                ),
            )
        conn.commit()


def load_backtest_stats_map() -> Dict[str, Any]:
    return _DB_VIEWS.load_backtest_stats_map()


def load_feature_context(code: str) -> Dict[str, Any]:
    return _DB_VIEWS.load_feature_context(code)


def _extract_trade_date(latest: Dict[str, Any]) -> str:
    for key in ("date", "trade_date", "ts"):
        value = latest.get(key)
        if value not in (None, ""):
            return str(value)[:10]
    return datetime.now().strftime("%Y-%m-%d")


def build_strategy_summary(
    code: str,
    latest: Dict[str, Any],
    trend: Dict[str, Any],
    structure: Dict[str, Any],
    fundamentals: Dict[str, Any],
    sentiment: Dict[str, Any],
    regime: Dict[str, Any],
    feature_context: Dict[str, Any] | None = None,
    stats_by_setup: Dict[str, Any] | None = None,
) -> tuple[Dict[str, float], Dict[str, Any], Dict[str, Any]]:
    feature_context = feature_context or load_feature_context(code)
    stats_by_setup = stats_by_setup or load_backtest_stats_map()

    factors = compute_six_factors(
        latest,
        trend,
        structure,
        fundamentals,
        sentiment,
        regime,
        feature_context=feature_context,
    )
    profiles = build_strategy_profiles(factors)
    candidates = build_setup_candidates(
        factors,
        profiles,
        trend,
        structure,
        sentiment,
        latest,
        fundamentals,
        regime,
        feature_context,
        stats_by_setup,
    )
    strategy = summarize_strategy(candidates, profiles)
    persist_snapshots(code, factors, profiles, trade_date=_extract_trade_date(latest))
    return factors, strategy, feature_context
