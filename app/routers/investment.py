"""
投资决策数据API路由
"""
from datetime import datetime
import inspect
import os
import platform
import sqlite3
import sys
import threading
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import get_investment_runtime_profile, settings
from app.db import get_sqlite_connection
from app.services.financial_news import FinancialNewsService
from app.services.intelligence_service import IntelligenceService
from app.services.investment_data import InvestmentDataService
from app.services.investment_db_service import InvestmentDataService as DbService
from app.utils.regime import compute_regime
from app.services.public_research import PublicResearchService
from app.services.coding_plan_service import CodingPlanService
from app.services.strategy_planning_service import StrategyPlanningService
from app.services.north_flow_service import get_north_flow_service
from app.services.research_workbench_service import get_research_workbench_service
from app.services.shortline_service import ShortlineService
try:
    from app.services.lead_lag_service import LeadLagService
except Exception:
    LeadLagService = None
try:
    from app.services.lead_lag_briefs import LeadLagBriefGenerator
except Exception:
    LeadLagBriefGenerator = None
from quant_workbench.service import QuantWorkbenchService
from quant_workbench.sync import QuantWorkbenchSync

router = APIRouter(prefix="/investment", tags=["investment"])
templates = Jinja2Templates(directory="templates")

DB_PATH = settings.investment_db_path

# 全局服务实例
_investment_service = None
_db_service = None
_news_service = None
_workbench_service = None
_public_research_service = None
_strategy_planner = None
_coding_plan_service = None
_intelligence_service = None
_research_workbench_service = None
_shortline_service = None
_lead_lag_service = None
_runtime_refresh_lock = threading.Lock()
_runtime_refresh_state: Dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "last_result": None,
    "last_error": None,
}

LEGACY_INDEX_ALIASES = {
    "FTA50": "ftsea50",
    "YANG": "yang",
    "hkHSI": "hsi",
    "usDJI": "dji",
    "usIXIC": "ixic",
    "usSPX": "inx",
}


def _to_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value in (None, "", "-", "--"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_div(numerator: float, denominator: float, fallback: float = 0.0) -> float:
    if denominator == 0:
        return fallback
    return numerator / denominator


def _driver_signal(value: float, good_threshold: float, bad_threshold: float, reverse: bool = False) -> str:
    if reverse:
        if value <= good_threshold:
            return "positive"
        if value >= bad_threshold:
            return "negative"
    else:
        if value >= good_threshold:
            return "positive"
        if value <= bad_threshold:
            return "negative"
    return "neutral"


def _macro_trend_label(north_money_5d: float) -> str:
    if north_money_5d > 100:
        return "strong_inflow"
    if north_money_5d > 0:
        return "inflow"
    if north_money_5d < -100:
        return "strong_outflow"
    if north_money_5d < 0:
        return "outflow"
    return "neutral"


def _flow_impulse_label(flow_impulse: float) -> str:
    if flow_impulse >= 100:
        return "accelerating_inflow"
    if flow_impulse >= 30:
        return "inflow_strengthening"
    if flow_impulse <= -100:
        return "accelerating_outflow"
    if flow_impulse <= -30:
        return "outflow_strengthening"
    return "stable"


def _parse_windows(raw: str) -> List[int]:
    defaults = [20, 60, 120]
    if not raw:
        return defaults
    values: List[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            window = int(part)
            if window > 0:
                values.append(window)
        except ValueError:
            continue
    return sorted(set(values)) or defaults


def _load_snapshot_opportunity_summary(db_path: str = DB_PATH) -> Dict[str, Any]:
    """
    使用本地 stock_factor_snapshot 生成轻量机会池摘要，避免在线计算过慢。
    """
    summary = {
        "total_candidates": 0,
        "grade_counts": {"A": 0, "B": 0, "C": 0},
        "average_score": 0.0,
        "top_score": 0.0,
        "top_name": None,
        "top_candidates": [],
        "risk_flagged": 0,
        "buy_count": 0,
        "buy_ratio_pct": 0.0,
        "top_setup": None,
    }
    if not os.path.exists(db_path):
        return summary

    conn = get_sqlite_connection(db_path)
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT total, risk
            FROM stock_factor_snapshot
            WHERE model = 'conservative'
              AND trade_date = (
                SELECT MAX(trade_date)
                FROM stock_factor_snapshot
                WHERE model = 'conservative'
              )
            """
        )
        rows = c.fetchall()
        if not rows:
            return summary

        total_score = 0.0
        for total, risk in rows:
            score = _to_float(total, 0.0) or 0.0
            risk_value = _to_float(risk, 100.0) or 100.0
            total_score += score
            if score >= 70 and risk_value <= 45:
                summary["grade_counts"]["A"] += 1
            elif score >= 60:
                summary["grade_counts"]["B"] += 1
            else:
                summary["grade_counts"]["C"] += 1
            if risk_value >= 70:
                summary["risk_flagged"] += 1
            if score > summary["top_score"]:
                summary["top_score"] = round(score, 1)

        total_count = len(rows)
        buy_count = summary["grade_counts"]["A"] + summary["grade_counts"]["B"]
        summary["total_candidates"] = total_count
        summary["buy_count"] = buy_count
        summary["buy_ratio_pct"] = round(_safe_div(buy_count * 100.0, max(1, total_count)), 1)
        summary["average_score"] = round(_safe_div(total_score, max(1, total_count)), 1)
        return summary
    except Exception as exc:
        print(f"机会池快照摘要读取失败: {exc}")
        return summary
    finally:
        conn.close()


def _lead_lag_fallback_overview() -> Dict[str, Any]:
    today = datetime.now().date().isoformat()
    return {
        "headline": "Lead-Lag Alpha Engine",
        "summary": "Lead-Lag 服务暂未就绪，当前返回稳定回退结构，前端仍可用于 mock 或联调。",
        "flags": ["Fallback Ready", "Mock-friendly", "Service Optional"],
        "metrics": [
            {"label": "模型数", "value": "-", "note": "等待 /models"},
            {"label": "机会数", "value": "-", "note": "等待 /opportunities"},
            {"label": "事件窗口", "value": "-", "note": "等待 /events-calendar"},
            {"label": "记忆命中", "value": "-", "note": "等待 /obsidian-memory"},
        ],
        "regions": ["all", "cn", "hk", "us"],
        "regimes": ["all", "risk_on", "risk_off"],
        "as_of": today,
        "status_text": "使用稳定回退数据，等待 Lead-LagService 接入",
        "source": "fallback",
    }


def _lead_lag_fallback_decision_center() -> Dict[str, Any]:
    return {
        "as_of": datetime.now().isoformat(),
        "headline": "Decision Center 待接入",
        "main_conclusion": "V2 后端尚未生成今日主结论。",
        "do_not_do_today": ["不要在缺少验证链时追高。"],
        "top_directions": [],
        "baton_summary": {"first_baton": [], "second_baton": [], "pre_trigger": []},
        "risk_budget": {"label": "no_new_risk", "reason": "V2 评分尚未完成。"},
        "key_invalidations": ["等待 OpportunityCard invalidation_rules。"],
        "next_check_time": None,
        "source_count": 0,
        "cache_status": "sample_fallback",
    }


def _lead_lag_fallback_what_changed() -> Dict[str, Any]:
    return {
        "as_of": datetime.now().isoformat(),
        "since": None,
        "new_signals": [],
        "upgraded_opportunities": [],
        "downgraded_or_invalidated": [],
        "crowding_up": [],
        "macro_external_policy_changes": [],
    }


def _lead_lag_fallback_macro_bridge() -> Dict[str, Any]:
    return {
        "as_of": datetime.now().isoformat(),
        "macro_regime": {"label": "待接入", "drivers": []},
        "external_risk": {"label": "待接入", "drivers": []},
        "hk_liquidity": {"label": "待接入", "drivers": []},
        "decision_impact": "V2 bridge scorer 尚未生成。",
        "cache_status": "sample_fallback",
    }


def _lead_lag_fallback_brief(slot: str) -> Dict[str, Any]:
    return {
        "slot": slot,
        "as_of": datetime.now().isoformat(),
        "headline": "Lead-Lag 简报待接入",
        "today_focus": [],
        "new_catalysts": [],
        "invalidation_alerts": [],
        "next_checkpoints": [],
        "top_opportunities": [],
        "do_not_chase": [],
        "macro_external_hk_context": {},
        "source_summary": {"source_count": 0, "freshness": "unknown", "cache_status": "sample_fallback"},
    }


def _lead_lag_fallback_list(section: str) -> List[Dict[str, Any]]:
    today = datetime.now().date().isoformat()
    fallback_map: Dict[str, List[Dict[str, Any]]] = {
        "models": [
            {
                "name": "Cross-Market Lead",
                "summary": "用跨市场价格、汇率和波动率信号判断风险偏好变化。",
                "status": "ready",
                "lead_window": "1-5d",
                "universe": "CN/HK/US",
                "confidence": "high",
            },
            {
                "name": "Industry Transmission",
                "summary": "跟踪行业上游到下游的价格和盈利传导链路。",
                "status": "watch",
                "lead_window": "3-20d",
                "universe": "A-share",
                "confidence": "medium",
            },
        ],
        "opportunities": [
            {
                "title": "Risk-on switch watch",
                "rationale": "流动性与风险偏好同步改善时，优先关注高 beta 方向。",
                "score": 72,
                "driver": "Liquidity",
                "confirmation": "Credit and FX follow-through",
                "risk": "Policy reversal",
            }
        ],
        "crossMarket": [
            {
                "name": "USD/CNH -> HK growth",
                "summary": "美元与离岸人民币波动先于港股成长风格切换。",
                "tone": "positive",
                "signal": "confirm",
                "lag": "2-4d",
            }
        ],
        "transmission": [
            {
                "name": "Rates -> Banks -> Brokers",
                "summary": "利率方向变化通过金融链条扩散到风险偏好。",
                "signal": "positive",
                "steps": ["Rates", "Banks", "Brokers", "Broad market"],
            }
        ],
        "liquidity": [
            {
                "name": "USD Liquidity",
                "summary": "美元流动性与风险资产估值压缩/扩张的共同驱动。",
                "tone": "neutral",
                "signal": "watch",
                "value": "n/a",
            }
        ],
        "thesis": [
            {
                "name": "AI infrastructure",
                "summary": "景气与资本开支同步时，半导体和算力链更容易形成持续行情。",
                "evidence": ["Capex", "Earnings", "Order flow"],
                "invalidation": "订单与利润预期下修",
                "crowding": "medium",
            }
        ],
        "events": [
            {
                "title": "Macro data release",
                "date": today,
                "type": "macro",
                "importance": "high",
                "notes": "观察利率、汇率和风险偏好联动。",
            }
        ],
        "validation": [
            {
                "title": "Replay sample",
                "outcome": "pending",
                "hit_rate": "-",
                "reason": "等待历史回放验证服务返回。",
                "reference": "fallback",
            }
        ],
        "memory": [
            {
                "title": "Lead-lag notes",
                "tags": ["lead-lag", "fallback", "research"],
                "notes": "服务未接入时的占位笔记。",
                "links": [],
            }
        ],
    }
    return fallback_map.get(section, [])


def _unwrap_lead_lag_payload(payload: Any, fallback: Any) -> Any:
    if payload is None:
        return fallback
    if isinstance(payload, dict):
        if "data" in payload and payload["data"] is not None:
            return payload["data"]
        if "items" in payload and isinstance(payload["items"], list):
            return payload["items"]
    return payload


def _call_lead_lag_service(method_names: List[str], fallback: Any, unwrap: bool = True, **kwargs: Any) -> Any:
    service = get_lead_lag_service()
    if service is None:
        return fallback

    last_error: Optional[Exception] = None
    for method_name in method_names:
        method = getattr(service, method_name, None)
        if not callable(method):
            continue
        try:
            signature = inspect.signature(method)
            has_var_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())
            accepted_kwargs = kwargs if has_var_kwargs else {
                key: value for key, value in kwargs.items() if key in signature.parameters
            }
        except (TypeError, ValueError):
            accepted_kwargs = kwargs

        try:
            result = method(**accepted_kwargs) if accepted_kwargs else method()
            return _unwrap_lead_lag_payload(result, fallback) if unwrap else result
        except Exception as exc:
            last_error = exc
            continue

    if last_error:
        print(f"lead-lag service call failed for {method_names}: {last_error}")
    return fallback


def get_lead_lag_service():
    global _lead_lag_service
    if _lead_lag_service is not None:
        return _lead_lag_service
    if LeadLagService is None:
        return None
    try:
        _lead_lag_service = LeadLagService()
    except Exception as exc:
        print(f"lead-lag service init failed: {exc}")
        _lead_lag_service = None
    return _lead_lag_service

def get_investment_service():
    global _investment_service
    if _investment_service is None:
        _investment_service = InvestmentDataService()
    return _investment_service

def get_db_service():
    global _db_service
    if _db_service is None:
        _db_service = DbService()
    return _db_service

def get_news_service():
    global _news_service
    if _news_service is None:
        _news_service = FinancialNewsService()
    return _news_service


def get_workbench_service():
    global _workbench_service
    if _workbench_service is None:
        _workbench_service = QuantWorkbenchService()
    return _workbench_service


def get_public_research_service():
    global _public_research_service
    if _public_research_service is None:
        _public_research_service = PublicResearchService()
    return _public_research_service


def get_strategy_planner():
    global _strategy_planner
    if _strategy_planner is None:
        _strategy_planner = StrategyPlanningService(
            realtime_service=get_investment_service(),
            db_service=get_db_service(),
            news_service=get_news_service(),
        )
    return _strategy_planner


def get_coding_plan_service():
    global _coding_plan_service
    if _coding_plan_service is None:
        _coding_plan_service = CodingPlanService(
            realtime_service=get_investment_service(),
            db_service=get_db_service(),
            news_service=get_news_service(),
        )
    return _coding_plan_service


def get_intelligence_service():
    global _intelligence_service
    if _intelligence_service is None:
        _intelligence_service = IntelligenceService()
    return _intelligence_service


def get_research_workbench_svc():
    global _research_workbench_service
    if _research_workbench_service is None:
        _research_workbench_service = get_research_workbench_service()
    return _research_workbench_service


def get_shortline_service():
    global _shortline_service
    if _shortline_service is None:
        _shortline_service = ShortlineService(DB_PATH)
    return _shortline_service


def _summarize_workbench(opportunities: List[Dict[str, Any]]) -> Dict[str, Any]:
    grade_counts = {"A": 0, "B": 0, "C": 0}
    total_score = 0.0
    for item in opportunities:
        grade = (item.get("grade") or "C").upper()
        if grade in grade_counts:
            grade_counts[grade] += 1
        total_score += float(item.get("score", 0) or 0)

    best = opportunities[0] if opportunities else None
    return {
        "total_candidates": len(opportunities),
        "grade_counts": grade_counts,
        "average_score": round(total_score / len(opportunities), 1) if opportunities else 0,
        "top_score": best.get("score") if best else 0,
        "top_name": best.get("name") if best else None,
        "top_candidates": opportunities[:5],
        "risk_flagged": sum(1 for item in opportunities if item.get("risk_flags")),
    }


def _action_bucket(action: Any, grade: Any = None) -> str:
    text = str(action or "").strip().upper()
    if text in {"BUY", "ADD", "BUILD", "STRONG_BUY", "BUY_NOW"} or text.startswith("BUY"):
        return "actionable"
    if text in {"WATCH", "HOLD", "OBSERVE"}:
        return "watch"
    if str(grade or "").strip().upper() == "A":
        return "watch"
    return "avoid"


def _display_action(action: Any, bucket: str) -> str:
    text = str(action or "").strip().upper()
    if bucket == "actionable":
        return "可行动" if text in {"", "BUY"} else text
    if bucket == "watch":
        return "观察"
    return "回避"


def _format_score(item: Dict[str, Any]) -> float:
    score = item.get("total_score", item.get("score", 0))
    try:
        return round(float(score or 0), 1)
    except (TypeError, ValueError):
        return 0.0


def _position_hint(execution_mode: str, gate_status: str, rank: int) -> str:
    if gate_status != "open":
        return "0%"
    if execution_mode == "offensive":
        return ["8%-12%", "5%-8%", "3%-5%"][min(rank, 2)]
    if execution_mode == "balanced":
        return ["5%-8%", "3%-5%", "≤3%"][min(rank, 2)]
    return "≤3% 试错"


def _build_execution_gate(decision: Dict[str, Any]) -> Dict[str, Any]:
    regime = decision.get("regime") or {}
    data_health = decision.get("data_health") or {}
    health_summary = data_health.get("summary") or {}
    storage_fresh_pct = _to_float(data_health.get("storage_fresh_pct"), 0.0) or 0.0
    error_count = int(health_summary.get("error_count") or 0)
    execution_mode = regime.get("execution_mode") or "defensive"

    if error_count > 0 or storage_fresh_pct < 60:
        return {
            "status": "blocked",
            "label": "禁止新增仓位",
            "reason": "核心数据错误或快照新鲜度不足，先修数据再恢复交易判断。",
            "position_ceiling": "0%-20%",
        }
    if execution_mode == "defensive" or storage_fresh_pct < 80:
        return {
            "status": "restricted",
            "label": "防守观察",
            "reason": "市场或数据状态不足以支持进攻，只允许小仓位试错。",
            "position_ceiling": "10%-30%",
        }
    if execution_mode == "offensive":
        return {
            "status": "open",
            "label": "可选择性进攻",
            "reason": "市场风险和数据状态允许执行高质量机会。",
            "position_ceiling": "40%-70%",
        }
    return {
        "status": "open",
        "label": "平衡执行",
        "reason": "只执行证据充分、入场条件清晰的机会。",
        "position_ceiling": "25%-50%",
    }


def _build_playbook_item(item: Dict[str, Any], rank: int, execution_mode: str, gate_status: str) -> Dict[str, Any]:
    bucket = _action_bucket(item.get("action"), item.get("grade"))
    score = _format_score(item)
    risk_flags = item.get("risk_flags")
    if isinstance(risk_flags, str):
        risk_flags_text = risk_flags.strip()
    elif isinstance(risk_flags, list):
        risk_flags_text = "；".join(str(flag) for flag in risk_flags if flag)
    else:
        risk_flags_text = ""

    action_reason = item.get("action_reason") or item.get("reason") or item.get("setup_label") or "等待入场条件确认"
    setup = item.get("setup_label") or item.get("setup_name") or "未分类"
    return {
        "rank": rank + 1,
        "code": item.get("code") or item.get("symbol") or "",
        "name": item.get("name") or item.get("symbol_name") or item.get("code") or "-",
        "market": item.get("market") or "",
        "category": item.get("category") or "",
        "setup": setup,
        "grade": item.get("grade") or "-",
        "score": score,
        "action": item.get("action") or "",
        "action_label": _display_action(item.get("action"), bucket),
        "bucket": bucket,
        "position_hint": _position_hint(execution_mode, gate_status, rank),
        "entry": action_reason,
        "stop_loss": "跌破关键均线或单笔亏损 6%-8% 后退出",
        "invalidation": risk_flags_text or "数据转为不新鲜、事件证据失效或市场状态转防守",
        "review_time": "今日 11:40 / 15:15",
        "source_table": item.get("source_table") or "workbench",
    }


async def _build_practical_brief() -> Dict[str, Any]:
    warnings: List[str] = []
    db = get_db_service()
    runtime_profile = get_investment_runtime_profile()

    try:
        decision = await get_decision_center(with_workbench=False)
    except Exception as exc:
        warnings.append(f"decision-center 读取失败: {exc}")
        decision = {
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "regime": {"label": "unknown", "score": 0, "execution_mode": "defensive"},
            "drivers": [],
            "data_health": {"summary": {"status": "error", "error_count": 1}, "storage_fresh_pct": 0},
            "opportunity_summary": {},
            "watch_summary": {},
            "strategy_stability": {"windows": [], "setups": [], "recent_signals": []},
        }

    try:
        pool = db.get_opportunity_pool_overview(pool_code="watch", limit=30)
    except Exception as exc:
        warnings.append(f"watch 机会池读取失败: {exc}")
        try:
            pool = db.get_opportunity_pool_overview(pool_code="all", limit=30)
        except Exception as inner_exc:
            warnings.append(f"全量机会池读取失败: {inner_exc}")
            pool = {"summary": {}, "leaderboard": []}

    leaderboard = pool.get("leaderboard") or pool.get("opportunities") or []
    execution_mode = (decision.get("regime") or {}).get("execution_mode") or "defensive"
    gate = _build_execution_gate(decision)
    gate_status = "open" if gate.get("status") == "open" else "blocked"

    actionable = [
        item for item in leaderboard
        if _action_bucket(item.get("action"), item.get("grade")) == "actionable"
    ]
    watch_items = [
        item for item in leaderboard
        if _action_bucket(item.get("action"), item.get("grade")) == "watch"
    ]
    avoid_items = [
        item for item in leaderboard
        if _action_bucket(item.get("action"), item.get("grade")) == "avoid"
    ]

    primary_source = actionable if gate.get("status") == "open" else []
    top_actions = [
        _build_playbook_item(item, idx, execution_mode, gate_status)
        for idx, item in enumerate(primary_source[:3])
    ]
    watchlist = [
        _build_playbook_item(item, idx, execution_mode, "blocked")
        for idx, item in enumerate((watch_items or actionable or avoid_items)[:8])
    ]

    data_health = decision.get("data_health") or {}
    health_summary = data_health.get("summary") or {}
    storage_fresh_pct = _to_float(data_health.get("storage_fresh_pct"), 0.0) or 0.0
    strategy_stability = decision.get("strategy_stability") or {}

    active_metrics = [
        "数据健康/快照新鲜度",
        "市场广度",
        "涨跌停结构",
        "VIX 外部风险",
        "资金流动量(仅新鲜时)",
        "候选动作/仓位/止损",
        "策略样本胜率",
    ]
    hidden_metrics = [
        "多层模型原始分",
        "Evidence chunk/citation 统计",
        "Universe/Entity 注册表覆盖率",
        "空表或单样本宏观指标",
        "sample_demo / fallback_placeholder",
    ]

    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "runtime_profile": runtime_profile,
        "execution_gate": gate,
        "regime": decision.get("regime") or {},
        "drivers": (decision.get("drivers") or [])[:6],
        "top_actions": top_actions,
        "watchlist": watchlist,
        "counts": {
            "candidate_count": len(leaderboard),
            "actionable_count": len(actionable),
            "watch_count": len(watch_items),
            "avoid_count": len(avoid_items),
        },
        "opportunity_summary": {
            **(decision.get("opportunity_summary") or {}),
            **(pool.get("summary") or {}),
        },
        "watch_summary": decision.get("watch_summary") or {},
        "data_status": {
            "status": health_summary.get("status") or "unknown",
            "health_score": health_summary.get("health_score"),
            "storage_fresh_pct": storage_fresh_pct,
            "error_count": health_summary.get("error_count", 0),
            "warning_count": health_summary.get("warning_count", 0),
            "db_path": DB_PATH,
        },
        "strategy_stability": {
            "as_of_date": strategy_stability.get("as_of_date"),
            "windows": (strategy_stability.get("windows") or [])[:3],
            "setups": (strategy_stability.get("setups") or [])[:5],
        },
        "active_metrics": active_metrics,
        "hidden_metrics": hidden_metrics,
        "warnings": warnings,
    }


def normalize_index_keys(indices: Dict) -> Dict:
    normalized = dict(indices or {})
    for old_key, new_key in LEGACY_INDEX_ALIASES.items():
        if old_key in normalized and new_key not in normalized:
            item = dict(normalized[old_key])
            item["code"] = new_key
            normalized[new_key] = item
    return normalized


@router.get("/", response_class=HTMLResponse)
async def investment_dashboard(request: Request):
    """每日执行工作台页面。"""
    return templates.TemplateResponse(request, "investment_daily.html", {})


@router.get("/legacy", response_class=HTMLResponse)
async def investment_legacy_dashboard(request: Request):
    """旧版多指标投资决策仪表板页面。"""
    return templates.TemplateResponse(request, "investment.html", {})


@router.get("/intelligence", response_class=HTMLResponse)
async def intelligence_hub_page(request: Request):
    """重大事项情报雷达页面。"""
    return templates.TemplateResponse(request, "intelligence.html", {})


@router.get("/lead-lag", response_class=HTMLResponse)
async def lead_lag_page(request: Request):
    """Lead-Lag Alpha Engine 页面。"""
    return templates.TemplateResponse(request, "lead_lag.html", {})


@router.get("/api/lead-lag/overview")
async def lead_lag_overview(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 总览。"""
    fallback = _lead_lag_fallback_overview()
    payload = _call_lead_lag_service(
        ["get_overview", "overview", "build_overview"],
        fallback,
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/models")
async def lead_lag_models(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 模型库。"""
    return _call_lead_lag_service(
        ["list_models", "get_models", "models"],
        _lead_lag_fallback_list("models"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/decision-center")
async def lead_lag_decision_center(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 决策中心。"""
    fallback = _lead_lag_fallback_decision_center()
    payload = _call_lead_lag_service(
        ["decision_center", "get_decision_center", "build_decision_center"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/opportunity-queue")
async def lead_lag_opportunity_queue(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    family: Optional[str] = None,
    regime: Optional[str] = None,
    include_sample: bool = False,
    live_only: bool = False,
    archived_only: bool = False,
    q: Optional[str] = None,
):
    """Lead-Lag V2 可执行机会队列。"""
    return _call_lead_lag_service(
        ["opportunity_queue", "get_opportunity_queue", "build_opportunity_cards"],
        {"cards": [], "items": [], "count": 0},
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        family=family,
        regime=regime,
        include_sample=include_sample,
        live_only=live_only,
        archived_only=archived_only,
        q=q,
    )


@router.get("/api/lead-lag/what-changed")
async def lead_lag_what_changed(
    as_of: Optional[str] = None,
    since: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 昨日至今变化。"""
    fallback = _lead_lag_fallback_what_changed()
    payload = _call_lead_lag_service(
        ["what_changed", "get_what_changed", "build_what_changed"],
        fallback,
        unwrap=False,
        as_of=as_of,
        since=since,
        region=region,
        regime=regime,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/event-frontline")
async def lead_lag_event_frontline(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    event_class: Optional[str] = "market-facing",
    include_sample: bool = False,
    include_research_facing: bool = False,
    window: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 事件前线，默认只返回 market-facing 催化。"""
    return _call_lead_lag_service(
        ["event_frontline", "get_event_frontline", "list_event_frontline"],
        {"events": [], "items": [], "count": 0, "default_filter": event_class or "market-facing"},
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        event_class=event_class,
        include_sample=include_sample,
        include_research_facing=include_research_facing,
        window=window,
        q=q,
    )


@router.get("/api/lead-lag/source-quality-lineage")
async def lead_lag_source_quality_lineage(
    limit: int = 20,
):
    """Lead-Lag V3 来源可信度与数据血缘诊断。"""
    return _call_lead_lag_service(
        ["source_quality_lineage", "get_source_quality_lineage"],
        {"lineage": {}, "evidence_vault": {}},
        unwrap=False,
        limit=limit,
    )


@router.get("/api/lead-lag/report-center")
async def lead_lag_report_center(
    q: Optional[str] = None,
    limit: int = 20,
):
    """Lead-Lag V3 报告中心与全文检索。"""
    return _call_lead_lag_service(
        ["report_center", "get_report_center"],
        {"reports": [], "count": 0},
        unwrap=False,
        q=q,
        limit=limit,
    )


@router.get("/api/lead-lag/opportunity-universe")
async def lead_lag_opportunity_universe():
    """Lead-Lag V3 机会宇宙注册表。"""
    return _call_lead_lag_service(
        ["opportunity_universe_registry", "get_opportunity_universe_registry"],
        {"counts": {}, "sectors": []},
        unwrap=False,
    )


@router.get("/api/lead-lag/dossier/sector/{sector_id}")
async def lead_lag_sector_dossier(
    sector_id: str,
    limit: int = 10,
):
    """Lead-Lag V3 Sector Dossier。"""
    return _call_lead_lag_service(
        ["sector_dossier"],
        {"sector_id": sector_id, "sector": {}, "current_opportunities": []},
        unwrap=False,
        sector_id=sector_id,
        limit=limit,
    )


@router.get("/api/lead-lag/dossier/entity/{entity_id}")
async def lead_lag_entity_dossier(
    entity_id: str,
    limit: int = 10,
):
    """Lead-Lag V3 Entity Dossier。"""
    return _call_lead_lag_service(
        ["entity_dossier"],
        {"entity_id": entity_id, "entity": {}, "instruments": []},
        unwrap=False,
        entity_id=entity_id,
        limit=limit,
    )


@router.get("/api/lead-lag/dossier/instrument/{instrument_id}")
async def lead_lag_instrument_dossier(
    instrument_id: str,
    limit: int = 10,
):
    """Lead-Lag V3 Instrument Dossier。"""
    return _call_lead_lag_service(
        ["instrument_dossier"],
        {"instrument_id": instrument_id, "instrument": {}, "related_opportunity_cards": []},
        unwrap=False,
        instrument_id=instrument_id,
        limit=limit,
    )


@router.get("/api/lead-lag/avoid-board")
async def lead_lag_avoid_board(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 不追高/回避列表。"""
    return _call_lead_lag_service(
        ["avoid_board", "get_avoid_board", "do_not_chase"],
        {"items": [], "count": 0},
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/macro-bridge")
async def lead_lag_macro_bridge(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
):
    """Lead-Lag V2 宏观 / 外部 / 港股桥接层。"""
    fallback = _lead_lag_fallback_macro_bridge()
    payload = _call_lead_lag_service(
        ["macro_bridge", "get_macro_bridge", "build_macro_bridge"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        regime=regime,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/transmission-workspace")
async def lead_lag_transmission_workspace(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 传导图谱工作区。"""
    fallback = {"nodes": [], "edges": [], "baton_tiers": {}, "current_bottlenecks": []}
    payload = _call_lead_lag_service(
        ["transmission_workspace", "get_transmission_workspace", "industry_transmission"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/replay-diagnostics")
async def lead_lag_replay_diagnostics(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 回放诊断。"""
    fallback = {"horizon_distribution": [], "regime_split": [], "failure_modes": [], "stage_transitions": []}
    payload = _call_lead_lag_service(
        ["replay_diagnostics", "get_replay_diagnostics", "replay_validation"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        regime=regime,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/research-memory/actions")
async def lead_lag_research_memory_actions(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 Obsidian 行动记忆。"""
    fallback = {"items": [], "status": "missing"}
    payload = _call_lead_lag_service(
        ["research_memory_actions", "get_research_memory_actions", "obsidian_memory"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/sector-evidence")
async def lead_lag_sector_evidence(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    sector: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag V2 五大赛道深证据层。"""
    fallback = {"sectors": [], "count": 0, "cache_status": "fallback"}
    payload = _call_lead_lag_service(
        ["sector_deep_evidence", "get_sector_deep_evidence"],
        fallback,
        unwrap=False,
        as_of=as_of,
        region=region,
        sector=sector,
        q=q,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/briefs/{slot}")
async def lead_lag_brief(
    slot: str,
    as_of: Optional[str] = None,
):
    """Lead-Lag V2 固定时点简报。"""
    fallback = _lead_lag_fallback_brief(slot)
    if LeadLagBriefGenerator is not None:
        try:
            generator = LeadLagBriefGenerator(service=get_lead_lag_service())
            return generator.generate(slot, as_of=as_of)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            print(f"lead-lag brief generation failed for {slot}: {exc}")
    payload = _call_lead_lag_service(
        ["build_brief", "get_brief", "lead_lag_brief"],
        fallback,
        unwrap=False,
        slot=slot,
        as_of=as_of,
    )
    return payload if isinstance(payload, dict) else fallback


@router.get("/api/lead-lag/opportunities")
async def lead_lag_opportunities(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 机会列表。"""
    return _call_lead_lag_service(
        ["list_opportunities", "get_opportunities", "opportunities"],
        _lead_lag_fallback_list("opportunities"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/cross-market-map")
async def lead_lag_cross_market_map(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 跨市场映射。"""
    return _call_lead_lag_service(
        ["get_cross_market_map", "list_cross_market_map", "cross_market_map"],
        _lead_lag_fallback_list("crossMarket"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/industry-transmission")
async def lead_lag_industry_transmission(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 行业传导。"""
    return _call_lead_lag_service(
        ["get_industry_transmission", "list_industry_transmission", "industry_transmission"],
        _lead_lag_fallback_list("transmission"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/liquidity")
async def lead_lag_liquidity(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 流动性。"""
    return _call_lead_lag_service(
        ["get_liquidity", "list_liquidity", "liquidity"],
        _lead_lag_fallback_list("liquidity"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/sector-thesis")
async def lead_lag_sector_thesis(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 赛道 thesis。"""
    return _call_lead_lag_service(
        ["list_sector_thesis", "get_sector_thesis", "sector_thesis"],
        _lead_lag_fallback_list("thesis"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/events-calendar")
async def lead_lag_events_calendar(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 事件日历。"""
    return _call_lead_lag_service(
        ["get_events_calendar", "list_events_calendar", "events_calendar"],
        _lead_lag_fallback_list("events"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/replay-validation")
async def lead_lag_replay_validation(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag 回放验证。"""
    return _call_lead_lag_service(
        ["get_replay_validation", "list_replay_validation", "replay_validation"],
        _lead_lag_fallback_list("validation"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/lead-lag/obsidian-memory")
async def lead_lag_obsidian_memory(
    as_of: Optional[str] = None,
    region: Optional[str] = None,
    regime: Optional[str] = None,
    q: Optional[str] = None,
):
    """Lead-Lag Obsidian 研究记忆。"""
    return _call_lead_lag_service(
        ["get_obsidian_memory", "list_obsidian_memory", "obsidian_memory"],
        _lead_lag_fallback_list("memory"),
        as_of=as_of,
        region=region,
        regime=regime,
        q=q,
    )


@router.get("/api/overview")
async def get_market_overview():
    """获取市场概览数据 - 优先使用 Windows 本地快照，过期后再刷新实时数据"""
    db = get_db_service()
    realtime = get_investment_service()
    overview_storage = {}

    try:
        realtime_data = realtime.get_market_overview()
        indices = normalize_index_keys(db.get_all_indices_latest())
        indices.update(realtime_data.get("indices", {}))
        rates = realtime_data.get("rates", {})
        sentiment = realtime_data.get("sentiment", {})
        vix_raw = realtime_data.get("fear_greed", {}).get("vix", {})
        vix = {
            "close": vix_raw.get("value"),
            "change_pct": vix_raw.get("change_pct"),
            "date": (vix_raw.get("quote_time") or "")[:10] or None,
        } if vix_raw else {}
        watch_stocks = realtime_data.get("watch_stocks") or realtime.get_watch_stocks()
        overview_storage = realtime_data.get("storage", {})
    except Exception as exc:
        print(f"实时市场概览获取失败: {exc}")
        indices = normalize_index_keys(db.get_all_indices_latest())
        rates = db.get_interest_rates_latest()
        sentiment = db.get_market_sentiment_latest()
        vix = db.get_vix_latest()
        watch_stocks = realtime.get_watch_stocks()

    return {
        "update_time": datetime.now().isoformat(),
        "indices": indices,
        "rates": rates,
        "sentiment": sentiment,
        "vix": vix,
        "watch_stocks": watch_stocks,
        "storage": overview_storage,
        "runtime_profile": get_investment_runtime_profile(),
    }


@router.get("/api/decision-center")
async def get_decision_center(with_workbench: bool = False):
    """
    投资决策中枢聚合数据（本地优先）:
    - 胜率驱动因子
    - 执行矩阵
    - 机会池质量
    - 本地快照/数据健康
    """
    db = get_db_service()

    overview_snapshot = db.get_market_snapshot("investment.market_overview.v2")
    watch_snapshot = db.get_market_snapshot("investment.watch_stocks.v2")

    if overview_snapshot and overview_snapshot.get("payload"):
        overview = dict(overview_snapshot.get("payload") or {})
        overview["storage"] = {
            "mode": "sqlite_snapshot",
            "source": overview_snapshot.get("source"),
            "updated_at": overview_snapshot.get("updated_at"),
            "age_seconds": overview_snapshot.get("age_seconds"),
            "is_fresh": overview_snapshot.get("is_fresh"),
            "snapshot_key": "investment.market_overview.v2",
        }
    else:
        overview = await get_market_overview()

    if watch_snapshot and watch_snapshot.get("payload"):
        watch_data = dict(watch_snapshot.get("payload") or {})
        watch_data["storage"] = {
            "mode": "sqlite_snapshot",
            "source": watch_snapshot.get("source"),
            "updated_at": watch_snapshot.get("updated_at"),
            "age_seconds": watch_snapshot.get("age_seconds"),
            "is_fresh": watch_snapshot.get("is_fresh"),
            "snapshot_key": "investment.watch_stocks.v2",
        }
    else:
        watch_data = overview.get("watch_stocks") or {}

    watch_summary = watch_data.get("summary") or {}
    sentiment = overview.get("sentiment") or {}
    rates = overview.get("rates") or {}
    vix = overview.get("vix") or {}
    if _to_float(vix.get("close"), None) is None:
        latest_vix = db.get_vix_latest() or {}
        if latest_vix:
            vix = {
                "close": latest_vix.get("close"),
                "change_pct": vix.get("change_pct"),
                "date": latest_vix.get("date"),
            }

    north_money = db.get_north_money(30)
    north_money_5d = sum(_to_float(row.get("total_inflow"), 0.0) for row in north_money[-5:])
    north_money_20d_avg = _safe_div(
        sum(_to_float(row.get("total_inflow"), 0.0) for row in north_money[-20:]),
        max(1, min(20, len(north_money))),
        0.0,
    )
    north_flow_baseline_5d = north_money_20d_avg * 5.0
    north_flow_impulse = north_money_5d - north_flow_baseline_5d

    up_count = _to_float(sentiment.get("up_count"), 0.0)
    down_count = _to_float(sentiment.get("down_count"), 0.0)
    limit_up_count = _to_float(sentiment.get("limit_up_count"), 0.0)
    limit_down_count = _to_float(sentiment.get("limit_down_count"), 0.0)
    breadth_ratio = _safe_div(up_count, max(1.0, down_count), 0.0)
    limit_ratio = _safe_div(limit_up_count, max(1.0, limit_down_count), 0.0)
    limit_net = limit_up_count - limit_down_count

    vix_close = _to_float(vix.get("close"), 0.0)

    data_health = db.get_data_health_overview(
        {
            "investment.market_overview.v2": 300,
            "investment.watch_stocks.v2": 300,
        }
    )
    health_summary = data_health.get("summary") or {}
    storage_items = data_health.get("storage") or []
    fresh_count = sum(1 for item in storage_items if item.get("is_fresh"))
    storage_fresh_pct = round(_safe_div(fresh_count * 100.0, max(1, len(storage_items))), 1)

    opportunities: List[Dict[str, Any]] = []
    workbench_summary = _load_snapshot_opportunity_summary(DB_PATH)
    buy_ratio_pct = _to_float(workbench_summary.get("buy_ratio_pct"), 0.0) or 0.0
    buy_count = int(workbench_summary.get("buy_count") or 0)
    top_setup = workbench_summary.get("top_setup")

    if with_workbench:
        try:
            workbench = get_workbench_service()
            opportunities = workbench.list_opportunities()
            live_summary = _summarize_workbench(opportunities)
            actionable = [
                item
                for item in opportunities
                if str(item.get("action") or "").lower() in {"buy", "add", "build"}
            ]
            buy_count = len(actionable)
            buy_ratio_pct = round(_safe_div(buy_count * 100.0, max(1, len(opportunities))), 1)

            setup_count: Dict[str, int] = {}
            for item in actionable:
                setup = item.get("setup_label") or item.get("setup_name") or "未分类"
                setup_count[setup] = setup_count.get(setup, 0) + 1
            if setup_count:
                top_setup = sorted(setup_count.items(), key=lambda kv: kv[1], reverse=True)[0][0]

            workbench_summary = {
                **live_summary,
                "buy_count": buy_count,
                "buy_ratio_pct": buy_ratio_pct,
                "top_setup": top_setup,
            }
        except Exception as exc:
            print(f"量化工作台聚合失败，回退快照摘要: {exc}")

    regime = compute_regime(
        breadth_ratio=breadth_ratio,
        limit_ratio=limit_ratio,
        north_flow_impulse=north_flow_impulse,
        vix_close=vix_close,
        error_count=int(health_summary.get("error_count") or 0),
    )
    regime_label = regime["label"]
    regime_score = regime["score"]

    if int(health_summary.get("error_count") or 0) > 0 or storage_fresh_pct < 60:
        execution_mode = "defensive"
    elif regime_label == "risk_on" and buy_ratio_pct >= 20:
        execution_mode = "offensive"
    else:
        execution_mode = "balanced"

    if execution_mode == "offensive":
        action_matrix = [
            {
                "scenario": "趋势共振",
                "condition": "广度>1.1 且 北向5日净流入为正",
                "position": "60%-80%",
                "action": "优先A档候选，分两段建仓",
            },
            {
                "scenario": "轮动确认",
                "condition": "A/B候选占比提升且风险标记下降",
                "position": "40%-60%",
                "action": "只做有催化的主线行业",
            },
            {
                "scenario": "高波动突发",
                "condition": "VIX快速抬升或北向转负",
                "position": "降至30%-40%",
                "action": "暂停追高，先减弱势仓位",
            },
        ]
    elif execution_mode == "defensive":
        action_matrix = [
            {
                "scenario": "风险收缩",
                "condition": "VIX高位或资金连续流出",
                "position": "10%-30%",
                "action": "仅保留低波动防御仓位",
            },
            {
                "scenario": "观察等待",
                "condition": "数据新鲜度不足或错误未修复",
                "position": "≤20%",
                "action": "先修复数据，再恢复进攻仓位",
            },
            {
                "scenario": "技术反弹",
                "condition": "短期超跌但趋势未反转",
                "position": "20%-35%",
                "action": "小仓位试错，严格止损",
            },
        ]
    else:
        action_matrix = [
            {
                "scenario": "中性震荡",
                "condition": "广度与资金信号分化",
                "position": "35%-55%",
                "action": "核心仓位持有 + 事件驱动增减",
            },
            {
                "scenario": "信号确认前夜",
                "condition": "A档候选增加但北向未确认",
                "position": "30%-45%",
                "action": "先配流动性好的A/B档标的",
            },
            {
                "scenario": "信号破坏",
                "condition": "涨跌停比恶化或VIX上冲",
                "position": "降至25%-35%",
                "action": "快速收缩高beta仓位",
            },
        ]

    drivers = [
        {
            "key": "breadth_ratio",
            "label": "市场广度",
            "value": round(breadth_ratio, 2),
            "display": f"{up_count:.0f}/{down_count:.0f}",
            "signal": _driver_signal(breadth_ratio, good_threshold=1.1, bad_threshold=0.9),
            "hint": "上涨/下跌家数比，衡量赚钱效应扩散",
        },
        {
            "key": "limit_ratio",
            "label": "涨跌停比",
            "value": round(limit_ratio, 2),
            "display": f"{limit_up_count:.0f}/{limit_down_count:.0f}",
            "signal": _driver_signal(limit_ratio, good_threshold=1.5, bad_threshold=0.8),
            "hint": "涨停相对跌停的优势，衡量风险偏好",
        },
        {
            "key": "north_money_5d",
            "label": "北向5日净流入",
            "value": round(north_money_5d, 1),
            "display": f"{north_money_5d:+.1f} 亿",
            "signal": _driver_signal(north_money_5d, good_threshold=80, bad_threshold=-80),
            "hint": "外资方向与强度，常领先于风格切换",
        },
        {
            "key": "north_flow_impulse",
            "label": "北向动量偏离",
            "value": round(north_flow_impulse, 1),
            "display": f"{north_flow_impulse:+.1f} 亿",
            "signal": _driver_signal(north_flow_impulse, good_threshold=50, bad_threshold=-50),
            "hint": "近5日相对20日基线的增减速，识别资金拐点",
        },
        {
            "key": "vix_close",
            "label": "波动温度(VIX)",
            "value": round(vix_close, 2),
            "display": f"{vix_close:.2f}",
            "signal": _driver_signal(vix_close, good_threshold=16, bad_threshold=24, reverse=True),
            "hint": "外部风险温度计，越高越需收缩仓位",
        },
        {
            "key": "storage_fresh_pct",
            "label": "数据新鲜度",
            "value": storage_fresh_pct,
            "display": f"{storage_fresh_pct:.1f}%",
            "signal": _driver_signal(storage_fresh_pct, good_threshold=90, bad_threshold=60),
            "hint": "快照新鲜度不足时，降低策略权重",
        },
    ]

    driver_map = {
        item["key"]: {
            "value": item.get("value"),
            "signal": item.get("signal"),
            "comment": item.get("hint"),
            "band": item.get("display"),
        }
        for item in drivers
    }
    opportunity_summary = {
        **workbench_summary,
        "buy_count": buy_count,
        "buy_ratio_pct": buy_ratio_pct,
        "top_setup": top_setup,
    }
    strategy_stability = db.get_strategy_perf_overview(windows=[20, 60, 120])

    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "regime": {
            "label": regime_label,
            "score": regime_score,
            "execution_mode": execution_mode,
        },
        "drivers": drivers,
        "drivers_map": driver_map,
        "macro": {
            "north_money_5d": round(north_money_5d, 2),
            "north_money_20d_avg": round(north_money_20d_avg, 2),
            "north_flow_impulse": round(north_flow_impulse, 2),
            "north_flow_regime": _flow_impulse_label(north_flow_impulse),
            "north_money_trend": _macro_trend_label(north_money_5d),
            "breadth_ratio": round(breadth_ratio, 2),
            "limit_ratio": round(limit_ratio, 2),
            "limit_net": int(limit_net),
            "vix_close": round(vix_close, 2),
        },
        "action_matrix": action_matrix,
        "opportunity_snapshot": opportunity_summary,
        "opportunity_summary": opportunity_summary,
        "watch_summary": watch_summary,
        "strategy_stability": {
            "as_of_date": strategy_stability.get("as_of_date"),
            "windows": strategy_stability.get("windows", []),
            "setups": strategy_stability.get("setups", []),
            "trend_120d": strategy_stability.get("trend_120d", []),
            "recent_signals": strategy_stability.get("recent_signals", []),
        },
        "data_health": {
            "summary": health_summary,
            "storage_fresh_pct": storage_fresh_pct,
            "fresh_count": fresh_count,
            "total_storage": len(storage_items),
        },
        "storage": watch_data.get("storage") or overview.get("storage") or {},
        "local_storage": {
            "db_path": DB_PATH,
            "db_exists": os.path.exists(DB_PATH),
            "snapshot_keys": [item.get("snapshot_key") for item in storage_items],
        },
    }


@router.get("/api/practical-brief")
async def get_practical_brief():
    """面向日常投资执行的一页式聚合结果。"""
    return await _build_practical_brief()


@router.get("/api/watch-stocks")
async def get_watch_stocks():
    """获取关注股票行情"""
    service = get_investment_service()
    return service.get_watch_stocks()


@router.post("/api/strategy/sync")
async def strategy_sync(
    force_rebuild: bool = False,
    windows: str = "20,60,120",
    allow_workbench_fallback: bool = True,
):
    """同步策略日志与分层绩效快照。"""
    db = get_db_service()
    return {
        "ok": True,
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "result": db.sync_strategy_runtime_data(
            force_rebuild=force_rebuild,
            windows=_parse_windows(windows),
            allow_workbench_fallback=allow_workbench_fallback,
        ),
    }


@router.get("/api/strategy/perf")
async def strategy_perf(
    setup_name: Optional[str] = None,
    windows: str = "20,60,120",
    auto_sync: bool = True,
):
    """
    策略分层稳定性视图:
    - 近20/60/120样本胜率、盈亏比、回撤
    - setup分层对比
    - 最近信号日志
    """
    db = get_db_service()
    window_list = _parse_windows(windows)
    sync_meta: Dict[str, Any] = {}
    if auto_sync:
        sync_meta = db.sync_strategy_runtime_data(
            windows=window_list,
            allow_workbench_fallback=False,
        )

    payload = db.get_strategy_perf_overview(setup_name=setup_name, windows=window_list)
    payload["generated_at"] = datetime.now().replace(microsecond=0).isoformat()
    payload["sync"] = sync_meta
    return payload


@router.get("/api/strategy/journal")
async def strategy_journal(limit: int = 40, setup_name: Optional[str] = None):
    """最近策略信号日志。"""
    db = get_db_service()
    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "count": max(1, min(int(limit or 40), 500)),
        "items": db.get_signal_journal(limit=limit, setup_name=setup_name),
    }


@router.get("/api/strategy/preopen")
async def strategy_preopen(force_refresh: bool = False, persist: bool = True):
    """
    盘前策略聚合:
    - 前一交易日策略稳定性
    - 当前分钟级市场状态/国际动态
    - 当日执行仓位与风控约束
    """
    planner = get_strategy_planner()
    return planner.build_preopen_strategy(force_refresh=force_refresh, persist=persist)


@router.get("/api/strategy/preopen/latest")
async def strategy_preopen_latest(max_age_seconds: int = 16 * 3600):
    """读取最近一次盘前策略快照。若不存在则后台异步生成，避免阻塞请求。"""
    planner = get_strategy_planner()
    payload = planner.get_latest_preopen_strategy(max_age_seconds=max(60, int(max_age_seconds or 0)))
    if payload:
        return payload

    def _build_async():
        try:
            planner.build_preopen_strategy(force_refresh=False, persist=True)
        except Exception as exc:
            print(f"盘前策略异步生成失败: {exc}")

    threading.Thread(target=_build_async, daemon=True).start()
    return {
        "status": "building",
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "message": "盘前策略快照尚未就绪，已触发后台生成。",
    }


@router.get("/api/intelligence/brief/latest")
async def intelligence_brief_latest(max_age_seconds: int = 16 * 3600):
    """读取最近百炼增强情报, 若不存在则即时生成。"""
    service = get_coding_plan_service()
    payload = service.get_latest_daily_brief(max_age_seconds=max(60, int(max_age_seconds or 0)))
    if payload:
        return payload
    return service.generate_daily_brief(force_refresh=False, persist=True)


@router.post("/api/intelligence/brief/refresh")
async def intelligence_brief_refresh(force_refresh: bool = True):
    """强制刷新百炼增强情报。"""
    service = get_coding_plan_service()
    return service.generate_daily_brief(force_refresh=force_refresh, persist=True)


@router.get("/api/intelligence/hub")
async def intelligence_hub_overview():
    """获取重大事项情报雷达概览。"""
    service = get_intelligence_service()
    return service.get_overview()


@router.get("/api/intelligence/events")
async def intelligence_events(limit: int = 50, priority: str = None, category: str = None):
    """获取已入库的重大事项。"""
    service = get_intelligence_service()
    return {"data": service.list_events(limit=limit, priority=priority, category=category)}


@router.get("/api/intelligence/events/{event_key}")
async def intelligence_event_detail(event_key: str):
    """获取事件档案。"""
    service = get_intelligence_service()
    payload = service.get_event(event_key)
    if not payload:
        raise HTTPException(status_code=404, detail="event not found")
    return payload


@router.get("/api/intelligence/research")
async def intelligence_research(limit: int = 50):
    """获取研报和深度材料库。"""
    service = get_intelligence_service()
    return {"data": service.list_research(limit=limit)}


@router.get("/api/intelligence/sources")
async def intelligence_sources():
    """获取情报源健康状态。"""
    service = get_intelligence_service()
    return {"data": service.list_sources()}


@router.post("/api/runtime/minute-refresh")
async def runtime_minute_refresh(sync_strategy: bool = True, wait: bool = False):
    """
    分钟级刷新接口:
    - 强制刷新概览与自选快照
    - 可选刷新策略运行时统计
    - 盘前策略快照更新
    """
    def _run_job() -> Dict[str, Any]:
        realtime = get_investment_service()
        db = get_db_service()
        planner = get_strategy_planner()

        overview = realtime.get_market_overview(force_refresh=True)
        watch = realtime.get_watch_stocks(force_refresh=True)
        sync_result: Dict[str, Any] = {"skipped": True}
        if sync_strategy:
            sync_result = db.sync_strategy_runtime_data(
                windows=[20, 60, 120],
                allow_workbench_fallback=False,
            )
        preopen = planner.build_preopen_strategy(force_refresh=False, persist=True)
        return {
            "ok": True,
            "generated_at": datetime.now().replace(microsecond=0).isoformat(),
            "overview_storage": overview.get("storage"),
            "watch_storage": watch.get("storage"),
            "strategy_sync": sync_result,
            "preopen_generated_at": preopen.get("generated_at"),
        }

    if wait:
        return _run_job()

    with _runtime_refresh_lock:
        if _runtime_refresh_state.get("running"):
            return {
                "ok": True,
                "status": "running",
                "generated_at": datetime.now().replace(microsecond=0).isoformat(),
                "state": dict(_runtime_refresh_state),
            }
        _runtime_refresh_state["running"] = True
        _runtime_refresh_state["started_at"] = datetime.now().replace(microsecond=0).isoformat()
        _runtime_refresh_state["last_error"] = None

    def _worker():
        try:
            result = _run_job()
            with _runtime_refresh_lock:
                _runtime_refresh_state["last_result"] = result
                _runtime_refresh_state["last_error"] = None
        except Exception as exc:
            with _runtime_refresh_lock:
                _runtime_refresh_state["last_error"] = str(exc)[:300]
        finally:
            with _runtime_refresh_lock:
                _runtime_refresh_state["running"] = False
                _runtime_refresh_state["finished_at"] = datetime.now().replace(microsecond=0).isoformat()

    threading.Thread(target=_worker, daemon=True).start()
    return {
        "ok": True,
        "status": "queued",
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "state": dict(_runtime_refresh_state),
    }


@router.get("/api/index-history/{symbol}")
async def get_index_history(symbol: str, days: int = 365):
    """获取指数历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_index_history(symbol, days)
    return {"symbol": symbol, "data": data}


@router.get("/api/interest-rates")
async def get_interest_rates(days: int = 365):
    """获取利率历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_interest_rates(days)
    return {"data": data}


@router.get("/api/north-money")
async def get_north_money(days: int = 180):
    """获取北向资金数据 - 增强版，优先走北向资金服务"""
    service = get_north_flow_service()
    data = service.get_north_daily(days)
    if data:
        return {"data": data, "source": "north_flow_service"}
    db = get_db_service()
    data = db.get_north_money(days)
    return {"data": data, "source": "db_fallback"}


@router.get("/api/north/flow-daily")
async def get_north_flow_daily(days: int = 180):
    """北向日度流向"""
    service = get_north_flow_service()
    data = service.get_north_daily(days)
    return {"data": data}


@router.get("/api/north/stock-hold")
async def get_north_stock_hold():
    """北向个股持仓排行 - 前端字段名适配"""
    service = get_north_flow_service()
    data = service.get_north_stock_hold()
    # 字段名适配
    holdings = []
    for d in data:
        holdings.append({
            "code": d.get("stock_code", ""),
            "name": d.get("stock_name", ""),
            "price": d.get("price"),
            "change_pct": d.get("change_pct"),
            "hold_ratio": d.get("hold_ratio"),
            "hold_value": d.get("hold_value"),
            "change_pct_1d": d.get("change_pct_1d"),
        })
    return {"total": len(holdings), "holdings": holdings}


@router.get("/api/north/sector-flow")
async def get_north_sector_flow():
    """北向板块流向 - 前端字段名适配"""
    service = get_north_flow_service()
    data = service.get_north_sector_flow()
    # 字段名适配
    sectors = []
    for d in data:
        sectors.append({
            "sector_name": d.get("sector", ""),
            "name": d.get("sector", ""),
            "net_inflow": d.get("total_change", 0),
        })
    return {"sectors": sectors}


@router.get("/api/north/summary")
async def get_north_summary(days: int = 30):
    """北向资金总结 - 前端字段名适配"""
    service = get_north_flow_service()
    data = service.get_north_summary(days)
    summary = data.get("summary", {})
    # 字段名适配：前端期望 five_day_net, twenty_day_net, today_net, avg_5d
    daily = data.get("daily_trend", [])
    today_net = (daily[-1].get("total_net") or 0) if daily else 0
    last_5 = daily[-5:] if len(daily) >= 5 else daily
    avg_5d = sum(d.get("total_net") or 0 for d in last_5) / max(len(last_5), 1)

    return {
        "today_net": round(today_net, 1),
        "five_day_net": summary.get("total_net_5d", 0),
        "twenty_day_net": summary.get("total_net_20d", 0),
        "avg_5d": round(avg_5d, 1),
        "inflow_ratio_pct": summary.get("inflow_ratio_pct", 0),
        "inflow_days": summary.get("inflow_days", 0),
    }


# ==================== 港股数据增强模块 ====================

@router.get("/api/hk/hot-rank")
async def get_hk_hot_rank(limit: int = 50):
    """港股热门排行 (DB优先 + API降级)"""
    db = get_db_service()
    data = db.get_hk_hot_rank_latest(limit)
    if not data:
        service = get_investment_service()
        data = service.get_hk_stock_hot_rank(limit)
    return {"total": len(data), "data": data}


@router.get("/api/hk/indices")
async def get_hk_indices():
    """港股主要指数行情 (DB优先 + API降级)"""
    db = get_db_service()
    data = db.get_hk_indices_latest()
    if not data:
        service = get_investment_service()
        data = service.get_hk_index_list()
    return {"data": data}


@router.get("/api/hk/history/{symbol}")
async def get_hk_history(symbol: str, days: int = 365):
    """港股历史行情"""
    service = get_investment_service()
    data = service.get_hk_stock_history(symbol, days)
    return {"symbol": symbol, "total": len(data), "data": data}


@router.get("/api/hk/financial/{symbol}")
async def get_hk_financial(symbol: str):
    """港股财务数据"""
    service = get_investment_service()
    data = service.get_hk_stock_financial(symbol)
    return {"symbol": symbol, "data": data}


@router.get("/api/vix-history")
async def get_vix_history(days: int = 365):
    """获取VIX历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_vix_history(days)
    return {"data": data}


@router.get("/api/global-risk")
async def get_global_risk(days: int = 180):
    """获取全球风险雷达数据"""
    service = get_investment_service()
    return service.get_global_risk_radar(days=days)


@router.get("/api/a-stocks")
async def get_a_stocks(keywords: str = None):
    """获取A股行情"""
    service = get_investment_service()
    kw_list = keywords.split(",") if keywords else None
    return service.get_a_stocks_direct(kw_list)


@router.get("/api/hk-stocks")
async def get_hk_stocks(keywords: str = None):
    """获取港股行情"""
    service = get_investment_service()
    kw_list = keywords.split(",") if keywords else None
    return service.get_hk_stocks_direct(kw_list)


@router.post("/api/import-index-data")
async def import_index_data(data: Dict):
    """导入指数数据 - 从阿里云服务器接收"""
    code = data.get("code")
    name = data.get("name")
    records = data.get("data", [])

    if not code or not records:
        return {"status": "error", "message": "缺少数据"}

    try:
        conn = get_sqlite_connection(DB_PATH)
        c = conn.cursor()

        added = 0
        for record in records:
            try:
                c.execute('''
                    INSERT OR REPLACE INTO index_history
                    (code, name, trade_date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code, name, record.get("date"),
                    float(record.get("open", 0) or 0),
                    float(record.get("high", 0) or 0),
                    float(record.get("low", 0) or 0),
                    float(record.get("close", 0) or 0),
                    float(record.get("volume", 0) or 0)
                ))
                added += 1
            except Exception as ex:
                continue

        conn.commit()
        conn.close()

        return {"status": "success", "code": code, "name": name, "added": added}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/api/news")
async def get_financial_news():
    """获取金融新闻"""
    service = get_news_service()
    return service.get_all_news()


@router.get("/api/research/public-hub")
async def get_public_research_hub():
    """获取公开国际研究聚合数据"""
    service = get_public_research_service()
    return service.get_public_research_hub()


# ==================== 宏观流动性模块 ====================

@router.get("/api/macro/overview")
async def get_macro_overview(force_realtime: bool = False):
    """获取宏观流动性概览"""
    db = get_db_service()
    north_money = db.get_north_money(30)

    snapshot = db.get_market_snapshot("investment.market_overview.v2")
    if snapshot and snapshot.get("payload"):
        payload = snapshot.get("payload") or {}
        rates = payload.get("rates") or db.get_interest_rates_latest()
        sentiment = payload.get("sentiment") or db.get_market_sentiment_latest()
        fear_greed = payload.get("fear_greed") or {}
        vix_raw = fear_greed.get("vix") or {}
        vix = {
            "close": vix_raw.get("value"),
            "change_pct": vix_raw.get("change_pct"),
            "source": vix_raw.get("source"),
        } if vix_raw else db.get_vix_latest()
    else:
        rates = db.get_interest_rates_latest()
        sentiment = db.get_market_sentiment_latest()
        vix = db.get_vix_latest()

    if force_realtime:
        realtime = get_investment_service()
        try:
            realtime_data = realtime.get_market_overview(force_refresh=True)
            rates = realtime_data.get("rates", {}) or rates
            vix_raw = realtime_data.get("fear_greed", {}).get("vix", {})
            if vix_raw:
                vix = {
                    "close": vix_raw.get("value"),
                    "change_pct": vix_raw.get("change_pct"),
                    "source": vix_raw.get("source"),
                }
            sentiment = realtime_data.get("sentiment", {}) or sentiment
        except Exception as exc:
            print(f"macro实时刷新失败，继续使用本地快照: {exc}")

    recent = sum(_to_float(n.get("total_inflow"), 0.0) for n in north_money[-5:]) if north_money else 0.0
    north_money_20d_avg = _safe_div(
        sum(_to_float(row.get("total_inflow"), 0.0) for row in north_money[-20:]),
        max(1, min(20, len(north_money))),
        0.0,
    )
    north_flow_impulse = recent - (north_money_20d_avg * 5.0)
    trend = _macro_trend_label(recent)

    up_count = _to_float((sentiment or {}).get("up_count"), 0.0)
    down_count = _to_float((sentiment or {}).get("down_count"), 0.0)
    limit_up_count = _to_float((sentiment or {}).get("limit_up_count"), 0.0)
    limit_down_count = _to_float((sentiment or {}).get("limit_down_count"), 0.0)
    breadth_ratio = _safe_div(up_count, max(1.0, down_count), 0.0)
    limit_ratio = _safe_div(limit_up_count, max(1.0, limit_down_count), 0.0)
    limit_net = limit_up_count - limit_down_count

    vix_close = _to_float((vix or {}).get("close"), 0.0)
    vix_band = "high" if vix_close >= 24 else "low" if vix_close <= 16 else "normal"

    regime = compute_regime(
        breadth_ratio=breadth_ratio,
        limit_ratio=limit_ratio,
        north_flow_impulse=north_flow_impulse,
        vix_close=vix_close,
    )
    regime_label = regime["label"]
    regime_score = regime["score"]

    dimensions = [
        {
            "key": "volatility",
            "label": "波动",
            "value": round(vix_close, 2),
            "display": f"VIX {vix_close:.2f}",
            "status": "positive" if vix_band == "low" else "negative" if vix_band == "high" else "neutral",
            "hint": "高波动下收缩仓位，低波动提高进攻权重",
        },
        {
            "key": "capital_flow",
            "label": "北向资金",
            "value": round(recent, 1),
            "display": f"{recent:+.1f} 亿",
            "status": "positive" if recent > 0 else "negative" if recent < 0 else "neutral",
            "hint": "北向持续流入时优先顺势配置",
        },
        {
            "key": "flow_impulse",
            "label": "资金动量",
            "value": round(north_flow_impulse, 1),
            "display": f"{north_flow_impulse:+.1f} 亿",
            "status": "positive" if north_flow_impulse >= 50 else "negative" if north_flow_impulse <= -50 else "neutral",
            "hint": "近5日相对20日基线，识别资金加速或转弱",
        },
        {
            "key": "breadth",
            "label": "广度",
            "value": round(breadth_ratio, 2),
            "display": f"涨跌比 {breadth_ratio:.2f}",
            "status": "positive" if breadth_ratio >= 1.1 else "negative" if breadth_ratio <= 0.9 else "neutral",
            "hint": "广度恶化时减少尾部题材仓位",
        },
        {
            "key": "limit_structure",
            "label": "涨跌停结构",
            "value": round(limit_ratio, 2),
            "display": f"涨停/跌停 {limit_up_count:.0f}/{limit_down_count:.0f}",
            "status": "positive" if limit_ratio >= 1.5 else "negative" if limit_ratio <= 0.8 else "neutral",
            "hint": "连板扩散优于跌停扩散时，进攻成功率更高",
        },
    ]

    return {
        "rates": rates,
        "vix": vix,
        "sentiment": sentiment,
        "north_money_trend": trend,
        "north_money_5d": recent,
        "north_money_20d_avg": round(north_money_20d_avg, 2),
        "north_flow_impulse": round(north_flow_impulse, 2),
        "breadth_ratio": round(breadth_ratio, 2),
        "limit_ratio": round(limit_ratio, 2),
        "limit_net": int(limit_net),
        "vix_band": vix_band,
        "regime": {
            "label": regime_label,
            "score": regime_score,
        },
        "dimensions": dimensions,
    }


@router.get("/api/macro/rates-history")
async def get_rates_history(days: int = 365):
    """获取利率历史"""
    db = get_db_service()
    data = db.get_interest_rates(days)
    return {"data": data}


@router.get("/api/macro/liquidity-indicators")
async def get_liquidity_indicators():
    """获取市场风向标指标"""
    macro = await get_macro_overview(force_realtime=False)
    vix = macro.get("vix") or {}

    breadth_ratio = _to_float(macro.get("breadth_ratio"), 0.0) or 0.0
    limit_ratio = _to_float(macro.get("limit_ratio"), 0.0) or 0.0
    north_money_5d = _to_float(macro.get("north_money_5d"), 0.0) or 0.0
    north_flow_impulse = _to_float(macro.get("north_flow_impulse"), 0.0) or 0.0
    vix_close = _to_float(vix.get("close"), 0.0) or 0.0

    indicators = [
        {
            "name": "北向5日净流入",
            "value": round(north_money_5d, 1),
            "display": f"{north_money_5d:+.1f} 亿",
            "level": "positive" if north_money_5d >= 0 else "negative",
            "comment": "观察外资是否持续给增量流动性",
        },
        {
            "name": "北向动量偏离(5D-20D基线)",
            "value": round(north_flow_impulse, 1),
            "display": f"{north_flow_impulse:+.1f} 亿",
            "level": "positive" if north_flow_impulse >= 50 else "negative" if north_flow_impulse <= -50 else "neutral",
            "comment": "资金在加速流入还是加速流出",
        },
        {
            "name": "市场广度(涨跌比)",
            "value": round(breadth_ratio, 2),
            "display": f"{breadth_ratio:.2f} x",
            "level": "positive" if breadth_ratio >= 1.1 else "negative" if breadth_ratio <= 0.9 else "neutral",
            "comment": "广度健康时，策略胜率更稳定",
        },
        {
            "name": "涨跌停结构",
            "value": round(limit_ratio, 2),
            "display": f"{limit_ratio:.2f} x",
            "level": "positive" if limit_ratio >= 1.5 else "negative" if limit_ratio <= 0.8 else "neutral",
            "comment": "连板扩散优于跌停扩散，风险偏好更强",
        },
        {
            "name": "VIX风险温度",
            "value": round(vix_close, 2),
            "display": f"{vix_close:.2f}",
            "level": "positive" if vix_close <= 16 else "negative" if vix_close >= 24 else "neutral",
            "comment": "外盘波动上行时需降低仓位弹性",
        },
    ]

    return {"indicators": indicators}


# ==================== 微观基本面模块 ====================

@router.get("/api/fundamentals/stocks")
async def get_stocks_fundamentals(codes: str = None):
    """获取股票基本面数据"""
    db = get_db_service()
    code_list = codes.split(",") if codes else None
    data = db.get_stock_fundamentals(code_list)
    return {"stocks": data}


@router.get("/api/fundamentals/watch-list")
async def get_watch_list_fundamentals():
    """获取关注股票的基本面数据"""
    db = get_db_service()
    data = db.get_watch_stocks_fundamentals()
    return {"stocks": data}


@router.get("/api/opportunity-pools/overview")
async def get_opportunity_pool_overview(pool: str = "all", limit: int = 180):
    """核心股票池总览与评分榜。"""
    db = get_db_service()
    return db.get_opportunity_pool_overview(pool_code=pool, limit=limit)


@router.get("/api/opportunity-pools/detail/{code}")
async def get_opportunity_pool_detail(code: str, pool: str = "all"):
    """单只股票的机会池下钻分析。"""
    db = get_db_service()
    try:
        return db.get_opportunity_stock_detail(code=code, pool_code=pool)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown stock: {code}") from exc


@router.post("/api/opportunity-pools/sync")
async def sync_opportunity_pools(pool: str = "all"):
    """同步核心股票池到本地 SQLite。"""
    db = get_db_service()
    return {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "result": db.sync_stock_pools(pool_code=pool),
    }


@router.get("/api/fundamentals/financial/{code}")
async def get_stock_financial(code: str):
    """获取单只股票财务数据"""
    # TODO: 实现
    return {"code": code, "data": []}


# ==================== 行业模型模块 ====================

@router.get("/api/sector/tmt")
async def get_tmt_sector(code: str = None):
    """获取TMT行业数据"""
    db = get_db_service()
    data = db.get_tmt_metrics(code)
    return {"data": data}


@router.get("/api/sector/biotech")
async def get_biotech_sector(company: str = None, phase: str = None):
    """获取创新药管线数据"""
    db = get_db_service()
    data = db.get_biotech_pipeline(company, phase)
    return {"data": data}


@router.get("/api/sector/consumer")
async def get_consumer_sector(code: str = None):
    """获取消费行业数据"""
    db = get_db_service()
    data = db.get_consumer_metrics(code)
    return {"data": data}


@router.get("/api/sector/overview")
async def get_sector_overview():
    """获取行业概览"""
    db = get_db_service()
    return {
        "tmt_count": len(db.get_tmt_metrics()),
        "biotech_count": len(db.get_biotech_pipeline()),
        "consumer_count": len(db.get_consumer_metrics())
    }


# ==================== 量化技术模块 ====================

@router.get("/api/quant/valuation")
async def get_valuation(code: str = None):
    """获取估值水位数据"""
    db = get_db_service()
    data = db.get_valuation_latest(code)
    return {"data": data}


@router.get("/api/quant/valuation-history/{code}")
async def get_valuation_history(code: str, days: int = 365):
    """获取估值历史"""
    db = get_db_service()
    data = db.get_valuation_history(code, days)
    return {"code": code, "data": data}


@router.get("/api/quant/technical")
async def get_technical(code: str = None):
    """获取技术指标"""
    db = get_db_service()
    data = db.get_technical_latest(code)
    return {"data": data}


@router.get("/api/quant/technical-history/{code}")
async def get_technical_history(code: str, days: int = 365):
    """获取技术指标历史"""
    db = get_db_service()
    data = db.get_technical_history(code, days)
    return {"code": code, "data": data}


@router.get("/api/quant/screener")
async def stock_screener(
    pe_max: float = None,
    pe_min: float = None,
    pb_max: float = None,
    rsi_max: float = None,
    rsi_min: float = None
):
    """股票筛选器"""
    db = get_db_service()
    valuation = db.get_valuation_latest()
    technical = db.get_technical_latest()

    # 合并数据
    result = []
    tech_dict = {t["code"]: t for t in technical}
    for v in valuation:
        item = {**v}
        if v["code"] in tech_dict:
            item.update(tech_dict[v["code"]])

        # 应用筛选条件
        if pe_max and (item.get("pe_ttm") or 999) > pe_max:
            continue
        if pe_min and (item.get("pe_ttm") or 0) < pe_min:
            continue
        if pb_max and (item.get("pb") or 999) > pb_max:
            continue
        if rsi_max and (item.get("rsi_14") or 999) > rsi_max:
            continue
        if rsi_min and (item.get("rsi_14") or 0) < rsi_min:
            continue

        result.append(item)

    return {"results": result, "count": len(result)}


# ==================== 数据管理模块 ====================

@router.get("/api/data-health/overview")
async def get_data_health_overview():
    """获取投资中枢数据健康概览"""
    db = get_db_service()
    return db.get_data_health_overview(
        {
            "investment.market_overview.v2": 300,
            "investment.watch_stocks.v2": 300,
        }
    )


@router.get("/api/data-health/assets")
async def get_data_asset_overview():
    """获取数据资产状态，用于指标下线和页面过滤。"""
    db = get_db_service()
    return db.get_data_asset_overview()


@router.get("/api/etl/logs")
async def get_etl_logs(limit: int = 50, job_type: str = None):
    """获取ETL日志"""
    db = get_db_service()
    data = db.get_etl_logs(limit, job_type)
    return {"data": data}


@router.post("/api/etl/import-csv")
async def import_csv(data: Dict):
    """导入CSV数据"""
    db = get_db_service()
    table_name = data.get("table")
    csv_path = data.get("path")

    if not table_name or not csv_path:
        return {"status": "error", "message": "缺少表名或文件路径"}

    result = db.import_csv_to_table(table_name, csv_path)
    return result


@router.get("/api/etl/tables")
async def get_db_tables():
    """获取数据库表列表"""
    conn = get_sqlite_connection(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall()]

    result = []
    for table in tables:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        count = c.fetchone()[0]
        result.append({"name": table, "count": count})

    conn.close()
    return {"tables": result}


@router.get("/api/etl/status")
async def get_etl_status():
    """获取ETL状态"""
    db = get_db_service()
    logs = db.get_etl_logs(10)

    last_success = None
    last_error = None
    for log in logs:
        if log["status"] == "success" and not last_success:
            last_success = log
        if log["status"] in {"error", "failed"} and not last_error:
            last_error = log

    return {
        "recent_logs": logs[:5],
        "last_success": last_success,
        "last_error": last_error
    }


# ==================== 量化工作台 ====================

@router.get("/api/workbench/overview")
async def workbench_overview():
    service = get_workbench_service()
    status = service.get_status()
    error: Optional[str] = None
    daily_files = int(status.get("daily_files") or 0)
    available_daily_files = int(status.get("available_daily_files") or 0)
    error_count = int(status.get("error_count") or 0)
    skip_recompute = daily_files == 0 and error_count > 0

    if skip_recompute:
        opportunities = []
        summary = _summarize_workbench(opportunities)
        market_regime = {
            "label": "unknown",
            "score": 0,
            "reasons": ["行情同步缺失，先修复 quant_workbench 数据源"],
        }
        error = (
            "daily_files=0 且 error_count>0，已跳过候选重算"
            f" (available_daily_files={available_daily_files})"
        )
    else:
        try:
            opportunities = service.list_opportunities()
            summary = _summarize_workbench(opportunities)
            market_regime = service.get_market_regime()
        except Exception as exc:
            print(f"workbench_overview失败: {exc}")
            opportunities = []
            summary = _summarize_workbench(opportunities)
            market_regime = {
                "label": "unknown",
                "score": 0,
                "reasons": ["量化工作台计算失败，请检查同步日志"],
            }
            error = str(exc)
    return {
        "status": status,
        "market_regime": market_regime,
        "summary": summary,
        "opportunities": opportunities,
        "top_candidates": summary["top_candidates"],
        "generated_at": status.get("last_sync_at"),
        "error": error,
    }


@router.get("/api/workbench/stocks/{code}")
async def workbench_stock_detail(code: str):
    service = get_workbench_service()
    try:
        return service.get_stock_detail(code)
    except KeyError:
        return {"error": f"unknown stock: {code}"}


@router.get("/api/workbench/setups/overview")
async def workbench_setups_overview():
    service = get_workbench_service()
    items = service.list_opportunities()
    groups: Dict[str, Dict[str, Any]] = {}
    for item in items:
        setup_name = item.get("setup_name") or "unknown"
        group = groups.setdefault(
            setup_name,
            {
                "setup_name": setup_name,
                "setup_label": item.get("setup_label") or setup_name,
                "count": 0,
                "average_score": 0.0,
                "top_candidate": None,
            },
        )
        group["count"] += 1
        group["average_score"] += float(item.get("score", 0) or 0)
        if group["top_candidate"] is None:
            group["top_candidate"] = item

    result = []
    for group in groups.values():
        if group["count"]:
            group["average_score"] = round(group["average_score"] / group["count"], 1)
        result.append(group)

    result.sort(key=lambda item: (item["count"], item["average_score"]), reverse=True)
    return {"items": result}


@router.get("/api/workbench/setups/{setup_name}/candidates")
async def workbench_setup_candidates(setup_name: str):
    service = get_workbench_service()
    items = [item for item in service.list_opportunities() if item.get("setup_name") == setup_name]
    return {
        "setup_name": setup_name,
        "count": len(items),
        "items": items,
    }


@router.post("/api/workbench/refresh")
async def workbench_refresh():
    syncer = QuantWorkbenchSync()
    result = syncer.run()
    service = get_workbench_service()
    status = service.get_status()
    return {
        "ok": True,
        "result": result,
        "status": status,
    }


@router.get("/api/runtime/profile")
async def get_runtime_profile(request: Request):
    """获取当前 Investment Hub 运行时配置."""
    profile = get_investment_runtime_profile()
    host = request.url.hostname or settings.host
    port = request.url.port or settings.port
    profile.update({
        "service_host": host,
        "service_port": port,
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
    })
    return profile


# ==================== 短线执行层 ====================

@router.get("/shortline", response_class=HTMLResponse)
async def shortline_page(request: Request):
    """短线执行层页面。"""
    return templates.TemplateResponse("shortline.html", {"request": request})


@router.get("/api/shortline/overview")
async def shortline_overview(auto_refresh: bool = False):
    """短线执行层概览。"""
    svc = get_shortline_service()
    if auto_refresh:
        try:
            svc.ensure_fresh_data(max_age_hours=8)
        except Exception as exc:
            print(f"shortline auto refresh failed: {exc}")
    return svc.get_overview()


@router.get("/api/shortline/events")
async def shortline_events(limit: int = 30, theme: Optional[str] = None, auto_refresh: bool = False):
    """短线事件列表。"""
    svc = get_shortline_service()
    if auto_refresh:
        try:
            svc.ensure_fresh_data(max_age_hours=8)
        except Exception as exc:
            print(f"shortline events auto refresh failed: {exc}")
    items = svc.list_events(limit=limit, theme=theme)
    return {"total": len(items), "items": items}


@router.get("/api/shortline/candidates")
async def shortline_candidates(
    limit: int = 60,
    priority: Optional[str] = None,
    theme: Optional[str] = None,
    market: Optional[str] = None,
    auto_refresh: bool = False,
):
    """短线候选列表。"""
    svc = get_shortline_service()
    if auto_refresh:
        try:
            svc.ensure_fresh_data(max_age_hours=8)
        except Exception as exc:
            print(f"shortline candidates auto refresh failed: {exc}")
    items = svc.list_candidates(limit=limit, priority=priority, theme=theme, market=market)
    return {"total": len(items), "items": items}


@router.get("/api/shortline/playbooks")
async def shortline_playbooks():
    """短线套利模板。"""
    svc = get_shortline_service()
    items = svc.list_playbooks()
    return {"total": len(items), "items": items}


@router.post("/api/shortline/refresh")
async def shortline_refresh(include_official: bool = True, translate: bool = False):
    """刷新短线执行层事件与候选。"""
    svc = get_shortline_service()
    return svc.refresh_pipeline(include_official=include_official, translate=translate)


# ==================== 研究工作台 ====================

@router.get("/research", response_class=HTMLResponse)
async def research_workbench_page(request: Request):
    """研究工作台页面。"""
    return templates.TemplateResponse("research_workbench.html", {"request": request})


@router.get("/api/research/workbench")
async def research_workbench_overview():
    """研究工作台聚合概览。"""
    svc = get_research_workbench_svc()
    try:
        return svc.get_overview()
    except Exception as exc:
        print(f"research_workbench_overview失败: {exc}")
        return {"error": str(exc), "total": 0}


@router.get("/api/research/list")
async def research_workbench_list(
    limit: int = 50,
    focus_area: Optional[str] = None,
    publisher_region: Optional[str] = None,
    target_scope: Optional[str] = None,
    report_type: Optional[str] = None,
    query: Optional[str] = None,
):
    """研究工作台报告列表（支持筛选）。"""
    svc = get_research_workbench_svc()
    try:
        items = svc.list_reports(
            limit=limit,
            focus_area=focus_area,
            publisher_region=publisher_region,
            target_scope=target_scope,
            report_type=report_type,
            query=query,
        )
        return {"total": len(items), "items": items, "reports": items}
    except Exception as exc:
        print(f"research_workbench_list失败: {exc}")
        return {"error": str(exc), "total": 0, "items": []}


@router.get("/api/research/{report_key}")
async def research_workbench_detail(report_key: str):
    """单份研究报告详情（含证据链）。"""
    svc = get_research_workbench_svc()
    try:
        detail = svc.get_report_detail(report_key)
        if not detail:
            raise HTTPException(status_code=404, detail=f"report not found: {report_key}")
        return detail
    except HTTPException:
        raise
    except Exception as exc:
        print(f"research_workbench_detail失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
