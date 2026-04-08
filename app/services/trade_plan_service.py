"""
交易计划生成服务
将信号转化为结构化可执行计划
"""
from __future__ import annotations

import sqlite3
import json
import hashlib
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"

# ==================== 默认参数配置 ====================

DEFAULT_PARAMS = {
    "catalyst_breakout": {
        "entry_rule": "突破前高或放量启动时入场",
        "max_chase_pct": 3.0,
        "stop_loss_pct": 5.0,
        "take_profit_pct": 10.0,
        "time_stop_days": 5,
        "max_position_pct": 5.0,
        "invalidation_rule": "事件落空或大幅低开",
    },
    "trend_continuation": {
        "entry_rule": "回调至均线支撑或突破确认后入场",
        "max_chase_pct": 2.0,
        "stop_loss_pct": 6.0,
        "take_profit_pct": 12.0,
        "time_stop_days": 10,
        "max_position_pct": 6.0,
        "invalidation_rule": "趋势反转或跌破关键支撑",
    },
    "quality_rerate": {
        "entry_rule": "估值低点或基本面改善确认时入场",
        "max_chase_pct": 2.0,
        "stop_loss_pct": 8.0,
        "take_profit_pct": 15.0,
        "time_stop_days": 20,
        "max_position_pct": 8.0,
        "invalidation_rule": "基本面恶化或估值修复完成",
    },
}

# Regime调整因子
REGIME_ADJUSTMENTS = {
    "risk_off": {
        "max_position_multiplier": 0.5,
        "stop_loss_multiplier": 0.8,  # 更紧止损
        "take_profit_multiplier": 0.7,  # 更早止盈
        "min_grade": "A",
    },
    "risk_on": {
        "max_position_multiplier": 1.2,
        "stop_loss_multiplier": 1.0,
        "take_profit_multiplier": 1.2,
        "min_grade": "B",
    },
    "neutral": {
        "max_position_multiplier": 1.0,
        "stop_loss_multiplier": 1.0,
        "take_profit_multiplier": 1.0,
        "min_grade": "B",
    },
}


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except:
        return default


def generate_plan_id(signal_id: str, plan_date: str) -> str:
    """生成交易计划ID"""
    raw = f"{signal_id}_{plan_date}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


class TradePlanGenerator:
    """交易计划生成器"""

    def __init__(self, regime: str = "neutral"):
        self.regime = regime
        self.adjustments = REGIME_ADJUSTMENTS.get(regime, REGIME_ADJUSTMENTS["neutral"])

    def generate_plan(self, signal: Dict[str, Any],
                      additional_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        从信号生成交易计划
        """
        now = datetime.now()
        plan_date = signal.get("as_of_date", now.strftime("%Y-%m-%d"))
        signal_id = signal.get("signal_id", "")
        symbol = signal.get("symbol", "")
        setup_type = signal.get("setup_type", "quality_rerate")
        grade = signal.get("grade", "C")

        # 获取默认参数
        base_params = DEFAULT_PARAMS.get(setup_type, DEFAULT_PARAMS["quality_rerate"])

        # 应用regime调整
        max_position = base_params["max_position_pct"] * self.adjustments["max_position_multiplier"]
        stop_loss = base_params["stop_loss_pct"] * self.adjustments["stop_loss_multiplier"]
        take_profit = base_params["take_profit_pct"] * self.adjustments["take_profit_multiplier"]

        # Grade调整
        if grade == "A":
            max_position *= 1.2
            priority_rank = 1
        elif grade == "B":
            priority_rank = 3
        else:
            max_position *= 0.5
            priority_rank = 5

        # 额外上下文
        ctx = additional_context or {}
        current_price = ctx.get("current_price")
        ma20 = ctx.get("ma20")
        atr_pct = ctx.get("atr_pct", 3.0)

        # 动态止损(基于ATR)
        if atr_pct > 4:
            stop_loss = min(stop_loss, atr_pct * 1.5)

        # 入场规则细化
        entry_rule = base_params["entry_rule"]
        if current_price and ma20:
            if current_price > ma20:
                entry_rule += f" (当前价{current_price:.2f}高于MA20)"
            else:
                entry_rule += f" (当前价{current_price:.2f}接近MA20支撑)"

        # 作废条件细化
        invalidation = base_params["invalidation_rule"]
        invalidation_conditions = [
            {"type": "price_drop", "threshold": stop_loss, "desc": f"跌幅超过{stop_loss:.1f}%"},
            {"type": "time_expiry", "threshold": base_params["time_stop_days"], "desc": f"持有超过{base_params['time_stop_days']}天未达预期"},
            {"type": "fundamental_change", "desc": "基本面出现重大负面变化"},
        ]

        # 计算入场窗口
        entry_window_start = time(9, 35).strftime("%H:%M")  # 开盘后5分钟
        entry_window_end = time(10, 30).strftime("%H:%M")   # 10:30前完成入场

        plan = {
            "trade_plan_id": generate_plan_id(signal_id, plan_date),
            "signal_id": signal_id,
            "plan_date": plan_date,
            "symbol": symbol,
            "symbol_name": signal.get("symbol_name", ""),
            "setup_type": setup_type,
            "setup_name": signal.get("setup_name", ""),

            # 入场
            "entry_rule": entry_rule,
            "entry_window_start": entry_window_start,
            "entry_window_end": entry_window_end,
            "max_chase_pct": round(base_params["max_chase_pct"], 1),

            # 止盈止损
            "stop_loss_pct": round(stop_loss, 1),
            "take_profit_pct": round(take_profit, 1),
            "time_stop_days": base_params["time_stop_days"],

            # 仓位
            "max_position_pct": round(min(max_position, 10.0), 1),  # 上限10%

            # 作废条件
            "invalidation_rule": invalidation,
            "invalidation_conditions": invalidation_conditions,

            # 优先级
            "priority_rank": priority_rank,

            # 状态
            "status": "pending",

            # 关联
            "execution_id": None,

            # 元数据
            "grade": grade,
            "score_total": signal.get("score_total", 0),
            "risk_penalty": signal.get("risk_penalty", 0),
            "regime": self.regime,

            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        return plan

    def persist_plan(self, plan: Dict[str, Any]) -> bool:
        """保存交易计划"""
        conn = get_db_connection()
        c = conn.cursor()

        try:
            c.execute("""
                INSERT OR REPLACE INTO trade_plans (
                    trade_plan_id, signal_id, plan_date, symbol, setup_type,
                    entry_rule, entry_window_start, entry_window_end, max_chase_pct,
                    stop_loss_pct, take_profit_pct, time_stop_days, max_position_pct,
                    invalidation_rule, invalidation_conditions, priority_rank, status,
                    execution_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                plan["trade_plan_id"],
                plan["signal_id"],
                plan["plan_date"],
                plan["symbol"],
                plan["setup_type"],
                plan["entry_rule"],
                plan["entry_window_start"],
                plan["entry_window_end"],
                plan["max_chase_pct"],
                plan["stop_loss_pct"],
                plan["take_profit_pct"],
                plan["time_stop_days"],
                plan["max_position_pct"],
                plan["invalidation_rule"],
                json.dumps(plan.get("invalidation_conditions", [])),
                plan["priority_rank"],
                plan["status"],
                plan.get("execution_id"),
                plan["created_at"],
                plan["updated_at"],
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"保存交易计划失败: {e}")
            return False
        finally:
            conn.close()

    def generate_daily_plans(self, as_of_date: str = None,
                              max_plans: int = 10) -> List[Dict[str, Any]]:
        """
        生成每日交易计划
        从策略信号中选择最优的N个生成计划
        """
        from app.services.strategy_v2_service import get_strategy_v2_service

        as_of_date = as_of_date or datetime.now().strftime("%Y-%m-%d")

        # 获取可执行信号
        service = get_strategy_v2_service(self.regime)
        signals = service.get_actionable_signals(as_of_date, min_grade="B")

        # 按优先级排序并取前N个
        signals = sorted(signals, key=lambda x: (
            {"A": 0, "B": 1, "C": 2}.get(x.get("grade", "C"), 2),
            -x.get("score_total", 0)
        ))[:max_plans]

        plans = []
        for signal in signals:
            plan = self.generate_plan(signal)
            self.persist_plan(plan)
            plans.append(plan)

        return plans

    def get_pending_plans(self, plan_date: str = None) -> List[Dict[str, Any]]:
        """获取待执行计划"""
        conn = get_db_connection()
        c = conn.cursor()

        date_filter = plan_date or datetime.now().strftime("%Y-%m-%d")

        c.execute("""
            SELECT * FROM trade_plans
            WHERE plan_date = ? AND status = 'pending'
            ORDER BY priority_rank, score_total DESC
        """, (date_filter,))

        results = [dict(row) for row in c.fetchall()]
        conn.close()

        return results

    def update_plan_status(self, trade_plan_id: str, status: str,
                           execution_id: str = None) -> bool:
        """更新计划状态"""
        conn = get_db_connection()
        c = conn.cursor()

        try:
            c.execute("""
                UPDATE trade_plans
                SET status = ?, execution_id = ?, updated_at = ?
                WHERE trade_plan_id = ?
            """, (status, execution_id, datetime.now().isoformat(), trade_plan_id))
            conn.commit()
            return True
        except Exception as e:
            print(f"更新计划状态失败: {e}")
            return False
        finally:
            conn.close()


# ==================== 盘前策略集成 ====================

def build_preopen_trade_plans(regime: str = None) -> Dict[str, Any]:
    """
    构建盘前交易计划
    用于strategy_planning_service集成
    """
    from app.services.strategy_v2_service import get_strategy_v2_service

    now = datetime.now()
    plan_date = now.strftime("%Y-%m-%d")

    # 获取当前regime
    if regime is None:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT payload_json FROM market_snapshots WHERE snapshot_key = 'investment.strategy.preopen.v1' ORDER BY updated_at DESC LIMIT 1")
            row = c.fetchone()
            if row:
                payload = json.loads(row["payload_json"])
                regime = payload.get("regime", "neutral")
            conn.close()
        except:
            regime = "neutral"

    # 生成计划
    generator = TradePlanGenerator(regime=regime)
    plans = generator.generate_daily_plans(plan_date, max_plans=8)

    # 统计摘要
    summary = {
        "plan_date": plan_date,
        "regime": regime,
        "total_plans": len(plans),
        "grade_distribution": {},
        "setup_distribution": {},
        "total_max_position": 0,
        "plans": plans,
    }

    for plan in plans:
        grade = plan.get("grade", "C")
        setup = plan.get("setup_type", "unknown")
        summary["grade_distribution"][grade] = summary["grade_distribution"].get(grade, 0) + 1
        summary["setup_distribution"][setup] = summary["setup_distribution"].get(setup, 0) + 1
        summary["total_max_position"] += plan.get("max_position_pct", 0)

    # 风险提示
    warnings = []
    if summary["total_max_position"] > 30:
        warnings.append(f"总计划仓位{summary['total_max_position']:.1f}%超过30%，建议分批执行")

    if regime == "risk_off" and len(plans) > 3:
        warnings.append("risk_off模式下计划数量较多，建议精选")

    summary["warnings"] = warnings

    return summary


if __name__ == "__main__":
    # 测试
    from app.services.strategy_v2_service import StrategyV2Service

    service = StrategyV2Service(regime="neutral")

    # 模拟信号
    signal = {
        "signal_id": "test001",
        "as_of_date": "2026-04-04",
        "symbol": "600519",
        "symbol_name": "贵州茅台",
        "setup_type": "quality_rerate",
        "grade": "A",
        "score_total": 78.5,
        "risk_penalty": 5.2,
    }

    generator = TradePlanGenerator(regime="neutral")
    plan = generator.generate_plan(signal, {"current_price": 1850, "ma20": 1800, "atr_pct": 2.5})

    print(json.dumps(plan, indent=2, ensure_ascii=False))