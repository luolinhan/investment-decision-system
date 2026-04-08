"""
策略服务V2 - 三段式评分系统
Eligibility Filter -> Setup Classifier -> Within-Setup Score
"""
from __future__ import annotations

import sqlite3
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"

# ==================== Setup定义 ====================

SETUP_TYPES = {
    "catalyst_breakout": {
        "name": "催化突破",
        "description": "事件/业绩催化驱动的突破机会",
        "holding_horizon": "short",
        "min_quality": 50,
        "min_technical": 55,
        "min_catalyst": 60,
        "weights": {
            "quality": 0.15,
            "growth": 0.15,
            "valuation": 0.10,
            "technical": 0.25,
            "flow": 0.15,
            "catalyst": 0.20,
        }
    },
    "trend_continuation": {
        "name": "趋势延续",
        "description": "已有趋势确认，等待回调或持续",
        "holding_horizon": "medium",
        "min_technical": 60,
        "min_flow": 50,
        "weights": {
            "quality": 0.15,
            "growth": 0.10,
            "valuation": 0.05,
            "technical": 0.35,
            "flow": 0.20,
            "catalyst": 0.15,
        }
    },
    "quality_rerate": {
        "name": "质量重估",
        "description": "优质资产估值修复机会",
        "holding_horizon": "medium",
        "min_quality": 65,
        "min_valuation": 55,
        "weights": {
            "quality": 0.30,
            "growth": 0.15,
            "valuation": 0.25,
            "technical": 0.10,
            "flow": 0.10,
            "catalyst": 0.10,
        }
    },
}

# ==================== 过滤规则 ====================

FILTER_RULES = {
    # 数据质量类
    "data_stale": {
        "name": "数据过期",
        "check": lambda ctx: ctx.get("data_age_hours", 999) > 72,
        "severity": "hard",  # hard = 直接淘汰, soft = 降级
        "reason": "数据超过72小时未更新",
    },
    "missing_fundamentals": {
        "name": "财务数据缺失",
        "check": lambda ctx: not ctx.get("has_fundamentals", False),
        "severity": "soft",
        "reason": "缺少财务数据",
    },
    "missing_technical": {
        "name": "技术指标缺失",
        "check": lambda ctx: not ctx.get("has_technical", False),
        "severity": "soft",
        "reason": "缺少技术指标",
    },

    # 风险类
    "high_risk_score": {
        "name": "风险分过高",
        "check": lambda ctx: ctx.get("risk_score", 100) > 70,
        "severity": "hard",
        "reason": "风险评分超过70",
    },
    "high_debt": {
        "name": "高负债",
        "check": lambda ctx: ctx.get("debt_ratio", 0) > 80,
        "severity": "hard",
        "reason": "资产负债率超过80%",
    },
    "negative_profit_trend": {
        "name": "利润恶化",
        "check": lambda ctx: ctx.get("net_profit_yoy", 0) < -30,
        "severity": "soft",
        "reason": "净利润同比下滑超30%",
    },

    # 流动性类
    "low_turnover": {
        "name": "换手率过低",
        "check": lambda ctx: ctx.get("turnover_rate", 0) < 0.5 and ctx.get("market", "") != "hk",
        "severity": "soft",
        "reason": "日均换手率低于0.5%",
    },

    # 估值类
    "extreme_valuation": {
        "name": "估值极端",
        "check": lambda ctx: ctx.get("pe_ttm", 0) > 100 or ctx.get("pb", 0) > 20,
        "severity": "soft",
        "reason": "PE>100或PB>20",
    },
}

# Regime相关过滤
REGIME_FILTERS = {
    "risk_off": {
        "blocked_setups": ["trend_continuation"],  # risk_off时禁止趋势延续
        "min_grade": "A",  # 只允许A级
        "max_position": 3.0,
    },
    "risk_on": {
        "blocked_setups": [],
        "min_grade": "B",
        "max_position": 8.0,
    },
    "neutral": {
        "blocked_setups": [],
        "min_grade": "B",
        "max_position": 5.0,
    },
}


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "" or value == "-":
        return default
    try:
        return float(value)
    except:
        return default


def _clamp(value: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
    return max(min_val, min(max_val, value))


def generate_signal_id(symbol: str, as_of_date: str, setup_type: str) -> str:
    """生成唯一信号ID"""
    raw = f"{symbol}_{as_of_date}_{setup_type}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ==================== 阶段1: Eligibility Filter ====================

class EligibilityFilter:
    """资格过滤器 - 不通过则淘汰"""

    def __init__(self, regime: str = "neutral"):
        self.regime = regime
        self.gate_results = []

    def check(self, context: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        检查是否通过所有过滤器
        返回: (是否通过, 失败原因列表)
        """
        fail_reasons = []
        soft_fails = []

        for rule_key, rule in FILTER_RULES.items():
            try:
                if rule["check"](context):
                    reason = rule["reason"]
                    if rule["severity"] == "hard":
                        fail_reasons.append(f"[HARD] {reason}")
                    else:
                        soft_fails.append(f"[SOFT] {reason}")
            except Exception as e:
                # 检查失败时保守处理
                soft_fails.append(f"[SOFT] {rule['name']}检查异常")

        # Regime相关过滤
        regime_config = REGIME_FILTERS.get(self.regime, REGIME_FILTERS["neutral"])
        setup_type = context.get("setup_type", "")

        if setup_type in regime_config.get("blocked_setups", []):
            fail_reasons.append(f"[HARD] {self.regime}模式下不允许{setup_type}策略")

        # 记录门控结果
        self.gate_results.append({
            "symbol": context.get("symbol"),
            "hard_fails": len(fail_reasons),
            "soft_fails": len(soft_fails),
            "regime": self.regime,
        })

        # hard失败直接淘汰
        passed = len(fail_reasons) == 0
        all_reasons = fail_reasons + soft_fails

        return passed, all_reasons

    def get_gate_results(self) -> List[Dict]:
        return self.gate_results


# ==================== 阶段2: Setup Classifier ====================

class SetupClassifier:
    """策略分类器 - 判断属于哪种策略类型"""

    def classify(self, factors: Dict[str, float], context: Dict[str, Any]) -> str:
        """
        根据因子特征判断最适合的策略类型
        """
        scores = {}

        for setup_type, config in SETUP_TYPES.items():
            score = 0
            match_count = 0

            # 检查最小要求
            if "min_quality" in config:
                if factors.get("quality", 0) >= config["min_quality"]:
                    score += 20
                    match_count += 1
                else:
                    score -= 10

            if "min_technical" in config:
                if factors.get("technical", 0) >= config["min_technical"]:
                    score += 20
                    match_count += 1
                else:
                    score -= 10

            if "min_valuation" in config:
                if factors.get("valuation", 0) >= config["min_valuation"]:
                    score += 15
                    match_count += 1

            if "min_flow" in config:
                if factors.get("flow", 0) >= config["min_flow"]:
                    score += 10
                    match_count += 1

            if "min_catalyst" in config:
                # 催化剂来自研报/事件
                catalyst_score = context.get("catalyst_score", 50)
                if catalyst_score >= config["min_catalyst"]:
                    score += 25
                    match_count += 1

            # 特征加分
            if setup_type == "catalyst_breakout":
                if context.get("has_recent_event", False):
                    score += 20
                if context.get("report_coverage", 0) > 0:
                    score += 15

            elif setup_type == "trend_continuation":
                trend_signal = context.get("trend_signal", "").lower()
                if trend_signal == "bullish":
                    score += 25
                macd = factors.get("technical", 0)
                if macd >= 65:
                    score += 15

            elif setup_type == "quality_rerate":
                if factors.get("quality", 0) >= 70:
                    score += 20
                pe_pctile = context.get("pe_percentile_5y", 50)
                if pe_pctile < 30:
                    score += 25  # 低估值加分

            scores[setup_type] = score

        # 返回得分最高的setup
        if not scores:
            return "quality_rerate"  # 默认

        best_setup = max(scores.items(), key=lambda x: x[1])
        return best_setup[0]


# ==================== 阶段3: Within-Setup Scorer ====================

class WithinSetupScorer:
    """同策略内评分 - 只在同一setup内比较"""

    def score(self, factors: Dict[str, float], setup_type: str) -> Dict[str, Any]:
        """
        计算setup内的相对评分
        """
        config = SETUP_TYPES.get(setup_type, SETUP_TYPES["quality_rerate"])
        weights = config.get("weights", SETUP_TYPES["quality_rerate"]["weights"])

        # 加权总分
        total = 0
        for key, weight in weights.items():
            factor_value = factors.get(key, 50)
            total += weight * factor_value

        # 风险惩罚 (独立处理)
        risk = factors.get("risk", 30)
        risk_penalty = max(0, (risk - 30) * 0.5)  # 风险>30开始惩罚

        total = _clamp(total - risk_penalty)

        return {
            "total": round(total, 1),
            "risk_penalty": round(risk_penalty, 1),
            "weights_used": weights,
            "holding_horizon": config.get("holding_horizon", "medium"),
        }


# ==================== Grade/Action决策 ====================

def determine_grade(total: float, risk: float, setup_type: str, sample_size: int = 0) -> Tuple[str, str]:
    """
    确定Grade (A/B/C/X)
    基于历史校准，样本不足时保守
    """
    # 样本不足时使用保守阈值
    if sample_size < 30:
        # 样本不足，保守输出
        if total >= 75 and risk <= 40:
            return "A", "样本不足，但指标强势"
        elif total >= 65:
            return "B", "样本不足，谨慎观察"
        else:
            return "C", "样本不足，建议观望"
    else:
        # 样本充足，正常判断
        if total >= 72 and risk <= 40:
            return "A", "高置信度机会"
        elif total >= 62 and risk <= 55:
            return "B", "中等置信度"
        else:
            return "C", "置信度不足"


def determine_action(grade: str, eligibility_pass: bool, regime: str,
                     filter_reasons: List[str]) -> Tuple[str, str]:
    """
    确定执行建议
    action不等于信号强度，是执行层面的建议
    """
    if not eligibility_pass:
        return "BLOCKED", filter_reasons[0] if filter_reasons else "未通过资格检查"

    if grade == "A":
        if regime == "risk_off":
            return "BUY_ON_PULLBACK", "risk_off下等待回调再入"
        else:
            return "BUY_NOW", "优先执行"
    elif grade == "B":
        return "WATCH", "观察等待确认信号"
    else:
        return "SKIP", "暂不具备操作价值"


# ==================== 主服务类 ====================

class StrategyV2Service:
    """策略服务V2"""

    def __init__(self, regime: str = "neutral"):
        self.regime = regime
        self.filter = EligibilityFilter(regime)
        self.classifier = SetupClassifier()
        self.scorer = WithinSetupScorer()

    def evaluate_symbol(self, symbol: str, factors: Dict[str, float],
                        context: Dict[str, Any]) -> Dict[str, Any]:
        """
        完整评估一个标的
        """
        now = datetime.now()
        as_of_date = context.get("trade_date", now.strftime("%Y-%m-%d"))

        # 阶段1: 资格过滤
        context["symbol"] = symbol
        eligibility_pass, filter_reasons = self.filter.check(context)

        if not eligibility_pass:
            # 直接淘汰
            return {
                "signal_id": generate_signal_id(symbol, as_of_date, "rejected"),
                "symbol": symbol,
                "as_of_date": as_of_date,
                "eligibility_pass": False,
                "filter_fail_reasons": filter_reasons,
                "grade": "X",
                "action": "BLOCKED",
                "action_reason": filter_reasons[0] if filter_reasons else "未通过资格检查",
            }

        # 阶段2: 策略分类
        setup_type = self.classifier.classify(factors, context)

        # 阶段3: 策略内评分
        score_result = self.scorer.score(factors, setup_type)

        # Grade和Action
        grade, grade_reason = determine_grade(
            score_result["total"],
            factors.get("risk", 30),
            setup_type,
            context.get("sample_size", 0)
        )

        action, action_reason = determine_action(
            grade, eligibility_pass, self.regime, filter_reasons
        )

        # 生成信号
        signal = {
            "signal_id": generate_signal_id(symbol, as_of_date, setup_type),
            "as_of_date": as_of_date,
            "as_of_ts": now.isoformat(),
            "symbol": symbol,
            "symbol_name": context.get("name", ""),
            "setup_type": setup_type,
            "setup_name": SETUP_TYPES.get(setup_type, {}).get("name", setup_type),
            "holding_horizon": score_result.get("holding_horizon", "medium"),
            "regime": self.regime,

            "score_total": score_result["total"],
            "score_quality": factors.get("quality", 0),
            "score_growth": factors.get("growth", 0),
            "score_valuation": factors.get("valuation", 0),
            "score_technical": factors.get("technical", 0),
            "score_flow": factors.get("flow", 0),
            "score_catalyst": context.get("catalyst_score", 50),
            "score_sentiment": context.get("sentiment_score", 50),
            "risk_penalty": score_result["risk_penalty"],

            "eligibility_pass": eligibility_pass,
            "filter_fail_reasons": filter_reasons,
            "risk_flags": context.get("risk_flags", []),

            "grade": grade,
            "grade_reason": grade_reason,
            "action": action,
            "action_reason": action_reason,

            "data_freshness_hours": context.get("data_age_hours", 24),
            "data_coverage_pct": context.get("coverage_pct", 0),
            "source_confidence": context.get("source_confidence", 0.5),

            "strategy_version": "v2.0",
            "weights_version": setup_type,

            "created_at": now.isoformat(),
        }

        return signal

    def persist_signal(self, signal: Dict[str, Any]) -> bool:
        """保存信号到数据库"""
        conn = get_db_connection()
        c = conn.cursor()

        try:
            c.execute("""
                INSERT OR REPLACE INTO strategy_signals_v2 (
                    signal_id, as_of_date, as_of_ts, symbol, symbol_name,
                    setup_type, holding_horizon, regime,
                    score_total, score_quality, score_growth, score_valuation,
                    score_technical, score_flow, score_catalyst, score_sentiment,
                    risk_penalty, eligibility_pass, filter_fail_reasons, risk_flags,
                    grade, action, action_reason,
                    data_freshness_hours, data_coverage_pct, source_confidence,
                    strategy_version, weights_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal["signal_id"],
                signal["as_of_date"],
                signal.get("as_of_ts"),
                signal["symbol"],
                signal.get("symbol_name"),
                signal["setup_type"],
                signal.get("holding_horizon"),
                signal["regime"],
                signal["score_total"],
                signal["score_quality"],
                signal["score_growth"],
                signal["score_valuation"],
                signal["score_technical"],
                signal["score_flow"],
                signal.get("score_catalyst"),
                signal.get("score_sentiment"),
                signal["risk_penalty"],
                1 if signal["eligibility_pass"] else 0,
                json.dumps(signal.get("filter_fail_reasons", [])),
                json.dumps(signal.get("risk_flags", [])),
                signal["grade"],
                signal["action"],
                signal.get("action_reason"),
                signal.get("data_freshness_hours"),
                signal.get("data_coverage_pct"),
                signal.get("source_confidence"),
                signal.get("strategy_version", "v2.0"),
                signal.get("weights_version"),
                signal["created_at"],
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"保存信号失败: {e}")
            return False
        finally:
            conn.close()

    def batch_evaluate(self, symbols: List[str], factors_map: Dict[str, Dict],
                       context_map: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """批量评估"""
        results = []
        for symbol in symbols:
            factors = factors_map.get(symbol, {})
            context = context_map.get(symbol, {})
            signal = self.evaluate_symbol(symbol, factors, context)
            self.persist_signal(signal)
            results.append(signal)
        return results

    def get_actionable_signals(self, as_of_date: str = None,
                                min_grade: str = "B") -> List[Dict[str, Any]]:
        """获取可执行信号"""
        conn = get_db_connection()
        c = conn.cursor()

        date_filter = as_of_date or datetime.now().strftime("%Y-%m-%d")

        grade_order = {"A": 1, "B": 2, "C": 3, "X": 4}
        min_rank = grade_order.get(min_grade, 2)

        c.execute("""
            SELECT * FROM strategy_signals_v2
            WHERE as_of_date = ?
              AND eligibility_pass = 1
              AND action IN ('BUY_NOW', 'BUY_ON_PULLBACK', 'WATCH')
            ORDER BY
                CASE grade
                    WHEN 'A' THEN 1
                    WHEN 'B' THEN 2
                    WHEN 'C' THEN 3
                    ELSE 4
                END,
                score_total DESC
        """, (date_filter,))

        results = [dict(row) for row in c.fetchall()]
        conn.close()

        return results


# ==================== 便捷函数 ====================

def get_strategy_v2_service(regime: str = None) -> StrategyV2Service:
    """获取策略服务实例"""
    if regime is None:
        # 尝试从数据库获取当前regime
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT payload_json FROM market_snapshots WHERE snapshot_key = 'investment.market_overview.v2' ORDER BY updated_at DESC LIMIT 1")
            row = c.fetchone()
            if row:
                payload = json.loads(row["payload_json"])
                regime = payload.get("regime", {}).get("label", "neutral")
            conn.close()
        except:
            regime = "neutral"

    return StrategyV2Service(regime=regime)


if __name__ == "__main__":
    # 测试
    service = StrategyV2Service(regime="neutral")

    # 测试数据
    factors = {
        "quality": 72,
        "growth": 65,
        "valuation": 58,
        "technical": 70,
        "flow": 55,
        "risk": 35,
    }

    context = {
        "trade_date": "2026-04-04",
        "name": "测试股票",
        "has_fundamentals": True,
        "has_technical": True,
        "data_age_hours": 12,
        "debt_ratio": 45,
        "pe_ttm": 25,
        "catalyst_score": 65,
        "trend_signal": "bullish",
        "pe_percentile_5y": 25,
    }

    signal = service.evaluate_symbol("600519", factors, context)
    print(json.dumps(signal, indent=2, ensure_ascii=False))