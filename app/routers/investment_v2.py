"""
投资决策API路由V2增量
新增V2接口，保持旧接口兼容
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

# 新增V2路由
router_v2 = APIRouter(prefix="/investment/api/v2", tags=["investment-v2"])


# ==================== 响应模型 ====================

class SignalV2Response(BaseModel):
    signal_id: str
    symbol: str
    symbol_name: Optional[str] = None
    setup_type: str
    setup_name: Optional[str] = None
    grade: str
    action: str
    score_total: float
    eligibility_pass: bool
    filter_fail_reasons: List[str] = []


class TradePlanResponse(BaseModel):
    trade_plan_id: str
    signal_id: str
    symbol: str
    setup_type: str
    grade: str
    entry_rule: str
    stop_loss_pct: float
    take_profit_pct: float
    max_position_pct: float
    priority_rank: int
    status: str


class StatsResponse(BaseModel):
    by_grade: Dict[str, Any]
    by_setup: Dict[str, Any]
    calibration: Dict[str, Any]


# ==================== V2 API端点 ====================

@router_v2.get("/signals")
async def get_signals_v2(
    as_of_date: Optional[str] = None,
    grade: Optional[str] = None,
    setup_type: Optional[str] = None,
    min_score: float = Query(0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
):
    """
    获取V2信号列表
    支持多维度筛选
    """
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    date_filter = as_of_date or datetime.now().strftime("%Y-%m-%d")

    query = """
        SELECT * FROM strategy_signals_v2
        WHERE as_of_date = ?
          AND eligibility_pass = 1
          AND score_total >= ?
    """
    params = [date_filter, min_score]

    if grade:
        query += " AND grade = ?"
        params.append(grade.upper())

    if setup_type:
        query += " AND setup_type = ?"
        params.append(setup_type)

    query += " ORDER BY score_total DESC LIMIT ?"
    params.append(limit)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    return [dict(row) for row in rows]


@router_v2.get("/signals/{signal_id}")
async def get_signal_detail_v2(signal_id: str):
    """获取单个信号详情"""
    import sqlite3
    from pathlib import Path
    from fastapi import HTTPException

    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT * FROM strategy_signals_v2 WHERE signal_id = ?", (signal_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Signal not found")

    return dict(row)


@router_v2.get("/trade-plans")
async def get_trade_plans_v2(
    plan_date: Optional[str] = None,
    status: str = "pending",
):
    """获取交易计划列表"""
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    date_filter = plan_date or datetime.now().strftime("%Y-%m-%d")

    c.execute("""
        SELECT * FROM trade_plans
        WHERE plan_date = ? AND status = ?
        ORDER BY priority_rank
    """, (date_filter, status))

    plans = [dict(row) for row in c.fetchall()]
    conn.close()

    return plans


@router_v2.post("/trade-plans/generate")
async def generate_trade_plans_v2(
    regime: str = "neutral",
    max_plans: int = Query(8, ge=1, le=20),
):
    """生成当日交易计划"""
    from app.services.trade_plan_service import TradePlanGenerator

    generator = TradePlanGenerator(regime=regime)
    plans = generator.generate_daily_plans(max_plans=max_plans)

    return {
        "generated_at": datetime.now().isoformat(),
        "regime": regime,
        "total_plans": len(plans),
        "plans": plans,
    }


@router_v2.get("/stats")
async def get_signal_stats_v2():
    """获取信号统计数据"""
    import sqlite3
    import json
    from pathlib import Path

    base_dir = Path(__file__).resolve().parent.parent.parent

    # 尝试读取最新的统计文件
    stats_dir = base_dir / "data" / "stats"
    if stats_dir.exists():
        stats_files = sorted(stats_dir.glob("signal_stats_*.json"), reverse=True)
        if stats_files:
            with open(stats_files[0], encoding="utf-8") as f:
                return json.load(f)

    # 实时计算
    db_path = base_dir / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Grade统计
    c.execute("""
        SELECT grade, COUNT(*) as n, AVG(score_total) as avg_score
        FROM strategy_signals_v2
        WHERE eligibility_pass = 1
        GROUP BY grade
    """)
    by_grade = {row["grade"]: {"count": row["n"], "avg_score": row["avg_score"]}
                for row in c.fetchall()}

    # Setup统计
    c.execute("""
        SELECT setup_type, COUNT(*) as n, AVG(score_total) as avg_score
        FROM strategy_signals_v2
        WHERE eligibility_pass = 1
        GROUP BY setup_type
    """)
    by_setup = {row["setup_type"]: {"count": row["n"], "avg_score": row["avg_score"]}
                for row in c.fetchall()}

    conn.close()

    return {
        "by_grade": by_grade,
        "by_setup": by_setup,
        "calibration": {},
    }


@router_v2.get("/opportunities")
async def get_opportunities_v2(
    regime: str = "neutral",
    min_grade: str = "B",
    limit: int = 50,
):
    """
    V2机会池接口
    使用三段式评分后的结果
    """
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    date_filter = datetime.now().strftime("%Y-%m-%d")

    # 获取可执行信号
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
        LIMIT ?
    """, (date_filter, limit))

    signals = [dict(row) for row in c.fetchall()]
    conn.close()

    # 按setup分组
    by_setup = {}
    for sig in signals:
        setup = sig.get("setup_type", "unknown")
        if setup not in by_setup:
            by_setup[setup] = []
        by_setup[setup].append(sig)

    return {
        "generated_at": datetime.now().isoformat(),
        "regime": regime,
        "total": len(signals),
        "by_setup": by_setup,
        "signals": signals,
    }


@router_v2.get("/eligibility-check/{symbol}")
async def check_eligibility_v2(symbol: str):
    """检查单个标的的资格状态"""
    import sqlite3
    from pathlib import Path
    from app.services.strategy_v2_service import EligibilityFilter

    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "investment.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    context = {"symbol": symbol}

    # 数据新鲜度
    c.execute("""
        SELECT MAX(trade_date) as last_update
        FROM stock_factor_snapshot
        WHERE code = ?
    """, (symbol,))
    row = c.fetchone()
    if row and row["last_update"]:
        last = datetime.strptime(row["last_update"], "%Y-%m-%d")
        age_hours = (datetime.now() - last).total_seconds() / 3600
        context["data_age_hours"] = age_hours

    # 财务数据
    c.execute("SELECT 1 FROM stock_financial WHERE code = ? LIMIT 1", (symbol,))
    context["has_fundamentals"] = c.fetchone() is not None

    # 技术指标
    c.execute("SELECT 1 FROM technical_indicators WHERE code = ? LIMIT 1", (symbol,))
    context["has_technical"] = c.fetchone() is not None

    # 风险数据
    c.execute("""
        SELECT risk FROM stock_factor_snapshot
        WHERE code = ? AND model = 'conservative'
        ORDER BY trade_date DESC LIMIT 1
    """, (symbol,))
    row = c.fetchone()
    if row:
        context["risk_score"] = row["risk"] or 50

    conn.close()

    # 执行检查
    filter_instance = EligibilityFilter()
    passed, reasons = filter_instance.check(context)

    return {
        "symbol": symbol,
        "eligibility_pass": passed,
        "filter_fail_reasons": reasons,
        "context": context,
    }


# ==================== 兼容旧接口的适配 ====================

def adapt_v2_to_v1_signal(v2_signal: Dict) -> Dict:
    """
    将V2信号转换为V1格式
    保证旧前端兼容
    """
    return {
        "code": v2_signal.get("symbol"),
        "name": v2_signal.get("symbol_name"),
        "grade": v2_signal.get("grade"),
        "action": v2_signal.get("action"),
        "total_score": v2_signal.get("score_total"),
        "quality_score": v2_signal.get("score_quality"),
        "growth_score": v2_signal.get("score_growth"),
        "valuation_score": v2_signal.get("score_valuation"),
        "technical_score": v2_signal.get("score_technical"),
        "risk_score": v2_signal.get("risk_penalty"),
        "setup_type": v2_signal.get("setup_type"),
        "setup_name": v2_signal.get("setup_name"),
        "eligibility_pass": v2_signal.get("eligibility_pass"),
        # 保留旧字段
        "theme": v2_signal.get("setup_name", "等待确认"),
        "action_key": v2_signal.get("action", "").lower(),
        "action_label": v2_signal.get("action_reason", ""),
    }


if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router_v2)

    uvicorn.run(app, host="0.0.0.0", port=8081)