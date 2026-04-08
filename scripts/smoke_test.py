"""
Smoke Test脚本
验证主应用不被改坏
"""
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"

TESTS = []
PASSED = 0
FAILED = 0


def test(name: str):
    """测试装饰器"""
    def decorator(func):
        def wrapper():
            global PASSED, FAILED
            try:
                func()
                print(f"  [PASS] {name}")
                PASSED += 1
                TESTS.append({"name": name, "status": "PASS"})
            except Exception as e:
                print(f"  [FAIL] {name}: {e}")
                FAILED += 1
                TESTS.append({"name": name, "status": "FAIL", "error": str(e)})
        return wrapper
    return decorator


# ==================== 数据库测试 ====================

@test("数据库连接")
def test_db_connection():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1")
    conn.close()


@test("V2信号表存在")
def test_signals_v2_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_signals_v2'")
    result = c.fetchone()
    conn.close()
    assert result is not None, "strategy_signals_v2表不存在"


@test("交易计划表存在")
def test_trade_plans_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_plans'")
    result = c.fetchone()
    conn.close()
    assert result is not None, "trade_plans表不存在"


@test("数据质量门控表存在")
def test_quality_gates_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data_quality_gates'")
    result = c.fetchone()
    conn.close()
    assert result is not None, "data_quality_gates表不存在"


@test("核心表未损坏")
def test_core_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    tables = ["stock_factor_snapshot", "signal_journal", "signal_labels",
              "strategy_perf_daily", "stock_financial", "technical_indicators"]

    for table in tables:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        count = c.fetchone()[0]
        # 不要求有数据，只要不报错

    conn.close()


# ==================== 服务导入测试 ====================

@test("策略V2服务导入")
def test_strategy_v2_import():
    sys.path.insert(0, str(BASE_DIR))
    from app.services.strategy_v2_service import StrategyV2Service, get_strategy_v2_service
    service = get_strategy_v2_service()
    assert service is not None


@test("交易计划服务导入")
def test_trade_plan_import():
    sys.path.insert(0, str(BASE_DIR))
    from app.services.trade_plan_service import TradePlanGenerator
    generator = TradePlanGenerator()
    assert generator is not None


@test("V2路由导入")
def test_router_v2_import():
    sys.path.insert(0, str(BASE_DIR))
    from app.routers.investment_v2 import router_v2
    assert router_v2 is not None


# ==================== 功能测试 ====================

@test("V2信号评估功能")
def test_signal_evaluation():
    sys.path.insert(0, str(BASE_DIR))
    from app.services.strategy_v2_service import StrategyV2Service

    service = StrategyV2Service(regime="neutral")

    factors = {
        "quality": 70,
        "growth": 60,
        "valuation": 55,
        "technical": 65,
        "flow": 50,
        "risk": 35,
    }

    context = {
        "trade_date": "2026-04-04",
        "has_fundamentals": True,
        "has_technical": True,
        "data_age_hours": 12,
        "debt_ratio": 45,
    }

    signal = service.evaluate_symbol("test001", factors, context)
    assert signal is not None
    assert "signal_id" in signal
    assert "grade" in signal
    assert "action" in signal


@test("交易计划生成功能")
def test_trade_plan_generation():
    sys.path.insert(0, str(BASE_DIR))
    from app.services.trade_plan_service import TradePlanGenerator

    generator = TradePlanGenerator(regime="neutral")

    signal = {
        "signal_id": "test_signal_001",
        "as_of_date": "2026-04-04",
        "symbol": "600519",
        "symbol_name": "测试股票",
        "setup_type": "quality_rerate",
        "grade": "A",
        "score_total": 75,
    }

    plan = generator.generate_plan(signal)
    assert plan is not None
    assert "trade_plan_id" in plan
    assert "stop_loss_pct" in plan
    assert "take_profit_pct" in plan


@test("API兼容适配")
def test_api_adapter():
    sys.path.insert(0, str(BASE_DIR))
    from app.routers.investment_v2 import adapt_v2_to_v1_signal

    v2_signal = {
        "symbol": "600519",
        "symbol_name": "贵州茅台",
        "grade": "A",
        "action": "BUY_NOW",
        "score_total": 78.5,
        "score_quality": 75,
        "score_growth": 70,
        "score_valuation": 65,
        "score_technical": 80,
        "risk_penalty": 5,
        "setup_type": "quality_rerate",
        "setup_name": "质量重估",
        "eligibility_pass": True,
        "action_reason": "优先执行",
    }

    v1_signal = adapt_v2_to_v1_signal(v2_signal)
    assert v1_signal["code"] == "600519"
    assert v1_signal["grade"] == "A"
    assert v1_signal["total_score"] == 78.5


# ==================== FastAPI应用测试 ====================

@test("FastAPI应用启动")
def test_fastapi_app():
    sys.path.insert(0, str(BASE_DIR))
    from app.main import app
    assert app is not None


@test("投资路由注册")
def test_investment_router():
    sys.path.insert(0, str(BASE_DIR))
    from app.main import app

    routes = [r.path for r in app.routes]
    assert any("/investment" in r for r in routes)


# ==================== 运行测试 ====================

def run_smoke_tests():
    print("=" * 60)
    print("Smoke Test")
    print("=" * 60)

    # 数据库测试
    print("\n[数据库测试]")
    test_db_connection()
    test_signals_v2_table()
    test_trade_plans_table()
    test_quality_gates_table()
    test_core_tables()

    # 服务导入测试
    print("\n[服务导入测试]")
    test_strategy_v2_import()
    test_trade_plan_import()
    test_router_v2_import()

    # 功能测试
    print("\n[功能测试]")
    test_signal_evaluation()
    test_trade_plan_generation()
    test_api_adapter()

    # 应用测试
    print("\n[应用测试]")
    test_fastapi_app()
    test_investment_router()

    # 结果
    print("\n" + "=" * 60)
    print(f"结果: {PASSED} 通过, {FAILED} 失败")
    print("=" * 60)

    return FAILED == 0


if __name__ == "__main__":
    success = run_smoke_tests()
    sys.exit(0 if success else 1)