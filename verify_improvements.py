"""
验证改进是否生效
"""
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "investment.db"
BACKTEST_DB = BASE_DIR / "data" / "backtest.db"
MARKET_DIR = BASE_DIR / "data" / "quant_workbench" / "market"


def check_fundamentals():
    """检查基本面数据"""
    if not DB_PATH.exists():
        return {"status": "error", "message": "数据库不存在"}

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(DISTINCT code) FROM stock_financial")
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM stock_financial WHERE roe IS NOT NULL")
    with_roe = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM stock_financial WHERE pe_ttm IS NOT NULL")
    with_pe = c.fetchone()[0]

    conn.close()

    return {
        "status": "ok",
        "total_stocks": total,
        "with_roe": with_roe,
        "with_pe": with_pe,
        "coverage": f"{total}/800 ({total/800*100:.1f}%)",
    }


def check_market_data():
    """检查行情数据"""
    if not MARKET_DIR.exists():
        return {"status": "error", "message": "行情目录不存在"}

    daily_files = list(MARKET_DIR.glob("*_1d.parquet"))
    intraday_files = list(MARKET_DIR.glob("*_5m.parquet"))

    return {
        "status": "ok",
        "daily_files": len(daily_files),
        "intraday_files": len(intraday_files),
    }


def check_backtest():
    """检查回测模块"""
    if not BACKTEST_DB.exists():
        return {"status": "not_initialized", "message": "回测数据库不存在"}

    conn = sqlite3.connect(BACKTEST_DB)
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in c.fetchall()]

    c.execute("SELECT COUNT(*) FROM signals")
    signals = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM backtest_results")
    results = c.fetchone()[0]

    conn.close()

    return {
        "status": "ok",
        "tables": tables,
        "signals_count": signals,
        "results_count": results,
    }


def main():
    print("=" * 60)
    print("量化工作台改进验证")
    print("=" * 60)

    # 1. 基本面数据
    print("\n[1] 基本面数据:")
    fund = check_fundamentals()
    if fund["status"] == "ok":
        print(f"    股票数: {fund['total_stocks']}")
        print(f"    有ROE: {fund['with_roe']}")
        print(f"    有PE: {fund['with_pe']}")
        print(f"    覆盖率: {fund['coverage']}")
    else:
        print(f"    错误: {fund.get('message')}")

    # 2. 行情数据
    print("\n[2] 行情数据:")
    market = check_market_data()
    if market["status"] == "ok":
        print(f"    日线文件: {market['daily_files']}")
        print(f"    5分钟文件: {market['intraday_files']}")
    else:
        print(f"    错误: {market.get('message')}")

    # 3. 回测模块
    print("\n[3] 回测模块:")
    bt = check_backtest()
    if bt["status"] == "ok":
        print(f"    表: {bt['tables']}")
        print(f"    信号数: {bt['signals_count']}")
        print(f"    回测结果: {bt['results_count']}")
    else:
        print(f"    状态: {bt.get('message')}")

    # 4. 股票池
    print("\n[4] 股票池测试:")
    try:
        sys.path.insert(0, str(BASE_DIR))
        from quant_workbench.universe_dynamic import load_full_universe
        universe = load_full_universe(include_hk=False)
        print(f"    沪深300+中证500: {len(universe)} 只")
    except Exception as e:
        print(f"    错误: {e}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()