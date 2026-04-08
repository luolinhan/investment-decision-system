"""
每日更新任务整合脚本
1. 同步行情数据
2. 记录信号（用于回测）
3. 运行回测验证
"""
from __future__ import annotations

import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).resolve().parent))

from quant_workbench.sync import QuantWorkbenchSync
from quant_workbench.service import QuantWorkbenchService
from quant_workbench import backtest


def main():
    print("=" * 60)
    print("量化工作台每日更新")
    print("=" * 60)

    # 1. 同步行情数据
    print("\n[1/3] 同步行情数据...")
    sync = QuantWorkbenchSync()
    sync.run()

    # 2. 生成信号并记录
    print("\n[2/3] 生成信号并记录...")
    service = QuantWorkbenchService()
    opportunities = service.list_opportunities()
    regime = service.get_market_regime()

    # 初始化回测数据库
    backtest.init_backtest_db()

    # 记录今日信号
    backtest.record_signals(opportunities, regime.get("label", "unknown"))

    # 3. 运行回测验证
    print("\n[3/3] 回测验证...")
    backtest.run_backtest(days_back=30)
    backtest.print_backtest_report(days_back=30)

    print("\n" + "=" * 60)
    print("更新完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()