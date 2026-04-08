"""
分钟级运行时刷新任务

用途:
1. 强制刷新市场概览与自选池快照, 保持分钟级时效
2. 每5分钟同步一次策略运行时统计
3. 持续更新盘前策略快照
"""
from datetime import datetime

from app.services.investment_data import InvestmentDataService
from app.services.investment_db_service import InvestmentDataService as DbService
from app.services.strategy_planning_service import StrategyPlanningService


def main() -> None:
    now = datetime.now()
    print("=" * 72)
    print(f"[minute_runtime_refresh] start {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    realtime = InvestmentDataService()
    db = DbService()
    planner = StrategyPlanningService(realtime_service=realtime, db_service=db)

    overview = realtime.get_market_overview(force_refresh=True)
    watch = realtime.get_watch_stocks(force_refresh=True)
    print("overview storage:", overview.get("storage"))
    print("watch storage:", watch.get("storage"))

    if now.minute % 5 == 0:
        sync_result = db.sync_strategy_runtime_data(
            windows=[20, 60, 120],
            allow_workbench_fallback=False,
        )
        print("strategy sync:", sync_result)
    else:
        print("strategy sync: skipped (run every 5 minutes)")

    preopen = planner.build_preopen_strategy(force_refresh=False, persist=True)
    print(
        "preopen:",
        {
            "generated_at": preopen.get("generated_at"),
            "regime": preopen.get("regime"),
            "target_exposure": preopen.get("target_exposure"),
        },
    )

    print("[minute_runtime_refresh] done")


if __name__ == "__main__":
    main()

