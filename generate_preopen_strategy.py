"""
盘前策略生成任务

建议在交易日 08:50 左右执行, 生成当日盘前参考策略。
"""
from datetime import datetime

from app.services.strategy_planning_service import StrategyPlanningService


def main() -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 72)
    print(f"[generate_preopen_strategy] start {now}")
    print("=" * 72)

    planner = StrategyPlanningService()
    payload = planner.build_preopen_strategy(force_refresh=True, persist=True)

    print(
        "result:",
        {
            "generated_at": payload.get("generated_at"),
            "trade_date": payload.get("trade_date"),
            "regime": payload.get("regime"),
            "target_exposure": payload.get("target_exposure"),
            "setup_focus": payload.get("setup_focus"),
        },
    )
    print("[generate_preopen_strategy] done")


if __name__ == "__main__":
    main()

