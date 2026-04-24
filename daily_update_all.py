"""每日数据更新 - 综合脚本"""
from __future__ import annotations

import os
import json
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = "data/investment.db"

BASE_DIR = Path(__file__).resolve().parent
TASKS = [
    ("fetch_stock_windows.py", "股票财务数据采集"),
    ("fetch_market_data.py", "市场指数数据采集"),
    ("sync_market_reference_data.py", "市场参考数据同步（VIX / 利率 / 情绪 / 当前指数）"),
    ("scripts/sync_global_risk.py", "全球风险雷达同步"),
    ("scripts/sync_north_money.py", "北向资金数据同步"),
    ("scripts/sync_south_flow.py", "南向资金(港股通)同步"),
    ("scripts/sync_currency_rates.py", "汇率数据同步"),
    ("scripts/sync_macro_indicators.py", "宏观指标同步(中国PMI)"),
    ("scripts/sync_indicator_registry.py", "指标注册表同步(数据治理状态)"),
    ("scripts/sync_hk_stocks.py", "港股数据同步(热榜/指数)"),
    ("scripts/sync_global_risk_extended.py", "全球风险扩展同步(国债收益率/DXY)"),
    ("scripts/sync_news.py", "新闻数据同步(Bloomberg RSS)"),
    ("scripts/sync_valuation_percentile.py", "估值百分位计算"),
]


def build_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def write_etl_log(job_name: str, start_time: str, end_time: str, status: str,
                  records_processed: int = 0, records_failed: int = 0,
                  error: str = "") -> None:
    """写入 ETL 日志到数据库。"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """INSERT INTO etl_logs
               (job_name, job_type, start_time, end_time, status,
                records_processed, records_failed, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_name, "daily_update", start_time, end_time, status,
             records_processed, records_failed, error)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  ETL日志写入失败: {e}")


def parse_etl_metrics(stdout: str) -> dict[str, int]:
    """从子任务输出解析标准化ETL统计。"""
    metrics = {
        "records_processed": 0,
        "records_failed": 0,
        "records_skipped": 0,
    }
    for line in stdout.splitlines():
        if not line.startswith("ETL_METRICS_JSON="):
            continue
        try:
            payload = json.loads(line.split("=", 1)[1])
            for key in metrics:
                metrics[key] = int(payload.get(key) or 0)
            return metrics
        except Exception:
            break

    # 临时兼容未标准化脚本的常见输出，后续脚本逐步改成 ETL_METRICS_JSON。
    processed_matches = re.findall(r"(\d+)\s*(?:new articles|records|rows|条|total)", stdout, re.IGNORECASE)
    if processed_matches:
        metrics["records_processed"] = sum(int(item) for item in processed_matches)
    return metrics


def run_script(script_name: str, description: str) -> dict[str, object]:
    script_path = BASE_DIR / script_name
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_name = Path(script_name).stem

    print(f"\n{'=' * 60}")
    print(f"[{start_time.split()[-1]}] {description}")
    print("=" * 60)

    if not script_path.exists():
        print(f"跳过: {script_name} 不存在")
        write_etl_log(job_name, start_time, start_time, "skipped")
        return {"ok": True, "status": "skipped", "returncode": None}

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True, encoding="utf-8", cwd=BASE_DIR,
        env=build_env(),
    )

    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print("Errors:", result.stderr, end="" if result.stderr.endswith("\n") else "\n")

    ok = result.returncode == 0
    status = "success" if ok else "failed"
    metrics = parse_etl_metrics(result.stdout or "")
    print(f"结果: {'OK' if ok else 'FAIL'} (returncode={result.returncode})")

    write_etl_log(
        job_name=job_name, start_time=start_time, end_time=end_time,
        status=status,
        records_processed=metrics["records_processed"] if ok else 0,
        records_failed=metrics["records_failed"] if ok else max(1, metrics["records_failed"]),
        error=result.stderr.strip()[:500] if not ok else ""
    )

    return {"ok": ok, "status": status, "returncode": result.returncode, **metrics}


def main() -> int:
    print("=" * 60)
    print(f"每日数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    available_tasks = [item for item in TASKS if (BASE_DIR / item[0]).exists()]
    skipped_tasks = [script_name for script_name, _ in TASKS if script_name not in {item[0] for item in available_tasks}]

    if skipped_tasks:
        print(f"跳过未找到的任务: {', '.join(skipped_tasks)}")

    results = {}
    for script_name, description in available_tasks:
        results[script_name] = run_script(script_name, description)

    print("\n" + "=" * 60)
    print("更新汇总")
    print("=" * 60)
    for task, result in results.items():
        status = "成功" if result["ok"] else "失败"
        if result["status"] == "skipped":
            status = "已跳过（文件不存在）"
        print(f"  {task}: {status}")

    failed = [task for task, result in results.items() if not result["ok"]]
    if failed:
        print(f"\n存在失败任务: {', '.join(failed)}")
        return 1

    if not available_tasks:
        print("\n未找到任何可执行任务")
        return 1

    print("\n数据更新完成!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
