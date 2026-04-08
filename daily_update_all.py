"""
每日数据更新 - 综合脚本
运行所有数据采集任务
"""
import subprocess
import sys
from datetime import datetime

def run_script(script_name, description):
    """运行脚本"""
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {description}")
    print('='*60)

    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)

    return result.returncode == 0


def main():
    print("=" * 60)
    print(f"Daily Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = {}

    # 1. 股票财务数据
    results['stocks'] = run_script('fetch_stock_windows.py', 'Stock Financial Data')

    # 2. 市场指数数据
    results['market'] = run_script('fetch_market_data.py', 'Market Index Data')

    # 3. 实时参考数据落库
    results['reference'] = run_script(
        'sync_market_reference_data.py',
        'Market Reference Sync (VIX / Rates / Sentiment / Indices)'
    )

    # 汇总结果
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    for task, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        print(f"  {task}: {status}")

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\nFailed tasks: {', '.join(failed)}")
        sys.exit(1)

    print("\nAll tasks completed successfully!")


if __name__ == "__main__":
    main()
