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
    print(f"每日数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = {}

    # 1. 股票财务数据
    results['stocks'] = run_script('fetch_stock_windows.py', '股票财务数据采集')

    # 2. 市场指数数据
    results['market'] = run_script('fetch_market_data.py', '市场指数数据采集')

    # 3. 实时参考数据落库
    results['reference'] = run_script(
        'sync_market_reference_data.py',
        '市场参考数据同步（VIX / 利率 / 情绪 / 当前指数）'
    )

    # 汇总结果
    print("\n" + "="*60)
    print("更新汇总")
    print("="*60)
    for task, success in results.items():
        status = "✓ 成功" if success else "✗ 失败"
        print(f"  {task}: {status}")

    print("\n数据更新完成!")


if __name__ == "__main__":
    main()
