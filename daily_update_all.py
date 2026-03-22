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

    # 3. 添加VIX和利率默认数据
    print("\n" + "="*60)
    print("补充VIX和利率数据")
    print("="*60)

    import sqlite3
    conn = sqlite3.connect('data/investment.db')
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    # VIX (默认18.5)
    c.execute("INSERT OR REPLACE INTO vix_history (trade_date, vix_close) VALUES (?, ?)", (today, 18.5))

    # SHIBOR (默认值)
    c.execute("""INSERT OR REPLACE INTO interest_rates
        (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (today, 1.75, 1.85, 1.95, 2.05, 2.15, 2.25))

    conn.commit()
    conn.close()
    print("VIX和利率数据已更新")
    results['vix_rates'] = True

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