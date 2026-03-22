"""
初始化数据库并采集研报
"""
import asyncio
import sys
import os

# 设置代理
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

sys.path.insert(0, '.')

from app.database import init_db, get_db
from app.services.collector import ReportCollector

# A股股票代码（不带后缀）
STOCKS = {
    "002459": "晶澳科技",
    "600438": "通威股份",
    "601012": "隆基绿能",
    "300763": "锦浪科技",
    "688235": "百济神州",
    "603259": "药明康德",
    "600196": "复星医药",
    "601888": "中国中免",
}


async def main():
    print("=== 初始化数据库 ===")

    # 初始化数据库
    await init_db()
    print("数据库初始化完成")

    # 获取数据库会话
    db_gen = get_db()
    db = await db_gen.__anext__()

    # 采集研报
    print("\n=== 开始采集研报 ===")
    collector = ReportCollector(db)

    total = 0
    for code, name in STOCKS.items():
        print(f"\n采集 {name} ({code})...")
        try:
            count = await collector.collect_by_stock(code, days=180)
            total += count
            print(f"  新增 {count} 条")
        except Exception as e:
            print(f"  失败: {e}")

    print(f"\n总共新增 {total} 条研报")


if __name__ == "__main__":
    asyncio.run(main())