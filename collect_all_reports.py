"""
采集所有关注股票的研报
"""
import asyncio
import sys
import os
sys.path.insert(0, '.')

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

from app.database import get_db, init_db
from app.services.collector import ReportCollector

# 关注的A股股票代码 (AKShare格式: 纯代码)
STOCKS = {
    # A股光伏
    "002459": "晶澳科技",
    "600438": "通威股份",
    "601012": "隆基绿能",
    "300763": "锦浪科技",
    # A股医药
    "688235": "百济神州",
    "603259": "药明康德",
    "600196": "复星医药",
    # A股消费
    "601888": "中国中免",
}


async def collect_all():
    """采集所有股票的研报"""
    print("=== 开始采集研报 ===\n")

    # 初始化数据库
    await init_db()

    db_gen = get_db()
    db = await db_gen.__anext__()
    collector = ReportCollector(db)

    total = 0
    for code, name in STOCKS.items():
        print(f"\n采集 {name} ({code})...")
        try:
            count = await collector.collect_by_stock(code, days=180, download_pdf=False)
            total += count
        except Exception as e:
            print(f"  失败: {e}")

    print(f"\n=== 采集完成，共新增 {total} 条研报 ===")


if __name__ == "__main__":
    asyncio.run(collect_all())