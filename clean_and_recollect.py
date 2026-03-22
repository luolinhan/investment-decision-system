"""
清理和重新采集研报
"""
import asyncio
import sys
sys.path.insert(0, '.')

from app.database import get_db, init_db
from app.models import Report
from sqlalchemy import delete, select
from app.services.collector import ReportCollector

STOCKS = ["002459", "600438", "601012", "300763", "688235", "603259", "600196", "601888"]


async def clean_and_recollect():
    """清理无效数据并重新采集"""
    await init_db()
    db_gen = get_db()
    db = await db_gen.__anext__()

    # 删除没有标题的研报
    print("清理无效研报...")
    result = await db.execute(
        delete(Report).where(Report.title == '')
    )
    deleted = result.rowcount
    await db.commit()
    print(f"删除了 {deleted} 条无效研报")

    # 重新采集
    print("\n重新采集研报...")
    collector = ReportCollector(db)

    total = 0
    for code in STOCKS:
        print(f"\n采集 {code}...")
        try:
            count = await collector.collect_by_stock(code, days=180)
            total += count
        except Exception as e:
            print(f"  失败: {e}")

    print(f"\n共新增 {total} 条研报")


if __name__ == "__main__":
    asyncio.run(clean_and_recollect())