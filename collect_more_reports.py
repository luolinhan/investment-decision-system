"""
采集更多股票研报
"""
import asyncio
import sys
sys.path.insert(0, '.')
from app.database import get_db, init_db
from app.services.hibor_client import HuiborClient
from app.services.collector import ReportCollector
import akshare as ak

# 关注的股票代码
STOCKS = {
    # A股光伏
    "002459.SZ": "晶澳科技",
    "600438.SH": "通威股份",
    "601012.SH": "隆基绿能",
    "300763.SZ": "锦浪科技",
    # A股医药
    "688235.SH": "百济神州",
    "603259.SH": "药明康德",
    "600196.SH": "复星医药",
    # A股消费
    "601888.SH": "中国中免",
    # 港股科技
    "09988.HK": "阿里巴巴",
    "00700.HK": "腾讯",
    "03690.HK": "美团",
    "01810.HK": "小米",
    "01024.HK": "快手",
    # 港股医药
    "06160.HK": "百济神州",
    "02269.HK": "药明生物",
}

async def collect_akshare_reports():
    """使用AKShare采集A股研报"""
    print("=== 使用AKShare采集A股研报 ===\n")

    db_gen = get_db()
    db = await db_gen.__anext__()
    collector = ReportCollector(db)

    total = 0
    for code, name in STOCKS.items():
        if code.endswith('.HK'):
            continue  # AKShare只支持A股
        try:
            print(f"采集 {name} ({code})...")
            count = await collector.collect_by_stock(code, days=60, download_pdf=False)
            print(f"  新增 {count} 条")
            total += count
        except Exception as e:
            print(f"  失败: {e}")

    print(f"\nAKShare采集完成，共新增 {total} 条研报")


def collect_huibor_reports():
    """使用慧博采集研报"""
    print("\n=== 使用慧博投研采集研报 ===\n")

    client = HuiborClient(username="luolinhan", password="LUOLINHAN666")
    client.login()

    # 搜索关键词
    keywords = [
        "阿里巴巴", "腾讯", "美团", "小米", "快手",
        "百济神州", "药明生物",
        "晶澳科技", "通威股份", "隆基绿能", "锦浪科技",
        "光伏", "新能源",
        "医药", "创新药",
    ]

    all_reports = []
    for kw in keywords[:3]:  # 先测试几个
        print(f"搜索: {kw}")
        try:
            # 慧博搜索
            reports = client.get_report_list(page=1)
            for r in reports[:5]:
                if kw in r.get('title', ''):
                    all_reports.append(r)
            print(f"  找到 {len(reports)} 条")
        except Exception as e:
            print(f"  失败: {e}")

    client.close()

    print(f"\n慧博找到 {len(all_reports)} 条相关研报")
    return all_reports


async def main():
    print("开始采集研报...\n")

    # 初始化数据库
    await init_db()

    # 1. AKShare采集A股
    await collect_akshare_reports()

    # 2. 慧博采集（测试）
    # collect_huibor_reports()

    print("\n采集完成!")


if __name__ == "__main__":
    asyncio.run(main())