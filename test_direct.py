# 测试数据库保存
import asyncio
import akshare as ak
import sys
sys.path.insert(0, 'C:/Projects/research_report_system')

from datetime import datetime, date, timedelta
from sqlalchemy import select

async def test():
    from app.database import async_session
    from app.models import Report

    print("获取研报数据...")
    df = ak.stock_research_report_em()
    print(f"获取到 {len(df)} 条研报")

    print("\n保存到数据库...")
    async with async_session() as db:
        new_count = 0
        for i, row in df.head(10).iterrows():  # 先测试10条
            try:
                # 生成唯一ID
                external_id = f"akshare_{row.get('序号', i)}"

                # 检查是否已存在
                result = await db.execute(
                    select(Report).where(Report.external_id == external_id)
                )
                if result.scalar_one_or_none():
                    continue

                # 解析日期
                date_str = str(row.get('日期', ''))
                try:
                    publish_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except:
                    publish_date = date.today()

                # 股票代码格式转换
                stock_code = str(row.get('股票代码', ''))
                if stock_code.startswith('6'):
                    stock_code = f"{stock_code}.SH"
                elif stock_code.startswith(('0', '3')):
                    stock_code = f"{stock_code}.SZ"

                # 创建研报记录
                report = Report(
                    external_id=external_id,
                    title=str(row.get('研报标题', '')),
                    stock_code=stock_code,
                    stock_name=str(row.get('股票简称', '')),
                    institution=str(row.get('机构', '')),
                    rating=str(row.get('研报类型', '')),
                    publish_date=publish_date,
                    pdf_url=str(row.get('研报PDF链接', '')),
                    source="eastmoney",
                )

                db.add(report)
                new_count += 1

            except Exception as e:
                print(f"处理第{i}条失败: {e}")

        await db.commit()
        print(f"新增 {new_count} 条研报")

asyncio.run(test())