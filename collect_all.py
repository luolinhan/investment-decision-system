# 重新采集研报（使用列索引）
import asyncio
import akshare as ak
import sys
sys.path.insert(0, 'C:/Projects/research_report_system')

from datetime import datetime, date
from sqlalchemy import select, delete

async def collect_all():
    from app.database import async_session
    from app.models import Report

    print("获取研报数据...")
    df = ak.stock_research_report_em()
    print(f"获取到 {len(df)} 条研报")

    print("\n清空旧数据...")
    async with async_session() as db:
        await db.execute(delete(Report))
        await db.commit()

    print("保存到数据库...")
    async with async_session() as db:
        new_count = 0
        for i, row in df.iterrows():
            try:
                # 使用列索引获取数据
                # 0:序号, 1:股票代码, 2:股票简称, 3:研报标题, 4:研报类型, 5:机构, 14:日期, 15:PDF链接
                row_values = list(row)
                seq = row_values[0]
                stock_code = str(row_values[1]) if row_values[1] else ""
                stock_name = str(row_values[2]) if row_values[2] else ""
                title = str(row_values[3]) if row_values[3] else ""
                rating = str(row_values[4]) if row_values[4] else ""
                institution = str(row_values[5]) if row_values[5] else ""
                date_str = str(row_values[14]) if row_values[14] else ""
                pdf_url = str(row_values[15]) if row_values[15] else ""

                # 生成唯一ID
                external_id = f"akshare_{seq}"

                # 解析日期
                try:
                    publish_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except:
                    publish_date = date.today()

                # 股票代码格式转换
                if stock_code.startswith('6'):
                    stock_code = f"{stock_code}.SH"
                elif stock_code.startswith(('0', '3')):
                    stock_code = f"{stock_code}.SZ"

                # 创建研报记录
                report = Report(
                    external_id=external_id,
                    title=title,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    institution=institution,
                    rating=rating,
                    publish_date=publish_date,
                    pdf_url=pdf_url,
                    source="eastmoney",
                )

                db.add(report)
                new_count += 1

                if new_count % 50 == 0:
                    print(f"已处理 {new_count} 条...")

            except Exception as e:
                print(f"处理第{i}条失败: {e}")

        await db.commit()
        print(f"\n采集完成！共保存 {new_count} 条研报")

asyncio.run(collect_all())