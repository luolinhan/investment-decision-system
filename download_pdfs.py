"""
下载研报PDF
"""
import asyncio
import sys
import os
sys.path.insert(0, '.')

os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

import httpx
from sqlalchemy import select
from app.database import get_db, init_db
from app.models import Report

async def download_pdfs(limit=50):
    """下载研报PDF"""
    print("=== 下载研报PDF ===\n")

    await init_db()
    db_gen = get_db()
    db = await db_gen.__anext__()

    # 获取没有本地PDF的研报
    result = await db.execute(
        select(Report).where(
            Report.pdf_url.isnot(None),
            Report.local_pdf_path.is_(None)
        ).limit(200)
    )
    reports = result.scalars().all()

    print(f"找到 {len(reports)} 条需要下载PDF的研报")

    # 创建PDF目录
    pdf_dir = os.path.join("data", "pdfs")
    os.makedirs(pdf_dir, exist_ok=True)

    proxy = "http://127.0.0.1:7890"
    downloaded = 0

    async with httpx.AsyncClient(timeout=60, proxy=proxy, follow_redirects=True) as client:
        for report in reports:
            if report.local_pdf_path and os.path.exists(report.local_pdf_path):
                continue

            if not report.pdf_url:
                continue

            try:
                print(f"下载: {report.title[:40]}...")
                resp = await client.get(report.pdf_url)

                if resp.status_code == 200:
                    # 生成文件名
                    safe_title = "".join(c for c in report.title[:50] if c.isalnum() or c in " -_")
                    filename = f"{report.id}_{safe_title}.pdf"
                    filepath = os.path.join(pdf_dir, filename)

                    with open(filepath, 'wb') as f:
                        f.write(resp.content)

                    report.local_pdf_path = filepath
                    downloaded += 1
                    print(f"  成功: {filename}")
                else:
                    print(f"  失败: HTTP {resp.status_code}")

            except Exception as e:
                print(f"  错误: {e}")

    await db.commit()
    print(f"\n下载完成，共下载 {downloaded} 个PDF")


if __name__ == "__main__":
    asyncio.run(download_pdfs())