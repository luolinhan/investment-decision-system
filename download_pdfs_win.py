"""
下载研报PDF（Windows版）
"""
import sqlite3
import os
import httpx
import asyncio

# 创建PDF目录
pdf_dir = os.path.join("data", "pdfs")
os.makedirs(pdf_dir, exist_ok=True)

# 连接数据库
conn = sqlite3.connect('data/reports.db')
c = conn.cursor()

# 获取需要下载PDF的研报
c.execute('SELECT id, title, pdf_url FROM reports WHERE pdf_url IS NOT NULL AND pdf_url != "" AND local_pdf_path IS NULL')
reports = c.fetchall()

print(f"找到 {len(reports)} 条需要下载PDF的研报")

# 设置代理
proxy = "http://127.0.0.1:7890"

async def download_pdfs():
    downloaded = 0
    failed = 0

    async with httpx.AsyncClient(timeout=60, proxy=proxy, follow_redirects=True) as client:
        for report_id, title, pdf_url in reports[:200]:  # 先下载前200个
            try:
                # 生成安全文件名
                safe_title = "".join(c for c in title[:50] if c.isalnum() or c in " -_")
                filename = f"{report_id}_{safe_title}.pdf"
                filepath = os.path.join(pdf_dir, filename)

                if os.path.exists(filepath):
                    c.execute('UPDATE reports SET local_pdf_path = ? WHERE id = ?', (filepath, report_id))
                    downloaded += 1
                    continue

                print(f"下载: {title[:40]}...")
                resp = await client.get(pdf_url)

                if resp.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(resp.content)
                    c.execute('UPDATE reports SET local_pdf_path = ? WHERE id = ?', (filepath, report_id))
                    downloaded += 1
                    print(f"  成功")
                else:
                    print(f"  失败: HTTP {resp.status_code}")
                    failed += 1

            except Exception as e:
                print(f"  错误: {e}")
                failed += 1

            # 每50个提交一次
            if downloaded % 50 == 0:
                conn.commit()

    conn.commit()
    print(f"\n下载完成: 成功 {downloaded}, 失败 {failed}")

asyncio.run(download_pdfs())
conn.close()