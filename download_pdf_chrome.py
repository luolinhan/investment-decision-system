"""
使用Chrome --print-to-pdf下载PDF
修复版：使用正确的命令行调用
"""
import os
import subprocess
import sqlite3
import time

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PDF_DIR = os.path.abspath("data/pdfs")

def download_pdf_chrome(pdf_url, output_path):
    """使用Chrome打印PDF"""
    try:
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 删除已存在的文件
        if os.path.exists(output_path):
            os.remove(output_path)

        # 使用shell=True运行Chrome命令
        cmd = f'"{CHROME_PATH}" --headless --disable-gpu --no-sandbox --print-to-pdf="{output_path}" "{pdf_url}"'

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            timeout=60
        )

        # 等待文件创建
        time.sleep(1)

        # 检查文件是否存在且有效
        if os.path.exists(output_path):
            size = os.path.getsize(output_path)
            if size > 5000:
                return True, f"成功，大小: {size}"
            else:
                return False, f"文件过小: {size}"
        else:
            return False, f"文件未创建"

    except subprocess.TimeoutExpired:
        return False, "超时"
    except Exception as e:
        return False, str(e)


def download_pdf(report_id):
    """下载单个研报PDF"""
    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()
    c.execute('SELECT id, title, pdf_url FROM reports WHERE id = ?', (report_id,))
    row = c.fetchone()

    if not row:
        print(f"报告 {report_id} 不存在")
        conn.close()
        return False

    report_id, title, pdf_url = row
    conn.close()

    print(f"\n下载: {title[:50]}...")
    print(f"URL: {pdf_url}")

    # 使用简单的数字文件名
    filename = f"report_{report_id}.pdf"
    output_path = os.path.join(PDF_DIR, filename)

    ok, msg = download_pdf_chrome(pdf_url, output_path)
    print(f"  结果: {msg}")

    if ok:
        conn = sqlite3.connect('data/reports.db')
        c = conn.cursor()
        c.execute('UPDATE reports SET local_pdf_path = ? WHERE id = ?', (output_path, report_id))
        conn.commit()
        conn.close()

    return ok


def download_batch(limit=50):
    """批量下载PDF"""
    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()

    c.execute('''
        SELECT id
        FROM reports
        WHERE pdf_url IS NOT NULL
        AND pdf_url != ''
        AND (local_pdf_path IS NULL OR local_pdf_path = '')
        ORDER BY publish_date DESC
        LIMIT ?
    ''', (limit,))

    ids = [row[0] for row in c.fetchall()]
    conn.close()

    print(f"找到 {len(ids)} 条需要下载PDF的研报")
    os.makedirs(PDF_DIR, exist_ok=True)

    success = 0
    for i, report_id in enumerate(ids):
        print(f"\n[{i+1}/{len(ids)}]", end="")
        if download_pdf(report_id):
            success += 1
        time.sleep(1)  # 避免请求过快

    print(f"\n\n完成: 成功 {success}/{len(ids)}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        download_pdf(int(sys.argv[1]))
    else:
        download_batch(limit=50)