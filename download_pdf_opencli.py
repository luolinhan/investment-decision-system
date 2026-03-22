"""
使用OpenCLI下载PDF（绑过安全防护）
"""
import subprocess
import os
import sqlite3
import time

PDF_DIR = "data/pdfs"
os.makedirs(PDF_DIR, exist_ok=True)

def download_pdf_with_opencli(pdf_url, output_path, timeout=60):
    """使用OpenCLI下载PDF"""
    try:
        # OpenCLI使用浏览器下载，可以绑过安全防护
        cmd = [
            "opencli", "browse",
            "--url", pdf_url,
            "--wait", "5",
            "--download", output_path
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.getcwd()
        )

        if os.path.exists(output_path) and os.path.getsize(output_path) > 5000:
            return True, "成功"
        else:
            return False, f"文件过小或不存在: {result.stderr}"

    except subprocess.TimeoutExpired:
        return False, "超时"
    except Exception as e:
        return False, str(e)


def download_pdf_with_curl(pdf_url, output_path, proxy="http://127.0.0.1:7890"):
    """使用curl下载PDF（设置正确的headers）"""
    import httpx

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*",
        "Referer": "https://data.eastmoney.com/",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    try:
        with httpx.Client(proxy=proxy, timeout=60, follow_redirects=True, headers=headers) as client:
            resp = client.get(pdf_url)

            if resp.status_code == 200 and len(resp.content) > 5000:
                with open(output_path, 'wb') as f:
                    f.write(resp.content)
                return True, f"成功，大小: {len(resp.content)}"
            else:
                return False, f"HTTP {resp.status_code}, 大小: {len(resp.content)}"

    except Exception as e:
        return False, str(e)


def download_pdfs(limit=10):
    """下载PDF"""

    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()

    # 获取需要下载PDF的研报
    c.execute('''
        SELECT id, title, pdf_url
        FROM reports
        WHERE pdf_url IS NOT NULL
        AND pdf_url != ''
        AND (local_pdf_path IS NULL OR local_pdf_path = '')
        ORDER BY publish_date DESC
        LIMIT ?
    ''', (limit,))

    reports = c.fetchall()
    print(f"找到 {len(reports)} 条需要下载PDF的研报")

    success = 0
    failed = 0

    for report_id, title, pdf_url in reports:
        safe_title = "".join(c for c in title[:50] if c.isalnum() or c in " -_")
        filename = f"{report_id}_{safe_title}.pdf"
        filepath = os.path.join(PDF_DIR, filename)

        print(f"\n下载: {title[:40]}...")
        print(f"  URL: {pdf_url}")

        # 先尝试curl方式
        ok, msg = download_pdf_with_curl(pdf_url, filepath)

        if not ok:
            print(f"  curl失败: {msg}")
            # 如果curl失败，尝试OpenCLI
            print(f"  尝试OpenCLI...")
            ok, msg = download_pdf_with_opencli(pdf_url, filepath)

        if ok:
            c.execute('UPDATE reports SET local_pdf_path = ? WHERE id = ?', (filepath, report_id))
            conn.commit()
            success += 1
            print(f"  成功!")
        else:
            failed += 1
            print(f"  失败: {msg}")

        time.sleep(1)  # 避免请求过快

    conn.close()
    print(f"\n完成: 成功 {success}, 失败 {failed}")


if __name__ == "__main__":
    download_pdfs(limit=20)