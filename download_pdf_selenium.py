"""
使用Selenium + Chrome下载PDF
"""
import os
import time
import sqlite3
import sys

def download_pdf_selenium(pdf_url, output_path):
    """使用Selenium下载PDF"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    # 配置Chrome选项
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')

    # 设置下载目录
    download_dir = os.path.dirname(output_path)
    prefs = {
        'download.default_directory': download_dir,
        'download.prompt_for_download': False,
        'plugins.always_open_pdf_externally': True,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': False
    }
    options.add_experimental_option('prefs', prefs)

    # 设置User-Agent
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    try:
        # 创建driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        try:
            # 先访问东方财富建立session
            print("访问东方财富首页...")
            driver.get('https://www.eastmoney.com/')
            time.sleep(2)

            # 访问数据页面
            print("访问数据页面...")
            driver.get('https://data.eastmoney.com/report/')
            time.sleep(2)

            # 访问PDF URL
            print(f"下载PDF: {pdf_url}")
            driver.get(pdf_url)
            time.sleep(5)

            # 检查下载目录
            filename = os.path.basename(output_path)

            # 等待下载完成
            for _ in range(30):
                files = os.listdir(download_dir)
                pdf_files = [f for f in files if f.endswith('.pdf') and not f.endswith('.crdownload')]
                if pdf_files:
                    for f in pdf_files:
                        src = os.path.join(download_dir, f)
                        if os.path.getsize(src) > 5000:
                            if f != filename:
                                os.rename(src, output_path)
                            return True, f"成功，大小: {os.path.getsize(output_path)}"
                time.sleep(1)

            return False, "下载超时或文件过小"

        finally:
            driver.quit()

    except Exception as e:
        return False, str(e)


def test_single_pdf(report_id):
    """测试单个PDF下载"""
    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()
    c.execute('SELECT id, title, pdf_url FROM reports WHERE id = ?', (report_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        print(f"报告 {report_id} 不存在")
        return False

    report_id, title, pdf_url = row
    print(f"\n下载研报: {title[:50]}...")
    print(f"URL: {pdf_url}")

    # 创建输出目录
    pdf_dir = "data/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    safe_title = "".join(c for c in title[:50] if c.isalnum() or c in " -_")
    output_path = os.path.join(pdf_dir, f"{report_id}_{safe_title}.pdf")

    # 删除旧文件
    if os.path.exists(output_path):
        os.remove(output_path)

    ok, msg = download_pdf_selenium(pdf_url, output_path)
    print(f"结果: {msg}")

    if ok:
        # 更新数据库
        conn = sqlite3.connect('data/reports.db')
        c = conn.cursor()
        c.execute('UPDATE reports SET local_pdf_path = ? WHERE id = ?', (output_path, report_id))
        conn.commit()
        conn.close()

    return ok


def download_batch(limit=10):
    """批量下载PDF"""
    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()

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
    conn.close()

    print(f"找到 {len(reports)} 条需要下载PDF的研报")

    success = 0
    for report_id, title, pdf_url in reports:
        if test_single_pdf(report_id):
            success += 1
        time.sleep(2)  # 避免请求过快

    print(f"\n完成: 成功 {success}/{len(reports)}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_single_pdf(int(sys.argv[1]))
    else:
        download_batch(limit=5)