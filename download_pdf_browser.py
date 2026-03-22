"""
使用Selenium下载PDF（绑过安全防护）
需要安装: pip install selenium webdriver-manager
"""
import os
import time
import sqlite3

def download_pdf_selenium(pdf_url, output_path):
    """使用Selenium下载PDF"""
    try:
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
        options.add_experimental_option('prefs', {
            'download.default_directory': os.path.dirname(output_path),
            'download.prompt_for_download': False,
            'plugins.always_open_pdf_externally': True
        })

        # 设置User-Agent
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        # 创建driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        try:
            # 访问PDF URL
            driver.get(pdf_url)
            time.sleep(5)  # 等待下载

            # 检查文件是否下载成功
            if os.path.exists(output_path) and os.path.getsize(output_path) > 5000:
                return True, "成功"

            # 可能下载到了默认下载目录
            downloads_dir = os.path.expanduser("~/Downloads")
            for f in os.listdir(downloads_dir):
                if f.endswith('.pdf') and os.path.getsize(os.path.join(downloads_dir, f)) > 5000:
                    os.rename(os.path.join(downloads_dir, f), output_path)
                    return True, "成功"

            return False, "文件未找到或过小"

        finally:
            driver.quit()

    except ImportError:
        return False, "需要安装selenium和webdriver-manager"
    except Exception as e:
        return False, str(e)


def download_pdf_playwright(pdf_url, output_path):
    """使用Playwright下载PDF（更轻量）"""
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                accept_downloads=True,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            # 访问PDF URL并等待下载
            with page.expect_download() as download_info:
                page.goto(pdf_url)
                download = download_info.value

            # 保存文件
            download.save_as(output_path)
            browser.close()

            if os.path.exists(output_path) and os.path.getsize(output_path) > 5000:
                return True, "成功"
            return False, "文件过小"

    except ImportError:
        return False, "需要安装playwright"
    except Exception as e:
        return False, str(e)


def download_pdf_requests(pdf_url, output_path, proxy="http://127.0.0.1:7890"):
    """使用requests with session下载PDF"""
    import requests

    session = requests.Session()
    session.proxies = {'http': proxy, 'https': proxy}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://data.eastmoney.com/',
        'Connection': 'keep-alive',
    }

    try:
        # 首先访问东方财富主页建立session
        session.get('https://www.eastmoney.com/', headers=headers, timeout=10)
        time.sleep(1)

        # 然后访问数据页面
        session.get('https://data.eastmoney.com/report/', headers=headers, timeout=10)
        time.sleep(1)

        # 最后下载PDF
        headers['Accept'] = 'application/pdf,*/*'
        resp = session.get(pdf_url, headers=headers, timeout=60)

        if resp.status_code == 200 and len(resp.content) > 5000:
            with open(output_path, 'wb') as f:
                f.write(resp.content)
            return True, f"成功，大小: {len(resp.content)}"
        else:
            return False, f"HTTP {resp.status_code}, 大小: {len(resp.content)}"

    except Exception as e:
        return False, str(e)


def test_pdf_download(report_id):
    """测试PDF下载"""
    conn = sqlite3.connect('data/reports.db')
    c = conn.cursor()

    c.execute('SELECT id, title, pdf_url FROM reports WHERE id = ?', (report_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        print(f"报告 {report_id} 不存在")
        return

    report_id, title, pdf_url = row
    print(f"测试下载: {title}")
    print(f"URL: {pdf_url}")

    # 创建输出目录
    pdf_dir = "data/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    safe_title = "".join(c for c in title[:50] if c.isalnum() or c in " -_")
    output_path = os.path.join(pdf_dir, f"{report_id}_{safe_title}.pdf")

    # 尝试不同的方法
    print("\n1. 尝试requests with session...")
    ok, msg = download_pdf_requests(pdf_url, output_path)
    print(f"   结果: {msg}")

    if not ok:
        print("\n2. 尝试playwright...")
        ok, msg = download_pdf_playwright(pdf_url, output_path)
        print(f"   结果: {msg}")

    if not ok:
        print("\n3. 尝试selenium...")
        ok, msg = download_pdf_selenium(pdf_url, output_path)
        print(f"   结果: {msg}")

    return ok


if __name__ == "__main__":
    # 测试下载ID为1234的研报
    test_pdf_download(1234)