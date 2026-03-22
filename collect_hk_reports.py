"""
港股研报采集 - 通过东方财富网站爬取
"""
import sqlite3
import os
import requests
import re
import time
from datetime import datetime

# 创建数据目录
os.makedirs('data', exist_ok=True)

# 初始化数据库
conn = sqlite3.connect('data/reports.db')
c = conn.cursor()

# 港股代码
HK_STOCKS = {
    "09988": "阿里巴巴-W",
    "00700": "腾讯控股",
    "03690": "美团-W",
    "01810": "小米集团-W",
    "01024": "快手-W",
    "06160": "百济神州",
    "02269": "药明生物",
    "00883": "中国海洋石油",
    "01880": "中国中免",
    "01177": "中国生物制药",
}

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

PROXY = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}


def fetch_hk_reports_from_eastmoney(code, page=1):
    """从东方财富获取港股研报"""
    # 东方财富港股研报API
    url = f"https://reportapi.eastmoney.com/report/list?cb=&pageNo={page}&pageSize=20&code={code}&industryCode=*&qType=0&beginTime=&endTime=&lastUpdateTime="

    try:
        resp = requests.get(url, headers=HEADERS, proxies=PROXY, timeout=30)
        text = resp.text

        # 解析JSONP响应
        if text.startswith('('):
            text = text[1:]
        if text.endswith(')'):
            text = text[:-1]

        import json
        data = json.loads(text)

        reports = []
        if 'data' in data and isinstance(data['data'], list):
            for item in data['data']:
                reports.append({
                    'title': item.get('title', ''),
                    'institution': item.get('orgSName', ''),
                    'date': item.get('publishDate', ''),
                    'pdf_url': item.get('pdfUrl', ''),
                    'rating': item.get('emRatingName', ''),
                })

        return reports

    except Exception as e:
        print(f"  API请求失败: {e}")
        return []


def fetch_hk_reports_from_em_web(code):
    """通过网页爬取港股研报"""
    reports = []

    # 东方财富港股F10研报页面
    url = f"https://emweb.eastmoney.com/PC_HKF10/ResearchReport/Index?code={code}"

    try:
        resp = requests.get(url, headers=HEADERS, proxies=PROXY, timeout=30)
        if resp.status_code == 200:
            # 解析HTML获取研报数据
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'html.parser')

            # 查找研报表格
            for row in soup.select('table tbody tr'):
                cells = row.select('td')
                if len(cells) >= 4:
                    title_cell = cells[0]
                    link = title_cell.select_one('a')

                    if link:
                        title = link.get_text(strip=True)
                        href = link.get('href', '')

                        reports.append({
                            'title': title,
                            'institution': cells[1].get_text(strip=True) if len(cells) > 1 else '',
                            'date': cells[2].get_text(strip=True) if len(cells) > 2 else '',
                            'pdf_url': href if href.startswith('http') else f"https:{href}" if href.startswith('//') else '',
                            'rating': cells[3].get_text(strip=True) if len(cells) > 3 else '',
                        })

    except Exception as e:
        print(f"  网页爬取失败: {e}")

    return reports


def collect_hk_reports(code, name):
    """采集港股研报"""
    print(f'\n采集 {name} ({code})...')

    # 尝试API方式
    reports = fetch_hk_reports_from_eastmoney(code)

    if not reports:
        # 尝试网页爬取
        print("  尝试网页爬取...")
        reports = fetch_hk_reports_from_em_web(code)

    if not reports:
        print("  无数据")
        return 0

    print(f"  获取到 {len(reports)} 条")

    added = 0
    for report in reports:
        title = report.get('title', '')
        pdf_url = report.get('pdf_url', '')

        if not title or len(title) < 5:
            continue
        if not pdf_url.startswith('http'):
            continue

        # 检查是否已存在
        c.execute('SELECT id FROM reports WHERE title = ?', (title,))
        if c.fetchone():
            continue

        # 港股代码格式
        stock_code = f"{code}.HK"
        external_id = f"em_hk_{code}_{hashlib.md5(title.encode()).hexdigest()[:8]}"
        import hashlib

        try:
            c.execute('''
                INSERT INTO reports (external_id, title, stock_code, stock_name, institution, rating, publish_date, pdf_url, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (external_id, title, stock_code, name, report.get('institution', ''), report.get('rating', ''), report.get('date', ''), pdf_url, 'eastmoney_hk'))
            added += 1
        except sqlite3.IntegrityError:
            continue

    conn.commit()
    print(f"  新增 {added} 条")
    return added


def collect_from_xueqiu():
    """从雪球获取港股研报"""
    print("\n从雪球获取港股研报...")

    # 雪球研报API
    stocks = [
        ("BABA", "阿里巴巴", "k_cb_09988"),
        ("00700", "腾讯控股", "k_cb_00700"),
    ]

    for code, name, stock_id in stocks:
        url = f"https://stock.xueqiu.com/v5/stock/research/list.json?symbol={code}&size=20"

        try:
            resp = requests.get(url, headers={
                **HEADERS,
                "Cookie": "xq_a_token=your_token"  # 可能需要登录
            }, proxies=PROXY, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                print(f"  {name}: {len(data.get('items', []))} 条")

        except Exception as e:
            print(f"  {name}: 失败 {e}")


if __name__ == "__main__":
    import hashlib

    print('=== 采集港股研报 ===')

    total = 0
    for code, name in HK_STOCKS.items():
        count = collect_hk_reports(code, name)
        total += count
        time.sleep(2)  # 避免请求过快

    conn.close()
    print(f'\n总共新增 {total} 条研报')