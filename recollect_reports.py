# 重新采集研报（清理external_id避免冲突）
import asyncio
import sys
sys.path.insert(0, '.')
import os
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

import akshare as ak
import sqlite3
from datetime import datetime

STOCKS = {
    "002459": "晶澳科技",
    "600438": "通威股份",
    "601012": "隆基绿能",
    "300763": "锦浪科技",
    "688235": "百济神州",
    "603259": "药明康德",
    "600196": "复星医药",
    "601888": "中国中免",
}

def collect_stock(code, name):
    """采集单只股票研报"""
    print(f'\n采集 {name} ({code})...')
    try:
        df = ak.stock_research_report_em(symbol=code)
        if df is None or len(df) == 0:
            print(f'  无数据')
            return 0

        print(f'  获取到 {len(df)} 条')

        cols = list(df.columns)
        # 找到各列的位置
        title_col = pdf_col = date_col = inst_col = rating_col = None
        for c in cols:
            c_str = str(c)
            if '标题' in c_str:
                title_col = c
            elif 'PDF' in c_str or '链接' in c_str:
                pdf_col = c
            elif '日期' in c_str:
                date_col = c
            elif '机构' in c_str:
                inst_col = c
            elif '类型' in c_str or '评级' in c_str:
                rating_col = c

        conn = sqlite3.connect('data/reports.db')
        c = conn.cursor()

        added = 0
        for idx, row in df.iterrows():
            title = str(row.get(title_col, '')) if title_col else ''
            pdf_url = str(row.get(pdf_col, '')) if pdf_col else ''
            date_str = str(row.get(date_col, '')) if date_col else ''
            institution = str(row.get(inst_col, '')) if inst_col else ''
            rating = str(row.get(rating_col, '')) if rating_col else ''

            if not title or len(title) < 5:
                continue
            if not pdf_url.startswith('http'):
                continue

            # 检查是否已存在（用标题判断）
            c.execute('SELECT id FROM reports WHERE title = ?', (title,))
            if c.fetchone():
                continue

            # 转换股票代码格式
            if code.startswith('6'):
                stock_code = f"{code}.SH"
            else:
                stock_code = f"{code}.SZ"

            external_id = f"akshare_{code}_{idx}"

            c.execute('''
                INSERT INTO reports (external_id, title, stock_code, stock_name, institution, rating, publish_date, pdf_url, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (external_id, title, stock_code, name, institution, rating, date_str, pdf_url, 'eastmoney'))
            added += 1

        conn.commit()
        conn.close()
        print(f'  新增 {added} 条')
        return added

    except Exception as e:
        print(f'  失败: {e}')
        return 0


if __name__ == "__main__":
    print('=== 重新采集研报 ===')

    total = 0
    for code, name in STOCKS.items():
        count = collect_stock(code, name)
        total += count

    print(f'\n总共新增 {total} 条研报')