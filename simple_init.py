"""
初始化数据库并采集研报（简化版）
"""
import sqlite3
import os
import akshare as ak
from datetime import datetime, date, timedelta

# 创建数据目录
os.makedirs('data', exist_ok=True)

# 初始化数据库
conn = sqlite3.connect('data/reports.db')
c = conn.cursor()

# 创建表
c.execute('''
CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    stock_code TEXT,
    stock_name TEXT,
    institution TEXT,
    author TEXT,
    rating TEXT,
    publish_date TEXT,
    pdf_url TEXT,
    local_pdf_path TEXT,
    summary TEXT,
    raw_content TEXT,
    source TEXT,
    external_id TEXT UNIQUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
''')

# 创建索引
c.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON reports(stock_code)')
c.execute('CREATE INDEX IF NOT EXISTS idx_publish_date ON reports(publish_date)')
c.execute('CREATE INDEX IF NOT EXISTS idx_external_id ON reports(external_id)')

conn.commit()
print("数据库初始化完成")

# A股股票代码
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
        for col in cols:
            col_str = str(col)
            if '标题' in col_str or '报告名称' in col_str:
                title_col = col
            elif 'PDF' in col_str or '链接' in col_str:
                pdf_col = col
            elif '日期' in col_str:
                date_col = col
            elif '机构' in col_str:
                inst_col = col
            elif '类型' in col_str or '评级' in col_str or '东财评级' in col_str:
                rating_col = col

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

            # 检查是否已存在
            c.execute('SELECT id FROM reports WHERE title = ?', (title,))
            if c.fetchone():
                continue

            # 转换股票代码格式
            if code.startswith('6'):
                stock_code = f"{code}.SH"
            else:
                stock_code = f"{code}.SZ"

            external_id = f"akshare_{code}_{idx}"

            try:
                c.execute('''
                    INSERT INTO reports (external_id, title, stock_code, stock_name, institution, rating, publish_date, pdf_url, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (external_id, title, stock_code, name, institution, rating, date_str, pdf_url, 'eastmoney'))
                added += 1
            except sqlite3.IntegrityError:
                continue

        conn.commit()
        print(f'  新增 {added} 条')
        return added

    except Exception as e:
        print(f'  失败: {e}')
        return 0


if __name__ == "__main__":
    print('=== 开始采集研报 ===')

    total = 0
    for code, name in STOCKS.items():
        count = collect_stock(code, name)
        total += count

    conn.close()
    print(f'\n总共新增 {total} 条研报')