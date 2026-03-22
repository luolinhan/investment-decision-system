"""
调试采集脚本
"""
import sqlite3
import os
import akshare as ak

# 连接数据库
conn = sqlite3.connect('data/reports.db')
c = conn.cursor()

# 测试采集一只股票
code = "601888"
name = "中国中免"

print(f'测试采集 {name} ({code})...')
df = ak.stock_research_report_em(symbol=code)

if df is None or len(df) == 0:
    print('无数据')
else:
    print(f'获取到 {len(df)} 条')
    print(f'\n列名: {list(df.columns)}')

    # 打印前3行数据
    for idx, row in df.head(3).iterrows():
        print(f'\n--- 第{idx}行 ---')
        for col in df.columns:
            val = row[col]
            print(f'  {col}: {str(val)[:100]}')

    # 检查标题列
    cols = list(df.columns)
    title_col = None
    pdf_col = None
    for col in cols:
        col_str = str(col)
        if '标题' in col_str:
            title_col = col
        elif 'PDF' in col_str or '链接' in col_str:
            pdf_col = col

    print(f'\n标题列: {title_col}')
    print(f'PDF列: {pdf_col}')

    if title_col:
        print(f'\n第一个标题: {df.iloc[0][title_col]}')
    if pdf_col:
        print(f'第一个PDF URL: {df.iloc[0][pdf_col][:100] if len(str(df.iloc[0][pdf_col])) > 100 else df.iloc[0][pdf_col]}')

conn.close()