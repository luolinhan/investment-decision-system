# 详细检查研报分布
import sqlite3

conn = sqlite3.connect('data/reports.db')
c = conn.cursor()

# 按股票代码统计
c.execute('SELECT stock_code, stock_name, COUNT(*) FROM reports GROUP BY stock_code ORDER BY COUNT(*) DESC')
print('按股票代码统计:')
for row in c.fetchall():
    print(f'  {row[0]} ({row[1]}): {row[2]}条')

# 总数
c.execute('SELECT COUNT(*) FROM reports')
print(f'\n总数: {c.fetchone()[0]}')

conn.close()