"""测试指数数据采集"""
import akshare as ak
import sqlite3
from datetime import datetime, timedelta

DB_PATH = "data/investment.db"

print("开始测试...")
conn = sqlite3.connect(DB_PATH)

# 采集上证指数
print("采集上证指数...")
df = ak.stock_zh_index_daily(symbol='sh000001')
print(f"  获取到 {len(df)} 条数据")

# 转换日期
df['date'] = df['date'].astype(str)
print(f"  日期列类型: {df['date'].dtype}")

# 筛选
cutoff = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
df_filtered = df[df['date'] >= cutoff]
print(f"  筛选后 {len(df_filtered)} 条")

# 写入数据库
c = conn.cursor()
added = 0
for _, row in df_filtered.iterrows():
    try:
        trade_date = str(row['date'])[:10]
        c.execute('''
            INSERT OR REPLACE INTO index_history
            (code, name, trade_date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('sh000001', '上证指数', trade_date,
              float(row.get('open', 0) or 0),
              float(row.get('high', 0) or 0),
              float(row.get('low', 0) or 0),
              float(row['close']),
              float(row.get('volume', 0) or 0)))
        added += 1
    except Exception as ex:
        print(f"  插入错误: {ex}")

conn.commit()
print(f"  写入 {added} 条")
conn.close()
print("测试完成!")