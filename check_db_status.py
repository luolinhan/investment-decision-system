"""检查数据库状态"""
import sqlite3
import os

db_path = "C:/Users/Administrator/research_report_system/data/investment.db"
conn = sqlite3.connect(db_path)
c = conn.cursor()

print("=" * 60)
print("数据库状态检查")
print("=" * 60)

# VIX数据
c.execute("SELECT COUNT(*), MAX(trade_date), MIN(trade_date) FROM vix_history")
row = c.fetchone()
print(f"\n1. VIX历史: {row[0]}条记录, 最新: {row[1]}, 最早: {row[2]}")
if row[0] > 0:
    c.execute("SELECT * FROM vix_history LIMIT 3")
    print("   样本数据:", c.fetchall())

# 利率数据
c.execute("SELECT COUNT(*), MAX(trade_date) FROM interest_rates")
row = c.fetchone()
print(f"\n2. 利率数据: {row[0]}条记录, 最新: {row[1]}")
if row[0] > 0:
    c.execute("SELECT trade_date, us_10y_bond_yield FROM interest_rates ORDER BY trade_date DESC LIMIT 3")
    print("   美债10Y样本:", c.fetchall())

# 因子快照
c.execute("SELECT COUNT(*), MAX(trade_date), MIN(trade_date) FROM stock_factor_snapshot WHERE model='conservative'")
row = c.fetchone()
print(f"\n3. 因子快照(conservative): {row[0]}条, 最新: {row[1]}, 最早: {row[2]}")

# 评分分布
c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE model='conservative' AND trade_date=? AND total>=70", (row[1],))
high_score = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE model='conservative' AND trade_date=? AND total>=60", (row[1],))
mid_score = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE model='conservative' AND trade_date=? AND total>=50", (row[1],))
low_score = c.fetchone()[0]
print(f"   评分分布(最新日期): >=70:{high_score}, >=60:{mid_score}, >=50:{low_score}")

# TOP10评分股票
c.execute("SELECT code, total, quality, growth, valuation, technical FROM stock_factor_snapshot WHERE model='conservative' AND trade_date=? ORDER BY total DESC LIMIT 10", (row[1],))
print("\n   TOP10评分股票:")
for r in c.fetchall():
    print(f"      {r[0]}: total={r[1]:.1f}, quality={r[2]:.1f}, growth={r[3]:.1f}, valuation={r[4]:.1f}, tech={r[5]:.1f}")

# 核心池成分
c.execute("SELECT COUNT(*) FROM stock_pool_constituents")
pool_count = c.fetchone()[0]
c.execute("SELECT pool_code, COUNT(*) FROM stock_pool_constituents GROUP BY pool_code")
print(f"\n4. 核心池成分: 总计{pool_count}条")
for r in c.fetchall():
    print(f"   {r[0]}: {r[1]}只")

# 技术指标
c.execute("SELECT COUNT(*), MAX(trade_date) FROM technical_indicators")
row = c.fetchone()
print(f"\n5. 技术指标: {row[0]}条, 最新: {row[1]}")

# 估值带
c.execute("SELECT COUNT(*), MAX(trade_date) FROM valuation_bands")
row = c.fetchone()
print(f"\n6. 估值带: {row[0]}条, 最新: {row[1]}")

# 财务数据
c.execute("SELECT COUNT(*), MAX(report_date) FROM stock_financial")
row = c.fetchone()
print(f"\n7. 财务数据: {row[0]}条, 最新报告期: {row[1]}")

conn.close()
print("\n" + "=" * 60)