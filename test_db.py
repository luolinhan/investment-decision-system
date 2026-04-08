import sqlite3
conn = sqlite3.connect(r"C:\Users\Administrator\research_report_system\data\investment.db")
c = conn.cursor()

# 检查表结构
c.execute("PRAGMA table_info(stock_financial)")
print("stock_financial columns:")
for col in c.fetchall():
    print(f"  {col[1]}")

# 检查数据
c.execute("SELECT COUNT(*) FROM stock_financial")
print(f"\nstock_financial: {c.fetchone()[0]} records")

c.execute("SELECT code, name, pe_ttm FROM stock_financial LIMIT 3")
print("\nSample data:")
for row in c.fetchall():
    print(f"  {row}")

conn.close()
