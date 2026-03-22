"""检查VIX数据"""
import sqlite3

conn = sqlite3.connect("data/investment.db")
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM vix_history")
count = c.fetchone()[0]
print(f"VIX记录数: {count}")

if count > 0:
    c.execute("SELECT * FROM vix_history ORDER BY trade_date DESC LIMIT 5")
    print("\n最近5条VIX数据:")
    for row in c.fetchall():
        print(f"  {row[1]}: open={row[2]:.2f}, high={row[3]:.2f}, low={row[4]:.2f}, close={row[5]:.2f}")

conn.close()