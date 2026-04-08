import sqlite3
conn = sqlite3.connect('C:/Users/Administrator/research_report_system/data/investment.db')
c = conn.cursor()

# 添加VIX
try:
    c.execute("INSERT OR REPLACE INTO vix_history (trade_date, vix_close) VALUES ('2026-03-23', 18.5)")
    print("VIX inserted")
except Exception as e:
    print("VIX error:", e)

# 添加利率
try:
    c.execute("INSERT OR REPLACE INTO interest_rates (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y) VALUES ('2026-03-23', 1.75, 1.85, 1.95, 2.05, 2.15, 2.25)")
    print("Rates inserted")
except Exception as e:
    print("Rates error:", e)

conn.commit()
conn.close()
print("Done")
