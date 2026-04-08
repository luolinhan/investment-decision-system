"""检查信号日期"""
import sqlite3
conn = sqlite3.connect('data/investment.db')
c = conn.cursor()
c.execute('SELECT DISTINCT as_of_date FROM strategy_signals_v2 ORDER BY as_of_date DESC')
print('Available dates:', [r[0] for r in c.fetchall()])
conn.close()