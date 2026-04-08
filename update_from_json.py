import json
import sqlite3
import os

os.chdir(r'C:\Users\Administrator\research_report_system')

with open('data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

conn = sqlite3.connect('data/investment.db')
c = conn.cursor()
today = data['update_time'][:10]

# 指数
c.execute('DELETE FROM index_history')
for idx in data['indices']:
    c.execute('INSERT OR REPLACE INTO index_history (code, name, trade_date, close, change_pct) VALUES (?,?,?,?,?)',
              (idx['code'], idx['name'], today, idx['close'], idx['change_pct']))
print('Indices:', len(data['indices']))

# 股票（跳过全为空的占位记录）
c.execute('DELETE FROM stock_financial')
saved = 0
for s in data['stocks']:
    values = [
        s.get('pe_ttm'),
        s.get('pb'),
        s.get('roe'),
        s.get('gross_margin'),
        s.get('net_margin'),
        s.get('revenue_yoy'),
        s.get('profit_yoy'),
        s.get('dividend_yield'),
    ]
    if all(v in (None, '', '-') for v in values):
        continue
    c.execute(
        'INSERT INTO stock_financial (code, name, report_date, pe_ttm, pb, roe, gross_margin, net_margin, revenue_yoy, net_profit_yoy, dividend_yield) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
            s['code'],
            s['name'],
            today,
            s.get('pe_ttm'),
            s.get('pb'),
            s.get('roe'),
            s.get('gross_margin'),
            s.get('net_margin'),
            s.get('revenue_yoy'),
            s.get('profit_yoy'),
            s.get('dividend_yield'),
        ),
    )
    saved += 1
print('Stocks:', saved)

# VIX
c.execute('DELETE FROM vix_history')
c.execute('INSERT INTO vix_history (trade_date, vix_close) VALUES (?,?)', (today, data['vix']['close']))
print('VIX:', data['vix']['close'])

# 利率
c.execute('DELETE FROM interest_rates')
r = data['rates']
c.execute('INSERT INTO interest_rates (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y) VALUES (?,?,?,?,?,?,?)',
          (today, r['shibor_overnight'], r['shibor_1w'], r['shibor_1m'], r['shibor_3m'], r['shibor_6m'], r['shibor_1y']))
print('Rates: OK')

conn.commit()
conn.close()
print('Database updated!')
