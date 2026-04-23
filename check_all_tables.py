# -*- coding: utf-8 -*-
import sqlite3
conn = sqlite3.connect('data/investment.db')
c = conn.cursor()
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for t in tables:
    tn = t[0]
    if tn.startswith('sqlite'):
        continue
    try:
        cnt = c.execute(f'SELECT COUNT(*) FROM {tn}').fetchone()[0]
        cols = [r[1] for r in c.execute(f'PRAGMA table_info({tn})').fetchall()]
        date_col = None
        for col in ['trade_date', 'snapshot_date', 'created_at', 'date']:
            if col in cols:
                date_col = col
                break
        latest = c.execute(f'SELECT MAX({date_col}) FROM {tn}').fetchone()[0] if date_col else '-'
        print(f'{tn:30s} count={cnt:6d}  latest={latest}')
    except Exception as e:
        print(f'{tn:30s} ERROR: {e}')
conn.close()
