"""清理重复数据并重新计算涨跌幅"""
import sqlite3

DB_PATH = "data/investment.db"

def clean_and_recalculate():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 美股和富时指数代码
    us_codes = ['dji', 'ixic', 'inx', 'ftsea50', 'yang']

    for code in us_codes:
        # 获取所有数据
        c.execute('SELECT id, trade_date, close FROM index_history WHERE code = ? ORDER BY trade_date', (code,))
        rows = c.fetchall()

        # 找出重复数据（收盘价与前一天相同）
        prev_close = None
        prev_id = None
        to_delete = []

        for row in rows:
            row_id, trade_date, close = row
            if prev_close is not None and abs(close - prev_close) < 0.01:
                # 当前价格与前一天几乎相同，删除当前记录
                to_delete.append(row_id)
            prev_close = close
            prev_id = row_id

        # 删除重复数据
        if to_delete:
            placeholders = ','.join('?' * len(to_delete))
            c.execute(f'DELETE FROM index_history WHERE id IN ({placeholders})', to_delete)
            print(f'{code}: 删除 {len(to_delete)} 条重复数据')

    conn.commit()

    # 重新计算所有指数的涨跌幅
    print('\n重新计算涨跌幅...')

    c.execute("SELECT DISTINCT code FROM index_history")
    all_codes = [row[0] for row in c.fetchall()]

    for code in all_codes:
        c.execute('SELECT id, trade_date, close FROM index_history WHERE code = ? ORDER BY trade_date', (code,))
        rows = c.fetchall()

        prev_close = None
        for row in rows:
            row_id, trade_date, close = row
            if prev_close is not None and prev_close > 0:
                change = round((close - prev_close) / prev_close * 100, 2)
                c.execute('UPDATE index_history SET change_pct = ? WHERE id = ?', (change, row_id))
            prev_close = close

    conn.commit()

    # 显示结果
    print('\n=== 美股最新数据 ===')
    c.execute('''
        SELECT code, name, trade_date, close, change_pct
        FROM index_history
        WHERE code IN ('dji', 'ixic', 'inx', 'ftsea50', 'yang')
        ORDER BY code, trade_date DESC
    ''')

    current_code = None
    for row in c.fetchall():
        if row[0] != current_code:
            print(f'\n{row[1]}:')
            current_code = row[0]
        if len(row) > 4:
            print(f'  {row[2]}: {row[3]:.2f} ({row[4]}%)')

    conn.close()

if __name__ == "__main__":
    clean_and_recalculate()