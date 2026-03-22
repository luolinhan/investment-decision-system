"""导入美股和富时指数数据到数据库"""
import json
import sqlite3
import os

DB_PATH = "data/investment.db"
DATA_DIR = "data"

def import_all():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    files = ['dji.json', 'ixic.json', 'inx.json', 'ftsea50.json', 'yang.json']

    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"文件不存在: {filepath}")
            continue

        with open(filepath, 'r') as f:
            data = json.load(f)

        code = data['code']
        name = data['name']
        records = data['data']

        added = 0
        for r in records:
            try:
                c.execute('''
                    INSERT OR REPLACE INTO index_history
                    (code, name, trade_date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (code, name, r['date'], r['open'], r['high'], r['low'], r['close'], r['volume']))
                added += 1
            except Exception as ex:
                pass

        conn.commit()
        print(f"{name}: {added} 条")

    conn.close()
    print("\n导入完成!")

if __name__ == "__main__":
    import_all()