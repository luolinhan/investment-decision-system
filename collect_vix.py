"""采集VIX恐慌指数"""
import sqlite3
import os

# 禁用代理
os.environ.pop('HTTP_PROXY', None)
os.environ.pop('HTTPS_PROXY', None)
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)

import requests
from datetime import datetime, timedelta

DB_PATH = "data/investment.db"

def collect_vix():
    print("采集VIX恐慌指数...")

    session = requests.Session()
    session.trust_env = False
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    end_ts = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=730)).timestamp())

    url = 'https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX'
    params = {'period1': start_ts, 'period2': end_ts, 'interval': '1d'}

    try:
        resp = session.get(url, params=params, headers=headers, timeout=30)
        print(f"HTTP Status: {resp.status_code}")

        data = resp.json()

        if data.get('chart', {}).get('result'):
            result = data['chart']['result'][0]
            timestamps = result['timestamp']
            quotes = result['indicators']['quote'][0]

            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            added = 0

            for i, ts in enumerate(timestamps):
                try:
                    date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                    close = quotes['close'][i]
                    if close:
                        c.execute('''
                            INSERT OR REPLACE INTO vix_history
                            (trade_date, vix_open, vix_high, vix_low, vix_close)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (date,
                              float(quotes['open'][i] or 0),
                              float(quotes['high'][i] or 0),
                              float(quotes['low'][i] or 0),
                              float(close)))
                        added += 1
                except Exception as ex:
                    pass

            conn.commit()
            conn.close()
            print(f"新增 {added} 条")
            print(f"最新: {date} close={close:.2f}")
            return added
        else:
            print("获取数据失败")
            return 0

    except Exception as e:
        print(f"错误: {e}")
        return 0

if __name__ == "__main__":
    collect_vix()