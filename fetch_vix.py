"""通过Yahoo Finance API直接获取VIX数据"""
import requests
import json
from datetime import datetime, timedelta

def fetch_vix_from_yahoo():
    """使用Yahoo Finance API获取VIX历史数据"""
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"
    params = {
        "interval": "1d",
        "range": "1y"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"请求失败: {r.status_code}")
            return None

        data = r.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            print("无数据返回")
            return None

        timestamps = result[0]['timestamp']
        indicators = result[0]['indicators']['quote'][0]

        records = []
        for i, ts in enumerate(timestamps):
            date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
            record = {
                'trade_date': date,
                'vix_open': indicators.get('open', [])[i],
                'vix_high': indicators.get('high', [])[i],
                'vix_low': indicators.get('low', [])[i],
                'vix_close': indicators.get('close', [])[i]
            }
            # 过滤无效数据
            if record['vix_close'] is not None:
                records.append(record)

        print(f"获取到 {len(records)} 条VIX数据")
        return records

    except Exception as e:
        print(f"获取失败: {e}")
        return None

if __name__ == "__main__":
    data = fetch_vix_from_yahoo()
    if data:
        # 保存到数据库
        import sqlite3
        conn = sqlite3.connect("data/investment.db")
        c = conn.cursor()
        now = datetime.now().isoformat()
        inserted = 0

        for row in data:
            try:
                c.execute("""
                    INSERT OR REPLACE INTO vix_history
                    (trade_date, vix_open, vix_high, vix_low, vix_close, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row['trade_date'],
                    row['vix_open'],
                    row['vix_high'],
                    row['vix_low'],
                    row['vix_close'],
                    now
                ))
                inserted += 1
            except Exception as e:
                pass

        conn.commit()
        conn.close()
        print(f"保存成功: {inserted}条")