"""采集北向资金数据 - 直接调用东方财富API"""
import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json

DB_PATH = "D:/research_report_system/data/investment.db"

def collect_north_money_direct():
    """直接从东方财富API采集北向资金数据"""
    print("采集北向资金数据...")

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # 东方财富北向资金历史API
        # 北上资金 = 沪股通 + 深股通
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

        params = {
            "sortColumns": "TRADE_DATE",
            "sortTypes": "-1",
            "pageSize": 500,
            "pageNumber": 1,
            "reportName": "RPT_MUTUAL_DEAL_TREND",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": '(MUTUAL_TYPE="003")'  # 北上资金合计
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://data.eastmoney.com/"
        }

        resp = requests.get(url, params=params, headers=headers, timeout=30)
        data = resp.json()

        if data.get("result") and data["result"].get("data"):
            records = data["result"]["data"]
            print(f"获取到 {len(records)} 条数据")

            added = 0
            for item in records:
                try:
                    trade_date = item.get("TRADE_DATE", "")[:10]
                    # 北向资金净买入额 (亿元)
                    net_buy = item.get("NET_BUY", 0) or 0

                    c.execute('''
                        INSERT OR REPLACE INTO north_money
                        (trade_date, total_net_inflow)
                        VALUES (?, ?)
                    ''', (trade_date, float(net_buy)))
                    added += 1
                except Exception as ex:
                    continue

            conn.commit()
            print(f"  写入 {added} 条")

            # 显示最近数据
            c.execute('''
                SELECT trade_date, total_net_inflow
                FROM north_money
                WHERE total_net_inflow != 0
                ORDER BY trade_date DESC LIMIT 5
            ''')
            print("\n最近数据:")
            for row in c.fetchall():
                print(f"  {row[0]}: {row[1]:.2f}亿")

        conn.close()

    except Exception as e:
        print(f"失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    collect_north_money_direct()