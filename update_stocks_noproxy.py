"""
获取实时股票数据 - 不使用代理
"""
import sqlite3
import urllib.request
import json
import os
from datetime import datetime

# 清除代理设置
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
    if key in os.environ:
        del os.environ[key]

DB_PATH = r"C:\Users\Administrator\research_report_system\data\investment.db"

# 关注股票
STOCKS = [
    # A股 (secid格式: 市场.代码, 1=沪市, 0=深市)
    ("1.603259", "sh603259", "药明康德", "A", "CXO"),
    ("1.600438", "sh600438", "通威股份", "A", "光伏"),
    ("1.601012", "sh601012", "隆基绿能", "A", "光伏"),
    ("0.002459", "sz002459", "晶澳科技", "A", "光伏"),
    ("0.300763", "sz300763", "锦浪科技", "A", "光伏"),
    ("1.688235", "sh688235", "百济神州", "A", "医药"),
    ("1.600196", "sh600196", "复星医药", "A", "医药"),
    ("1.601888", "sh601888", "中国中免", "A", "消费"),
    # 港股 (116=港股)
    ("116.02269", "hk02269", "药明生物", "HK", "CXO"),
    ("116.06160", "hk06160", "百济神州", "HK", "医药"),
    ("116.01177", "hk01177", "中国生物制药", "HK", "医药"),
    ("116.01880", "hk01880", "中国中免", "HK", "消费"),
    ("116.00700", "hk00700", "腾讯控股", "HK", "科技"),
    ("116.03690", "hk03690", "美团-W", "HK", "科技"),
    ("116.01810", "hk01810", "小米集团-W", "HK", "科技"),
    ("116.01024", "hk01024", "快手-W", "HK", "科技"),
    ("116.09988", "hk09988", "阿里巴巴-W", "HK", "科技"),
    ("116.00883", "hk00883", "中国海洋石油", "HK", "能源"),
]

def get_stock_data(secid, code, name, market, category):
    """获取单只股票数据"""
    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f43,f50,f51,f58,f162,f167,f173&ut=fa5fd1943c7b386f172d6893dbfba10b"

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())

            if data.get("data"):
                d = data["data"]
                price = d.get("f43", 0) or 0
                change_pct = d.get("f51", 0) or 0
                pe = d.get("f162", 0) or 0
                pb = d.get("f167", 0) or 0
                roe = d.get("f173", 0) or 0

                # 过滤异常值
                if abs(pe) > 10000:
                    pe = None
                if abs(pb) > 1000:
                    pb = None

                return {
                    "code": code,
                    "name": name,
                    "market": market,
                    "category": category,
                    "price": price / 100 if price else None,
                    "change_pct": change_pct / 100 if change_pct else None,
                    "pe_ttm": pe / 100 if pe else None,
                    "pb": pb / 100 if pb else None,
                    "roe": roe / 100 if roe else None,
                }
    except Exception as e:
        print(f"  Error: {e}")

    return None

def main():
    print("=" * 60)
    print(f"Stock Data Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    updated = 0

    for secid, code, name, market, category in STOCKS:
        print(f"\nFetching {name} ({code})...")

        data = get_stock_data(secid, code, name, market, category)

        if data:
            try:
                c.execute('''
                    INSERT OR REPLACE INTO stock_financial
                    (code, name, report_date, pe_ttm, pb, dividend_yield)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (code, name, today, data['pe_ttm'], data['pb'], None))

                pe_str = f"{data['pe_ttm']:.2f}" if data['pe_ttm'] else "-"
                pb_str = f"{data['pb']:.2f}" if data['pb'] else "-"
                price_str = f"{data['price']:.2f}" if data['price'] else "-"
                chg_str = f"{data['change_pct']:+.2f}%" if data['change_pct'] else "-"

                print(f"  Price: {price_str}  Change: {chg_str}  PE: {pe_str}  PB: {pb_str}")
                updated += 1
            except Exception as e:
                print(f"  Save error: {e}")
        else:
            print(f"  Failed to fetch data")

    conn.commit()
    conn.close()
    print(f"\n{'=' * 60}")
    print(f"Updated {updated} stocks")

if __name__ == "__main__":
    main()