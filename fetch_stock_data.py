"""
获取实时股票财务数据
从新浪/腾讯API获取实时行情，从东方财富获取财务指标
"""
import os
import sqlite3
import requests
import json
import re
from datetime import datetime

# Windows代理设置
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

DB_PATH = r"C:\Users\Administrator\research_report_system\data\investment.db"

# 关注股票配置
WATCH_STOCKS = {
    # CXO
    "sh603259": {"name": "药明康德", "market": "A", "category": "CXO"},
    "hk02269": {"name": "药明生物", "market": "HK", "category": "CXO"},
    # 光伏
    "sh600438": {"name": "通威股份", "market": "A", "category": "光伏"},
    "sh601012": {"name": "隆基绿能", "market": "A", "category": "光伏"},
    "sz002459": {"name": "晶澳科技", "market": "A", "category": "光伏"},
    "sz300763": {"name": "锦浪科技", "market": "A", "category": "光伏"},
    # 医药
    "sh688235": {"name": "百济神州", "market": "A", "category": "医药"},
    "hk06160": {"name": "百济神州", "market": "HK", "category": "医药"},
    "hk01177": {"name": "中国生物制药", "market": "HK", "category": "医药"},
    "sh600196": {"name": "复星医药", "market": "A", "category": "医药"},
    # 消费
    "sh601888": {"name": "中国中免", "market": "A", "category": "消费"},
    "hk01880": {"name": "中国中免", "market": "HK", "category": "消费"},
    # 科技
    "hk00700": {"name": "腾讯控股", "market": "HK", "category": "科技"},
    "hk03690": {"name": "美团-W", "market": "HK", "category": "科技"},
    "hk01810": {"name": "小米集团-W", "market": "HK", "category": "科技"},
    "hk01024": {"name": "快手-W", "market": "HK", "category": "科技"},
    "hk09988": {"name": "阿里巴巴-W", "market": "HK", "category": "科技"},
    # 能源
    "hk00883": {"name": "中国海洋石油", "market": "HK", "category": "能源"},
}


class StockDataFetcher:
    """股票数据获取器"""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.session.trust_env = True

    def close(self):
        self.conn.close()

    def get_a_stock_realtime(self, code):
        """获取A股实时行情 - 新浪API"""
        # 转换代码格式 sh600519 -> sh600519
        try:
            url = f"https://hq.sinajs.cn/list={code}"
            resp = self.session.get(url, timeout=10)
            resp.encoding = 'gbk'

            match = re.search(r'="([^"]+)"', resp.text)
            if match:
                data = match.group(1).split(',')
                if len(data) >= 32:
                    return {
                        "name": data[0],
                        "open": float(data[1]) if data[1] else 0,
                        "last_close": float(data[2]) if data[2] else 0,
                        "price": float(data[3]) if data[3] else 0,
                        "high": float(data[4]) if data[4] else 0,
                        "low": float(data[5]) if data[5] else 0,
                        "volume": float(data[8]) if data[8] else 0,
                        "amount": float(data[9]) if data[9] else 0,
                        "date": data[30],
                        "time": data[31],
                    }
        except Exception as e:
            print(f"  获取{code}失败: {e}")
        return None

    def get_hk_stock_realtime(self, code):
        """获取港股实时行情 - 腾讯API"""
        # hk00700 -> 00700
        symbol = code.replace("hk", "")
        try:
            url = f"https://web.sqt.gtimg.cn/q=r_{symbol}"
            resp = self.session.get(url, timeout=10)
            resp.encoding = 'gbk'

            match = re.search(r'="([^"]+)"', resp.text)
            if match:
                data = match.group(1).split('~')
                if len(data) >= 45:
                    return {
                        "name": data[1],
                        "price": float(data[3]) if data[3] else 0,
                        "last_close": float(data[4]) if data[4] else 0,
                        "open": float(data[5]) if data[5] else 0,
                        "volume": float(data[6]) if data[6] else 0,
                        "high": float(data[33]) if data[33] else 0,
                        "low": float(data[34]) if data[34] else 0,
                        "amount": float(data[37]) if data[37] else 0,
                    }
        except Exception as e:
            print(f"  获取{code}失败: {e}")
        return None

    def get_a_stock_financial(self, code):
        """获取A股财务指标 - 东方财富API"""
        # sh603259 -> 603259
        symbol = code.replace("sh", "").replace("sz", "")
        secid = f"1.{symbol}" if code.startswith("sh") else f"0.{symbol}"

        try:
            # 使用更详细的API
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": secid,
                "fields": "f57,f58,f43,f169,f170,f46,f44,f51,f168,f47,f48,f60,f45,f52,f50,f49,f171,f113,f114,f115,f117",
                "ut": "fa5fd1943c7b386f172d6893dbfba10b"
            }
            resp = self.session.get(url, params=params, timeout=10)
            data = resp.json()

            if data.get("data"):
                d = data["data"]
                # f162=PE-TTM, f167=PB, f173=ROE
                pe_ttm = d.get("f162")
                pb = d.get("f167")
                roe = d.get("f173")

                # 过滤异常值
                if pe_ttm and abs(pe_ttm) > 10000:
                    pe_ttm = None
                if pb and abs(pb) > 1000:
                    pb = None

                return {
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "ps_ttm": d.get("f92"),
                    "roe": roe,
                    "dv_ratio": d.get("f187"),
                }
        except Exception as e:
            print(f"  获取{code}财务数据失败: {e}")
        return None

    def get_hk_stock_financial(self, code):
        """获取港股财务指标 - 东方财富API"""
        # hk00700 -> 00700
        symbol = code.replace("hk", "")
        secid = f"116.{symbol}"

        try:
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": secid,
                "fields": "f57,f58,f43,f169,f170,f46,f44,f51,f168,f47,f48,f60,f45,f52,f50,f49",
                "ut": "fa5fd1943c7b386f172d6893dbfba10b"
            }
            resp = self.session.get(url, params=params, timeout=10)
            data = resp.json()

            if data.get("data"):
                d = data["data"]
                # 港股字段映射不同
                pe_ttm = d.get("f162")
                pb = d.get("f167")

                # 过滤异常值
                if pe_ttm and abs(pe_ttm) > 10000:
                    pe_ttm = None
                if pb and abs(pb) > 1000:
                    pb = None

                return {
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "ps_ttm": d.get("f92"),
                    "roe": d.get("f173"),
                    "dv_ratio": d.get("f187"),
                }
        except Exception as e:
            print(f"  获取{code}财务数据失败: {e}")
        return None

    def update_stock_data(self):
        """更新所有关注股票数据"""
        print("=" * 50)
        print(f"股票数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

        c = self.conn.cursor()
        updated = 0

        for code, info in WATCH_STOCKS.items():
            name = info["name"]
            market = info["market"]
            category = info["category"]

            print(f"\n获取 {name} ({code})...")

            # 获取实时行情
            realtime = None
            if market == "A":
                realtime = self.get_a_stock_realtime(code)
                financial = self.get_a_stock_financial(code)
            elif market == "HK":
                realtime = self.get_hk_stock_realtime(code)
                financial = self.get_hk_stock_financial(code)

            # 计算涨跌幅
            change_pct = None
            if realtime and realtime.get("last_close") and realtime["last_close"] > 0:
                change_pct = round((realtime["price"] - realtime["last_close"]) / realtime["last_close"] * 100, 2)

            # 更新stock_financial表
            today = datetime.now().strftime("%Y-%m-%d")

            try:
                c.execute('''
                    INSERT OR REPLACE INTO stock_financial
                    (code, name, report_date, pe_ttm, pb, ps_ttm, roe, dividend_yield)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code,
                    name,
                    today,
                    financial.get("pe_ttm") if financial else None,
                    financial.get("pb") if financial else None,
                    financial.get("ps_ttm") if financial else None,
                    financial.get("roe") if financial else None,
                    financial.get("dv_ratio") if financial else None,
                ))

                # 显示结果
                pe = financial.get("pe_ttm") if financial and financial.get("pe_ttm") else "-"
                pb = financial.get("pb") if financial and financial.get("pb") else "-"
                price = realtime.get("price") if realtime else "-"
                chg = f"{change_pct:+.2f}%" if change_pct is not None else "-"

                print(f"  价格: {price}  涨跌: {chg}  PE: {pe}  PB: {pb}")
                updated += 1

            except Exception as e:
                print(f"  保存失败: {e}")

        self.conn.commit()
        print(f"\n更新完成，共更新 {updated} 只股票")
        return updated

    def update_watch_list(self):
        """更新关注列表"""
        c = self.conn.cursor()

        for code, info in WATCH_STOCKS.items():
            c.execute('''
                INSERT OR REPLACE INTO watch_list (code, name, market, category, enabled)
                VALUES (?, ?, ?, ?, 1)
            ''', (code, info["name"], info["market"], info["category"]))

        self.conn.commit()
        print(f"关注列表已更新，共 {len(WATCH_STOCKS)} 只股票")


def main():
    fetcher = StockDataFetcher()
    try:
        fetcher.update_watch_list()
        fetcher.update_stock_data()
    finally:
        fetcher.close()


if __name__ == "__main__":
    main()