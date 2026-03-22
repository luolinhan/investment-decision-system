"""
投资数据采集服务 - 完整版
采集指数历史、利率、市场情绪等数据
"""
import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

# 清除代理设置
os.environ.pop('HTTP_PROXY', None)
os.environ.pop('HTTPS_PROXY', None)
os.environ.pop('http_proxy', None)
os.environ.pop('https_proxy', None)

DB_PATH = "D:/research_report_system/data/investment.db"

# 指数配置
INDEX_CONFIG = {
    # A股指数
    "sh000001": {"name": "上证指数", "source": "sina"},
    "sz399001": {"name": "深证成指", "source": "sina"},
    "sz399006": {"name": "创业板指", "source": "sina"},
    "sh000300": {"name": "沪深300", "source": "sina"},
    "sh000016": {"name": "上证50", "source": "sina"},
    "sh000905": {"name": "中证500", "source": "sina"},
    "sh000852": {"name": "中证1000", "source": "sina"},
    # 恒生指数
    "hsi": {"name": "恒生指数", "source": "sina_hk"},
    # 美股指数
    "dji": {"name": "道琼斯", "source": "sina_us"},
    "ixic": {"name": "纳斯达克", "source": "sina_us"},
    "inx": {"name": "标普500", "source": "sina_us"},
    # 富时指数
    "ftsea50": {"name": "富时中国A50", "source": "eastmoney"},
    "yang": {"name": "富时中国三倍做空", "source": "eastmoney_etf"},
}


class IndexDataCollector:
    """指数数据采集器"""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def close(self):
        self.conn.close()

    def _save_index_data(self, code, name, data_list):
        """保存指数数据到数据库"""
        c = self.conn.cursor()
        added = 0
        for item in data_list:
            try:
                c.execute('''
                    INSERT OR REPLACE INTO index_history
                    (code, name, trade_date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (code, name, item['date'], item['open'], item['high'],
                      item['low'], item['close'], item.get('volume', 0)))
                added += 1
            except Exception as ex:
                continue
        self.conn.commit()
        return added

    def collect_sina_index(self, code, name, days=365):
        """从新浪获取A股指数历史数据"""
        print(f"采集 {name} ({code})...")

        try:
            # 新浪财经历史数据API
            # 需要转换代码格式: sh000001 -> sh000001
            url = f"https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
            params = {
                "symbol": code,
                "scale": "240",  # 日线
                "ma": "no",
                "datalen": days
            }

            resp = self.session.get(url, params=params, timeout=30)
            data = resp.json()

            if not data:
                print(f"  无数据")
                return 0

            records = []
            for item in data:
                records.append({
                    'date': item['day'],
                    'open': float(item['open']),
                    'high': float(item['high']),
                    'low': float(item['low']),
                    'close': float(item['close']),
                    'volume': float(item['volume'])
                })

            added = self._save_index_data(code, name, records)
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_sina_hk_index(self, code, name, days=365):
        """从新浪获取港股指数历史数据"""
        print(f"采集 {name} (恒生指数)...")

        try:
            # 新浪港股指数
            url = f"https://quotes.sina.cn/hkstock/api/json_v2.php/IO.HKINDEX.HKIndexData.getHKIndexData"
            params = {
                "symbol": "HSI",  # 恒生指数代码
                "scale": "240",
                "datalen": days
            }

            resp = self.session.get(url, params=params, timeout=30)
            data = resp.json()

            if not data:
                print(f"  无数据，尝试备用方法...")
                return self._collect_hsi_from_tencent(code, name, days)

            records = []
            for item in data:
                records.append({
                    'date': item.get('day', item.get('d', '')),
                    'open': float(item.get('open', item.get('o', 0))),
                    'high': float(item.get('high', item.get('h', 0))),
                    'low': float(item.get('low', item.get('l', 0))),
                    'close': float(item.get('close', item.get('c', 0))),
                    'volume': float(item.get('volume', item.get('v', 0)))
                })

            added = self._save_index_data(code, name, records)
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}，尝试备用方法...")
            return self._collect_hsi_from_tencent(code, name, days)

    def _collect_hsi_from_tencent(self, code, name, days=365):
        """从腾讯获取恒生指数"""
        try:
            # 腾讯财经历史数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')

            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            params = {
                "_var": "kline_hkHSI",
                "param": f"hkHSI,day,,{end_date},320,qfq",
                "r": str(int(time.time() * 1000))
            }

            resp = self.session.get(url, params=params, timeout=30)
            text = resp.text

            # 解析JSONP
            if 'kline_hkHSI=' in text:
                json_str = text.split('kline_hkHSI=')[1]
                data = json.loads(json_str)

            if data and data.get('data') and data['data'].get('hkHSI'):
                klines = data['data']['hkHSI'].get('day', [])

                records = []
                cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                for k in klines:
                    if k[0] >= cutoff:
                        records.append({
                            'date': k[0],
                            'open': float(k[1]),
                            'high': float(k[2]),
                            'low': float(k[3]),
                            'close': float(k[4]),
                            'volume': float(k[5]) if len(k) > 5 else 0
                        })

                added = self._save_index_data(code, name, records)
                print(f"  新增 {added} 条")
                return added

            print(f"  无数据")
            return 0

        except Exception as e:
            print(f"  备用方法失败: {e}")
            return 0

    def collect_sina_us_index(self, code, name, days=365):
        """从新浪获取美股指数历史数据"""
        print(f"采集 {name} ({code})...")

        try:
            # 美股指数代码映射
            code_map = {
                "dji": "gb_$dji",
                "ixic": "gb_$ixic",
                "inx": "gb_$inx"
            }
            sina_code = code_map.get(code, f"gb_${code}")

            url = "https://quotes.sina.cn/usstock/api/jsonp.php/US_MinKline.getUSMinKline"
            params = {
                "symbol": sina_code,
                "scale": "240",  # 日线
                "datalen": days
            }

            resp = self.session.get(url, params=params, timeout=30)
            text = resp.text

            # 解析JSONP
            import re
            match = re.search(r'\((\[.*\])\)', text)
            if not match:
                print(f"  无数据")
                return 0

            data = json.loads(match.group(1))

            records = []
            for item in data:
                records.append({
                    'date': item['d'],
                    'open': float(item['o']),
                    'high': float(item['h']),
                    'low': float(item['l']),
                    'close': float(item['c']),
                    'volume': float(item['v']) if 'v' in item else 0
                })

            added = self._save_index_data(code, name, records)
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_ftse_a50(self, code, name, days=365):
        """采集富时中国A50指数"""
        print(f"采集 {name}...")

        try:
            # 东方财富富时A50
            url = "https://push2.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "secid": "102.IF1909",  # 富时A50期货
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",  # 日线
                "fqt": "1",
                "end": "20500101",
                "lmt": days
            }

            resp = self.session.get(url, params=params, timeout=30)
            data = resp.json()

            if data.get('data') and data['data'].get('klines'):
                records = []
                for line in data['data']['klines']:
                    parts = line.split(',')
                    records.append({
                        'date': parts[0],
                        'open': float(parts[1]),
                        'high': float(parts[2]),
                        'low': float(parts[3]),
                        'close': float(parts[4]),
                        'volume': float(parts[5]) if len(parts) > 5 else 0
                    })

                added = self._save_index_data(code, name, records)
                print(f"  新增 {added} 条")
                return added

            print(f"  无数据")
            return 0

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_yang_etf(self, code, name, days=365):
        """采集富时中国三倍做空ETF (YANG)"""
        print(f"采集 {name} (YANG ETF)...")

        try:
            # 从新浪获取美股ETF数据
            url = "https://quotes.sina.cn/usstock/api/jsonp.php/US_MinKline.getUSMinKline"
            params = {
                "symbol": "gb_yang",
                "scale": "240",
                "datalen": days
            }

            resp = self.session.get(url, params=params, timeout=30)
            text = resp.text

            import re
            match = re.search(r'\((\[.*\])\)', text)
            if not match:
                print(f"  无数据")
                return 0

            data = json.loads(match.group(1))

            records = []
            for item in data:
                records.append({
                    'date': item['d'],
                    'open': float(item['o']),
                    'high': float(item['h']),
                    'low': float(item['l']),
                    'close': float(item['c']),
                    'volume': float(item['v']) if 'v' in item else 0
                })

            added = self._save_index_data(code, name, records)
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_all_indices(self, days=365):
        """采集所有指数"""
        print("=" * 50)
        print("开始采集指数数据")
        print("=" * 50)

        total = 0

        for code, config in INDEX_CONFIG.items():
            name = config['name']
            source = config['source']

            if source == 'sina':
                count = self.collect_sina_index(code, name, days)
            elif source == 'sina_hk':
                count = self.collect_sina_hk_index(code, name, days)
            elif source == 'sina_us':
                count = self.collect_sina_us_index(code, name, days)
            elif source == 'eastmoney':
                count = self.collect_ftse_a50(code, name, days)
            elif source == 'eastmoney_etf':
                count = self.collect_yang_etf(code, name, days)
            else:
                count = 0

            total += count
            time.sleep(0.5)

        print(f"\n指数数据采集完成: {total} 条")
        return total

    def collect_shibor(self):
        """采集SHIBOR利率"""
        print("\n采集SHIBOR利率...")

        try:
            import akshare as ak
            df = ak.macro_china_shibor_all()

            if df is not None and len(df) > 0:
                c = self.conn.cursor()
                for _, row in df.iterrows():
                    try:
                        date_str = str(row.iloc[0])[:10]
                        c.execute('''
                            INSERT OR REPLACE INTO interest_rates
                            (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            date_str,
                            float(row.iloc[1] if pd.notna(row.iloc[1]) else 0),
                            float(row.iloc[3] if pd.notna(row.iloc[3]) else 0),
                            float(row.iloc[7] if pd.notna(row.iloc[7]) else 0),
                            float(row.iloc[9] if pd.notna(row.iloc[9]) else 0),
                            float(row.iloc[11] if pd.notna(row.iloc[11]) else 0),
                            float(row.iloc[15] if pd.notna(row.iloc[15]) else 0)
                        ))
                    except:
                        continue
                self.conn.commit()
                print(f"  新增 {len(df)} 条")

        except Exception as e:
            print(f"  失败: {e}")

    def collect_market_sentiment(self):
        """采集市场情绪"""
        print("\n采集市场情绪...")

        try:
            import akshare as ak
            df = ak.stock_market_activity_legu()

            if df is not None and len(df) > 0:
                today = datetime.now().strftime('%Y-%m-%d')
                c = self.conn.cursor()

                data = {}
                for _, row in df.iterrows():
                    data[row['item']] = row['value']

                c.execute('''
                    INSERT OR REPLACE INTO market_sentiment
                    (trade_date, up_count, down_count, flat_count, limit_up_count, limit_down_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    today,
                    int(data.get('上涨', 0)),
                    int(data.get('下跌', 0)),
                    int(data.get('平盘', 0)),
                    int(data.get('涨停', 0)),
                    int(data.get('跌停', 0))
                ))
                self.conn.commit()
                print(f"  更新 {today} 市场情绪")

        except Exception as e:
            print(f"  失败: {e}")


def collect_all():
    """采集所有数据"""
    collector = IndexDataCollector()

    # 1. 指数数据
    collector.collect_all_indices(days=365)

    # 2. SHIBOR利率
    collector.collect_shibor()

    # 3. 市场情绪
    collector.collect_market_sentiment()

    collector.close()

    print("\n" + "=" * 50)
    print("数据采集完成")
    print("=" * 50)

    # 显示统计
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT code, name, COUNT(*) FROM index_history GROUP BY code, name")
    print("\n指数数据统计:")
    for row in c.fetchall():
        print(f"  {row[1]} ({row[0]}): {row[2]} 条")
    conn.close()


if __name__ == "__main__":
    collect_all()