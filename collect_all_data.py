"""
投资数据采集服务 - Windows本地版本
采集A股、港股、美股、富时指数等数据
"""
import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

# 设置代理环境变量（Windows Clash Verge默认端口）
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'

DB_PATH = "data/investment.db"

# 指数配置
A_INDICES = [
    ("sh000001", "上证指数"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000300", "沪深300"),
    ("sh000016", "上证50"),
    ("sh000905", "中证500"),
    ("sh000852", "中证1000"),
    ("sz399005", "中小板指"),
]

US_INDICES = {
    "^DJI": ("dji", "道琼斯"),
    "^IXIC": ("ixic", "纳斯达克"),
    "^GSPC": ("inx", "标普500"),
}

FTSE_INDICES = {
    "FXI": ("ftsea50", "富时中国A50"),
    "YANG": ("yang", "富时中国三倍做空"),
}

# VIX恐慌指数
VIX_SYMBOL = ("^VIX", "vix", "VIX恐慌指数")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


class InvestmentDataCollector:
    """投资数据采集器"""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        # 使用系统代理
        self.session.trust_env = True

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

    def _calculate_change_pct(self, code):
        """计算涨跌幅"""
        c = self.conn.cursor()
        c.execute('SELECT id, close FROM index_history WHERE code = ? ORDER BY trade_date', (code,))
        rows = c.fetchall()
        prev_close = None
        for row in rows:
            if prev_close and prev_close > 0:
                change = round((row[1] - prev_close) / prev_close * 100, 2)
                c.execute('UPDATE index_history SET change_pct = ? WHERE id = ?', (change, row[0]))
            prev_close = row[1]
        self.conn.commit()

    # ==================== A股指数 ====================
    def collect_a_indices(self, days=365):
        """采集A股指数 - 使用新浪API"""
        print("\n【A股指数】")
        total = 0

        for code, name in A_INDICES:
            print(f"  {name} ({code})...", end=" ")
            try:
                url = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
                params = {"symbol": code, "scale": "240", "ma": "no", "datalen": days}
                resp = self.session.get(url, params=params, timeout=30)
                data = resp.json()

                if data:
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
                    self._calculate_change_pct(code)
                    print(f"{added} 条")
                    total += added
                else:
                    print("无数据")
            except Exception as e:
                print(f"失败: {e}")
            time.sleep(0.3)

        return total

    # ==================== 恒生指数 ====================
    def collect_hsi(self, days=365):
        """采集恒生指数 - 使用腾讯API"""
        print("\n【恒生指数】")
        print("  恒生指数...", end=" ")
        try:
            url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            params = {
                "_var": "kline_hkHSI",
                "param": f"hkHSI,day,,{datetime.now().strftime('%Y-%m-%d')},{days*2},qfq",
                "r": str(int(time.time() * 1000))
            }
            resp = self.session.get(url, params=params, timeout=30)
            text = resp.text

            if 'kline_hkHSI=' in text:
                json_str = text.split('kline_hkHSI=')[1]
                data = json.loads(json_str)

                if data.get('data') and data['data'].get('hkHSI'):
                    klines = data['data']['hkHSI'].get('day', [])
                    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

                    records = []
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

                    added = self._save_index_data('hsi', '恒生指数', records)
                    self._calculate_change_pct('hsi')
                    print(f"{added} 条")
                    return added

            print("无数据")
            return 0
        except Exception as e:
            print(f"失败: {e}")
            return 0

    # ==================== 美股指数 ====================
    def collect_us_indices(self, days=365):
        """采集美股指数 - 使用Yahoo Finance API"""
        print("\n【美股指数】")
        total = 0

        for symbol, (code, name) in US_INDICES.items():
            print(f"  {name}...", end=" ")
            try:
                data = self._get_yahoo_history(symbol, days)
                if data:
                    added = self._save_index_data(code, name, data)
                    self._calculate_change_pct(code)
                    print(f"{added} 条")
                    total += added
                else:
                    print("无数据")
            except Exception as e:
                print(f"失败: {e}")
            time.sleep(1)

        return total

    # ==================== 富时指数 ====================
    def collect_ftse_indices(self, days=365):
        """采集富时指数 - 使用Yahoo Finance API"""
        print("\n【富时指数】")
        total = 0

        for symbol, (code, name) in FTSE_INDICES.items():
            print(f"  {name}...", end=" ")
            try:
                data = self._get_yahoo_history(symbol, days)
                if data:
                    added = self._save_index_data(code, name, data)
                    self._calculate_change_pct(code)
                    print(f"{added} 条")
                    total += added
                else:
                    print("无数据")
            except Exception as e:
                print(f"失败: {e}")
            time.sleep(1)

        return total

    # ==================== VIX恐慌指数 ====================
    def collect_vix(self, days=365):
        """采集VIX恐慌指数"""
        print("\n【VIX恐慌指数】")
        print("  VIX...", end=" ")
        try:
            data = self._get_yahoo_history("^VIX", days)
            if data:
                # 保存到vix_history表
                c = self.conn.cursor()
                added = 0
                for item in data:
                    try:
                        c.execute('''
                            INSERT OR REPLACE INTO vix_history
                            (trade_date, vix_open, vix_high, vix_low, vix_close)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (item['date'], item['open'], item['high'], item['low'], item['close']))
                        added += 1
                    except:
                        continue
                self.conn.commit()
                print(f"{added} 条")
                return added
            else:
                print("无数据")
                return 0
        except Exception as e:
            print(f"失败: {e}")
            return 0

    def _get_yahoo_history(self, symbol, days=365):
        """从Yahoo Finance获取历史数据 - 使用curl绕过403"""
        import subprocess
        try:
            end_timestamp = int(datetime.now().timestamp())
            start_timestamp = int((datetime.now() - timedelta(days=days*2)).timestamp())

            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {
                "period1": start_timestamp,
                "period2": end_timestamp,
                "interval": "1d",
                "includePrePost": "false"
            }

            # 构建完整URL
            full_url = f"{url}?period1={params['period1']}&period2={params['period2']}&interval={params['interval']}"

            # 使用curl获取数据（通过代理）
            result = subprocess.run([
                'curl', '-s',
                '--proxy', 'http://127.0.0.1:7890',
                '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                '-H', 'Accept: application/json',
                full_url
            ], capture_output=True, text=True, timeout=60)

            if result.returncode != 0 or not result.stdout:
                print(f"curl失败: {result.stderr}")
                return None

            data = json.loads(result.stdout)

            if data.get("chart", {}).get("result"):
                result = data["chart"]["result"][0]
                timestamps = result.get("timestamp", [])
                quotes = result.get("indicators", {}).get("quote", [{}])[0]

                records = []
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

                for i, ts in enumerate(timestamps):
                    try:
                        date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        if date < cutoff:
                            continue
                        close = quotes.get("close", [])[i]
                        if close:
                            records.append({
                                "date": date,
                                "open": float(quotes.get("open", [])[i] or 0),
                                "high": float(quotes.get("high", [])[i] or 0),
                                "low": float(quotes.get("low", [])[i] or 0),
                                "close": float(close),
                                "volume": float(quotes.get("volume", [])[i] or 0)
                            })
                    except (IndexError, TypeError, ValueError):
                        continue

                return records
            return None
        except Exception as e:
            print(f"Yahoo API错误: {e}")
            return None

    # ==================== SHIBOR利率 ====================
    def collect_shibor(self):
        """采集SHIBOR利率"""
        print("\n【SHIBOR利率】")
        print("  采集中...", end=" ")
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
                print(f"{len(df)} 条")
        except Exception as e:
            print(f"失败: {e}")

    # ==================== 市场情绪 ====================
    def collect_market_sentiment(self):
        """采集市场情绪"""
        print("\n【市场情绪】")
        print("  采集中...", end=" ")
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
                print(f"{today}")
        except Exception as e:
            print(f"失败: {e}")


def collect_all():
    """采集所有数据"""
    collector = InvestmentDataCollector()

    print("=" * 50)
    print("投资数据采集 - Windows本地版")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # 1. A股指数
    collector.collect_a_indices(days=365)

    # 2. 恒生指数
    collector.collect_hsi(days=365)

    # 3. 美股指数
    collector.collect_us_indices(days=365)

    # 4. 富时指数
    collector.collect_ftse_indices(days=365)

    # 5. VIX恐慌指数
    collector.collect_vix(days=365)

    # 6. SHIBOR利率
    collector.collect_shibor()

    # 6. 市场情绪
    collector.collect_market_sentiment()

    # 7. 清理重复数据并重新计算涨跌幅
    print("\n【数据清理】")
    clean_duplicates(collector.conn)

    collector.close()

    # 显示统计
    print("\n" + "=" * 50)
    print("数据采集完成")
    print("=" * 50)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT code, name, COUNT(*) FROM index_history GROUP BY code, name ORDER BY code")
    print("\n指数数据统计:")
    for row in c.fetchall():
        print(f"  {row[1]}: {row[2]} 条")
    conn.close()


def clean_duplicates(conn):
    """清理重复数据并重新计算涨跌幅"""
    c = conn.cursor()

    # 删除收盘价相同的重复数据
    for code in ['dji', 'ixic', 'inx', 'ftsea50', 'yang']:
        c.execute('SELECT id, trade_date, close FROM index_history WHERE code = ? ORDER BY trade_date', (code,))
        rows = c.fetchall()

        prev_close = None
        to_delete = []
        for row in rows:
            if prev_close is not None and abs(row[2] - prev_close) < 0.01:
                to_delete.append(row[0])
            prev_close = row[2]

        if to_delete:
            c.execute(f'DELETE FROM index_history WHERE id IN ({",".join(map(str, to_delete))})')
            print(f"  {code}: 删除 {len(to_delete)} 条重复")

    conn.commit()

    # 重新计算所有指数涨跌幅
    c.execute("SELECT DISTINCT code FROM index_history")
    for (code,) in c.fetchall():
        c.execute('SELECT id, close FROM index_history WHERE code = ? ORDER BY trade_date', (code,))
        rows = c.fetchall()
        prev_close = None
        for row in rows:
            if prev_close and prev_close > 0:
                change = round((row[1] - prev_close) / prev_close * 100, 2)
                c.execute('UPDATE index_history SET change_pct = ? WHERE id = ?', (change, row[0]))
            prev_close = row[1]

    conn.commit()
    print("  涨跌幅计算完成")


if __name__ == "__main__":
    collect_all()