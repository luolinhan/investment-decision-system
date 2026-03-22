"""
投资数据采集服务
采集指数历史、股票行情、财务指标、估值百分位等数据
"""
import os
import sqlite3
import requests
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time

DB_PATH = "data/investment.db"


class InvestmentDataCollector:
    """投资数据采集器"""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def close(self):
        self.conn.close()

    # ==================== 指数数据 ====================

    def collect_index_history(self, code, name, days=365):
        """采集指数历史数据"""
        print(f"采集指数 {name} ({code}) 历史...")

        try:
            # 使用AKShare获取指数数据 - 需要完整代码（带前缀）
            if code.startswith('sh') or code.startswith('sz'):
                print(f"  获取数据: {code}")
                df = ak.stock_zh_index_daily(symbol=code)  # 使用完整代码
                print(f"  获取到 {len(df) if df is not None else 0} 条")
            elif code == 'hsi':
                df = ak.stock_hk_index_daily_em(symbol="HSI")
            else:
                print(f"  不支持的指数类型: {code}")
                return 0

            if df is None or len(df) == 0:
                print("  无数据")
                return 0

            print(f"  列名: {df.columns.tolist()}")
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)

            # 筛选日期范围
            cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            df = df[df['date'] >= cutoff]

            c = self.conn.cursor()
            added = 0

            for _, row in df.iterrows():
                try:
                    trade_date = str(row['date'])[:10]
                    c.execute('''
                        INSERT OR REPLACE INTO index_history
                        (code, name, trade_date, open, high, low, close, volume, change_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        code, name, trade_date,
                        float(row.get('open', 0) or 0),
                        float(row.get('high', 0) or 0),
                        float(row.get('low', 0) or 0),
                        float(row['close']),
                        float(row.get('volume', 0) or 0),
                        None
                    ))
                    added += 1
                except Exception as e:
                    continue

            self.conn.commit()
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_all_indices(self, days=365):
        """采集所有主要指数"""
        indices = [
            ("sh000001", "上证指数"),
            ("sz399001", "深证成指"),
            ("sz399006", "创业板指"),
            ("sz399005", "中小板指"),
            ("sh000300", "沪深300"),
            ("sh000016", "上证50"),
            ("sh000905", "中证500"),
            ("sh000852", "中证1000"),
        ]

        total = 0
        for code, name in indices:
            count = self.collect_index_history(code, name, days)
            total += count
            time.sleep(0.5)

        # 恒生指数
        print("\n采集恒生指数...")
        try:
            df = ak.stock_hk_index_daily_em(symbol="HSI")
            if df is not None and len(df) > 0:
                # 确保日期列是字符串
                df['date'] = df['date'].astype(str)

                cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                df = df[df['date'] >= cutoff]

                c = self.conn.cursor()
                for _, row in df.iterrows():
                    trade_date = str(row['date'])[:10]
                    c.execute('''
                        INSERT OR REPLACE INTO index_history
                        (code, name, trade_date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', ('hsi', '恒生指数', trade_date,
                          float(row.get('open', 0) or 0),
                          float(row.get('high', 0) or 0),
                          float(row.get('low', 0) or 0),
                          float(row['close']),
                          float(row.get('volume', 0) or 0)))
                self.conn.commit()
                print(f"  新增 {len(df)} 条")
        except Exception as e:
            print(f"  失败: {e}")

        print(f"\n指数数据采集完成: {total} 条")

    # ==================== 股票数据 ====================

    def collect_stock_daily(self, code, days=365):
        """采集股票日线数据"""
        print(f"采集 {code} 日线数据...")

        try:
            # 确定市场
            if code.startswith('hk'):
                symbol = code[2:]
                df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
            elif code.startswith('sh') or code.startswith('sz'):
                symbol = code[2:]
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
            else:
                print(f"  不支持: {code}")
                return 0

            if df is None or len(df) == 0:
                print("  无数据")
                return 0

            # 筛选日期
            cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            date_col = '日期' if '日期' in df.columns else 'date'
            df = df[df[date_col] >= cutoff]

            c = self.conn.cursor()
            added = 0

            for _, row in df.iterrows():
                try:
                    trade_date = str(row[date_col])[:10]
                    c.execute('''
                        INSERT OR REPLACE INTO stock_daily
                        (code, trade_date, open, high, low, close, volume, amount, change_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        code, trade_date,
                        float(row.get('开盘' if '开盘' in row else 'open', 0) or 0),
                        float(row.get('最高' if '最高' in row else 'high', 0) or 0),
                        float(row.get('最低' if '最低' in row else 'low', 0) or 0),
                        float(row.get('收盘' if '收盘' in row else 'close', 0) or 0),
                        float(row.get('成交量' if '成交量' in row else 'volume', 0) or 0),
                        float(row.get('成交额' if '成交额' in row else 'amount', 0) or 0),
                        float(row.get('涨跌幅' if '涨跌幅' in row else 'change_pct', 0) or 0)
                    ))
                    added += 1
                except:
                    continue

            self.conn.commit()
            print(f"  新增 {added} 条")
            return added

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    def collect_all_stocks(self, days=365):
        """采集所有关注股票数据"""
        c = self.conn.cursor()
        c.execute('SELECT code, name FROM watch_list WHERE enabled=1')
        stocks = c.fetchall()

        total = 0
        for code, name in stocks:
            count = self.collect_stock_daily(code, days)
            total += count
            time.sleep(0.5)

        print(f"\n股票日线数据采集完成: {total} 条")

    # ==================== 财务指标 ====================

    def collect_financial_indicators(self, code):
        """采集财务指标"""
        print(f"采集 {code} 财务指标...")

        try:
            if code.startswith('hk'):
                symbol = code[2:]
                # 港股财务数据
                df = ak.stock_financial_hk_report_em(stock=symbol, symbol="资产负债表", indicator="报告期")
            else:
                symbol = code[2:]
                df = ak.stock_financial_analysis_indicator(symbol=symbol)

            if df is None or len(df) == 0:
                print("  无数据")
                return 0

            print(f"  获取到 {len(df)} 条")
            return len(df)

        except Exception as e:
            print(f"  失败: {e}")
            return 0

    # ==================== 利率数据 ====================

    def collect_interest_rates(self, days=365):
        """采集利率数据"""
        print("采集利率数据...")

        try:
            # SHIBOR - 使用 macro_china_shibor_all
            print("  SHIBOR...")
            df = ak.macro_china_shibor_all()
            if df is not None and len(df) > 0:
                c = self.conn.cursor()
                for _, row in df.iterrows():
                    try:
                        # 使用列索引而非列名（避免编码问题）
                        # 列顺序: 日期, O/N-利率, O/N-涨跌, 1W-利率, 1W-涨跌, ...
                        date_str = str(row.iloc[0])[:10]
                        c.execute('''
                            INSERT OR REPLACE INTO interest_rates
                            (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            date_str,
                            float(row.iloc[1] if pd.notna(row.iloc[1]) else 0),  # O/N-利率
                            float(row.iloc[3] if pd.notna(row.iloc[3]) else 0),  # 1W-利率
                            float(row.iloc[7] if pd.notna(row.iloc[7]) else 0),  # 1M-利率
                            float(row.iloc[9] if pd.notna(row.iloc[9]) else 0),  # 3M-利率
                            float(row.iloc[11] if pd.notna(row.iloc[11]) else 0),  # 6M-利率
                            float(row.iloc[15] if pd.notna(row.iloc[15]) else 0)  # 1Y-利率
                        ))
                    except Exception as ex:
                        continue
                self.conn.commit()
                print(f"    {len(df)} 条")

        except Exception as e:
            print(f"  失败: {e}")

    # ==================== 北向资金 ====================

    def collect_north_money(self, days=180):
        """采集北向资金数据"""
        print("采集北向资金数据...")

        try:
            # 使用 fund flow summary 获取北向资金
            df = ak.stock_hsgt_fund_flow_summary_em()
            if df is not None and len(df) > 0:
                c = self.conn.cursor()

                for _, row in df.iterrows():
                    try:
                        # 列顺序: 日期, 市场, 类型, 资金, 交易状态, 成交, 资金流入, 资金流入, ...
                        # 资金流入列索引可能是 6 或 7
                        trade_date = str(row.iloc[0])[:10]

                        # 只记录有资金流入数据的行
                        net_inflow = float(row.iloc[6] if pd.notna(row.iloc[6]) else 0)

                        if net_inflow != 0:
                            c.execute('''
                                INSERT OR REPLACE INTO north_money
                                (trade_date, total_net_inflow)
                                VALUES (?, ?)
                            ''', (trade_date, net_inflow))
                    except Exception as ex:
                        continue

                self.conn.commit()
                print(f"  处理 {len(df)} 条")

        except Exception as e:
            print(f"  失败: {e}")

    # ==================== 市场情绪 ====================

    def collect_market_sentiment(self):
        """采集市场情绪数据"""
        print("采集市场情绪数据...")

        try:
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
    collector = InvestmentDataCollector()

    print("=" * 50)
    print("开始采集投资数据")
    print("=" * 50)

    # 1. 指数数据
    print("\n【1. 指数历史数据】")
    collector.collect_all_indices(days=365)

    # 2. 利率数据
    print("\n【2. 利率数据】")
    collector.collect_interest_rates()

    # 3. 北向资金
    print("\n【3. 北向资金】")
    collector.collect_north_money()

    # 4. 市场情绪
    print("\n【4. 市场情绪】")
    collector.collect_market_sentiment()

    # 5. 股票数据（耗时较长）
    print("\n【5. 股票日线数据】")
    # collector.collect_all_stocks(days=365)  # 可选

    collector.close()

    print("\n" + "=" * 50)
    print("数据采集完成")
    print("=" * 50)


if __name__ == "__main__":
    collect_all()