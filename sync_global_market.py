"""
全球市场数据采集脚本
- VIX恐慌指数 (通过yfinance)
- 美债10年期利率 (AkShare)
- 黄金价格 (上海黄金交易所)
- 原油价格 (国内期货)
"""
import os
import sys
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

# 禁用代理
for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
    os.environ.pop(proxy_var, None)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

import pandas as pd
import akshare as ak

# 尝试导入yfinance
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("警告: yfinance未安装, VIX数据将无法获取")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "investment.db"


def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables_exist(conn):
    """确保需要的表存在"""
    c = conn.cursor()

    # VIX历史表
    c.execute("""
        CREATE TABLE IF NOT EXISTS vix_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE,
            vix_open REAL,
            vix_high REAL,
            vix_low REAL,
            vix_close REAL,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    # 美债利率历史表（扩展）
    c.execute("""
        CREATE TABLE IF NOT EXISTS us_treasury_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE,
            us_2y_yield REAL,
            us_5y_yield REAL,
            us_10y_yield REAL,
            us_30y_yield REAL,
            us_10y_2y_spread REAL,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    # 商品价格表
    c.execute("""
        CREATE TABLE IF NOT EXISTS commodity_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            commodity_type TEXT,
            commodity_name TEXT,
            price REAL,
            unit TEXT,
            source TEXT,
            created_at TEXT,
            UNIQUE(trade_date, commodity_type)
        )
    """)

    conn.commit()


def fetch_vix_yfinance(days=365):
    """使用yfinance获取VIX历史数据"""
    if not YFINANCE_AVAILABLE:
        print("yfinance不可用，跳过VIX获取")
        return pd.DataFrame()

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        print(f"获取VIX数据: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

        # VIX的Yahoo Finance代码是 ^VIX
        vix = yf.Ticker("^VIX")
        df = vix.history(start=start_date, end=end_date)

        if df.empty:
            print("VIX数据为空")
            return pd.DataFrame()

        df = df.reset_index()
        df['trade_date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df = df.rename(columns={
            'Open': 'vix_open',
            'High': 'vix_high',
            'Low': 'vix_low',
            'Close': 'vix_close'
        })

        print(f"获取到 {len(df)} 条VIX数据")
        return df[['trade_date', 'vix_open', 'vix_high', 'vix_low', 'vix_close']]

    except Exception as e:
        print(f"VIX获取失败: {e}")
        return pd.DataFrame()


def fetch_us_treasury_rates():
    """使用AkShare获取美债利率"""
    try:
        print("获取美债利率数据...")
        df = ak.bond_zh_us_rate()

        # 重命名列
        df = df.rename(columns={
            '日期': 'trade_date',
            '美国国债收益率2年': 'us_2y_yield',
            '美国国债收益率5年': 'us_5y_yield',
            '美国国债收益率10年': 'us_10y_yield',
            '美国国债收益率30年': 'us_30y_yield',
            '美国国债收益率10年-2年': 'us_10y_2y_spread'
        })

        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

        print(f"获取到 {len(df)} 条美债利率数据, 最新: {df['trade_date'].max()}")
        return df[['trade_date', 'us_2y_yield', 'us_5y_yield', 'us_10y_yield', 'us_30y_yield', 'us_10y_2y_spread']]

    except Exception as e:
        print(f"美债利率获取失败: {e}")
        return pd.DataFrame()


def fetch_gold_prices():
    """获取黄金价格"""
    try:
        print("获取黄金价格数据...")
        df = ak.spot_golden_benchmark_sge()

        # 使用列索引避免编码问题
        cols = df.columns.tolist()
        df = df.rename(columns={
            cols[0]: 'trade_date',  # 交易时间
            cols[1]: 'gold_open',   # 开盘价
            cols[2]: 'gold_close'   # 收盘价
        })

        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

        print(f"获取到 {len(df)} 条黄金价格数据, 最新: {df['trade_date'].max()}")
        return df[['trade_date', 'gold_open', 'gold_close']]

    except Exception as e:
        print(f"黄金价格获取失败: {e}")
        return pd.DataFrame()


def fetch_oil_prices():
    """获取原油价格"""
    try:
        print("获取原油价格数据...")
        df = ak.energy_oil_hist()

        # 使用列索引避免编码问题
        cols = df.columns.tolist()
        df = df.rename(columns={
            cols[0]: 'trade_date',    # 交易日期
            cols[1]: 'oil_low',       # 最低价格
            cols[2]: 'oil_high',      # 最高价格
            cols[3]: 'oil_prev_close',# 前结价格
            cols[4]: 'oil_close'      # 今结价格
        })

        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

        print(f"获取到 {len(df)} 条原油价格数据, 最新: {df['trade_date'].max()}")
        return df[['trade_date', 'oil_low', 'oil_high', 'oil_prev_close', 'oil_close']]

    except Exception as e:
        print(f"原油价格获取失败: {e}")
        return pd.DataFrame()


def save_vix_to_db(conn, df):
    """保存VIX数据到数据库"""
    if df.empty:
        return 0

    c = conn.cursor()
    now = datetime.now().isoformat()
    inserted = 0

    for _, row in df.iterrows():
        try:
            c.execute("""
                INSERT OR REPLACE INTO vix_history
                (trade_date, vix_open, vix_high, vix_low, vix_close, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['trade_date'],
                float(row['vix_open']) if pd.notna(row['vix_open']) else None,
                float(row['vix_high']) if pd.notna(row['vix_high']) else None,
                float(row['vix_low']) if pd.notna(row['vix_low']) else None,
                float(row['vix_close']) if pd.notna(row['vix_close']) else None,
                now
            ))
            inserted += 1
        except Exception as e:
            print(f"  VIX保存失败 {row['trade_date']}: {e}")

    conn.commit()
    return inserted


def save_treasury_to_db(conn, df):
    """保存美债利率到数据库"""
    if df.empty:
        return 0

    c = conn.cursor()
    now = datetime.now().isoformat()
    inserted = 0

    for _, row in df.iterrows():
        try:
            c.execute("""
                INSERT OR REPLACE INTO us_treasury_history
                (trade_date, us_2y_yield, us_5y_yield, us_10y_yield, us_30y_yield, us_10y_2y_spread, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['trade_date'],
                float(row['us_2y_yield']) if pd.notna(row['us_2y_yield']) else None,
                float(row['us_5y_yield']) if pd.notna(row['us_5y_yield']) else None,
                float(row['us_10y_yield']) if pd.notna(row['us_10y_yield']) else None,
                float(row['us_30y_yield']) if pd.notna(row['us_30y_yield']) else None,
                float(row['us_10y_2y_spread']) if pd.notna(row['us_10y_2y_spread']) else None,
                now
            ))
            inserted += 1
        except Exception as e:
            print(f"  美债保存失败 {row['trade_date']}: {e}")

    # 同时更新interest_rates表
    latest = df.iloc[-1]
    try:
        c.execute("""
            INSERT OR REPLACE INTO interest_rates
            (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y,
             hibor_overnight, hibor_1w, hibor_1m, hibor_3m, libor_usd_3m, china_10y_bond_yield, us_10y_bond_yield)
            SELECT trade_date,
                   COALESCE(shibor_overnight, 0),
                   COALESCE(shibor_1w, 0),
                   COALESCE(shibor_1m, 0),
                   COALESCE(shibor_3m, 0),
                   COALESCE(shibor_6m, 0),
                   COALESCE(shibor_1y, 0),
                   COALESCE(hibor_overnight, 0),
                   COALESCE(hibor_1w, 0),
                   COALESCE(hibor_1m, 0),
                   COALESCE(hibor_3m, 0),
                   COALESCE(libor_usd_3m, 0),
                   COALESCE(china_10y_bond_yield, 0),
                   ? as us_10y_bond_yield
            FROM interest_rates
            WHERE trade_date = ?
        """, (float(latest['us_10y_yield']) if pd.notna(latest['us_10y_yield']) else None, latest['trade_date']))
    except Exception as e:
        # 如果interest_rates表没有对应日期，直接插入
        try:
            c.execute("""
                INSERT OR REPLACE INTO interest_rates
                (trade_date, us_10y_bond_yield, created_at)
                VALUES (?, ?, ?)
            """, (latest['trade_date'], float(latest['us_10y_yield']) if pd.notna(latest['us_10y_yield']) else None, now))
        except:
            pass

    conn.commit()
    return inserted


def save_commodity_to_db(conn, gold_df, oil_df):
    """保存商品价格到数据库"""
    c = conn.cursor()
    now = datetime.now().isoformat()
    inserted = 0

    # 保存黄金
    if not gold_df.empty:
        for _, row in gold_df.iterrows():
            try:
                c.execute("""
                    INSERT OR REPLACE INTO commodity_prices
                    (trade_date, commodity_type, commodity_name, price, unit, source, created_at)
                    VALUES (?, 'gold', '上海黄金基准价', ?, '元/克', 'sge', ?)
                """, (row['trade_date'], float(row['gold_close']) if pd.notna(row['gold_close']) else None, now))
                inserted += 1
            except Exception as e:
                pass

    # 保存原油
    if not oil_df.empty:
        for _, row in oil_df.iterrows():
            try:
                c.execute("""
                    INSERT OR REPLACE INTO commodity_prices
                    (trade_date, commodity_type, commodity_name, price, unit, source, created_at)
                    VALUES (?, 'oil', '国内原油期货', ?, '元/桶', 'akshare', ?)
                """, (row['trade_date'], float(row['oil_close']) if pd.notna(row['oil_close']) else None, now))
                inserted += 1
            except Exception as e:
                pass

    conn.commit()
    return inserted


def main():
    print("=" * 60)
    print("全球市场数据采集")
    print("=" * 60)

    conn = get_db_connection()
    ensure_tables_exist(conn)

    results = {
        "vix": {"count": 0, "status": "skip"},
        "treasury": {"count": 0, "status": "skip"},
        "gold": {"count": 0, "status": "skip"},
        "oil": {"count": 0, "status": "skip"}
    }

    # 1. VIX
    print("\n[1/4] VIX恐慌指数")
    vix_df = fetch_vix_yfinance(365)
    if not vix_df.empty:
        count = save_vix_to_db(conn, vix_df)
        results["vix"] = {"count": count, "status": "success", "latest": vix_df['trade_date'].max()}
        print(f"  保存成功: {count}条")
    else:
        results["vix"]["status"] = "failed"
        print("  获取失败或无数据")

    # 2. 美债利率
    print("\n[2/4] 美债10年期利率")
    treasury_df = fetch_us_treasury_rates()
    if not treasury_df.empty:
        count = save_treasury_to_db(conn, treasury_df)
        results["treasury"] = {"count": count, "status": "success", "latest": treasury_df['trade_date'].max()}
        print(f"  保存成功: {count}条")
    else:
        results["treasury"]["status"] = "failed"

    # 3. 黄金
    print("\n[3/4] 黄金价格")
    gold_df = fetch_gold_prices()
    if not gold_df.empty:
        count = save_commodity_to_db(conn, gold_df, pd.DataFrame())
        results["gold"] = {"count": count, "status": "success", "latest": gold_df['trade_date'].max()}
        print(f"  保存成功: {count}条")
    else:
        results["gold"]["status"] = "failed"

    # 4. 原油
    print("\n[4/4] 原油价格")
    oil_df = fetch_oil_prices()
    if not oil_df.empty:
        count = save_commodity_to_db(conn, pd.DataFrame(), oil_df)
        results["oil"] = {"count": count, "status": "success", "latest": oil_df['trade_date'].max()}
        print(f"  保存成功: {count}条")
    else:
        results["oil"]["status"] = "failed"

    conn.close()

    # 输出结果摘要
    print("\n" + "=" * 60)
    print("采集结果摘要")
    print("=" * 60)
    for name, info in results.items():
        print(f"{name}: {info['status']} - {info.get('count', 0)}条 - 最新: {info.get('latest', 'N/A')}")

    # 保存结果到日志
    log_path = BASE_DIR / "logs" / "global_market_sync.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(f"\n{datetime.now().isoformat()} - {json.dumps(results, ensure_ascii=False)}\n")

    print("\n完成!")
    return results


if __name__ == "__main__":
    main()