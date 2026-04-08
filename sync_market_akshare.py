"""
使用AkShare同步行情数据（避免Yahoo Finance限流）
"""
from __future__ import annotations

import os
# 禁用所有代理
for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
    os.environ.pop(proxy_var, None)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

import json
import time
import requests

# 强制设置requests不使用代理
requests.Session.trust_env = False
original_session = requests.Session
class NoProxySession(requests.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trust_env = False
        self.proxies = {}
requests.Session = NoProxySession
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import akshare as ak
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "quant_workbench"
MARKET_DIR = DATA_DIR / "market"
STATUS_FILE = DATA_DIR / "status.json"
INVESTMENT_DB_PATH = BASE_DIR / "data" / "investment.db"


def ensure_dirs():
    MARKET_DIR.mkdir(parents=True, exist_ok=True)


def save_parquet(df: pd.DataFrame, code: str, interval: str):
    """保存为parquet格式"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    safe_code = code.replace("/", "_")
    path = MARKET_DIR / f"{safe_code}_{interval}.parquet"

    # 标准化列名
    df = df.copy()
    if "date" not in df.columns and "trade_date" in df.columns:
        df["date"] = df["trade_date"]

    df.to_parquet(path, index=False, compression="zstd")


def fetch_a_stock_daily(code: str) -> pd.DataFrame:
    """获取A股日线数据"""
    try:
        # 去掉sh/sz前缀
        pure_code = code[2:] if code.startswith(("sh", "sz")) else code
        df = ak.stock_zh_a_hist(symbol=pure_code, period="daily", adjust="qfq")

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turnover",
        })

        df["code"] = code
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        return df[["date", "open", "high", "low", "close", "volume", "code"]]

    except Exception as e:
        print(f"  {code} 日线获取失败: {e}")
        return pd.DataFrame()


def fetch_index_daily(symbol: str, name: str) -> pd.DataFrame:
    """获取指数日线数据"""
    try:
        if symbol in ["hsi", "fxi", "yinn", "yang", "vix"]:
            # 这些需要从Yahoo获取，暂时跳过
            return pd.DataFrame()

        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        })

        df["code"] = symbol
        return df[["date", "open", "high", "low", "close", "volume", "code"]]

    except Exception as e:
        print(f"  {symbol} 指数获取失败: {e}")
        return pd.DataFrame()


def get_stock_pool() -> List[str]:
    """从数据库获取股票池"""
    import sqlite3

    conn = sqlite3.connect(INVESTMENT_DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT code FROM stock_financial")
    codes = [r[0] for r in c.fetchall()]
    conn.close()

    # 转换为内部格式
    result = []
    for code in codes:
        if code.startswith("6"):
            result.append(f"sh{code}")
        elif code.startswith(("0", "3")):
            result.append(f"sz{code}")
        elif code.startswith("hk"):
            result.append(code)

    return result


def sync_market_data():
    """同步行情数据"""
    ensure_dirs()

    print("=" * 50)
    print("使用AkShare同步行情数据")
    print("=" * 50)

    codes = get_stock_pool()
    print(f"股票池: {len(codes)} 只")

    counters = {"daily": 0, "errors": 0}

    for i, code in enumerate(codes):
        if (i + 1) % 50 == 0:
            print(f"进度: {i+1}/{len(codes)}")

        # 只同步A股，港股需要其他数据源
        if not code.startswith(("sh", "sz")):
            continue

        try:
            df = fetch_a_stock_daily(code)
            if not df.empty:
                save_parquet(df, code, "1d")
                counters["daily"] += 1
            time.sleep(0.1)  # 避免请求过快
        except Exception as e:
            counters["errors"] += 1

    # 同步主要指数
    print("\n同步主要指数...")
    indices = [
        ("sh000001", "上证指数"),
        ("sh000300", "沪深300"),
        ("sh000905", "中证500"),
        ("sh000016", "上证50"),
    ]

    for symbol, name in indices:
        try:
            df = ak.stock_zh_index_daily(symbol=symbol[2:])  # 去掉sh前缀
            if df is not None and not df.empty:
                df["code"] = symbol
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                save_parquet(df[["date", "open", "high", "low", "close", "volume", "code"]], symbol, "1d")
                print(f"  {name} OK")
        except Exception as e:
            print(f"  {name} 失败: {e}")

    # 保存状态
    status = {
        "last_sync_at": datetime.now().isoformat(),
        "daily_files": counters["daily"],
        "intraday_files": 0,
        "benchmark_files": len(indices),
        "error_count": counters["errors"],
        "errors": [],
        "data_source": "akshare"
    }
    STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 50)
    print(f"完成! 日线: {counters['daily']}, 错误: {counters['errors']}")
    print("=" * 50)


if __name__ == "__main__":
    sync_market_data()