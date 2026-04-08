"""
补全A股基本面数据
从AkShare获取沪深300+中证500成分股的财务指标
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import akshare as ak
import pandas as pd

DB_PATH = Path(__file__).resolve().parent / "data" / "investment.db"


def get_hs300_codes() -> List[str]:
    """获取沪深300成分股代码"""
    try:
        df = ak.index_stock_cons_weight_csindex(symbol="000300")
        codes = df["成分券代码"].tolist()
        print(f"沪深300成分股: {len(codes)} 只")
        return codes
    except Exception as e:
        print(f"获取沪深300失败: {e}")
        return []


def get_zz500_codes() -> List[str]:
    """获取中证500成分股代码"""
    try:
        df = ak.index_stock_cons_weight_csindex(symbol="000905")
        codes = df["成分券代码"].tolist()
        print(f"中证500成分股: {len(codes)} 只")
        return codes
    except Exception as e:
        print(f"获取中证500失败: {e}")
        return []


def get_financial_indicator(code: str) -> Optional[Dict[str, Any]]:
    """获取单只股票的财务指标"""
    try:
        # 获取关键财务指标
        df = ak.stock_financial_abstract(symbol=code)

        if df is None or df.empty:
            return None

        # 取最新一期数据
        latest = df.iloc[0]

        # 构建财务数据字典
        data = {
            "code": code,
            "report_date": str(latest.get("报告期", "")),
            "roe": _safe_float(latest.get("净资产收益率")),
            "roa": _safe_float(latest.get("总资产净利率")),
            "gross_margin": _safe_float(latest.get("销售毛利率")),
            "net_margin": _safe_float(latest.get("销售净利率")),
            "debt_ratio": _safe_float(latest.get("资产负债率")),
            "current_ratio": _safe_float(latest.get("流动比率")),
            "quick_ratio": _safe_float(latest.get("速动比率")),
            "eps": _safe_float(latest.get("每股收益")),
            "bvps": _safe_float(latest.get("每股净资产")),
            "pe_ttm": None,  # 需要单独获取
            "pb": None,
            "total_revenue": _safe_float(latest.get("营业总收入")),
            "net_profit": _safe_float(latest.get("净利润")),
            "net_profit_yoy": _safe_float(latest.get("净利润同比增长率")),
            "revenue_yoy": _safe_float(latest.get("营业总收入同比增长率")),
            "operating_cash_flow": _safe_float(latest.get("经营活动产生的现金流量净额")),
            "dividend_yield": None,
        }

        # 获取估值数据
        try:
            realtime = ak.stock_zh_a_spot_em()
            stock_info = realtime[realtime["代码"] == code]
            if not stock_info.empty:
                data["pe_ttm"] = _safe_float(stock_info.iloc[0].get("市盈率-动态"))
                data["pb"] = _safe_float(stock_info.iloc[0].get("市净率"))
                data["dividend_yield"] = _safe_float(stock_info.iloc[0].get("股息率"))
        except Exception:
            pass

        return data

    except Exception as e:
        print(f"  {code} 获取失败: {e}")
        return None


def _safe_float(value: Any) -> Optional[float]:
    """安全转换为浮点数"""
    if value is None or value == "" or pd.isna(value):
        return None
    try:
        # 处理百分比字符串
        if isinstance(value, str) and "%" in value:
            return round(float(value.replace("%", "")) / 100, 4)
        return round(float(value), 4)
    except (ValueError, TypeError):
        return None


def save_to_database(records: List[Dict[str, Any]]) -> int:
    """保存财务数据到数据库"""
    if not records:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    saved = 0
    for rec in records:
        try:
            c.execute('''
                INSERT OR REPLACE INTO stock_financial
                (code, report_date, roe, roa, gross_margin, net_margin,
                 debt_ratio, current_ratio, quick_ratio, eps, bvps,
                 pe_ttm, pb, total_revenue, net_profit, net_profit_yoy,
                 revenue_yoy, operating_cash_flow, dividend_yield)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                rec["code"], rec["report_date"], rec["roe"], rec["roa"],
                rec["gross_margin"], rec["net_margin"], rec["debt_ratio"],
                rec["current_ratio"], rec["quick_ratio"], rec["eps"],
                rec["bvps"], rec["pe_ttm"], rec["pb"], rec["total_revenue"],
                rec["net_profit"], rec["net_profit_yoy"], rec["revenue_yoy"],
                rec["operating_cash_flow"], rec["dividend_yield"]
            ))
            saved += 1
        except Exception as e:
            print(f"  保存 {rec['code']} 失败: {e}")

    conn.commit()
    conn.close()
    return saved


def main():
    print("=" * 50)
    print("开始补全A股基本面数据")
    print("=" * 50)

    # 获取目标股票池
    hs300 = get_hs300_codes()
    zz500 = get_zz500_codes()
    all_codes = list(set(hs300 + zz500))
    print(f"目标股票总数: {len(all_codes)} 只")

    # 检查已有数据
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT code FROM stock_financial")
    existing = set(r[0] for r in c.fetchall())
    conn.close()

    # 只获取缺失的数据
    missing = [c for c in all_codes if c not in existing]
    print(f"已有数据: {len(existing)} 只, 需补充: {len(missing)} 只")

    if not missing:
        print("所有股票数据已完整，无需更新")
        return

    # 分批获取数据
    records = []
    batch_size = 50

    for i, code in enumerate(missing):
        print(f"[{i+1}/{len(missing)}] 获取 {code}...", end="")

        data = get_financial_indicator(code)
        if data:
            records.append(data)
            print(" OK")
        else:
            print(" 跳过")

        # 每50只保存一次
        if len(records) >= batch_size:
            saved = save_to_database(records)
            print(f"  批量保存 {saved} 条")
            records = []

        # 避免请求过快
        time.sleep(0.3)

    # 保存剩余数据
    if records:
        saved = save_to_database(records)
        print(f"  最终保存 {saved} 条")

    # 统计结果
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(DISTINCT code) FROM stock_financial")
    total = c.fetchone()[0]
    conn.close()

    print("=" * 50)
    print(f"完成! 数据库现有 {total} 只股票的财务数据")
    print("=" * 50)


if __name__ == "__main__":
    main()