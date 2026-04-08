"""
每日因子快照更新脚本
计算每只股票的质量、增长、估值、技术、风险因子评分并更新到stock_factor_snapshot表
"""
import os
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# 禁用代理
for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
    os.environ.pop(proxy_var, None)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "investment.db"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_float(value: Any, default: float = 0.0) -> Optional[float]:
    """安全转换浮点数"""
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        if isinstance(value, str) and "%" in value:
            return float(value.replace("%", "")) / 100
        return float(value)
    except:
        return default


def _clamp(value: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
    """限制范围"""
    return max(min_val, min(max_val, value))


def calculate_quality_score(fundamentals: Dict) -> float:
    """
    计算质量因子评分
    基于：ROE、净利率、现金流质量、资产负债率
    """
    score = 50.0  # 基础分

    # ROE (净资产收益率)
    roe = _safe_float(fundamentals.get('roe'))
    if roe:
        if roe >= 15:
            score += 20
        elif roe >= 10:
            score += 15
        elif roe >= 5:
            score += 5
        elif roe < 0:
            score -= 15

    # 净利率 (字段名: net_margin)
    net_margin = _safe_float(fundamentals.get('net_margin'))
    if net_margin:
        if net_margin >= 20:
            score += 15
        elif net_margin >= 10:
            score += 10
        elif net_margin >= 5:
            score += 5
        elif net_margin < 0:
            score -= 10

    # 现金流/净利润比率 (需要计算)
    operating_cash = _safe_float(fundamentals.get('operating_cash_flow'))
    net_profit = _safe_float(fundamentals.get('net_profit'))
    if operating_cash and net_profit and net_profit != 0:
        cash_ratio = operating_cash / abs(net_profit)
        if cash_ratio >= 1.2:
            score += 10  # 现金流充沛
        elif cash_ratio < 0.5:
            score -= 10  # 现金流紧张

    # 资产负债率
    debt_ratio = _safe_float(fundamentals.get('debt_ratio'))
    if debt_ratio:
        if debt_ratio <= 30:
            score += 5  # 低负债
        elif debt_ratio >= 70:
            score -= 10  # 高负债风险

    return _clamp(score)


def calculate_growth_score(fundamentals: Dict) -> float:
    """
    计算增长因子评分
    基于：营收增长率、利润增长率
    """
    score = 50.0

    # 营收增长率 (字段名: revenue_yoy)
    revenue_growth = _safe_float(fundamentals.get('revenue_yoy'))
    if revenue_growth:
        if revenue_growth >= 30:
            score += 25
        elif revenue_growth >= 20:
            score += 20
        elif revenue_growth >= 10:
            score += 10
        elif revenue_growth >= 0:
            score += 5
        elif revenue_growth < -10:
            score -= 15

    # 利润增长率 (字段名: net_profit_yoy)
    profit_growth = _safe_float(fundamentals.get('net_profit_yoy'))
    if profit_growth:
        if profit_growth >= 30:
            score += 25
        elif profit_growth >= 20:
            score += 20
        elif profit_growth >= 10:
            score += 10
        elif profit_growth >= 0:
            score += 5
        elif profit_growth < -10:
            score -= 15

    return _clamp(score)


def calculate_valuation_score(valuation: Dict) -> float:
    """
    计算估值因子评分
    基于：PE、PB、PS、估值带位置
    """
    score = 50.0

    # PE (市盈率) - 字段名: pe_ttm
    pe = _safe_float(valuation.get('pe_ttm'))
    if pe and pe > 0:
        if pe <= 15:
            score += 20  # 低估值
        elif pe <= 25:
            score += 10
        elif pe <= 40:
            score += 0
        elif pe > 60:
            score -= 15  # 高估值

    # PB (市净率)
    pb = _safe_float(valuation.get('pb'))
    if pb and pb > 0:
        if pb <= 1.5:
            score += 15
        elif pb <= 3:
            score += 5
        elif pb > 5:
            score -= 10

    # 估值带位置 (PE 5年百分位)
    band_position = _safe_float(valuation.get('pe_percentile_5y'))
    if band_position:
        if band_position <= 20:
            score += 20  # 处于估值带低位
        elif band_position <= 40:
            score += 10
        elif band_position >= 80:
            score -= 15  # 处于估值带高位

    return _clamp(score)


def calculate_technical_score(technical: Dict) -> float:
    """
    计算技术因子评分
    基于：均线位置、趋势强度、RSI、MACD
    """
    score = 50.0

    # 趋势信号
    trend_signal = technical.get('trend_signal')
    if trend_signal:
        if trend_signal == 'bullish':
            score += 15
        elif trend_signal == 'bearish':
            score -= 15

    # MACD
    macd = _safe_float(technical.get('macd'))
    macd_signal = _safe_float(technical.get('macd_signal'))
    if macd and macd_signal:
        if macd > macd_signal:
            score += 10  # MACD金叉
        else:
            score -= 5  # MACD死叉

    # RSI
    rsi = _safe_float(technical.get('rsi_14'))
    if rsi:
        if 40 <= rsi <= 60:
            score += 5  # 中性区域
        elif rsi >= 70:
            score -= 5  # 超买
        elif rsi <= 30:
            score += 5  # 超卖可能反弹

    # MACD信号
    macd_signal = technical.get('macd_signal')
    # 趋势强度 (简化处理)
    if trend_signal == 'bullish':
        score += 10

    return _clamp(score)


def calculate_risk_score(fundamentals: Dict, valuation: Dict, technical: Dict) -> float:
    """
    计算风险因子评分
    注意：风险评分越高表示风险越大，需要在总评分中反向处理
    """
    score = 30.0  # 基础风险分（低风险）

    # 高负债风险
    debt_ratio = _safe_float(fundamentals.get('debt_ratio'))
    if debt_ratio and debt_ratio >= 70:
        score += 20

    # 业绩下滑风险 (字段名: net_profit_yoy)
    profit_growth = _safe_float(fundamentals.get('net_profit_yoy'))
    if profit_growth and profit_growth < -20:
        score += 15

    # 高估值风险 (字段名: pe_ttm)
    pe = _safe_float(valuation.get('pe_ttm'))
    if pe and pe > 50:
        score += 10

    # 技术破位风险 (使用trend_signal)
    trend_signal = technical.get('trend_signal')
    if trend_signal == 'bearish':
        score += 15

    return _clamp(score)


def load_fundamentals(conn, codes: List[str]) -> Dict[str, Dict]:
    """加载财务数据"""
    c = conn.cursor()
    placeholders = ','.join(['?' for _ in codes])
    c.execute(f"""
        SELECT code, report_date, roe, net_margin, revenue_yoy, net_profit_yoy,
               debt_ratio, operating_cash_flow, net_profit
        FROM stock_financial
        WHERE code IN ({placeholders})
        ORDER BY report_date DESC
    """, codes)

    result = {}
    for row in c.fetchall():
        code = row['code']
        if code not in result:  # 只取最新报告期
            result[code] = dict(row)
    return result


def load_valuation(conn, codes: List[str]) -> Dict[str, Dict]:
    """加载估值数据"""
    c = conn.cursor()
    placeholders = ','.join(['?' for _ in codes])
    c.execute(f"""
        SELECT code, trade_date, pe_ttm, pb, ps_ttm, pe_percentile_5y
        FROM valuation_bands
        WHERE code IN ({placeholders})
        ORDER BY trade_date DESC
    """, codes)

    result = {}
    for row in c.fetchall():
        code = row['code']
        if code not in result:
            result[code] = dict(row)
    return result


def load_technical(conn, codes: List[str]) -> Dict[str, Dict]:
    """加载技术指标"""
    c = conn.cursor()
    placeholders = ','.join(['?' for _ in codes])
    c.execute(f"""
        SELECT code, trade_date, ma20, ma50, rsi_14, macd, macd_signal, trend_signal
        FROM technical_indicators
        WHERE code IN ({placeholders})
        ORDER BY trade_date DESC
    """, codes)

    result = {}
    for row in c.fetchall():
        code = row['code']
        if code not in result:
            result[code] = dict(row)
    return result


def calculate_total_score(quality: float, growth: float, valuation: float,
                          technical: float, risk: float) -> float:
    """
    计算总评分
    权重：质量20%, 增长18%, 估值14%, 技术14%, 风险8%反向
    """
    total = (
        quality * 0.20 +
        growth * 0.18 +
        valuation * 0.14 +
        technical * 0.14 +
        (100 - risk) * 0.08 +
        30  # 基础分
    )
    return _clamp(total)


def update_factor_snapshot(conn, trade_date: str) -> Dict[str, int]:
    """
    更新因子快照表
    """
    c = conn.cursor()

    # 获取所有需要计算的股票代码
    c.execute("SELECT DISTINCT member_code FROM stock_pool_constituents")
    pool_codes = [row['member_code'] for row in c.fetchall()]

    c.execute("SELECT DISTINCT code FROM stock_financial")
    financial_codes = [row['code'] for row in c.fetchall()]

    # 合并所有代码
    all_codes = list(set(pool_codes + financial_codes))
    print(f"需要计算的股票数: {len(all_codes)}")

    if not all_codes:
        return {"total": 0, "updated": 0}

    # 加载基础数据
    print("加载财务数据...")
    fundamentals = load_fundamentals(conn, all_codes)

    print("加载估值数据...")
    valuation = load_valuation(conn, all_codes)

    print("加载技术指标...")
    technical = load_technical(conn, all_codes)

    # 计算因子评分
    print("计算因子评分...")
    now = datetime.now().isoformat()
    updated = 0
    model = 'conservative'

    for code in all_codes:
        fund = fundamentals.get(code, {})
        val = valuation.get(code, {})
        tech = technical.get(code, {})

        # 计算各因子评分
        quality_score = calculate_quality_score(fund)
        growth_score = calculate_growth_score(fund)
        valuation_score = calculate_valuation_score(val)
        technical_score = calculate_technical_score(tech)
        risk_score = calculate_risk_score(fund, val, tech)
        flow_score = 50.0  # 默认值，后续可从资金流向数据计算

        # 计算总评分
        total_score = calculate_total_score(
            quality_score, growth_score, valuation_score,
            technical_score, risk_score
        )

        # 插入或更新数据库
        try:
            c.execute("""
                INSERT OR REPLACE INTO stock_factor_snapshot
                (trade_date, code, model, quality, growth, valuation, flow, technical, risk, total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_date,
                code,
                model,
                quality_score,
                growth_score,
                valuation_score,
                flow_score,
                technical_score,
                risk_score,
                total_score
            ))
            updated += 1
        except Exception as e:
            print(f"  {code} 更新失败: {e}")

    conn.commit()

    # 统计结果
    c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE trade_date = ? AND model = ?", (trade_date, model))
    total = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE trade_date = ? AND model = ? AND total >= 70", (trade_date, model))
    high_score = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM stock_factor_snapshot WHERE trade_date = ? AND model = ? AND total >= 60", (trade_date, model))
    mid_score = c.fetchone()[0]

    print(f"更新完成: {updated}条")
    print(f"评分>=70: {high_score}只, >=60: {mid_score}只")

    return {
        "total": total,
        "updated": updated,
        "high_score_count": high_score,
        "mid_score_count": mid_score
    }


def main():
    print("=" * 60)
    print("每日因子快照更新")
    print("=" * 60)

    trade_date = datetime.now().strftime('%Y-%m-%d')
    print(f"计算日期: {trade_date}")

    conn = get_db_connection()
    result = update_factor_snapshot(conn, trade_date)
    conn.close()

    # 保存日志
    log_path = BASE_DIR / "logs" / "factor_snapshot_update.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        import json
        f.write(f"\n{datetime.now().isoformat()} - {json.dumps(result, ensure_ascii=False)}\n")

    print("\n完成!")
    return result


if __name__ == "__main__":
    main()