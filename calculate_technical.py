"""
技术指标计算脚本
从指数历史数据计算均线、ATR、Beta等技术指标
"""
import sqlite3
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional

DB_PATH = "data/investment.db"


def get_index_history(conn, code: str, days: int = 500) -> List[Dict]:
    """获取指数历史数据"""
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    c.execute('''
        SELECT trade_date, open, high, low, close, volume
        FROM index_history
        WHERE code = ? AND trade_date >= ?
        ORDER BY trade_date
    ''', (code, cutoff))

    return [{
        "date": row[0],
        "open": row[1],
        "high": row[2],
        "low": row[3],
        "close": row[4],
        "volume": row[5]
    } for row in c.fetchall()]


def calculate_ma(prices: List[float], period: int) -> Optional[float]:
    """计算移动平均线"""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def calculate_ema(prices: List[float], period: int) -> Optional[float]:
    """计算指数移动平均线"""
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period  # 初始SMA

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def calculate_macd(prices: List[float]) -> Dict:
    """计算MACD指标"""
    if len(prices) < 35:
        return {"macd": None, "signal": None, "hist": None}

    ema12 = calculate_ema(prices, 12)
    ema26 = calculate_ema(prices, 26)

    if ema12 is None or ema26 is None:
        return {"macd": None, "signal": None, "hist": None}

    # 计算MACD线
    macd_values = []
    for i in range(26, len(prices) + 1):
        ema12_val = calculate_ema(prices[:i], 12)
        ema26_val = calculate_ema(prices[:i], 26)
        if ema12_val and ema26_val:
            macd_values.append(ema12_val - ema26_val)

    if len(macd_values) < 9:
        return {"macd": macd_values[-1] if macd_values else None, "signal": None, "hist": None}

    # 计算信号线
    signal = calculate_ema(macd_values, 9)
    macd = macd_values[-1]

    return {
        "macd": macd,
        "signal": signal,
        "hist": macd - signal if macd and signal else None
    }


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """计算RSI指标"""
    if len(prices) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_atr(data: List[Dict], period: int = 14) -> Dict:
    """计算ATR（平均真实波幅）"""
    if len(data) < period + 1:
        return {"atr": None, "atr_pct": None}

    true_ranges = []
    for i in range(1, len(data)):
        high = data[i]["high"]
        low = data[i]["low"]
        prev_close = data[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return {"atr": None, "atr_pct": None}

    atr = sum(true_ranges[-period:]) / period
    atr_pct = (atr / data[-1]["close"]) * 100 if data[-1]["close"] else None

    return {"atr": atr, "atr_pct": atr_pct}


def calculate_beta(index_prices: List[float], market_prices: List[float]) -> Optional[float]:
    """计算Beta系数"""
    if len(index_prices) < 30 or len(market_prices) < 30:
        return None

    # 取最小长度
    n = min(len(index_prices), len(market_prices))
    index_returns = np.diff(index_prices[-n:]) / index_prices[-n:-1]
    market_returns = np.diff(market_prices[-n:]) / market_prices[-n:-1]

    if len(index_returns) < 20:
        return None

    # 计算协方差和方差
    covariance = np.cov(index_returns, market_returns)[0][1]
    variance = np.var(market_returns)

    if variance == 0:
        return None

    return covariance / variance


def calculate_volatility(prices: List[float], period: int = 30) -> Optional[float]:
    """计算年化波动率"""
    if len(prices) < period + 1:
        return None

    returns = np.diff(prices[-period - 1:]) / prices[-period - 1:-1]
    return np.std(returns) * np.sqrt(252) * 100  # 年化并转为百分比


def determine_trend(data: List[Dict], ma20: float, ma50: float, ma200: float, macd_hist: float) -> str:
    """判断趋势信号"""
    if not data:
        return "unknown"

    close = data[-1]["close"]
    signals = []

    # 均线趋势
    if ma20 and ma50:
        if ma20 > ma50:
            signals.append("MA金叉")
        else:
            signals.append("MA死叉")

    # 价格与均线位置
    if ma20 and close > ma20:
        signals.append("价格站上MA20")
    elif ma20:
        signals.append("价格跌破MA20")

    # MACD信号
    if macd_hist is not None:
        if macd_hist > 0:
            signals.append("MACD多头")
        else:
            signals.append("MACD空头")

    # 综合判断
    if len(signals) >= 2:
        if signals.count("MA金叉") + signals.count("价格站上MA20") + signals.count("MACD多头") >= 2:
            return "看涨"
        elif signals.count("MA死叉") + signals.count("价格跌破MA20") + signals.count("MACD空头") >= 2:
            return "看跌"

    return "震荡"


def calculate_all_indicators(conn, code: str, market_code: str = None) -> List[Dict]:
    """计算单个指数的所有技术指标"""
    data = get_index_history(conn, code, 500)

    if not data:
        return []

    prices = [d["close"] for d in data]
    results = []

    # 获取市场数据用于计算Beta
    market_prices = None
    if market_code:
        market_data = get_index_history(conn, market_code, 500)
        if market_data:
            market_prices = [d["close"] for d in market_data]

    # 从第200天开始计算（确保MA200有效）
    start_idx = max(200, 50)

    for i in range(start_idx, len(data)):
        current_prices = prices[:i + 1]
        current_data = data[:i + 1]

        # 计算均线
        ma5 = calculate_ma(current_prices, 5)
        ma10 = calculate_ma(current_prices, 10)
        ma20 = calculate_ma(current_prices, 20)
        ma50 = calculate_ma(current_prices, 50)
        ma200 = calculate_ma(current_prices, 200)

        # EMA
        ema12 = calculate_ema(current_prices, 12)
        ema26 = calculate_ema(current_prices, 26)

        # MACD
        macd_data = calculate_macd(current_prices)

        # RSI
        rsi = calculate_rsi(current_prices, 14)

        # ATR
        atr_data = calculate_atr(current_data, 14)

        # Beta
        beta_1y = None
        if market_prices:
            beta_1y = calculate_beta(current_prices[-252:], market_prices[-252:]) if len(current_prices) >= 252 else None

        # 波动率
        vol_30d = calculate_volatility(current_prices, 30)
        vol_90d = calculate_volatility(current_prices, 90)

        # 趋势判断
        trend = determine_trend(
            current_data,
            ma20, ma50, ma200,
            macd_data.get("hist")
        )

        results.append({
            "code": code,
            "name": data[i].get("name"),
            "trade_date": data[i]["date"],
            "ma5": round(ma5, 2) if ma5 else None,
            "ma10": round(ma10, 2) if ma10 else None,
            "ma20": round(ma20, 2) if ma20 else None,
            "ma50": round(ma50, 2) if ma50 else None,
            "ma200": round(ma200, 2) if ma200 else None,
            "ema12": round(ema12, 2) if ema12 else None,
            "ema26": round(ema26, 2) if ema26 else None,
            "macd": round(macd_data["macd"], 4) if macd_data["macd"] else None,
            "macd_signal": round(macd_data["signal"], 4) if macd_data["signal"] else None,
            "macd_hist": round(macd_data["hist"], 4) if macd_data["hist"] else None,
            "rsi_14": round(rsi, 2) if rsi else None,
            "atr_14": round(atr_data["atr"], 4) if atr_data["atr"] else None,
            "atr_pct": round(atr_data["atr_pct"], 4) if atr_data["atr_pct"] else None,
            "beta_1y": round(beta_1y, 4) if beta_1y else None,
            "volatility_30d": round(vol_30d, 2) if vol_30d else None,
            "volatility_90d": round(vol_90d, 2) if vol_90d else None,
            "trend_signal": trend
        })

    return results


def save_technical_indicators(conn, indicators: List[Dict]):
    """保存技术指标到数据库"""
    c = conn.cursor()

    for ind in indicators:
        c.execute('''
            INSERT OR REPLACE INTO technical_indicators
            (code, name, trade_date, ma5, ma10, ma20, ma50, ma200,
             ema12, ema26, macd, macd_signal, macd_hist, rsi_14,
             atr_14, atr_pct, beta_1y, volatility_30d, volatility_90d, trend_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ind["code"], ind["name"], ind["trade_date"],
            ind["ma5"], ind["ma10"], ind["ma20"], ind["ma50"], ind["ma200"],
            ind["ema12"], ind["ema26"], ind["macd"], ind["macd_signal"],
            ind["macd_hist"], ind["rsi_14"], ind["atr_14"], ind["atr_pct"],
            ind["beta_1y"], ind["volatility_30d"], ind["volatility_90d"], ind["trend_signal"]
        ))

    conn.commit()


def main():
    """主函数"""
    print("=" * 50)
    print("技术指标计算脚本")
    print("=" * 50)

    conn = sqlite3.connect(DB_PATH)

    # 获取所有指数代码
    c = conn.cursor()
    c.execute("SELECT DISTINCT code, name FROM index_history")
    indices = c.fetchall()

    if not indices:
        print("数据库中没有指数数据，请先采集数据")
        conn.close()
        return

    print(f"\n找到 {len(indices)} 个指数")

    # 定义市场基准（用于计算Beta）
    market_benchmark = {
        "sh": "sh000001",  # 上证指数
        "sz": "sz399001",  # 深证成指
        "hk": "hsi",       # 恒生指数
        "us": "^GSPC"      # 标普500
    }

    total_calculated = 0

    for code, name in indices:
        print(f"\n计算 {code} ({name}) 的技术指标...")

        # 确定市场基准
        market_code = None
        if code.startswith("sh") or code.startswith("sz"):
            market_code = market_benchmark["sh"]
        elif code.startswith("hk") or code == "hsi":
            market_code = market_benchmark["hk"]
        elif code.startswith("us") or code in ["dji", "ixic", "inx"]:
            market_code = None  # 美股暂时不计算Beta

        try:
            indicators = calculate_all_indicators(conn, code, market_code)
            if indicators:
                save_technical_indicators(conn, indicators)
                print(f"  保存了 {len(indicators)} 条记录")
                total_calculated += len(indicators)
            else:
                print("  数据不足，跳过")
        except Exception as e:
            print(f"  计算失败: {e}")

    # 打印统计
    c.execute("SELECT COUNT(*) FROM technical_indicators")
    total = c.fetchone()[0]

    print("\n" + "=" * 50)
    print(f"计算完成！共保存 {total_calculated} 条新记录")
    print(f"数据库中共有 {total} 条技术指标记录")

    # 打印最新数据示例
    print("\n最新技术指标示例:")
    c.execute('''
        SELECT code, name, trade_date, close, ma20, ma50, rsi_14, trend_signal
        FROM technical_indicators t
        JOIN index_history i ON t.code = i.code AND t.trade_date = i.trade_date
        WHERE t.trade_date = (SELECT MAX(trade_date) FROM technical_indicators)
        LIMIT 5
    ''')

    for row in c.fetchall():
        print(f"  {row[1]}: 收盘{row[3]:.2f} MA20={row[4]:.2f} MA50={row[5]:.2f} RSI={row[6]:.1f} 趋势={row[7]}")

    conn.close()


if __name__ == "__main__":
    main()