"""
信号标签回填脚本
对历史信号计算未来收益标签
"""
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_trading_dates(conn, start_date: str = None, end_date: str = None) -> List[str]:
    """获取交易日列表"""
    c = conn.cursor()

    # 从指数历史获取交易日
    query = "SELECT DISTINCT trade_date FROM index_history"
    params = []

    if start_date:
        query += " WHERE trade_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND trade_date <= ?" if params else " WHERE trade_date <= ?"
        params.append(end_date)

    query += " ORDER BY trade_date"

    c.execute(query, params)
    return [row[0] for row in c.fetchall()]


def get_price_on_date(conn, symbol: str, target_date: str) -> Optional[float]:
    """获取某日的收盘价"""
    c = conn.cursor()

    # 尝试从stock_daily获取
    c.execute("""
        SELECT close FROM stock_daily
        WHERE code = ? AND trade_date <= ?
        ORDER BY trade_date DESC LIMIT 1
    """, (symbol, target_date))
    row = c.fetchone()
    if row and row[0]:
        return row[0]

    # 尝试从index_history获取
    c.execute("""
        SELECT close FROM index_history
        WHERE code = ? AND trade_date <= ?
        ORDER BY trade_date DESC LIMIT 1
    """, (symbol, target_date))
    row = c.fetchone()
    if row and row[0]:
        return row[0]

    return None


def get_price_after_days(conn, symbol: str, start_date: str, days: int,
                         trading_dates: List[str]) -> Dict[str, Optional[float]]:
    """获取N个交易日后的价格和期间最高最低价"""
    try:
        start_idx = trading_dates.index(start_date)
    except ValueError:
        return {"exit_price": None, "max_price": None, "min_price": None}

    target_idx = start_idx + days
    if target_idx >= len(trading_dates):
        return {"exit_price": None, "max_price": None, "min_price": None}

    target_date = trading_dates[target_idx]
    exit_price = get_price_on_date(conn, symbol, target_date)

    # 获取期间最高最低价
    c = conn.cursor()
    period_dates = trading_dates[start_idx:target_idx + 1]

    if not period_dates:
        return {"exit_price": exit_price, "max_price": None, "min_price": None}

    placeholders = ",".join(["?" for _ in period_dates])
    c.execute(f"""
        SELECT MAX(high) as max_high, MIN(low) as min_low
        FROM stock_daily
        WHERE code = ? AND trade_date IN ({placeholders})
    """, [symbol] + period_dates)

    row = c.fetchone()

    return {
        "exit_price": exit_price,
        "max_price": row[0] if row else None,
        "min_price": row[1] if row else None,
    }


def calculate_label(entry_price: float, exit_price: Optional[float],
                    max_price: Optional[float], min_price: Optional[float],
                    stop_loss_pct: float = 5.0, take_profit_pct: float = 10.0) -> Dict:
    """计算标签"""
    if not exit_price or not entry_price:
        return {
            "return_pct": None,
            "max_upside": None,
            "max_drawdown": None,
            "hit_takeprofit": None,
            "hit_stoploss": None,
            "win_flag": None,
        }

    return_pct = (exit_price - entry_price) / entry_price * 100

    max_upside = None
    max_drawdown = None

    if max_price and entry_price:
        max_upside = (max_price - entry_price) / entry_price * 100

    if min_price and entry_price:
        max_drawdown = (entry_price - min_price) / entry_price * 100

    hit_takeprofit = 1 if max_upside and max_upside >= take_profit_pct else 0
    hit_stoploss = 1 if max_drawdown and max_drawdown >= stop_loss_pct else 0

    win_flag = 1 if return_pct > 0 else 0

    return {
        "return_pct": round(return_pct, 2),
        "max_upside": round(max_upside, 2) if max_upside else None,
        "max_drawdown": round(max_drawdown, 2) if max_drawdown else None,
        "hit_takeprofit": hit_takeprofit,
        "hit_stoploss": hit_stoploss,
        "win_flag": win_flag,
    }


def backfill_signal_labels(days_back: int = 365, dry_run: bool = False):
    """
    回填信号标签
    """
    conn = get_db_connection()
    c = conn.cursor()

    # 获取交易日
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_back + 30)).strftime("%Y-%m-%d")
    trading_dates = get_trading_dates(conn, start_date, end_date)

    print(f"交易日数量: {len(trading_dates)}")

    # 获取需要回填的信号
    c.execute("""
        SELECT signal_date, code, setup_name, hold_days, entry_price, exit_price
        FROM signal_labels
        WHERE win_flag IS NULL OR win_flag = 0
        ORDER BY signal_date DESC
        LIMIT 5000
    """)

    signals = c.fetchall()
    print(f"待处理信号数量: {len(signals)}")

    updated = 0
    errors = 0

    for signal in signals:
        signal_date = signal["signal_date"]
        code = signal["code"]
        setup_name = signal["setup_name"]
        entry_price = signal["entry_price"]

        if not entry_price or entry_price <= 0:
            errors += 1
            continue

        # 计算各周期收益
        results = {}
        for days in [1, 3, 5, 10]:
            price_info = get_price_after_days(conn, code, signal_date, days, trading_dates)
            if price_info["exit_price"]:
                label = calculate_label(
                    entry_price,
                    price_info["exit_price"],
                    price_info["max_price"],
                    price_info["min_price"]
                )
                results[f"t{days}_ret"] = label["return_pct"]
                results[f"t{days}_max_upside"] = label["max_upside"]
                results[f"t{days}_max_drawdown"] = label["max_drawdown"]

        if not results:
            errors += 1
            continue

        # 更新数据库
        if not dry_run:
            try:
                c.execute("""
                    UPDATE signal_labels
                    SET t1_ret = ?, t3_ret = ?, t5_ret = ?, t10_ret = ?,
                        max_gain = ?, max_drawdown = ?,
                        win_flag = ?, label_status = ?, labeled_at = ?
                    WHERE signal_date = ? AND code = ? AND setup_name = ?
                """, (
                    results.get("t1_ret"),
                    results.get("t3_ret"),
                    results.get("t5_ret"),
                    results.get("t10_ret"),
                    results.get("t5_max_upside"),
                    results.get("t5_max_drawdown"),
                    1 if results.get("t5_ret", 0) > 0 else 0,
                    "labeled",
                    datetime.now().isoformat(),
                    signal_date, code, setup_name
                ))
                updated += 1
            except Exception as e:
                print(f"更新失败 {signal_date} {code}: {e}")
                errors += 1

        if updated % 500 == 0 and updated > 0:
            print(f"进度: {updated}/{len(signals)}")
            conn.commit()

    conn.commit()
    conn.close()

    print(f"\n回填完成: 更新{updated}条, 错误{errors}条")
    return {"updated": updated, "errors": errors}


def backfill_v2_signals(dry_run: bool = False):
    """
    回填V2信号表
    从stock_factor_snapshot生成历史信号
    """
    conn = get_db_connection()
    c = conn.cursor()

    # 获取因子快照历史
    c.execute("""
        SELECT DISTINCT trade_date FROM stock_factor_snapshot
        WHERE model = 'conservative'
        ORDER BY trade_date DESC
        LIMIT 30
    """)

    dates = [row[0] for row in c.fetchall()]
    print(f"待处理日期: {len(dates)}个")

    # 获取交易日
    trading_dates = get_trading_dates(conn)

    updated = 0

    for trade_date in dates:
        # 获取当日因子
        c.execute("""
            SELECT code, quality, growth, valuation, flow, technical, risk, total
            FROM stock_factor_snapshot
            WHERE model = 'conservative' AND trade_date = ?
        """, (trade_date,))

        snapshots = c.fetchall()

        for snap in snapshots:
            code = snap["code"]
            factors = {
                "quality": snap["quality"] or 50,
                "growth": snap["growth"] or 50,
                "valuation": snap["valuation"] or 50,
                "technical": snap["technical"] or 50,
                "flow": snap["flow"] or 50,
                "risk": snap["risk"] or 30,
            }

            total = snap["total"] or 50
            risk = factors["risk"]

            # 简单分级
            if total >= 70 and risk <= 40:
                grade = "A"
            elif total >= 60 and risk <= 55:
                grade = "B"
            else:
                grade = "C"

            # 简单setup分类
            if factors["technical"] >= 60 and factors["flow"] >= 55:
                setup_type = "trend_continuation"
            elif factors["quality"] >= 65:
                setup_type = "quality_rerate"
            else:
                setup_type = "catalyst_breakout"

            # 生成信号ID
            import hashlib
            signal_id = hashlib.md5(f"{code}_{trade_date}_{setup_type}".encode()).hexdigest()[:16]

            # 写入V2表
            if not dry_run:
                try:
                    c.execute("""
                        INSERT OR REPLACE INTO strategy_signals_v2 (
                            signal_id, as_of_date, symbol, setup_type,
                            score_total, score_quality, score_growth, score_valuation,
                            score_technical, score_flow, risk_penalty,
                            grade, action, eligibility_pass, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """, (
                        signal_id,
                        trade_date,
                        code,
                        setup_type,
                        total,
                        factors["quality"],
                        factors["growth"],
                        factors["valuation"],
                        factors["technical"],
                        factors["flow"],
                        max(0, risk - 30) * 0.5,
                        grade,
                        "BUY_NOW" if grade == "A" else "WATCH" if grade == "B" else "SKIP",
                        datetime.now().isoformat()
                    ))
                    updated += 1
                except Exception as e:
                    pass

        if updated % 500 == 0 and updated > 0:
            print(f"进度: {updated}")
            conn.commit()

    conn.commit()
    conn.close()

    print(f"\nV2信号回填完成: {updated}条")
    return {"updated": updated}


if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("信号标签回填")
    print("=" * 60)

    dry_run = "--dry-run" in sys.argv

    # 回填V2信号
    print("\n[1/2] 回填V2信号...")
    result1 = backfill_v2_signals(dry_run=dry_run)

    # 回填标签
    print("\n[2/2] 回填信号标签...")
    result2 = backfill_signal_labels(days_back=365, dry_run=dry_run)

    print("\n完成!")
    print(f"V2信号: {result1}")
    print(f"标签: {result2}")