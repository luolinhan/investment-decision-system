"""
使用akshare获取实时股票财务数据
"""
import sqlite3
import akshare as ak
from datetime import datetime

DB_PATH = r"C:\Users\Administrator\research_report_system\data\investment.db"

# 关注股票代码映射
STOCK_CODES = {
    # A股
    "sh603259": {"symbol": "603259", "name": "药明康德", "market": "A", "category": "CXO"},
    "sh600438": {"symbol": "600438", "name": "通威股份", "market": "A", "category": "光伏"},
    "sh601012": {"symbol": "601012", "name": "隆基绿能", "market": "A", "category": "光伏"},
    "sz002459": {"symbol": "002459", "name": "晶澳科技", "market": "A", "category": "光伏"},
    "sz300763": {"symbol": "300763", "name": "锦浪科技", "market": "A", "category": "光伏"},
    "sh688235": {"symbol": "688235", "name": "百济神州", "market": "A", "category": "医药"},
    "sh600196": {"symbol": "600196", "name": "复星医药", "market": "A", "category": "医药"},
    "sh601888": {"symbol": "601888", "name": "中国中免", "market": "A", "category": "消费"},
    # 港股
    "hk02269": {"symbol": "02269", "name": "药明生物", "market": "HK", "category": "CXO"},
    "hk06160": {"symbol": "06160", "name": "百济神州", "market": "HK", "category": "医药"},
    "hk01177": {"symbol": "01177", "name": "中国生物制药", "market": "HK", "category": "医药"},
    "hk01880": {"symbol": "01880", "name": "中国中免", "market": "HK", "category": "消费"},
    "hk00700": {"symbol": "00700", "name": "腾讯控股", "market": "HK", "category": "科技"},
    "hk03690": {"symbol": "03690", "name": "美团-W", "market": "HK", "category": "科技"},
    "hk01810": {"symbol": "01810", "name": "小米集团-W", "market": "HK", "category": "科技"},
    "hk01024": {"symbol": "01024", "name": "快手-W", "market": "HK", "category": "科技"},
    "hk09988": {"symbol": "09988", "name": "阿里巴巴-W", "market": "HK", "category": "科技"},
    "hk00883": {"symbol": "00883", "name": "中国海洋石油", "market": "HK", "category": "能源"},
}


def get_a_stock_indicator(symbol):
    """获取A股个股指标"""
    try:
        df = ak.stock_a_lg_indicator(symbol=symbol)
        if df is not None and len(df) > 0:
            latest = df.iloc[-1]
            return {
                "pe_ttm": latest.get("pe_ttm"),
                "pb": latest.get("pb"),
                "ps_ttm": latest.get("ps_ttm"),
                "dv_ratio": latest.get("dv_ratio"),  # 股息率
                "total_mv": latest.get("total_mv"),  # 总市值
            }
    except Exception as e:
        print(f"  获取{symbol}指标失败: {e}")
    return None


def get_hk_stock_indicator(symbol):
    """获取港股个股指标"""
    try:
        df = ak.stock_hk_indicator(symbol=symbol)
        if df is not None and len(df) > 0:
            latest = df.iloc[-1]
            return {
                "pe_ttm": latest.get("pe_ttm"),
                "pb": latest.get("pb"),
                "ps_ttm": latest.get("ps_ttm"),
                "dv_ratio": latest.get("dv_ratio"),
            }
    except Exception as e:
        print(f"  获取{symbol}指标失败: {e}")
    return None


def get_a_stock_realtime(symbol):
    """获取A股实时行情"""
    try:
        df = ak.stock_zh_a_spot_em()
        stock = df[df['代码'] == symbol]
        if len(stock) > 0:
            s = stock.iloc[0]
            return {
                "price": float(s['最新价']),
                "change_pct": float(s['涨跌幅']),
                "volume": float(s['成交量']),
                "amount": float(s['成交额']),
                "high": float(s['最高']),
                "low": float(s['最低']),
            }
    except Exception as e:
        print(f"  获取{symbol}行情失败: {e}")
    return None


def get_hk_stock_realtime(symbol):
    """获取港股实时行情"""
    try:
        df = ak.stock_hk_spot_em()
        stock = df[df['代码'] == symbol]
        if len(stock) > 0:
            s = stock.iloc[0]
            return {
                "price": float(s['最新价']),
                "change_pct": float(s['涨跌幅']),
                "volume": float(s['成交量']) if '成交量' in s else 0,
            }
    except Exception as e:
        print(f"  获取{symbol}行情失败: {e}")
    return None


def main():
    print("=" * 60)
    print(f"股票数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    updated = 0

    for code, info in STOCK_CODES.items():
        symbol = info["symbol"]
        name = info["name"]
        market = info["market"]
        category = info["category"]

        print(f"\n获取 {name} ({code})...")

        indicator = None
        realtime = None

        if market == "A":
            indicator = get_a_stock_indicator(symbol)
            realtime = get_a_stock_realtime(symbol)
        elif market == "HK":
            indicator = get_hk_stock_indicator(symbol)
            realtime = get_hk_stock_realtime(symbol)

        # 保存到数据库
        try:
            pe = indicator.get("pe_ttm") if indicator else None
            pb = indicator.get("pb") if indicator else None
            ps = indicator.get("ps_ttm") if indicator else None
            dv = indicator.get("dv_ratio") if indicator else None

            # 过滤异常值
            if pe and (abs(pe) > 10000 or pe == 0):
                pe = None
            if pb and (abs(pb) > 1000 or pb == 0):
                pb = None

            c.execute('''
                INSERT OR REPLACE INTO stock_financial
                (code, name, report_date, pe_ttm, pb, ps_ttm, dividend_yield)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (code, name, today, pe, pb, ps, dv))

            # 显示结果
            pe_str = f"{pe:.2f}" if pe else "-"
            pb_str = f"{pb:.2f}" if pb else "-"
            price_str = f"{realtime['price']:.2f}" if realtime and realtime.get("price") else "-"
            chg_str = f"{realtime['change_pct']:+.2f}%" if realtime and realtime.get("change_pct") else "-"

            print(f"  价格: {price_str}  涨跌: {chg_str}  PE: {pe_str}  PB: {pb_str}")
            updated += 1

        except Exception as e:
            print(f"  保存失败: {e}")

    conn.commit()
    conn.close()
    print(f"\n{'=' * 60}")
    print(f"更新完成，共更新 {updated} 只股票")


if __name__ == "__main__":
    main()