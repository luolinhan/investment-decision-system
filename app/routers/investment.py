"""
投资决策数据API路由
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Dict
from app.services.investment_data import InvestmentDataService
from app.services.investment_db_service import InvestmentDataService as DbService
from app.services.financial_news import FinancialNewsService
import sqlite3

router = APIRouter(prefix="/investment", tags=["investment"])
templates = Jinja2Templates(directory="templates")

DB_PATH = "data/investment.db"

# 全局服务实例
_investment_service = None
_db_service = None
_news_service = None

def get_investment_service():
    global _investment_service
    if _investment_service is None:
        _investment_service = InvestmentDataService()
    return _investment_service

def get_db_service():
    global _db_service
    if _db_service is None:
        _db_service = DbService()
    return _db_service

def get_news_service():
    global _news_service
    if _news_service is None:
        _news_service = FinancialNewsService()
    return _news_service


@router.get("/", response_class=HTMLResponse)
async def investment_dashboard(request: Request):
    """投资决策仪表板页面"""
    return templates.TemplateResponse("investment.html", {"request": request})


@router.get("/api/overview")
async def get_market_overview():
    """获取市场概览数据 - 优先使用本地数据库"""
    db = get_db_service()
    realtime = get_investment_service()

    # 从本地数据库获取指数数据
    indices = db.get_all_indices_latest()

    # 从本地数据库获取利率
    rates = db.get_interest_rates_latest()

    # 从本地数据库获取市场情绪
    sentiment = db.get_market_sentiment_latest()

    # 从本地数据库获取VIX
    vix = db.get_vix_latest()

    # 实时获取关注股票行情（这个需要实时API）
    watch_stocks = realtime.get_watch_stocks()

    return {
        "update_time": __import__('datetime').datetime.now().isoformat(),
        "indices": indices,
        "rates": rates,
        "sentiment": sentiment,
        "vix": vix,
        "watch_stocks": watch_stocks
    }


@router.get("/api/watch-stocks")
async def get_watch_stocks():
    """获取关注股票行情"""
    service = get_investment_service()
    return service.get_watch_stocks()


@router.get("/api/index-history/{symbol}")
async def get_index_history(symbol: str, days: int = 365):
    """获取指数历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_index_history(symbol, days)
    return {"symbol": symbol, "data": data}


@router.get("/api/interest-rates")
async def get_interest_rates(days: int = 365):
    """获取利率历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_interest_rates(days)
    return {"data": data}


@router.get("/api/north-money")
async def get_north_money(days: int = 180):
    """获取北向资金数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_north_money(days)
    return {"data": data}


@router.get("/api/vix-history")
async def get_vix_history(days: int = 365):
    """获取VIX历史数据 - 从本地数据库"""
    db = get_db_service()
    data = db.get_vix_history(days)
    return {"data": data}


@router.get("/api/a-stocks")
async def get_a_stocks(keywords: str = None):
    """获取A股行情"""
    service = get_investment_service()
    kw_list = keywords.split(",") if keywords else None
    return service.get_a_stocks_direct(kw_list)


@router.get("/api/hk-stocks")
async def get_hk_stocks(keywords: str = None):
    """获取港股行情"""
    service = get_investment_service()
    kw_list = keywords.split(",") if keywords else None
    return service.get_hk_stocks_direct(kw_list)


@router.post("/api/import-index-data")
async def import_index_data(data: Dict):
    """导入指数数据 - 从阿里云服务器接收"""
    code = data.get("code")
    name = data.get("name")
    records = data.get("data", [])

    if not code or not records:
        return {"status": "error", "message": "缺少数据"}

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        added = 0
        for record in records:
            try:
                c.execute('''
                    INSERT OR REPLACE INTO index_history
                    (code, name, trade_date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code, name, record.get("date"),
                    float(record.get("open", 0) or 0),
                    float(record.get("high", 0) or 0),
                    float(record.get("low", 0) or 0),
                    float(record.get("close", 0) or 0),
                    float(record.get("volume", 0) or 0)
                ))
                added += 1
            except Exception as ex:
                continue

        conn.commit()
        conn.close()

        return {"status": "success", "code": code, "name": name, "added": added}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/api/news")
async def get_financial_news():
    """获取金融新闻"""
    service = get_news_service()
    return service.get_all_news()


# ==================== 宏观流动性模块 ====================

@router.get("/api/macro/overview")
async def get_macro_overview():
    """获取宏观流动性概览"""
    db = get_db_service()
    rates = db.get_interest_rates_latest()
    vix = db.get_vix_latest()
    sentiment = db.get_market_sentiment_latest()
    north_money = db.get_north_money(30)

    # 计算北向资金趋势
    trend = "neutral"
    if len(north_money) >= 5:
        recent = sum(n.get("total_inflow", 0) or 0 for n in north_money[-5:])
        if recent > 100:
            trend = "strong_inflow"
        elif recent > 0:
            trend = "inflow"
        elif recent < -100:
            trend = "strong_outflow"
        elif recent < 0:
            trend = "outflow"

    return {
        "rates": rates,
        "vix": vix,
        "sentiment": sentiment,
        "north_money_trend": trend,
        "north_money_5d": sum(n.get("total_inflow", 0) or 0 for n in north_money[-5:]) if north_money else 0
    }


@router.get("/api/macro/rates-history")
async def get_rates_history(days: int = 365):
    """获取利率历史"""
    db = get_db_service()
    data = db.get_interest_rates(days)
    return {"data": data}


@router.get("/api/macro/liquidity-indicators")
async def get_liquidity_indicators():
    """获取流动性指标"""
    db = get_db_service()
    rates = db.get_interest_rates_latest()

    indicators = []
    if rates and rates.get("shibor"):
        shibor = rates["shibor"]
        # 隔夜与7天利差 - 反映短期流动性紧张程度
        spread_1w = (shibor.get("1w") or 0) - (shibor.get("overnight") or 0)

        indicators.append({
            "name": "SHIBOR隔夜-1W利差",
            "value": spread_1w,
            "level": "tight" if spread_1w > 0.5 else "loose" if spread_1w < 0.2 else "normal"
        })

        # 1月与3月利差 - 反映中期预期
        spread_1m_3m = (shibor.get("3m") or 0) - (shibor.get("1m") or 0)
        indicators.append({
            "name": "SHIBOR 1M-3M利差",
            "value": spread_1m_3m,
            "level": "tight" if spread_1m_3m > 0.3 else "normal"
        })

    return {"indicators": indicators}


# ==================== 微观基本面模块 ====================

@router.get("/api/fundamentals/stocks")
async def get_stocks_fundamentals(codes: str = None):
    """获取股票基本面数据"""
    db = get_db_service()
    code_list = codes.split(",") if codes else None

    # TODO: 从stock_financial表获取数据
    return {"stocks": [], "message": "待实现"}


@router.get("/api/fundamentals/financial/{code}")
async def get_stock_financial(code: str):
    """获取单只股票财务数据"""
    # TODO: 实现
    return {"code": code, "data": []}


# ==================== 行业模型模块 ====================

@router.get("/api/sector/tmt")
async def get_tmt_sector(code: str = None):
    """获取TMT行业数据"""
    db = get_db_service()
    data = db.get_tmt_metrics(code)
    return {"data": data}


@router.get("/api/sector/biotech")
async def get_biotech_sector(company: str = None, phase: str = None):
    """获取创新药管线数据"""
    db = get_db_service()
    data = db.get_biotech_pipeline(company, phase)
    return {"data": data}


@router.get("/api/sector/consumer")
async def get_consumer_sector(code: str = None):
    """获取消费行业数据"""
    db = get_db_service()
    data = db.get_consumer_metrics(code)
    return {"data": data}


@router.get("/api/sector/overview")
async def get_sector_overview():
    """获取行业概览"""
    db = get_db_service()
    return {
        "tmt_count": len(db.get_tmt_metrics()),
        "biotech_count": len(db.get_biotech_pipeline()),
        "consumer_count": len(db.get_consumer_metrics())
    }


# ==================== 量化技术模块 ====================

@router.get("/api/quant/valuation")
async def get_valuation(code: str = None):
    """获取估值水位数据"""
    db = get_db_service()
    data = db.get_valuation_latest(code)
    return {"data": data}


@router.get("/api/quant/valuation-history/{code}")
async def get_valuation_history(code: str, days: int = 365):
    """获取估值历史"""
    db = get_db_service()
    data = db.get_valuation_history(code, days)
    return {"code": code, "data": data}


@router.get("/api/quant/technical")
async def get_technical(code: str = None):
    """获取技术指标"""
    db = get_db_service()
    data = db.get_technical_latest(code)
    return {"data": data}


@router.get("/api/quant/technical-history/{code}")
async def get_technical_history(code: str, days: int = 365):
    """获取技术指标历史"""
    db = get_db_service()
    data = db.get_technical_history(code, days)
    return {"code": code, "data": data}


@router.get("/api/quant/screener")
async def stock_screener(
    pe_max: float = None,
    pe_min: float = None,
    pb_max: float = None,
    rsi_max: float = None,
    rsi_min: float = None
):
    """股票筛选器"""
    db = get_db_service()
    valuation = db.get_valuation_latest()
    technical = db.get_technical_latest()

    # 合并数据
    result = []
    tech_dict = {t["code"]: t for t in technical}
    for v in valuation:
        item = {**v}
        if v["code"] in tech_dict:
            item.update(tech_dict[v["code"]])

        # 应用筛选条件
        if pe_max and (item.get("pe_ttm") or 999) > pe_max:
            continue
        if pe_min and (item.get("pe_ttm") or 0) < pe_min:
            continue
        if pb_max and (item.get("pb") or 999) > pb_max:
            continue
        if rsi_max and (item.get("rsi_14") or 999) > rsi_max:
            continue
        if rsi_min and (item.get("rsi_14") or 0) < rsi_min:
            continue

        result.append(item)

    return {"results": result, "count": len(result)}


# ==================== 数据管理模块 ====================

@router.get("/api/etl/logs")
async def get_etl_logs(limit: int = 50, job_type: str = None):
    """获取ETL日志"""
    db = get_db_service()
    data = db.get_etl_logs(limit, job_type)
    return {"data": data}


@router.post("/api/etl/import-csv")
async def import_csv(data: Dict):
    """导入CSV数据"""
    db = get_db_service()
    table_name = data.get("table")
    csv_path = data.get("path")

    if not table_name or not csv_path:
        return {"status": "error", "message": "缺少表名或文件路径"}

    result = db.import_csv_to_table(table_name, csv_path)
    return result


@router.get("/api/etl/tables")
async def get_db_tables():
    """获取数据库表列表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall()]

    result = []
    for table in tables:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        count = c.fetchone()[0]
        result.append({"name": table, "count": count})

    conn.close()
    return {"tables": result}


@router.get("/api/etl/status")
async def get_etl_status():
    """获取ETL状态"""
    db = get_db_service()
    logs = db.get_etl_logs(10)

    last_success = None
    last_error = None
    for log in logs:
        if log["status"] == "success" and not last_success:
            last_success = log
        if log["status"] == "error" and not last_error:
            last_error = log

    return {
        "recent_logs": logs[:5],
        "last_success": last_success,
        "last_error": last_error
    }