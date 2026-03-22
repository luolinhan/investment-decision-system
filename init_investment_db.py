"""
投资数据库设计和初始化
包含：指数历史、股票行情、财务指标、估值百分位、利率等
"""
import sqlite3
import os
from datetime import date

DB_PATH = "data/investment.db"

def init_database():
    """初始化投资数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. 指数历史数据表
    c.execute('''
        CREATE TABLE IF NOT EXISTS index_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            change_pct REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_index_code ON index_history(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_index_date ON index_history(trade_date)')

    # 2. 股票基本信息表
    c.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            market TEXT,
            industry TEXT,
            list_date TEXT,
            total_shares REAL,
            float_shares REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 3. 股票日线行情表
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL NOT NULL,
            volume REAL,
            amount REAL,
            turnover_rate REAL,
            change_pct REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_stock_daily_code ON stock_daily(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily(trade_date)')

    # 4. 股票财务指标表
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock_financial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            report_date TEXT NOT NULL,
            report_type TEXT,
            roe REAL,
            roa REAL,
            gross_margin REAL,
            net_margin REAL,
            debt_ratio REAL,
            current_ratio REAL,
            quick_ratio REAL,
            eps REAL,
            bvps REAL,
            pe_ttm REAL,
            pb REAL,
            ps_ttm REAL,
            total_revenue REAL,
            net_profit REAL,
            net_profit_yoy REAL,
            revenue_yoy REAL,
            operating_cash_flow REAL,
            free_cash_flow REAL,
            dividend_yield REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, report_date, report_type)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_financial_code ON stock_financial(code)')

    # 5. 估值百分位表
    c.execute('''
        CREATE TABLE IF NOT EXISTS valuation_percentile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pe_ttm REAL,
            pe_percentile_3y REAL,
            pe_percentile_5y REAL,
            pe_percentile_10y REAL,
            pb REAL,
            pb_percentile_3y REAL,
            pb_percentile_5y REAL,
            pb_percentile_10y REAL,
            ps_ttm REAL,
            ps_percentile_3y REAL,
            dividend_yield REAL,
            dy_percentile_3y REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')

    # 6. 利率数据表
    c.execute('''
        CREATE TABLE IF NOT EXISTS interest_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            shibor_overnight REAL,
            shibor_1w REAL,
            shibor_1m REAL,
            shibor_3m REAL,
            shibor_6m REAL,
            shibor_1y REAL,
            hibor_overnight REAL,
            hibor_1w REAL,
            hibor_1m REAL,
            hibor_3m REAL,
            libor_usd_3m REAL,
            china_10y_bond_yield REAL,
            us_10y_bond_yield REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 7. VIX恐慌指数历史表
    c.execute('''
        CREATE TABLE IF NOT EXISTS vix_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            vix_open REAL,
            vix_high REAL,
            vix_low REAL,
            vix_close REAL NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 8. 市场情绪数据表
    c.execute('''
        CREATE TABLE IF NOT EXISTS market_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            up_count INTEGER,
            down_count INTEGER,
            flat_count INTEGER,
            limit_up_count INTEGER,
            limit_down_count INTEGER,
            new_high_count INTEGER,
            new_low_count INTEGER,
            avg_turnover REAL,
            total_amount REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 9. 北向资金表
    c.execute('''
        CREATE TABLE IF NOT EXISTS north_money (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT UNIQUE NOT NULL,
            sh_net_inflow REAL,
            sz_net_inflow REAL,
            total_net_inflow REAL,
            sh_accumulated REAL,
            sz_accumulated REAL,
            total_accumulated REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 10. 行业指数表
    c.execute('''
        CREATE TABLE IF NOT EXISTS industry_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT,
            trade_date TEXT NOT NULL,
            close REAL NOT NULL,
            change_pct REAL,
            turnover REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')

    # 11. 关注股票列表
    c.execute('''
        CREATE TABLE IF NOT EXISTS watch_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            market TEXT,
            category TEXT,
            weight REAL DEFAULT 1.0,
            notes TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 12. 估值水位表 (PE/PB分位数)
    c.execute('''
        CREATE TABLE IF NOT EXISTS valuation_bands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT,
            trade_date TEXT NOT NULL,
            pe_ttm REAL,
            pe_percentile_3y REAL,
            pe_percentile_5y REAL,
            pe_percentile_10y REAL,
            pb REAL,
            pb_percentile_3y REAL,
            pb_percentile_5y REAL,
            pb_percentile_10y REAL,
            ps_ttm REAL,
            ps_percentile_3y REAL,
            dividend_yield REAL,
            dy_percentile_3y REAL,
            valuation_level TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_valuation_code ON valuation_bands(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_valuation_date ON valuation_bands(trade_date)')

    # 13. 技术指标表 (均线、ATR、Beta)
    c.execute('''
        CREATE TABLE IF NOT EXISTS technical_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT,
            trade_date TEXT NOT NULL,
            ma5 REAL,
            ma10 REAL,
            ma20 REAL,
            ma50 REAL,
            ma200 REAL,
            ema12 REAL,
            ema26 REAL,
            macd REAL,
            macd_signal REAL,
            macd_hist REAL,
            rsi_14 REAL,
            atr_14 REAL,
            atr_pct REAL,
            beta_1y REAL,
            beta_3y REAL,
            volatility_30d REAL,
            volatility_90d REAL,
            trend_signal TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_technical_code ON technical_indicators(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_technical_date ON technical_indicators(trade_date)')

    # 14. TMT行业指标表
    c.execute('''
        CREATE TABLE IF NOT EXISTS sector_tmt (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            report_date TEXT NOT NULL,
            mau REAL,
            dau REAL,
            arpu REAL,
            arppu REAL,
            paying_ratio REAL,
            retention_d1 REAL,
            retention_d7 REAL,
            retention_d30 REAL,
            revenue REAL,
            revenue_yoy REAL,
            revenue_qoq REAL,
            net_profit REAL,
            net_profit_yoy REAL,
            gross_margin REAL,
            operating_margin REAL,
            rd_ratio REAL,
            sales_ratio REAL,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, report_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tmt_code ON sector_tmt(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tmt_date ON sector_tmt(report_date)')

    # 15. 创新药管线表
    c.execute('''
        CREATE TABLE IF NOT EXISTS sector_biotech (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_code TEXT NOT NULL,
            company_name TEXT,
            drug_name TEXT NOT NULL,
            drug_type TEXT,
            indication TEXT,
            phase TEXT,
            phase_cn TEXT,
            start_date TEXT,
            expected_approval TEXT,
            status TEXT,
            region TEXT,
            partner TEXT,
            market_size_est REAL,
            notes TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_code, drug_name, indication)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_biotech_company ON sector_biotech(company_code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_biotech_phase ON sector_biotech(phase)')

    # 16. 消费行业指标表
    c.execute('''
        CREATE TABLE IF NOT EXISTS sector_consumer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            report_date TEXT NOT NULL,
            revenue REAL,
            revenue_yoy REAL,
            same_store_sales_yoy REAL,
            store_count INTEGER,
            store_change INTEGER,
            online_ratio REAL,
            gross_margin REAL,
            operating_margin REAL,
            inventory_turnover REAL,
            accounts_receivable_days REAL,
            marketing_ratio REAL,
            member_count REAL,
            member_growth_yoy REAL,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, report_date)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_consumer_code ON sector_consumer(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_consumer_date ON sector_consumer(report_date)')

    # 17. 数据采集日志表
    c.execute('''
        CREATE TABLE IF NOT EXISTS etl_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            job_type TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT,
            records_processed INTEGER,
            records_failed INTEGER,
            error_message TEXT,
            details TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_etl_job ON etl_logs(job_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_etl_time ON etl_logs(start_time)')

    conn.commit()
    conn.close()
    print("投资数据库初始化完成")


def init_watch_list():
    """初始化关注股票列表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    stocks = [
        # A股光伏
        ("sz002459", "晶澳科技", "A", "光伏"),
        ("sh600438", "通威股份", "A", "光伏"),
        ("sh601012", "隆基绿能", "A", "光伏"),
        ("sz300763", "锦浪科技", "A", "光伏"),

        # A股医药
        ("sh688235", "百济神州", "A", "医药"),
        ("sh603259", "药明康德", "A", "CXO"),
        ("sh600196", "复星医药", "A", "医药"),

        # A股消费
        ("sh601888", "中国中免", "A", "消费"),

        # 港股科技
        ("hk09988", "阿里巴巴-W", "HK", "科技"),
        ("hk00700", "腾讯控股", "HK", "科技"),
        ("hk03690", "美团-W", "HK", "科技"),
        ("hk01810", "小米集团-W", "HK", "科技"),
        ("hk01024", "快手-W", "HK", "科技"),

        # 港股医药
        ("hk06160", "百济神州", "HK", "医药"),
        ("hk02269", "药明生物", "HK", "CXO"),
        ("hk01177", "中国生物制药", "HK", "医药"),

        # 港股其他
        ("hk00883", "中国海洋石油", "HK", "能源"),
        ("hk01880", "中国中免", "HK", "消费"),

        # 美股
        ("usBABA", "阿里巴巴", "US", "科技"),
        ("usTCEHY", "腾讯ADR", "US", "科技"),
    ]

    for code, name, market, category in stocks:
        c.execute('''
            INSERT OR IGNORE INTO watch_list (code, name, market, category)
            VALUES (?, ?, ?, ?)
        ''', (code, name, market, category))

    conn.commit()
    conn.close()
    print(f"已添加 {len(stocks)} 只关注股票")


if __name__ == "__main__":
    init_database()
    init_watch_list()

    # 打印表结构
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = c.fetchall()
    print("\n数据库表:")
    for t in tables:
        c.execute(f"SELECT COUNT(*) FROM {t[0]}")
        count = c.fetchone()[0]
        print(f"  {t[0]}: {count} 条记录")
    conn.close()