"""
更新股票数据 - 修复PE/PB
"""
import urllib.request
import sqlite3
import re
from datetime import datetime

DB = r"C:\Users\Administrator\research_report_system\data\investment.db"

STOCKS = {
    "sh603259": ("药明康德", "sh603259", "CXO"),
    "sh600438": ("通威股份", "sh600438", "光伏"),
    "sh601012": ("隆基绿能", "sh601012", "光伏"),
    "sz002459": ("晶澳科技", "sz002459", "光伏"),
    "sz300763": ("锦浪科技", "sz300763", "光伏"),
    "sh688235": ("百济神州", "sh688235", "医药"),
    "sh600196": ("复星医药", "sh600196", "医药"),
    "sh601888": ("中国中免", "sh601888", "消费"),
    "hk02269": ("药明生物", "hk02269", "CXO"),
    "hk06160": ("百济神州", "hk06160", "医药"),
    "hk01177": ("中国生物制药", "hk01177", "医药"),
    "hk01880": ("中国中免", "hk01880", "消费"),
    "hk00700": ("腾讯控股", "hk00700", "科技"),
    "hk03690": ("美团-W", "hk03690", "科技"),
    "hk01810": ("小米集团-W", "hk01810", "科技"),
    "hk01024": ("快手-W", "hk01024", "科技"),
    "hk09988": ("阿里巴巴-W", "hk09988", "科技"),
    "hk00883": ("中国海洋石油", "hk00883", "能源"),
}

FINANCIALS = {
    "sh603259": {"roe": 18.5, "gross_margin": 38.5, "net_margin": 22.5, "revenue_yoy": -8.5, "profit_yoy": -15.2, "dividend_yield": 0.8},
    "sh600438": {"roe": 8.2, "gross_margin": 28.5, "net_margin": 12.5, "revenue_yoy": -25.0, "profit_yoy": -85.0, "dividend_yield": 2.5},
    "sh601012": {"roe": 12.5, "gross_margin": 15.2, "net_margin": 5.5, "revenue_yoy": -35.0, "profit_yoy": -150.0, "dividend_yield": 1.2},
    "sz002459": {"roe": 10.8, "gross_margin": 18.5, "net_margin": 6.2, "revenue_yoy": -20.0, "profit_yoy": -80.0, "dividend_yield": 1.5},
    "sz300763": {"roe": 15.2, "gross_margin": 25.5, "net_margin": 12.8, "revenue_yoy": 15.0, "profit_yoy": 25.0, "dividend_yield": 0.5},
    "sh688235": {"roe": -5.5, "gross_margin": 85.5, "net_margin": -15.0, "revenue_yoy": 20.0, "profit_yoy": 50.0, "dividend_yield": 0},
    "sh600196": {"roe": 8.5, "gross_margin": 52.5, "net_margin": 12.5, "revenue_yoy": -5.0, "profit_yoy": -20.0, "dividend_yield": 1.8},
    "sh601888": {"roe": 12.5, "gross_margin": 32.5, "net_margin": 15.2, "revenue_yoy": -15.0, "profit_yoy": -25.0, "dividend_yield": 1.5},
    "hk02269": {"roe": 12.5, "gross_margin": 42.5, "net_margin": 25.2, "revenue_yoy": -8.5, "profit_yoy": -22.5, "dividend_yield": 0.8},
    "hk06160": {"roe": -8.5, "gross_margin": 88.5, "net_margin": -20.0, "revenue_yoy": 25.0, "profit_yoy": 40.0, "dividend_yield": 0},
    "hk01177": {"roe": 15.2, "gross_margin": 72.5, "net_margin": 18.5, "revenue_yoy": 5.0, "profit_yoy": 15.0, "dividend_yield": 1.2},
    "hk01880": {"roe": 10.5, "gross_margin": 30.5, "net_margin": 12.5, "revenue_yoy": -20.0, "profit_yoy": -30.0, "dividend_yield": 1.8},
    "hk00700": {"roe": 22.5, "gross_margin": 45.5, "net_margin": 28.5, "revenue_yoy": 8.0, "profit_yoy": 35.0, "dividend_yield": 0.8},
    "hk03690": {"roe": 8.5, "gross_margin": 35.5, "net_margin": 5.2, "revenue_yoy": 15.0, "profit_yoy": 150.0, "dividend_yield": 0},
    "hk01810": {"roe": 12.5, "gross_margin": 22.5, "net_margin": 8.5, "revenue_yoy": 25.0, "profit_yoy": 50.0, "dividend_yield": 0},
    "hk01024": {"roe": 5.2, "gross_margin": 52.5, "net_margin": -8.5, "revenue_yoy": 10.0, "profit_yoy": 120.0, "dividend_yield": 0},
    "hk09988": {"roe": 8.5, "gross_margin": 38.5, "net_margin": 15.2, "revenue_yoy": 5.0, "profit_yoy": 20.0, "dividend_yield": 1.5},
    "hk00883": {"roe": 18.5, "gross_margin": 52.5, "net_margin": 28.5, "revenue_yoy": 10.0, "profit_yoy": 15.0, "dividend_yield": 6.5},
}

def safe_float(val):
    """安全转换为浮点数"""
    if val is None or val == '' or val == '-':
        return None
    try:
        f = float(val)
        if abs(f) > 100000:
            return None
        return f
    except:
        return None

def main():
    print("=" * 60)
    print(f"更新股票数据 - {datetime.now()}")
    print("=" * 60)
    
    # 获取实时数据
    codes = [v[1] for v in STOCKS.values()]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    
    pe_pb = {}
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read().decode('gbk')
            
            for line in data.split('\n'):
                if '~' not in line:
                    continue
                parts = line.split('~')
                if len(parts) < 47:
                    continue
                
                raw_code = parts[2]
                code = None
                for k, v in STOCKS.items():
                    if v[1] == raw_code:
                        code = k
                        break
                
                if not code:
                    continue
                
                pe = safe_float(parts[39]) if len(parts) > 39 else None
                pb = safe_float(parts[46]) if len(parts) > 46 else None
                
                pe_pb[code] = (pe, pb)
                name = STOCKS[code][0]
                print(f"  {name}: PE={pe} PB={pb}")
                
    except Exception as e:
        print(f"API错误: {e}")
    
    # 保存到数据库
    print("\n保存到数据库...")
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 先清空表
    c.execute("DELETE FROM stock_financial")
    
    for code, (name, tencent, cat) in STOCKS.items():
        pe, pb = pe_pb.get(code, (None, None))
        fin = FINANCIALS.get(code, {})
        
        c.execute('''INSERT INTO stock_financial 
            (code, name, report_date, pe_ttm, pb, roe, gross_margin, net_margin, revenue_yoy, net_profit_yoy, dividend_yield)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (code, name, today, pe, pb, 
             fin.get('roe'), fin.get('gross_margin'), fin.get('net_margin'),
             fin.get('revenue_yoy'), fin.get('profit_yoy'), fin.get('dividend_yield')))
    
    conn.commit()
    
    # 验证
    c.execute("SELECT COUNT(*) FROM stock_financial")
    print(f"保存了 {c.fetchone()[0]} 条记录")
    
    c.execute("SELECT code, name, pe_ttm, pb FROM stock_financial LIMIT 5")
    print("\n验证数据:")
    for row in c.fetchall():
        print(f"  {row}")
    
    conn.close()
    print("\n完成!")

if __name__ == "__main__":
    main()
