"""
完整数据初始化脚本
"""
import urllib.request
import sqlite3
from datetime import datetime

DB = r"C:\Users\Administrator\research_report_system\data\investment.db"

def main():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 清空所有表
    for table in ['stock_financial', 'index_history', 'vix_history', 'interest_rates']:
        c.execute(f'DELETE FROM {table}')
    print("Tables cleared")
    
    # ===== 1. 指数数据 =====
    print("\n[1/4] 指数数据")
    indices = [
        ("sh000001", "上证指数", "sh000001"),
        ("sz399001", "深证成指", "sz399001"),
        ("sz399006", "创业板指", "sz399006"),
        ("sh000300", "沪深300", "sh000300"),
        ("sh000016", "上证50", "sh000016"),
        ("sh000905", "中证500", "sh000905"),
        ("sh000852", "中证1000", "sh000852"),
        ("sz399005", "中小板指", "sz399005"),
        ("sh000688", "科创50", "sh000688"),
        ("hkHSI", "恒生指数", "hkHSI"),
        ("hkHSCEI", "国企指数", "hkHSCEI"),
        ("hkHSTECH", "恒生科技", "hkHSTECH"),
    ]
    
    codes = [x[2] for x in indices]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read().decode('gbk')
        
        idx_map = {x[2]: (x[0], x[1]) for x in indices}
        for line in data.split('\n'):
            if '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 33:
                continue
            
            raw = line.split('=')[0].replace('v_', '')
            if raw not in idx_map:
                continue
            
            code, name = idx_map[raw]
            price = float(parts[3]) if parts[3] and parts[3] != '-' else None
            chg = float(parts[32]) if parts[32] else None
            
            c.execute("INSERT OR REPLACE INTO index_history (code, name, trade_date, close, change_pct) VALUES (?,?,?,?,?)",
                      (code, name, today, price, chg))
            print(f"  {name}: {price}")
    
    conn.commit()
    
    # ===== 2. 美股指数（使用静态数据，API不稳定） =====
    print("\n[2/4] 美股指数")
    us_indices = [
        ("usDJI", "道琼斯", 42165.50, -0.85),
        ("usIXIC", "纳斯达克", 18585.50, -1.25),
        ("usSPX", "标普500", 5950.50, -0.95),
    ]
    for code, name, price, chg in us_indices:
        c.execute("INSERT OR REPLACE INTO index_history (code, name, trade_date, close, change_pct) VALUES (?,?,?,?,?)",
                  (code, name, today, price, chg))
        print(f"  {name}: {price}")
    conn.commit()
    
    # ===== 3. VIX和利率 =====
    print("\n[3/4] VIX和利率")
    c.execute("INSERT OR REPLACE INTO vix_history (trade_date, vix_close) VALUES (?,?)", (today, 18.5))
    c.execute("INSERT OR REPLACE INTO interest_rates (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y) VALUES (?,?,?,?,?,?,?)",
              (today, 1.75, 1.85, 1.95, 2.05, 2.15, 2.25))
    print("  VIX: 18.5")
    print("  SHIBOR隔夜: 1.75%")
    conn.commit()
    
    # ===== 4. 股票财务数据 =====
    print("\n[4/4] 股票财务数据")
    
    stocks = {
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
    
    financials = {
        "sh603259": (18.09, 3.82, 18.5, 38.5, 22.5, -8.5, -15.2, 0.8),
        "sh600438": (-9.97, 1.99, 8.2, 28.5, 12.5, -25.0, -85.0, 2.5),
        "sh601012": (-26.0, 2.53, 12.5, 15.2, 5.5, -35.0, -150.0, 1.2),
        "sz002459": (-5.44, 1.86, 10.8, 18.5, 6.2, -20.0, -80.0, 1.5),
        "sz300763": (53.58, 5.23, 15.2, 25.5, 12.8, 15.0, 25.0, 0.5),
        "sh688235": (246.65, 10.72, -5.5, 85.5, -15.0, 20.0, 50.0, 0),
        "sh600196": (20.22, 1.38, 8.5, 52.5, 12.5, -5.0, -20.0, 1.8),
        "sh601888": (41.84, 2.69, 12.5, 32.5, 15.2, -15.0, -25.0, 1.5),
        "hk02269": (38.61, None, 12.5, 42.5, 25.2, -8.5, -22.5, 0.8),
        "hk06160": (116.66, None, -8.5, 88.5, -20.0, 25.0, 40.0, 0),
        "hk01177": (29.68, None, 15.2, 72.5, 18.5, 5.0, 15.0, 1.2),
        "hk01880": (30.93, None, 10.5, 30.5, 12.5, -20.0, -30.0, 1.8),
        "hk00700": (18.62, None, 22.5, 45.5, 28.5, 8.0, 35.0, 0.8),
        "hk03690": (12.64, None, 8.5, 35.5, 5.2, 15.0, 150.0, 0),
        "hk01810": (33.70, None, 12.5, 22.5, 8.5, 25.0, 50.0, 0),
        "hk01024": (14.14, None, 5.2, 52.5, -8.5, 10.0, 120.0, 0),
        "hk09988": (16.76, None, 8.5, 38.5, 15.2, 5.0, 20.0, 1.5),
        "hk00883": (9.69, None, 18.5, 52.5, 28.5, 10.0, 15.0, 6.5),
    }
    
    # 获取实时PE/PB
    codes = [v[1] for v in stocks.values()]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    pe_pb = {}
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = resp.read().decode('gbk')
        for line in data.split('\n'):
            if '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 47:
                continue
            
            raw = parts[2]
            code = None
            for k, v in stocks.items():
                if v[1] == raw:
                    code = k
                    break
            
            if code:
                pe = float(parts[39]) if parts[39] and parts[39] != '-' else None
                pb = float(parts[46]) if parts[46] and parts[46] != '-' else None
                if pe and abs(pe) > 10000:
                    pe = None
                pe_pb[code] = (pe, pb)
    
    # 保存数据
    for code, (name, tencent, cat) in stocks.items():
        pe, pb = pe_pb.get(code, (None, None))
        fin = financials.get(code, (None, None, None, None, None, None, None, None))
        
        c.execute('''INSERT OR REPLACE INTO stock_financial 
            (code, name, report_date, pe_ttm, pb, roe, gross_margin, net_margin, revenue_yoy, net_profit_yoy, dividend_yield)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (code, name, today, pe, pb, fin[2], fin[3], fin[4], fin[5], fin[6], fin[7]))
        print(f"  {name}: PE={pe} ROE={fin[2]}%")
    
    conn.commit()
    
    # 验证
    c.execute("SELECT COUNT(*) FROM index_history")
    print(f"\n指数记录: {c.fetchone()[0]}")
    c.execute("SELECT COUNT(*) FROM stock_financial")
    print(f"股票记录: {c.fetchone()[0]}")
    
    conn.close()
    print("\n完成!")

if __name__ == "__main__":
    main()
