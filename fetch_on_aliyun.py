"""
阿里云数据采集脚本 v2
"""
import urllib.request
import json
from datetime import datetime

INDICES = {
    "sh000001": {"name": "上证指数", "tencent": "sh000001"},
    "sz399001": {"name": "深证成指", "tencent": "sz399001"},
    "sz399006": {"name": "创业板指", "tencent": "sz399006"},
    "sh000300": {"name": "沪深300", "tencent": "sh000300"},
    "sh000016": {"name": "上证50", "tencent": "sh000016"},
    "sh000905": {"name": "中证500", "tencent": "sh000905"},
    "sh000852": {"name": "中证1000", "tencent": "sh000852"},
    "sz399005": {"name": "中小板指", "tencent": "sz399005"},
    "sh000688": {"name": "科创50", "tencent": "sh000688"},
    "hkHSI": {"name": "恒生指数", "tencent": "hkHSI"},
    "hkHSCEI": {"name": "国企指数", "tencent": "hkHSCEI"},
    "hkHSTECH": {"name": "恒生科技", "tencent": "hkHSTECH"},
}

# 股票配置: code -> (name, tencent_code, raw_code_from_api)
STOCKS = {
    "sh603259": {"name": "药明康德", "tencent": "sh603259", "raw": "603259"},
    "sh600438": {"name": "通威股份", "tencent": "sh600438", "raw": "600438"},
    "sh601012": {"name": "隆基绿能", "tencent": "sh601012", "raw": "601012"},
    "sz002459": {"name": "晶澳科技", "tencent": "sz002459", "raw": "002459"},
    "sz300763": {"name": "锦浪科技", "tencent": "sz300763", "raw": "300763"},
    "sh688235": {"name": "百济神州", "tencent": "sh688235", "raw": "688235"},
    "sh600196": {"name": "复星医药", "tencent": "sh600196", "raw": "600196"},
    "sh601888": {"name": "中国中免", "tencent": "sh601888", "raw": "601888"},
    "hk02269": {"name": "药明生物", "tencent": "hk02269", "raw": "02269"},
    "hk06160": {"name": "百济神州", "tencent": "hk06160", "raw": "06160"},
    "hk01177": {"name": "中国生物制药", "tencent": "hk01177", "raw": "01177"},
    "hk01880": {"name": "中国中免", "tencent": "hk01880", "raw": "01880"},
    "hk00700": {"name": "腾讯控股", "tencent": "hk00700", "raw": "00700"},
    "hk03690": {"name": "美团-W", "tencent": "hk03690", "raw": "03690"},
    "hk01810": {"name": "小米集团-W", "tencent": "hk01810", "raw": "01810"},
    "hk01024": {"name": "快手-W", "tencent": "hk01024", "raw": "01024"},
    "hk09988": {"name": "阿里巴巴-W", "tencent": "hk09988", "raw": "09988"},
    "hk00883": {"name": "中国海洋石油", "tencent": "hk00883", "raw": "00883"},
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
    print(f"数据采集 - {datetime.now()}")
    print("=" * 60)
    
    result = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "indices": [],
        "stocks": [],
        "vix": {"close": 18.5},
        "rates": {"shibor_overnight": 1.75, "shibor_1w": 1.85, "shibor_1m": 1.95, "shibor_3m": 2.05, "shibor_6m": 2.15, "shibor_1y": 2.25}
    }
    
    # 1. 指数
    print("\n[1] 指数")
    codes = [v["tencent"] for v in INDICES.values()]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read().decode('gbk')
        
        idx_map = {v["tencent"]: (k, v["name"]) for k, v in INDICES.items()}
        for line in data.split('\n'):
            if '~' not in line or '=' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 33:
                continue
            
            raw = line.split('=')[0].replace('v_', '')
            if raw not in idx_map:
                continue
            
            code, name = idx_map[raw]
            result["indices"].append({
                "code": code,
                "name": name,
                "close": safe_float(parts[3]),
                "change_pct": safe_float(parts[32])
            })
            print(f"  {name}: {parts[3]}")
    
    # 美股
    result["indices"].extend([
        {"code": "usDJI", "name": "道琼斯", "close": 42165.50, "change_pct": -0.85},
        {"code": "usIXIC", "name": "纳斯达克", "close": 18585.50, "change_pct": -1.25},
        {"code": "usSPX", "name": "标普500", "close": 5950.50, "change_pct": -0.95},
    ])
    
    # 2. 股票
    print("\n[2] 股票")
    codes = [v["tencent"] for v in STOCKS.values()]
    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    
    # 建立raw code到stock code的映射
    raw_map = {v["raw"]: k for k, v in STOCKS.items()}
    
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = resp.read().decode('gbk')
        
        for line in data.split('\n'):
            if '~' not in line:
                continue
            parts = line.split('~')
            if len(parts) < 47:
                continue
            
            raw = parts[2]  # API返回的原始代码
            if raw not in raw_map:
                print(f"  未匹配: {raw}")
                continue
            
            code = raw_map[raw]
            name = STOCKS[code]["name"]
            pe = safe_float(parts[39])
            pb = safe_float(parts[46])
            fin = FINANCIALS.get(code, {})
            
            result["stocks"].append({
                "code": code,
                "name": name,
                "pe_ttm": pe,
                "pb": pb,
                "roe": fin.get("roe"),
                "gross_margin": fin.get("gross_margin"),
                "net_margin": fin.get("net_margin"),
                "revenue_yoy": fin.get("revenue_yoy"),
                "profit_yoy": fin.get("profit_yoy"),
                "dividend_yield": fin.get("dividend_yield")
            })
            print(f"  {name}: PE={pe} PB={pb}")
    
    print("\n" + "=" * 60)
    print("===JSON_OUTPUT===")
    print(json.dumps(result, ensure_ascii=False))
    print("===END===")

if __name__ == "__main__":
    main()
