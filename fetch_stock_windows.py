"""
Windows服务器股票数据采集脚本
使用腾讯API获取实时数据
"""
import urllib.request
import json
import sqlite3
import sys
from datetime import datetime

# 数据库路径 (Windows)
DB_PATH = r"C:\Users\Administrator\research_report_system\data\investment.db"

# 关注的股票配置
STOCKS = {
    # A股
    "sh603259": {"name": "药明康德", "market": "A", "category": "CXO"},
    "sh600438": {"name": "通威股份", "market": "A", "category": "光伏"},
    "sh601012": {"name": "隆基绿能", "market": "A", "category": "光伏"},
    "sz002459": {"name": "晶澳科技", "market": "A", "category": "光伏"},
    "sz300763": {"name": "锦浪科技", "market": "A", "category": "光伏"},
    "sh688235": {"name": "百济神州", "market": "A", "category": "医药"},
    "sh600196": {"name": "复星医药", "market": "A", "category": "医药"},
    "sh601888": {"name": "中国中免", "market": "A", "category": "消费"},
    # 港股
    "hk02269": {"name": "药明生物", "market": "HK", "category": "CXO"},
    "hk06160": {"name": "百济神州", "market": "HK", "category": "医药"},
    "hk01177": {"name": "中国生物制药", "market": "HK", "category": "医药"},
    "hk01880": {"name": "中国中免", "market": "HK", "category": "消费"},
    "hk00700": {"name": "腾讯控股", "market": "HK", "category": "科技"},
    "hk03690": {"name": "美团-W", "market": "HK", "category": "科技"},
    "hk01810": {"name": "小米集团-W", "market": "HK", "category": "科技"},
    "hk01024": {"name": "快手-W", "market": "HK", "category": "科技"},
    "hk09988": {"name": "阿里巴巴-W", "market": "HK", "category": "科技"},
    "hk00883": {"name": "中国海洋石油", "market": "HK", "category": "能源"},
}


def fetch_a_stocks():
    """获取A股数据 - 腾讯API"""
    codes = ["sh603259", "sh600438", "sh601012", "sz002459", "sz300763",
             "sh688235", "sh600196", "sh601888"]

    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    results = {}

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            for line in data.strip().split('\n'):
                if '~' not in line:
                    continue
                parts = line.split('~')
                if len(parts) < 40:
                    continue

                code_raw = parts[2]
                if code_raw.startswith('6'):
                    code = f"sh{code_raw}"
                else:
                    code = f"sz{code_raw}"

                name = parts[1]
                price = float(parts[3]) if parts[3] else None
                change_pct = float(parts[32]) if parts[32] else None
                pe = float(parts[39]) if parts[39] and parts[39] != '-' else None
                pb = float(parts[46]) if parts[46] and parts[46] != '-' else None

                if pe and abs(pe) > 10000:
                    pe = None

                results[code] = {
                    "name": name,
                    "price": price,
                    "change_pct": change_pct,
                    "pe_ttm": pe,
                    "pb": pb,
                }
                print(f"  {name} ({code}): 价格={price} 涨跌={change_pct}% PE={pe} PB={pb}")

    except Exception as e:
        print(f"  A股数据获取失败: {e}")

    return results


def fetch_hk_stocks():
    """获取港股数据 - 腾讯API"""
    codes = ["hk00700", "hk03690", "hk01810", "hk01024", "hk09988",
             "hk02269", "hk06160", "hk01177", "hk01880", "hk00883"]

    url = "https://qt.gtimg.cn/q=" + ",".join(codes)
    results = {}

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            for line in data.strip().split('\n'):
                if '~' not in line:
                    continue
                parts = line.split('~')
                if len(parts) < 35:
                    continue

                code_raw = parts[2]
                code = f"hk{code_raw}"

                name = parts[1]
                price = float(parts[3]) if parts[3] and parts[3] != '-' else None
                change_pct = float(parts[32]) if len(parts) > 32 and parts[32] and parts[32] != '-' else None

                pe = None
                pb = None
                if len(parts) > 39 and parts[39] and parts[39] != '-':
                    try:
                        pe = float(parts[39])
                        if abs(pe) > 10000:
                            pe = None
                    except:
                        pass
                if len(parts) > 46 and parts[46] and parts[46] != '-':
                    try:
                        pb = float(parts[46])
                        if abs(pb) > 1000:
                            pb = None
                    except:
                        pass

                results[code] = {
                    "name": name,
                    "price": price,
                    "change_pct": change_pct,
                    "pe_ttm": pe,
                    "pb": pb,
                }
                print(f"  {name} ({code}): 价格={price} 涨跌={change_pct}% PE={pe} PB={pb}")

    except Exception as e:
        print(f"  港股数据获取失败: {e}")

    return results


def update_database(a_stocks, hk_stocks):
    """更新数据库"""
    print("\n更新数据库...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    updated = 0

    for code, info in STOCKS.items():
        data = a_stocks.get(code) or hk_stocks.get(code)
        if not data:
            print(f"  {code}: 无数据")
            continue

        name = info["name"]
        pe = data.get("pe_ttm")
        pb = data.get("pb")

        try:
            c.execute('''
                INSERT OR REPLACE INTO stock_financial
                (code, name, report_date, pe_ttm, pb)
                VALUES (?, ?, ?, ?, ?)
            ''', (code, name, today, pe, pb))

            pe_str = f"{pe:.2f}" if pe else "-"
            pb_str = f"{pb:.2f}" if pb else "-"
            print(f"  {name}: PE={pe_str} PB={pb_str}")
            updated += 1
        except Exception as e:
            print(f"  {name}: 保存失败 - {e}")

    conn.commit()
    conn.close()
    return updated


def main():
    print("=" * 60)
    print(f"股票数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\n获取A股数据...")
    a_stocks = fetch_a_stocks()
    print(f"  获取到 {len(a_stocks)} 条A股数据")

    print("\n获取港股数据...")
    hk_stocks = fetch_hk_stocks()
    print(f"  获取到 {len(hk_stocks)} 条港股数据")

    updated = update_database(a_stocks, hk_stocks)
    print(f"\n数据库更新: {updated} 条")

    print("\n" + "=" * 60)
    print("更新完成!")


if __name__ == "__main__":
    main()