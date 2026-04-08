"""
采集市场数据 - 指数、VIX等
"""
import urllib.request
import json
import sqlite3
from datetime import datetime, timedelta
import re

DB_PATH = "data/investment.db"

# 指数配置
INDICES = {
    # A股指数 (腾讯代码)
    "sh000001": {"name": "上证指数", "market": "A", "tencent": "sh000001"},
    "sh000300": {"name": "沪深300", "market": "A", "tencent": "sh000300"},
    "sh000016": {"name": "上证50", "market": "A", "tencent": "sh000016"},
    "sh000905": {"name": "中证500", "market": "A", "tencent": "sh000905"},
    "sh000852": {"name": "中证1000", "market": "A", "tencent": "sh000852"},
    "sz399006": {"name": "创业板指", "market": "A", "tencent": "sz399006"},
    "sh000688": {"name": "科创50", "market": "A", "tencent": "sh000688"},
    # 港股指数
    "hkHSI": {"name": "恒生指数", "market": "HK", "tencent": "hkHSI"},
    "hkHSCEI": {"name": "国企指数", "market": "HK", "tencent": "hkHSCEI"},
    "hkHSTECH": {"name": "恒生科技", "market": "HK", "tencent": "hkHSTECH"},
    # 美股指数
    "usDJI": {"name": "道琼斯", "market": "US", "tencent": "gb_dji"},
    "usIXIC": {"name": "纳斯达克", "market": "US", "tencent": "gb_ixic"},
    "usSPX": {"name": "标普500", "market": "US", "tencent": "gb_spx"},
}


def fetch_index_data():
    """获取指数实时数据"""
    tencent_codes = [v["tencent"] for v in INDICES.values()]
    url = "https://qt.gtimg.cn/q=" + ",".join(tencent_codes)

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            results = []
            for line in data.strip().split('\n'):
                if '~' not in line or '=' not in line:
                    continue

                # 提取代码前缀 (v_sh000001 或 v_gb_dji)
                match = re.match(r'v_(\w+)="', line)
                if not match:
                    continue

                raw_code = match.group(1)
                parts = line.split('~')
                if len(parts) < 33:
                    continue

                # 匹配我们的代码
                code = None
                for k, v in INDICES.items():
                    if v["tencent"] == raw_code:
                        code = k
                        break

                if not code:
                    continue

                name = INDICES[code]["name"]
                market = INDICES[code]["market"]
                price = float(parts[3]) if parts[3] and parts[3] != '-' else None
                change_pct = float(parts[32]) if len(parts) > 32 and parts[32] else None

                results.append({
                    "code": code,
                    "name": name,
                    "market": market,
                    "price": price,
                    "change_pct": change_pct,
                })
                print(f"  {name}: {price} ({change_pct}%)")

            return results
    except Exception as e:
        print(f"  获取指数失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def fetch_vix_data():
    """获取VIX数据"""
    url = "https://qt.gtimg.cn/q=gb_vix"

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            if '~' in data:
                parts = data.split('~')
                if len(parts) > 32:
                    vix = float(parts[3]) if parts[3] and parts[3] != '-' else None
                    change = float(parts[32]) if parts[32] and parts[32] != '-' else None
                    print(f"  VIX恐慌指数: {vix} ({change}%)")
                    return {"vix": vix, "change": change}
    except Exception as e:
        print(f"  获取VIX失败: {e}")

    return None


def fetch_interest_rates():
    """获取SHIBOR利率 - 新浪API"""
    url = "https://hq.sinajs.cn/list=shibor"

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.sina.com.cn'
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode('gbk')

            # 解析: SHIBOR数据格式
            # shibor="日期,隔夜,1周,2周,1月,3月,6月,9月,1年"
            if '=' in data:
                values = data.split('"')[1].split(',')
                if len(values) >= 9:
                    return {
                        "date": values[0],
                        "overnight": float(values[1]) if values[1] else None,
                        "week_1": float(values[2]) if values[2] else None,
                        "week_2": float(values[3]) if values[3] else None,
                        "month_1": float(values[4]) if values[4] else None,
                        "month_3": float(values[5]) if values[5] else None,
                        "month_6": float(values[6]) if values[6] else None,
                        "month_9": float(values[7]) if values[7] else None,
                        "year_1": float(values[8]) if values[8] else None,
                    }
                    print(f"  SHIBOR: 隔夜={values[1]} 1周={values[2]}")
    except Exception as e:
        print(f"  获取SHIBOR失败: {e}")

    return None


def save_index_data(indices):
    """保存指数数据"""
    if not indices:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    saved = 0

    for idx in indices:
        try:
            c.execute('''
                INSERT OR REPLACE INTO index_history
                (code, name, trade_date, close, change_pct)
                VALUES (?, ?, ?, ?, ?)
            ''', (idx["code"], idx["name"], today, idx["price"], idx["change_pct"]))
            saved += 1
        except Exception as e:
            print(f"  保存{idx['name']}失败: {e}")

    conn.commit()
    conn.close()
    print(f"  已保存 {saved} 条指数数据")
    return saved


def save_vix_data(vix_data):
    """保存VIX数据"""
    if not vix_data:
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        c.execute('''
            INSERT OR REPLACE INTO vix_history
            (trade_date, vix_close, vix_change)
            VALUES (?, ?, ?)
        ''', (today, vix_data["vix"], vix_data["change"]))
        conn.commit()
        print(f"  已保存VIX数据")
    except Exception as e:
        print(f"  保存VIX失败: {e}")
    finally:
        conn.close()


def save_interest_rates(rates):
    """保存利率数据"""
    if not rates:
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute('''
            INSERT OR REPLACE INTO interest_rates
            (trade_date, shibor_overnight, shibor_1w, shibor_1m, shibor_3m, shibor_6m, shibor_1y)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            rates["date"],
            rates["overnight"],
            rates["week_1"],
            rates["month_1"],
            rates["month_3"],
            rates["month_6"],
            rates["year_1"]
        ))
        conn.commit()
        print(f"  已保存利率数据")
    except Exception as e:
        print(f"  保存利率失败: {e}")
    finally:
        conn.close()


def main():
    print("=" * 60)
    print(f"市场数据采集 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\n[1/3] 获取指数数据...")
    indices = fetch_index_data()
    save_index_data(indices)

    print("\n[2/3] 获取VIX数据...")
    vix = fetch_vix_data()
    save_vix_data(vix)

    print("\n[3/3] 获取利率数据...")
    rates = fetch_interest_rates()
    save_interest_rates(rates)

    print("\n" + "=" * 60)
    print("采集完成!")


if __name__ == "__main__":
    main()