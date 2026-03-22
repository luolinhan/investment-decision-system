"""
美股和全球指数数据采集 - 部署在阿里云硅谷服务器
采集道琼斯、纳斯达克、标普500、富时A50、富时三倍做空等数据
输出JSON文件，供同步到Windows
"""
import requests
import json
import time
import os
from datetime import datetime, timedelta

OUTPUT_DIR = "/tmp/index_data"

# 要采集的指数
INDICES = {
    # 美股指数
    "^DJI": {"code": "dji", "name": "道琼斯"},
    "^IXIC": {"code": "ixic", "name": "纳斯达克"},
    "^GSPC": {"code": "inx", "name": "标普500"},
    # 富时相关
    "FXI": {"code": "ftsea50", "name": "富时中国A50"},  # iShares中国大盘ETF作为代理
    "YANG": {"code": "yang", "name": "富时中国三倍做空"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def get_yahoo_history(symbol, days=365):
    """从Yahoo Finance获取历史数据"""
    try:
        end_timestamp = int(datetime.now().timestamp())
        start_timestamp = int((datetime.now() - timedelta(days=days*2)).timestamp())

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "period1": start_timestamp,
            "period2": end_timestamp,
            "interval": "1d",
            "includePrePost": "false"
        }

        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        data = resp.json()

        if data.get("chart", {}).get("result"):
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]

            records = []
            for i, ts in enumerate(timestamps):
                try:
                    date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    close_val = quotes.get("close", [])[i]
                    if close_val:  # 只保留有收盘价的数据
                        records.append({
                            "date": date,
                            "open": float(quotes.get("open", [])[i] or 0),
                            "high": float(quotes.get("high", [])[i] or 0),
                            "low": float(quotes.get("low", [])[i] or 0),
                            "close": float(close_val),
                            "volume": float(quotes.get("volume", [])[i] or 0)
                        })
                except (IndexError, TypeError, ValueError):
                    continue

            return records

        return None

    except Exception as e:
        print(f"获取 {symbol} 失败: {e}")
        return None


def save_to_file(code, name, data):
    """保存数据到JSON文件"""
    if not data:
        return False

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{code}.json")

    output = {
        "code": code,
        "name": name,
        "count": len(data),
        "data": data,
        "updated": datetime.now().isoformat()
    }

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  保存 {name} 到 {filepath}: {len(data)} 条")
    return True


def collect_all():
    """采集所有指数"""
    print("=" * 50)
    print("开始采集美股和全球指数数据")
    print(f"时间: {datetime.now()}")
    print("=" * 50)

    # 清理旧数据目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    success_count = 0
    for symbol, info in INDICES.items():
        print(f"\n采集 {info['name']} ({symbol})...")

        data = get_yahoo_history(symbol, days=365)
        if data:
            # 筛选最近365天
            cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            data = [d for d in data if d["date"] >= cutoff]

            print(f"  获取到 {len(data)} 条数据")
            if save_to_file(info["code"], info["name"], data):
                success_count += 1
        else:
            print(f"  获取数据失败")

        time.sleep(2)  # 避免请求过快

    print("\n" + "=" * 50)
    print(f"采集完成: {success_count}/{len(INDICES)} 个指数成功")
    print(f"数据文件保存在: {OUTPUT_DIR}")
    print("=" * 50)

    return success_count


if __name__ == "__main__":
    collect_all()