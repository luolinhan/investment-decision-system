"""
测试curl获取Yahoo Finance数据
验证是否能绕过403错误
"""
import subprocess
import json
from datetime import datetime, timedelta

def test_yahoo_curl(symbol):
    """使用curl测试Yahoo Finance API"""
    end_ts = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=30)).timestamp())

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    full_url = f"{url}?period1={start_ts}&period2={end_ts}&interval=1d"

    print(f"测试 {symbol}...")
    print(f"URL: {full_url}")

    result = subprocess.run([
        'curl', '-s',
        '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        '-H', 'Accept: application/json',
        full_url
    ], capture_output=True, text=True, timeout=60)

    if result.returncode != 0:
        print(f"curl错误: {result.stderr}")
        return None

    if not result.stdout:
        print("无响应内容")
        return None

    try:
        data = json.loads(result.stdout)

        if data.get("chart", {}).get("result"):
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]

            print(f"获取到 {len(timestamps)} 条数据")

            # 显示最近3条
            for i in range(max(0, len(timestamps)-3), len(timestamps)):
                ts = timestamps[i]
                date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                close = quotes.get("close", [])[i]
                print(f"  {date}: close={close}")

            return data
        else:
            print("API返回无数据")
            print(f"响应: {result.stdout[:500]}...")
            return None
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        print(f"响应: {result.stdout[:500]}...")
        return None

if __name__ == "__main__":
    print("=" * 50)
    print("测试curl获取Yahoo Finance数据")
    print("=" * 50)

    # 测试VIX
    test_yahoo_curl("^VIX")

    print()

    # 测试道琼斯
    test_yahoo_curl("^DJI")