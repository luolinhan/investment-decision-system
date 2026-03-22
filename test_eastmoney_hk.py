# 测试东方财富港股研报接口
import httpx
import json
from datetime import datetime, timedelta

print("=== 东方财富港股研报接口测试 ===\n")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}

# 港股研报接口
# 股票代码格式：港股用 market=116，美股用 market=105

stocks = [
    ("阿里巴巴", "09988", 116),  # 港股
    ("腾讯控股", "00700", 116),
    ("美团-W", "03690", 116),
    ("小米集团-W", "01810", 116),
    ("快手-W", "01024", 116),
]

client = httpx.Client(timeout=30, follow_redirects=True)

for name, code, market in stocks:
    print(f"\n--- {name} ({code}) ---")

    # 东方财富研报接口
    url = "https://reportapi.eastmoney.com/report/jg"
    params = {
        "cb": "datatable",
        "pageSize": 10,
        "industry": "*",
        "rating": "*",
        "ratingChange": "*",
        "beginTime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "endTime": datetime.now().strftime("%Y-%m-%d"),
        "pageNo": 1,
        "fields": "",
        "qType": "0",
        "orgCode": "",
        "code": code,
        "market": market,
        "_": int(datetime.now().timestamp() * 1000)
    }

    try:
        resp = client.get(url, params=params, headers=headers)
        text = resp.text

        if text.startswith("datatable("):
            json_str = text[9:-2]
            data = json.loads(json_str)
            reports = data.get("data", [])
            print(f"找到 {len(reports)} 条研报")

            for r in reports[:5]:
                title = r.get("title", "")
                institution = r.get("orgSName", "")
                date = r.get("publishDate", "")
                rating = r.get("emRatingName", "")
                info_code = r.get("infoCode", "")
                pdf_url = f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else ""

                print(f"  [{date}] {institution}: {title[:40]}...")
                print(f"      评级: {rating}, PDF: {pdf_url[:50]}...")
        else:
            print(f"响应格式错误: {text[:100]}")

    except Exception as e:
        print(f"错误: {e}")

    import time
    time.sleep(1)