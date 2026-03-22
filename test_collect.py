# 测试采集脚本 - 查看原始响应
import asyncio
import httpx
from datetime import datetime, timedelta

async def test():
    print("测试东方财富研报API - 查看原始响应:\n")

    async with httpx.AsyncClient(timeout=30) as client:
        url = "https://reportapi.eastmoney.com/report/list"

        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://data.eastmoney.com/",
        }

        # 测试不带code参数
        params = {
            "cb": "datatable",
            "industryCode": "*",
            "pageSize": 10,
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": start_date.strftime("%Y-%m-%d"),
            "endTime": end_date.strftime("%Y-%m-%d"),
            "pageNo": 1,
            "fields": "",
            "qType": "1",  # 行业研报
            "orgCode": "",
            "code": "",
            "rcode": "10",
        }

        resp = await client.get(url, params=params, headers=headers)
        print(f"状态: {resp.status_code}")
        print(f"响应长度: {len(resp.text)}")
        print(f"原始响应:\n{resp.text}")

asyncio.run(test())