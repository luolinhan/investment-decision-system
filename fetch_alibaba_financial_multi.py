#!/usr/bin/env python
"""
阿里巴巴财务数据采集脚本
从多个公开数据源获取阿里巴巴财报数据
"""

import requests
import json
import re
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("C:/Users/Administrator/research_report_system/data/alibaba_financial")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def fetch_sec_company_facts():
    """从SEC获取阿里巴巴的财务事实数据"""
    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0001577552.json"

    try:
        print("正在从SEC获取财务数据...")
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=60)

        if response.status_code == 200:
            data = response.json()

            # 保存完整数据
            with open(OUTPUT_DIR / "sec_company_facts.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"SEC数据已保存: {len(response.content)} bytes")
            return data
        else:
            print(f"SEC请求失败: {response.status_code}")
            return None
    except Exception as e:
        print(f"SEC获取失败: {e}")
        return None


def fetch_yahoo_finance():
    """从Yahoo Finance获取阿里巴巴财务数据"""
    url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/BABA?modules=financialData,earnings,earningsTrend,incomeStatementHistory,balanceSheetHistory,cashflowStatementHistory"

    try:
        print("正在从Yahoo Finance获取财务数据...")
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)

        if response.status_code == 200:
            data = response.json()

            with open(OUTPUT_DIR / "yahoo_finance_data.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print("Yahoo Finance数据已保存")

            # 提取关键数据
            result = data.get("quoteSummary", {}).get("result", [])
            if result:
                financial = result[0].get("financialData", {})
                print(f"\n关键财务数据:")
                print(f"  总收入: {financial.get('totalRevenue', {}).get('raw', 'N/A')}")
                print(f"  毛利润: {financial.get('grossProfits', {}).get('raw', 'N/A')}")
                print(f"  运营收入: {financial.get('operatingIncome', {}).get('raw', 'N/A')}")
                print(f"  净利润: {financial.get('netIncome', {}).get('raw', 'N/A')}")

            return data
        else:
            print(f"Yahoo Finance请求失败: {response.status_code}")
            return None
    except Exception as e:
        print(f"Yahoo Finance获取失败: {e}")
        return None


def fetch_sec_filings_list():
    """获取SEC提交列表"""
    url = "https://data.sec.gov/submissions/CIK0001577552.json"

    try:
        print("正在获取SEC提交列表...")
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)

        if response.status_code == 200:
            data = response.json()

            filings = data.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            accessions = filings.get("accessionNumber", [])

            # 提取最近20个财报提交
            results = []
            for i in range(min(20, len(forms))):
                if forms[i] in ["20-F", "6-K", "10-K", "10-Q"]:
                    results.append({
                        "form": forms[i],
                        "date": dates[i],
                        "accession": accessions[i]
                    })

            with open(OUTPUT_DIR / "sec_filings_list.json", "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            print(f"SEC提交列表已保存: {len(results)} 条")
            return results
        else:
            print(f"SEC列表请求失败: {response.status_code}")
            return None
    except Exception as e:
        print(f"SEC列表获取失败: {e}")
        return None


def fetch_google_finance():
    """尝试从Google Finance获取数据"""
    url = "https://www.google.com/finance/quote/BABA:NYSE"

    try:
        print("正在从Google Finance获取数据...")
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)

        if response.status_code == 200:
            # 保存HTML用于后续分析
            with open(OUTPUT_DIR / "google_finance.html", "w", encoding="utf-8") as f:
                f.write(response.text)

            print(f"Google Finance HTML已保存: {len(response.text)} bytes")
            return response.text
        else:
            print(f"Google Finance请求失败: {response.status_code}")
            return None
    except Exception as e:
        print(f"Google Finance获取失败: {e}")
        return None


def fetch_eastmoney_hk():
    """从东方财富获取港股阿里巴巴数据"""
    # 港股09988的财务摘要
    urls = [
        ("https://emweb.eastmoney.com/PC_HKF10/NewFinanceAnalysis/Index?type=web&code=HK09988", "finance_analysis"),
        ("https://quote.eastmoney.com/hk/09988.html", "quote"),
    ]

    for url, name in urls:
        try:
            print(f"正在从东方财富获取{name}数据...")
            response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=30)

            if response.status_code == 200:
                with open(OUTPUT_DIR / f"eastmoney_{name}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"东方财富{name}数据已保存: {len(response.text)} bytes")
            else:
                print(f"东方财富{name}请求失败: {response.status_code}")
        except Exception as e:
            print(f"东方财富{name}获取失败: {e}")


def extract_alibaba_cloud_data():
    """从已保存的数据中提取阿里云相关数据"""
    summary = {
        "source": "多数据源整合",
        "timestamp": datetime.now().isoformat(),
        "data": {}
    }

    # 读取Yahoo Finance数据
    try:
        with open(OUTPUT_DIR / "yahoo_finance_data.json", "r", encoding="utf-8") as f:
            yahoo_data = json.load(f)

        result = yahoo_data.get("quoteSummary", {}).get("result", [])
        if result:
            financial = result[0].get("financialData", {})
            summary["data"]["yahoo_finance"] = {
                "total_revenue": financial.get("totalRevenue", {}),
                "gross_profit": financial.get("grossProfits", {}),
                "operating_income": financial.get("operatingIncome", {}),
                "net_income": financial.get("netIncome", {}),
                "ebitda": financial.get("ebitda", {}),
            }
    except Exception as e:
        print(f"处理Yahoo Finance数据失败: {e}")

    # 读取SEC filings列表
    try:
        with open(OUTPUT_DIR / "sec_filings_list.json", "r", encoding="utf-8") as f:
            sec_filings = json.load(f)
            summary["data"]["sec_recent_filings"] = sec_filings[:10]
    except Exception as e:
        print(f"处理SEC数据失败: {e}")

    # 保存汇总
    with open(OUTPUT_DIR / "alibaba_financial_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary


def main():
    print("="*60)
    print("阿里巴巴财务数据采集")
    print(f"使用代理: {PROXIES['http']}")
    print("="*60)

    # 1. SEC Company Facts
    print("\n[1] 获取SEC Company Facts...")
    fetch_sec_company_facts()

    # 2. Yahoo Finance
    print("\n[2] 获取Yahoo Finance数据...")
    fetch_yahoo_finance()

    # 3. SEC Filings List
    print("\n[3] 获取SEC提交列表...")
    fetch_sec_filings_list()

    # 4. Google Finance
    print("\n[4] 获取Google Finance数据...")
    fetch_google_finance()

    # 5. 东方财富
    print("\n[5] 获取东方财富数据...")
    fetch_eastmoney_hk()

    # 6. 提取汇总
    print("\n[6] 提取并汇总数据...")
    summary = extract_alibaba_cloud_data()

    print("\n" + "="*60)
    print("采集完成!")
    print(f"数据保存在: {OUTPUT_DIR}")
    print("="*60)

    # 打印摘要
    if summary.get("data"):
        print("\n数据摘要:")
        for key, value in summary["data"].items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()