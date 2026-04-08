#!/usr/bin/env python
"""
阿里巴巴阿里云财报数据采集脚本
使用 Playwright 浏览器渲染获取动态页面数据
"""

import asyncio
import re
import json
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

OUTPUT_DIR = Path("C:/Users/Administrator/research_report_system/data/alibaba_financial")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PROXY_SERVER = "http://127.0.0.1:7890"


async def fetch_alibaba_earnings():
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # 使用有头模式便于调试
            proxy={"server": PROXY_SERVER}
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        earnings_urls = [
            ("https://www.alibabagroup.com/en-US/document-136452", "FY2025Q3"),
            ("https://www.alibabagroup.com/en-US/document-134785", "FY2025Q2"),
            ("https://www.alibabagroup.com/en-US/document-132950", "FY2025Q1"),
            ("https://www.alibabagroup.com/en-US/document-131240", "FY2024Q4"),
        ]

        for url, quarter in earnings_urls:
            try:
                print(f"正在获取 {quarter}: {url}")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # 等待页面基本加载
                await page.wait_for_timeout(3000)

                # 尝试点击cookie同意按钮
                try:
                    allow_btn = await page.query_selector('button:has-text("Allow"), button:has-text("Accept")')
                    if allow_btn:
                        await allow_btn.click()
                        print("已点击cookie同意")
                        await page.wait_for_timeout(1000)
                except Exception as e:
                    print(f"Cookie按钮: {e}")

                # 等待文档内容加载 - 增加等待时间
                print("等待内容加载...")
                await page.wait_for_timeout(10000)

                # 尝试多种选择器等待内容
                selectors = [
                    ".document-content",
                    ".content-body",
                    "article",
                    ".markdown-body",
                    "[class*='content']",
                    "[class*='document']"
                ]

                for selector in selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=5000)
                        print(f"找到内容选择器: {selector}")
                        break
                    except:
                        pass

                # 额外等待
                await page.wait_for_timeout(5000)

                # 获取页面文本内容
                text_content = await page.evaluate("""() => {
                    // 尝试获取主要内容区域
                    const selectors = ['.document-content', '.content-body', 'article', '.markdown-body', 'main', '#main'];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText.length > 1000) {
                            return el.innerText;
                        }
                    }
                    // 如果没找到，返回整个body
                    return document.body.innerText;
                }""")

                print(f"获取到内容: {len(text_content)} 字符")

                # 提取阿里云相关数据
                cloud_data = extract_cloud_data(text_content)

                results.append({
                    "quarter": quarter,
                    "url": url,
                    "content_length": len(text_content),
                    "cloud_data": cloud_data,
                    "timestamp": datetime.now().isoformat()
                })

                # 保存完整文本
                with open(OUTPUT_DIR / f"{quarter}_content.txt", "w", encoding="utf-8") as f:
                    f.write(text_content)

                print(f"{quarter} 数据已保存")

                if cloud_data:
                    print(f"找到阿里云数据: {cloud_data}")

            except Exception as e:
                print(f"获取 {quarter} 失败: {e}")

        await browser.close()

    return results


def extract_cloud_data(text):
    """从文本中提取阿里云相关数据"""
    data = {}

    # 搜索阿里云/Cloud Intelligence相关段落
    lines = text.split('\n')

    for i, line in enumerate(lines):
        line_lower = line.lower()

        # 阿里云收入
        if 'cloud' in line_lower and ('revenue' in line_lower or '收入' in line):
            numbers = re.findall(r'[\d,]+\.?\d*\s*(?:billion|million|RMB|CNY|¥|$)', line)
            if numbers:
                data.setdefault('cloud_revenue', []).extend(numbers)
            growth = re.findall(r'([+-]?\d+\.?\d*)\s*%', line)
            if growth:
                data.setdefault('cloud_growth', []).extend(growth)

        # 阿里云利润 EBITA
        if 'cloud' in line_lower and ('ebita' in line_lower or 'profit' in line_lower or '利润' in line):
            numbers = re.findall(r'[\d,]+\.?\d*\s*(?:billion|million|RMB|CNY|¥|$)', line)
            if numbers:
                data.setdefault('cloud_profit', []).extend(numbers)

        # 整体收入
        if 'total revenue' in line_lower or '总收入' in line:
            numbers = re.findall(r'[\d,]+\.?\d*\s*(?:billion|million|RMB|CNY|¥|$)', line)
            if numbers:
                data.setdefault('total_revenue', []).extend(numbers[:1])

    return data


async def main():
    print("="*60)
    print("阿里巴巴阿里云财报数据采集 (Playwright)")
    print(f"使用代理: {PROXY_SERVER}")
    print("="*60)

    print("\n正在获取财报数据...")
    results = await fetch_alibaba_earnings()

    # 保存汇总结果
    with open(OUTPUT_DIR / "alibaba_cloud_summary.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("\n" + "="*60)
    print("采集完成!")
    print(f"数据保存在: {OUTPUT_DIR}")
    print("="*60)

    print("\n数据摘要:")
    for r in results:
        print(f"\n{r['quarter']}:")
        print(f"  内容长度: {r['content_length']} 字符")
        if r['cloud_data']:
            print(f"  阿里云数据: {r['cloud_data']}")


if __name__ == "__main__":
    asyncio.run(main())