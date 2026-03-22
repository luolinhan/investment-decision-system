"""
阿里云服务器国外投行研报采集脚本
在阿里云硅谷服务器上运行，然后同步到Windows
"""
import os
import json
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import hashlib

# 代理设置（如果需要）
PROXIES = None  # 硅谷服务器可以直接访问

# 输出目录
OUTPUT_DIR = "/root/foreign_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 关注的港股
HK_STOCKS = {
    "BABA": "阿里巴巴",
    "TCEHY": "腾讯",
    "MEITUAN": "美团",
    "XIAOMI": "小米",
}

# 投行研报来源
SOURCES = {
    "seeking_alpha": {
        "name": "Seeking Alpha",
        "base_url": "https://seekingalpha.com",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
    },
    "morningstar": {
        "name": "Morningstar",
        "base_url": "https://www.morningstar.com",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
    },
    "zacks": {
        "name": "Zacks Investment Research",
        "base_url": "https://www.zacks.com",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
    }
}


def fetch_seeking_alpha_articles(symbol, limit=10):
    """获取Seeking Alpha文章"""
    articles = []
    try:
        url = f"https://seekingalpha.com/symbol/{symbol}"
        headers = SOURCES["seeking_alpha"]["headers"]

        resp = requests.get(url, headers=headers, proxies=PROXIES, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')

            # 查找文章链接
            for link in soup.select('a[data-test-id="post-link"]')[:limit]:
                title = link.get_text(strip=True)
                href = link.get('href', '')
                if href and title:
                    full_url = f"https://seekingalpha.com{href}" if href.startswith('/') else href
                    articles.append({
                        "title": title,
                        "url": full_url,
                        "source": "Seeking Alpha",
                        "symbol": symbol,
                        "date": datetime.now().isoformat()
                    })

        time.sleep(1)  # 避免请求过快

    except Exception as e:
        print(f"Seeking Alpha获取失败: {e}")

    return articles


def fetch_yahoo_finance_news(symbol, limit=10):
    """获取Yahoo Finance新闻"""
    articles = []
    try:
        url = f"https://finance.yahoo.com/quote/{symbol}/latest-news/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }

        resp = requests.get(url, headers=headers, proxies=PROXIES, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')

            # 查找新闻条目
            for item in soup.select('div[data-testid="story-item"]')[:limit]:
                title_elem = item.select_one('h3')
                link_elem = item.select_one('a')

                if title_elem and link_elem:
                    title = title_elem.get_text(strip=True)
                    href = link_elem.get('href', '')
                    if href and title:
                        articles.append({
                            "title": title,
                            "url": f"https://finance.yahoo.com{href}" if href.startswith('/') else href,
                            "source": "Yahoo Finance",
                            "symbol": symbol,
                            "date": datetime.now().isoformat()
                        })

        time.sleep(1)

    except Exception as e:
        print(f"Yahoo Finance获取失败: {e}")

    return articles


def fetch_investing_com_analysis(symbol, limit=10):
    """获取Investing.com分析文章"""
    articles = []
    try:
        # Investing.com的港股分析页面
        symbol_map = {
            "BABA": "alibaba-group-holding-ltd",
            "TCEHY": "tencent-holdings-ltd",
        }

        slug = symbol_map.get(symbol, symbol.lower())
        url = f"https://www.investing.com/equities/{slug}-news"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

        resp = requests.get(url, headers=headers, proxies=PROXIES, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')

            for item in soup.select('article.articleItem')[:limit]:
                title_elem = item.select_one('a.title')
                date_elem = item.select_one('span.date')

                if title_elem:
                    title = title_elem.get_text(strip=True)
                    href = title_elem.get('href', '')
                    date_str = date_elem.get_text(strip=True) if date_elem else ''

                    if href and title:
                        articles.append({
                            "title": title,
                            "url": f"https://www.investing.com{href}" if href.startswith('/') else href,
                            "source": "Investing.com",
                            "symbol": symbol,
                            "date": date_str
                        })

        time.sleep(1)

    except Exception as e:
        print(f"Investing.com获取失败: {e}")

    return articles


def download_article_content(url):
    """下载文章内容"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        resp = requests.get(url, headers=headers, proxies=PROXIES, timeout=30)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')

            # 移除脚本和样式
            for script in soup(['script', 'style', 'nav', 'footer', 'header']):
                script.decompose()

            # 获取主要内容
            content = soup.get_text(separator='\n', strip=True)
            return content[:5000]  # 限制长度

    except Exception as e:
        print(f"下载内容失败: {e}")

    return None


def save_articles(articles):
    """保存文章到JSON文件"""
    output_file = os.path.join(OUTPUT_DIR, f"foreign_reports_{datetime.now().strftime('%Y%m%d')}.json")

    # 读取已有数据
    existing = []
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            existing = json.load(f)

    # 合并去重
    existing_urls = {a['url'] for a in existing}
    new_articles = [a for a in articles if a['url'] not in existing_urls]

    all_articles = existing + new_articles

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    print(f"保存了 {len(new_articles)} 篇新文章到 {output_file}")
    return output_file


def main():
    print("=== 开始采集国外投行研报 ===")
    print(f"时间: {datetime.now()}")

    all_articles = []

    for symbol, name in HK_STOCKS.items():
        print(f"\n采集 {name} ({symbol})...")

        # Seeking Alpha
        articles = fetch_seeking_alpha_articles(symbol)
        print(f"  Seeking Alpha: {len(articles)} 篇")
        all_articles.extend(articles)

        # Yahoo Finance
        articles = fetch_yahoo_finance_news(symbol)
        print(f"  Yahoo Finance: {len(articles)} 篇")
        all_articles.extend(articles)

        # Investing.com
        articles = fetch_investing_com_analysis(symbol)
        print(f"  Investing.com: {len(articles)} 篇")
        all_articles.extend(articles)

        time.sleep(2)  # 避免请求过快

    # 保存
    output_file = save_articles(all_articles)

    print(f"\n总共采集 {len(all_articles)} 篇文章")
    print(f"输出文件: {output_file}")

    return output_file


if __name__ == "__main__":
    main()