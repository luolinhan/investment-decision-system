"""
获取金融新闻和市场数据 - 使用公开RSS和API
"""
import feedparser
import requests
import json
from datetime import datetime

# 设置代理
PROXIES = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

def get_rss_news(url, limit=10):
    """获取RSS新闻"""
    try:
        resp = requests.get(url, proxies=PROXIES, timeout=15)
        feed = feedparser.parse(resp.content)
        news = []
        for entry in feed.entries[:limit]:
            news.append({
                'title': entry.get('title', ''),
                'link': entry.get('link', ''),
                'published': entry.get('published', ''),
                'summary': entry.get('summary', '')[:200] if entry.get('summary') else ''
            })
        return news
    except Exception as e:
        return [{'error': str(e)}]

def get_bloomberg_markets(limit=10):
    """Bloomberg市场新闻"""
    return get_rss_news('https://feeds.bloomberg.com/markets/news.rss', limit)

def get_bloomberg_economics(limit=10):
    """Bloomberg经济新闻"""
    return get_rss_news('https://feeds.bloomberg.com/economics/news.rss', limit)

def get_bloomberg_tech(limit=10):
    """Bloomberg科技新闻"""
    return get_rss_news('https://feeds.bloomberg.com/technology/news.rss', limit)

def get_reuters_news():
    """路透社新闻"""
    return get_rss_news('https://www.reutersagency.com/feed/?taxonomy=best-topics&post_type=best')

def get_investing_news():
    """Investing.com新闻"""
    try:
        url = "https://www.investing.com/news/stock-market-news"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        resp = requests.get(url, headers=headers, proxies=PROXIES, timeout=15)
        # 简单解析
        return {'status': 'ok', 'length': len(resp.text)}
    except Exception as e:
        return {'error': str(e)}


if __name__ == "__main__":
    print("=== 金融新闻获取测试 ===\n")

    print("1. Bloomberg市场新闻:")
    news = get_bloomberg_markets(5)
    for i, n in enumerate(news, 1):
        if 'error' in n:
            print(f"   Error: {n['error']}")
        else:
            print(f"   {i}. {n['title'][:60]}...")

    print("\n2. Bloomberg经济新闻:")
    news = get_bloomberg_economics(5)
    for i, n in enumerate(news, 1):
        if 'error' in n:
            print(f"   Error: {n['error']}")
        else:
            print(f"   {i}. {n['title'][:60]}...")

    print("\n3. Bloomberg科技新闻:")
    news = get_bloomberg_tech(5)
    for i, n in enumerate(news, 1):
        if 'error' in n:
            print(f"   Error: {n['error']}")
        else:
            print(f"   {i}. {n['title'][:60]}...")

    # 保存结果
    result = {
        'bloomberg_markets': get_bloomberg_markets(20),
        'bloomberg_economics': get_bloomberg_economics(20),
        'bloomberg_tech': get_bloomberg_tech(20),
        'update_time': datetime.now().isoformat()
    }
    with open('financial_news.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("\n结果已保存到 financial_news.json")