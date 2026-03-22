"""
研报下载系统 - 金融新闻服务
"""
import feedparser
import requests
from datetime import datetime
from typing import List, Dict, Any

PROXIES = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

RSS_FEEDS = {
    'bloomberg_markets': 'https://feeds.bloomberg.com/markets/news.rss',
    'bloomberg_economics': 'https://feeds.bloomberg.com/economics/news.rss',
    'bloomberg_tech': 'https://feeds.bloomberg.com/technology/news.rss',
}


class FinancialNewsService:
    """金融新闻服务"""

    def __init__(self):
        self.session = requests.Session()
        self.session.proxies = PROXIES
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_rss_news(self, url: str, limit: int = 20) -> List[Dict[str, Any]]:
        """获取RSS新闻"""
        try:
            resp = self.session.get(url, timeout=15)
            feed = feedparser.parse(resp.content)
            news = []
            for entry in feed.entries[:limit]:
                news.append({
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'published': entry.get('published', ''),
                    'summary': entry.get('summary', '')[:300] if entry.get('summary') else ''
                })
            return news
        except Exception as e:
            print(f"RSS获取失败: {e}")
            return []

    def get_all_news(self) -> Dict[str, Any]:
        """获取所有新闻"""
        result = {
            'update_time': datetime.now().isoformat(),
            'news': {}
        }

        for name, url in RSS_FEEDS.items():
            result['news'][name] = self.get_rss_news(url, 15)

        return result

    def get_bloomberg_markets(self, limit: int = 15) -> List[Dict[str, Any]]:
        """Bloomberg市场新闻"""
        return self.get_rss_news(RSS_FEEDS['bloomberg_markets'], limit)

    def get_bloomberg_economics(self, limit: int = 15) -> List[Dict[str, Any]]:
        """Bloomberg经济新闻"""
        return self.get_rss_news(RSS_FEEDS['bloomberg_economics'], limit)


if __name__ == "__main__":
    service = FinancialNewsService()
    news = service.get_all_news()
    print(f"市场新闻: {len(news['news']['bloomberg_markets'])} 条")
    print(f"经济新闻: {len(news['news']['bloomberg_economics'])} 条")
    print(f"科技新闻: {len(news['news']['bloomberg_tech'])} 条")