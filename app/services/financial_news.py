"""
研报下载系统 - 金融新闻服务
"""
import feedparser
import os
import requests
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Tuple

PROXIES = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

RSS_FEEDS = {
    'bloomberg_markets': 'https://feeds.bloomberg.com/markets/news.rss',
    'bloomberg_economics': 'https://feeds.bloomberg.com/economics/news.rss',
    'bloomberg_tech': 'https://feeds.bloomberg.com/technology/news.rss',
}

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "investment.db")


class FinancialNewsService:
    """金融新闻服务"""

    def __init__(self):
        self.session = requests.Session()
        self.session.proxies = PROXIES
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def _fetch_rss_news(self, url: str, limit: int = 20) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """获取RSS新闻，返回数据和采集状态。"""
        status = {
            "mode": "rss",
            "ok": False,
            "error": None,
            "count": 0,
            "fetched_at": datetime.now().isoformat(),
        }
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            news = []
            for entry in feed.entries[:limit]:
                news.append({
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'published': entry.get('published', ''),
                    'summary': entry.get('summary', '')[:300] if entry.get('summary') else ''
                })
            status["ok"] = bool(news)
            status["count"] = len(news)
            if getattr(feed, "bozo", False) and not news:
                status["error"] = str(getattr(feed, "bozo_exception", "invalid rss"))[:200]
            return news, status
        except Exception as e:
            status["error"] = str(e)[:200]
            print(f"RSS获取失败: {e}")
            return [], status

    def get_rss_news(self, url: str, limit: int = 20) -> List[Dict[str, Any]]:
        """获取RSS新闻"""
        news, _status = self._fetch_rss_news(url, limit)
        return news

    def _get_db_news(self, source: str, limit: int = 20) -> List[Dict[str, Any]]:
        """从本地新闻表读取最近新闻，作为实时RSS失败时的降级。"""
        if not os.path.exists(DB_PATH):
            return []
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                """
                SELECT title, link, published, summary, fetched_at
                FROM news_articles
                WHERE source = ?
                ORDER BY fetched_at DESC, id DESC
                LIMIT ?
                """,
                (source, limit),
            )
            rows = c.fetchall()
            conn.close()
            return [
                {
                    "title": row[0] or "",
                    "link": row[1] or "",
                    "published": row[2] or row[4] or "",
                    "summary": (row[3] or "")[:300],
                    "fetched_at": row[4],
                    "storage": "sqlite_fallback",
                }
                for row in rows
            ]
        except Exception as exc:
            print(f"新闻DB降级读取失败({source}): {exc}")
            return []

    def get_all_news(self) -> Dict[str, Any]:
        """获取所有新闻"""
        result = {
            'update_time': datetime.now().isoformat(),
            'news': {},
            'source_status': {},
        }

        for name, url in RSS_FEEDS.items():
            rss_news, status = self._fetch_rss_news(url, 15)
            if rss_news:
                result['news'][name] = rss_news
                result['source_status'][name] = status
                continue

            fallback_news = self._get_db_news(name, 15)
            result['news'][name] = fallback_news
            status.update({
                "mode": "sqlite_fallback" if fallback_news else "empty",
                "ok": bool(fallback_news),
                "count": len(fallback_news),
                "fallback": True,
            })
            result['source_status'][name] = status

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
