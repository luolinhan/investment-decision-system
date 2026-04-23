# -*- coding: utf-8 -*-
"""
同步Bloomberg RSS新闻到数据库

数据源: feedparser 解析 Bloomberg RSS feeds
"""
import sqlite3
import sys
import os
from datetime import datetime, timedelta

os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import feedparser
except ImportError:
    print("[WARN] feedparser not installed, skipping news sync")
    sys.exit(0)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "investment.db")

RSS_FEEDS = {
    "bloomberg_markets": "https://feeds.bloomberg.com/markets/news.rss",
    "bloomberg_economics": "https://feeds.bloomberg.com/economics/news.rss",
    "bloomberg_tech": "https://feeds.bloomberg.com/technology/news.rss",
}


def main():
    print("=" * 60)
    print(f"News Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total = 0
    for source, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            if feed.entries:
                added = 0
                for entry in feed.entries[:50]:  # Limit to 50 per source
                    link = entry.get("link", "")
                    title = entry.get("title", "")
                    published = entry.get("published", "")
                    summary = entry.get("summary", "")[:500]

                    cursor.execute("""
                        INSERT OR IGNORE INTO news_articles
                        (source, title, link, published, summary)
                        VALUES (?, ?, ?, ?, ?)
                    """, (source, title, link, published, summary))
                    added += 1

                conn.commit()
                print(f"  {source}: {added} new articles")
                total += added
        except Exception as e:
            print(f"  {source} FAIL: {e}")

    # Clean old entries (>7 days)
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM news_articles WHERE fetched_at < ?", (cutoff,))
    conn.commit()

    conn.close()
    print(f"\n[OK] News sync done: {total} total new articles")


if __name__ == "__main__":
    main()
