# -*- coding: utf-8 -*-
"""
同步Bloomberg RSS新闻到数据库

数据源: feedparser 解析 Bloomberg RSS feeds
"""
import sqlite3
import sys
import os
import json
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


def ensure_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT,
            link TEXT UNIQUE,
            published TEXT,
            summary TEXT,
            fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news_articles(source)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_fetched ON news_articles(fetched_at)")


def main():
    print("=" * 60)
    print(f"News Sync - {datetime.now()}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    ensure_tables(cursor)

    total = 0
    failed = 0
    skipped = 0
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
                    if cursor.rowcount > 0:
                        added += 1
                    else:
                        skipped += 1

                conn.commit()
                print(f"  {source}: {added} new articles")
                total += added
            else:
                failed += 1
                reason = getattr(feed, "bozo_exception", "empty feed")
                print(f"  {source} FAIL: {str(reason)[:200]}")
        except Exception as e:
            failed += 1
            print(f"  {source} FAIL: {e}")

    # Clean old entries (>7 days)
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM news_articles WHERE fetched_at < ?", (cutoff,))
    deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
    conn.commit()

    conn.close()
    print(f"\n[OK] News sync done: {total} total new articles")
    print("ETL_METRICS_JSON=" + json.dumps({
        "records_processed": total,
        "records_failed": failed,
        "records_skipped": skipped,
        "records_deleted": deleted,
    }, ensure_ascii=False))
    if failed >= len(RSS_FEEDS):
        print("[FAIL] All RSS feeds failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
