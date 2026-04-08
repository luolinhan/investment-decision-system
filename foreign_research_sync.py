"""
海外研报同步入口

用途:
1. 在阿里云执行源站抓取和中转
2. 在 Windows 执行本地导入、分析和清理
3. 作为定时任务入口
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
from typing import Optional

from sqlalchemy import select

from app.database import async_session, init_db
from app.models import ForeignResearchSource
from app.services.foreign_research_service import ForeignResearchService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="海外研报同步工具")
    parser.add_argument("--crawl-all", action="store_true", help="抓取所有启用来源")
    parser.add_argument("--source-id", type=int, help="按来源ID抓取")
    parser.add_argument("--lookback-days", type=int, default=180, help="回溯天数")
    parser.add_argument("--limit", type=int, default=30, help="单来源最多抓取数量")
    analyze_group = parser.add_mutually_exclusive_group()
    analyze_group.add_argument("--auto-analyze", dest="auto_analyze", action="store_true", help="抓取后自动分析")
    analyze_group.add_argument("--no-auto-analyze", dest="auto_analyze", action="store_false", help="抓取后不自动分析")
    parser.set_defaults(auto_analyze=True)
    parser.add_argument("--cleanup", action="store_true", help="清理过期文档")
    parser.add_argument("--list-sources", action="store_true", help="列出来源")
    parser.add_argument("--ingest-url", type=str, help="直接导入 URL")
    parser.add_argument("--source-name", type=str, help="来源名")
    parser.add_argument("--institution-name", type=str, help="机构名")
    parser.add_argument("--title", type=str, help="标题")
    parser.add_argument("--publish-date", type=str, help="发布日期 YYYY-MM-DD")
    parser.add_argument("--ingest-file", type=str, help="导入本地文件")
    return parser


async def main_async(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    await init_db()
    async with async_session() as db:
        service = ForeignResearchService(db)

        if args.list_sources:
            items = await service.list_sources()
            for item in items:
                print(f"[{item['id']}] {item['source_name']} | {item.get('institution_name') or '-'} | {item.get('list_url') or item.get('base_url') or '-'}")
            return

        if args.cleanup:
            print(await service.cleanup_expired_documents())
            return

        if args.ingest_url:
            publish_date = date.fromisoformat(args.publish_date) if args.publish_date else None
            result = await service.ingest_from_url(
                url=args.ingest_url,
                source_name=args.source_name or "manual",
                institution_name=args.institution_name,
                title=args.title,
                publish_date=publish_date,
                auto_analyze=args.auto_analyze,
            )
            print(result)
            return

        if args.ingest_file:
            publish_date = date.fromisoformat(args.publish_date) if args.publish_date else None
            result = await service.ingest_from_local_file(
                file_path=args.ingest_file,
                source_name=args.source_name or "manual",
                institution_name=args.institution_name,
                title=args.title,
                publish_date=publish_date,
                auto_analyze=args.auto_analyze,
            )
            print(result)
            return

        if args.source_id:
            print(
                await service.crawl_source(
                    source_id=args.source_id,
                    lookback_days=args.lookback_days,
                    limit=args.limit,
                    auto_analyze=args.auto_analyze,
                )
            )
            return

        if args.crawl_all:
            result = []
            rows = (await db.execute(select(ForeignResearchSource).where(ForeignResearchSource.enabled.is_(True)))).scalars().all()
            for source in rows:
                try:
                    item = await service.crawl_source(
                        source_id=source.id,
                        lookback_days=args.lookback_days,
                        limit=args.limit,
                        auto_analyze=args.auto_analyze,
                    )
                    result.append(item)
                except Exception as exc:
                    result.append({"source_id": source.id, "error": str(exc)})
            print(result)
            return

        parser.print_help()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(main_async(args, parser))


if __name__ == "__main__":
    main()
