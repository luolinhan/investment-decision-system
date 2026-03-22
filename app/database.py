"""
研报下载系统 - 数据库操作
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from contextlib import asynccontextmanager
import os

from app.config import settings
from app.models import Base, Stock, DEFAULT_STOCKS


# 异步引擎
engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True
)

# 异步会话工厂
async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def init_db():
    """初始化数据库"""
    # 确保数据目录存在
    db_path = settings.database_url.replace("sqlite+aiosqlite:///", "")
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # 创建表
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 插入默认股票数据
    async with async_session() as session:
        for stock_data in DEFAULT_STOCKS:
            # 检查是否已存在
            from sqlalchemy import select
            result = await session.execute(
                select(Stock).where(Stock.code == stock_data["code"])
            )
            if not result.scalar_one_or_none():
                stock = Stock(**stock_data)
                session.add(stock)
        await session.commit()


async def get_db():
    """获取数据库会话"""
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise