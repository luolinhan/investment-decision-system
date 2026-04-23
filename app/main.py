"""
研报下载系统 - FastAPI 主应用
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from app.config import settings, ensure_directories, get_investment_runtime_profile
from app.database import init_db
from app.routers import reports, pages, investment, foreign_research, investment_v2


# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/app.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    logger.info("正在启动研报下载系统...")

    # 确保目录存在
    ensure_directories()

    # 记录运行时配置
    profile = get_investment_runtime_profile()
    logger.info(f"Investment runtime profile: {profile}")

    # 初始化数据库
    await init_db()
    logger.info("数据库初始化完成")

    # 启动定时任务（如果配置了）
    # scheduler = setup_scheduler()
    # scheduler.start()

    logger.info(f"研报下载系统启动成功，访问地址: http://{settings.host}:{settings.port}")

    yield

    # 关闭时
    logger.info("正在关闭研报下载系统...")
    # scheduler.shutdown()


# 创建应用
app = FastAPI(
    title="研报下载系统",
    description="自动采集港股、A股研报，提供智能摘要和便捷查阅",
    version="1.0.0",
    lifespan=lifespan
)

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")

# 注册路由
app.include_router(pages.router)
app.include_router(reports.router)
app.include_router(investment.router)
app.include_router(investment_v2.router_v2)
app.include_router(foreign_research.router)


# 健康检查
@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
        log_level="info"
    )
