"""
研报下载系统 - 路由模块
"""
from app.routers.reports import router as reports_router
from app.routers.pages import router as pages_router

__all__ = ["reports_router", "pages_router"]