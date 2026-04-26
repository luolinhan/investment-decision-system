"""Minimal legacy page routes kept for active operator surfaces."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from jinja2 import FileSystemLoader, Environment
from pathlib import Path

router = APIRouter(tags=["页面"])

# 模板目录 - 禁用缓存避免 Python 3.14 兼容性问题
templates = Jinja2Templates(
    env=Environment(
        loader=FileSystemLoader("templates"),
        auto_reload=False,
        cache_size=0
    )
)


@router.get("/stocks", response_class=HTMLResponse)
async def stocks_page(request: Request):
    """股票管理页面"""
    return templates.TemplateResponse(request, "stocks.html", {"request": request})
