"""
研报下载系统 - 页面路由
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

router = APIRouter(tags=["页面"])

# 模板目录
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """首页"""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/reports", response_class=HTMLResponse)
async def reports_page(
    request: Request,
    stock_code: str = Query(None),
    institution: str = Query(None)
):
    """研报列表页面"""
    return templates.TemplateResponse("reports.html", {
        "request": request,
        "stock_code": stock_code,
        "institution": institution
    })


@router.get("/report/{report_id}", response_class=HTMLResponse)
async def report_detail_page(request: Request, report_id: int):
    """研报详情页面"""
    return templates.TemplateResponse("report_detail.html", {
        "request": request,
        "report_id": report_id
    })


@router.get("/stocks", response_class=HTMLResponse)
async def stocks_page(request: Request):
    """股票管理页面"""
    return templates.TemplateResponse("stocks.html", {"request": request})


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """设置页面"""
    return templates.TemplateResponse("settings.html", {"request": request})