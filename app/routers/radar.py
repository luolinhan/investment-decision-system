"""
领先指标雷达页面与 API。
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.radar_service import RadarService


router = APIRouter(prefix="/investment", tags=["radar"])
templates = Jinja2Templates(directory="templates")
_radar_service: RadarService | None = None


def get_radar_service() -> RadarService:
    global _radar_service
    if _radar_service is None:
        _radar_service = RadarService()
    return _radar_service


@router.get("/radar", response_class=HTMLResponse)
async def radar_page(request: Request):
    return templates.TemplateResponse("radar.html", {"request": request})


@router.get("/api/radar/overview")
async def radar_overview(force_refresh: bool = False):
    return get_radar_service().get_overview(force_refresh=force_refresh)


@router.get("/api/radar/gaps")
async def radar_gaps():
    overview = get_radar_service().get_overview()
    return overview.get("gaps", {})


@router.get("/api/radar/indicator/{indicator_key}")
async def radar_indicator_series(indicator_key: str, limit: int = 60):
    return get_radar_service().get_indicator_series(indicator_key=indicator_key, limit=limit)
