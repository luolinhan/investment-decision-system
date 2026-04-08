"""
海外研报路由
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import ForeignResearchAnalysis, ForeignResearchDocument, ForeignResearchSource
from app.services.foreign_research_service import ForeignResearchService

router = APIRouter(prefix="/foreign-research", tags=["foreign-research"])
templates = Jinja2Templates(directory="templates")


class SourcePayload(BaseModel):
    source_name: str
    institution_name: Optional[str] = None
    source_type: Optional[str] = "public_web"
    base_url: Optional[str] = None
    list_url: Optional[str] = None
    login_required: bool = False
    enabled: bool = True
    crawl_frequency_minutes: int = 1440
    config_json: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None


class IngestUrlPayload(BaseModel):
    url: str
    source_name: str
    institution_name: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    publish_date: Optional[date] = None
    external_id: Optional[str] = None
    market_scope: Optional[str] = None
    doc_type: Optional[str] = None
    auto_analyze: bool = True
    region: str = "global"


class IngestFilePayload(BaseModel):
    file_path: str
    source_name: str
    institution_name: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    publish_date: Optional[date] = None
    external_id: Optional[str] = None
    market_scope: Optional[str] = None
    doc_type: Optional[str] = None
    auto_analyze: bool = True
    region: str = "global"


class CrawlPayload(BaseModel):
    lookback_days: int = 180
    limit: int = 30
    auto_analyze: bool = True


def get_foreign_research_service(db: AsyncSession = Depends(get_db)) -> ForeignResearchService:
    return ForeignResearchService(db)


@router.get("/", response_class=HTMLResponse)
async def foreign_research_page(request: Request):
    return templates.TemplateResponse("foreign_research.html", {"request": request})


@router.get("/api/summary")
async def summary(db: AsyncSession = Depends(get_db)):
    # Task-04: 统计口径修正
    # documents: 文档总数
    total = await db.execute(select(func.count(ForeignResearchDocument.id)))

    # analyzed: 有至少一条分析记录的去重文档数（而非分析表总行数）
    # 这样即使同一文档被多次分析，analyzed 也不会超过 documents
    analyzed = await db.execute(
        select(func.count(func.distinct(ForeignResearchAnalysis.document_id)))
    )

    # 按主题分类统计
    macro = await db.execute(
        select(func.count(ForeignResearchDocument.id)).where(ForeignResearchDocument.market_scope == "macro")
    )
    industry = await db.execute(
        select(func.count(ForeignResearchDocument.id)).where(ForeignResearchDocument.market_scope == "industry")
    )
    equity = await db.execute(
        select(func.count(ForeignResearchDocument.id)).where(ForeignResearchDocument.market_scope == "equity")
    )

    # 本月新增（本月1日起）
    recent = await db.execute(
        select(func.count(ForeignResearchDocument.id)).where(
            ForeignResearchDocument.publish_date >= date.today().replace(day=1)
        )
    )

    return {
        "documents": int(total.scalar() or 0),
        "analyzed": int(analyzed.scalar() or 0),  # Task-04: 现在保证 analyzed <= documents
        "macro": int(macro.scalar() or 0),
        "industry": int(industry.scalar() or 0),
        "equity": int(equity.scalar() or 0),
        "recent_month": int(recent.scalar() or 0),
    }


@router.get("/api/sources")
async def list_sources(service: ForeignResearchService = Depends(get_foreign_research_service)):
    return {"items": await service.list_sources()}


@router.post("/api/sources")
async def upsert_source(payload: SourcePayload, service: ForeignResearchService = Depends(get_foreign_research_service)):
    source = await service.upsert_source(payload.model_dump())
    return {"item": service._source_to_dict(source)}


@router.get("/api/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    keyword: Optional[str] = None,
    institution: Optional[str] = None,
    market_scope: Optional[str] = None,
    stance: Optional[str] = None,
    source_name: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    service: ForeignResearchService = Depends(get_foreign_research_service),
):
    return await service.list_documents(
        page=page,
        page_size=page_size,
        keyword=keyword,
        institution=institution,
        market_scope=market_scope,
        stance=stance,
        source_name=source_name,
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/api/documents/{document_id}")
async def get_document(document_id: int, service: ForeignResearchService = Depends(get_foreign_research_service)):
    document = await service.get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="document not found")
    return document


def _resolve_storage_path(storage_path: str) -> Path:
    path = Path(storage_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    if path.exists():
        return path

    fallback = Path(str(storage_path).replace("\\", "/"))
    if not fallback.is_absolute():
        fallback = Path.cwd() / fallback
    return fallback


def _extract_text_from_storage(path: Path, service: ForeignResearchService) -> str:
    suffix = path.suffix.lower()
    if suffix in (".txt", ".md"):
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""
    if suffix in (".html", ".htm"):
        try:
            html = path.read_text(encoding="utf-8", errors="ignore")
            _, text = service._extract_html_text(html)
            return text or ""
        except Exception:
            return ""
    if suffix == ".pdf":
        try:
            data = path.read_bytes()
            text, _ = service._extract_pdf_text_bytes(data)
            return text or ""
        except Exception:
            return ""
    return ""


@router.get("/api/documents/{document_id}/download")
async def download_document(
    document_id: int,
    service: ForeignResearchService = Depends(get_foreign_research_service),
):
    document = await service.get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="document not found")

    storage_path = document.get("storage_path")
    if not storage_path:
        raise HTTPException(status_code=404, detail="storage path not found")

    path = _resolve_storage_path(storage_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"file not found: {storage_path}")

    suffix = path.suffix.lower()
    media_type = "application/octet-stream"
    if suffix == ".pdf":
        media_type = "application/pdf"
    elif suffix in (".html", ".htm"):
        media_type = "text/html"
    elif suffix in (".txt", ".md"):
        media_type = "text/plain"

    return FileResponse(path=str(path), media_type=media_type, filename=path.name)


@router.get("/api/documents/{document_id}/content")
async def document_content(
    document_id: int,
    max_chars: int = Query(30000, ge=1000, le=200000),
    service: ForeignResearchService = Depends(get_foreign_research_service),
):
    document = await service.get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="document not found")

    text = ""
    text_path = Path(settings.foreign_research_text_path) / f"{document_id:08d}.txt"
    if not text_path.is_absolute():
        text_path = Path.cwd() / text_path
    if text_path.exists():
        try:
            text = text_path.read_text(encoding="utf-8")
        except Exception:
            text = ""

    if not text:
        text = document.get("raw_excerpt") or ""

    if not text and document.get("storage_path"):
        local_path = _resolve_storage_path(str(document["storage_path"]))
        if local_path.exists():
            text = _extract_text_from_storage(local_path, service)
            if text and not text_path.exists():
                try:
                    text_path.parent.mkdir(parents=True, exist_ok=True)
                    text_path.write_text(text, encoding="utf-8")
                except Exception:
                    pass

    if not text:
        source_url = document.get("source_url") or document.get("html_url") or document.get("pdf_url")
        if source_url:
            try:
                data, ctype, _ = await service._fetch_bytes(source_url)
                if "pdf" in ctype or str(source_url).lower().endswith(".pdf"):
                    text, _ = service._extract_pdf_text_bytes(data)
                else:
                    html = data.decode("utf-8", errors="ignore")
                    _, text = service._extract_html_text(html)
            except Exception:
                text = ""

    truncated = len(text) > max_chars
    content = text[:max_chars]

    return {
        "document_id": document_id,
        "title": document.get("title_original"),
        "content": content,
        "truncated": truncated,
        "content_length": len(text),
        "source_url": document.get("source_url"),
        "pdf_url": document.get("pdf_url"),
        "html_url": document.get("html_url"),
        "download_url": f"/foreign-research/api/documents/{document_id}/download",
    }


@router.post("/api/ingest/url")
async def ingest_url(payload: IngestUrlPayload, service: ForeignResearchService = Depends(get_foreign_research_service)):
    return await service.ingest_from_url(**payload.model_dump())


@router.post("/api/ingest/file")
async def ingest_file(payload: IngestFilePayload, service: ForeignResearchService = Depends(get_foreign_research_service)):
    return await service.ingest_from_local_file(**payload.model_dump())


@router.post("/api/analyze/{document_id}")
async def analyze_document(
    document_id: int,
    force_refresh: bool = False,
    service: ForeignResearchService = Depends(get_foreign_research_service),
):
    return await service.analyze_document(document_id=document_id, force_refresh=force_refresh)


@router.post("/api/crawl/{source_id}")
async def crawl_source(
    source_id: int,
    payload: CrawlPayload,
    service: ForeignResearchService = Depends(get_foreign_research_service),
):
    return await service.crawl_source(
        source_id=source_id,
        lookback_days=payload.lookback_days,
        limit=payload.limit,
        auto_analyze=payload.auto_analyze,
    )


@router.post("/api/cleanup")
async def cleanup(service: ForeignResearchService = Depends(get_foreign_research_service)):
    return await service.cleanup_expired_documents()
