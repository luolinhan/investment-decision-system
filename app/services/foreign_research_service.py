"""
海外研报采集与分析服务

目标:
1. 从公开页面或授权入口采集近 6 个月研报
2. 原文落盘到 Windows 存储目录
3. 使用百炼模型进行翻译、摘要和结构化抽取
4. 提供后续检索、聚合、回测的统一数据出口
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import (
    ForeignResearchAnalysis,
    ForeignResearchBatch,
    ForeignResearchDocument,
    ForeignResearchDocumentTag,
    ForeignResearchSource,
    ForeignResearchTag,
)

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover
    fitz = None

try:
    from pypdf import PdfReader  # type: ignore
except Exception:  # pragma: no cover
    PdfReader = None


FOREIGN_RESEARCH_SNAPSHOT_VERSION = "v1"
DEFAULT_LOOKBACK_DAYS = 180


def _to_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _from_json(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _safe_filename(value: str, max_length: int = 120) -> str:
    text = re.sub(r"[\\/:*?\"<>|\n\r\t]+", "_", value or "document").strip()
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        text = "document"
    return text[:max_length]


def _guess_market_scope(text: str) -> str:
    """判断文档主题类型（macro/industry/equity）"""
    lower = (text or "").lower()

    # 宏观关键词（扩充）
    macro_tokens = (
        "china", "china's", "chinese",
        "macro", "macroeconomic", "macroeconomics",
        "policy", "policies", "fiscal", "monetary",
        "cpi", "ppi", "gdp", "pmi",
        "liquidity", "rate", "rates", "interest rate",
        "fx", "foreign exchange", "yuan", "rmb", "renminbi",
        "credit", "inflation", "deflation", "growth", "recession",
        "strategy", "outlook", "forecast", "projection",
        "economy", "economic", "stimulus", "recovery",
        "trade", "tariff", "export", "import",
        "central bank", "pboc", "imf", "world bank", "oecd",
    )

    # 行业关键词（扩充）
    sector_tokens = (
        "sector", "sectors", "industry", "industries",
        "semiconductor", "chip", "semis",
        "internet", "tech", "technology", "digital",
        "bank", "banking", "financials", "finance",
        "consumer", "consumption", "retail", "e-commerce",
        "healthcare", "pharma", "biotech", "medical",
        "auto", "automotive", "ev", "electric vehicle",
        "energy", "oil", "gas", "renewable", "solar", "wind",
        "utilities", "power", "electricity",
        "telecom", "communication", "5g",
        "property", "real estate", "housing",
        "materials", "steel", "mining",
        "logistics", "shipping", "transport",
        "education", "tourism", "hospitality",
        "agriculture", "food",
    )

    # 股票关键词
    equity_tokens = (
        "stock", "stocks", "equity", "equities",
        "share", "shares", "valuation",
        "price target", "rating", "overweight", "underweight",
        "buy", "sell", "hold", "upgrade", "downgrade",
        "company", "companies", "ticker",
        "hk", "hk-listed", "a-share", "ashare",
    )

    # 计算命中数量
    macro_hits = sum(token in lower for token in macro_tokens)
    sector_hits = sum(token in lower for token in sector_tokens)
    equity_hits = sum(token in lower for token in equity_tokens)

    # 按命中数决定类型
    if macro_hits >= 2 and macro_hits >= max(sector_hits, equity_hits):
        return "macro"
    if sector_hits >= 2 and sector_hits >= equity_hits:
        return "industry"
    if equity_hits >= 2:
        return "equity"

    # 默认按标题关键词判断
    if any(token in lower for token in macro_tokens[:15]):
        return "macro"
    if any(token in lower for token in sector_tokens[:10]):
        return "industry"
    return "equity"


def _guess_stance(text: str) -> str:
    """判断观点立场（bullish/neutral/bearish）"""
    lower = (text or "").lower()

    # 看涨关键词（扩充）
    bullish = (
        "overweight", "buy", "bullish", "bull",
        "upgrade", "upgraded", "positive", "positively",
        "outperform", "outperformer", "strong buy",
        "long", "accumulate", "add", "attractive",
        "opportunity", "growth potential", "upside",
        "favorable", "improving", "recovery",
        "undervalued", "discount", "cheap",
        "momentum", "breakout", "rally",
        "recommend buy", "maintain buy", "reiterate buy",
    )

    # 看跌关键词（扩充）
    bearish = (
        "underweight", "sell", "bearish", "bear",
        "downgrade", "downgraded", "negative", "negatively",
        "underperform", "underperformer", "reduce", "trim",
        "short", "avoid", "unattractive", "concern",
        "risk", "downside", "threat", "challenge",
        "overvalued", "expensive", "premium",
        "weakness", "decline", "drop", "fall",
        "recommend sell", "maintain sell",
    )

    # 中性关键词
    neutral = (
        "neutral", "hold", "maintain", "fair value",
        "balanced", "mixed", "wait and see",
    )

    bullish_count = sum(token in lower for token in bullish)
    bearish_count = sum(token in lower for token in bearish)
    neutral_count = sum(token in lower for token in neutral)

    # 明确立场判断
    if bullish_count > bearish_count + neutral_count:
        return "bullish"
    if bearish_count > bullish_count + neutral_count:
        return "bearish"
    if neutral_count > bullish_count and neutral_count > bearish_count:
        return "neutral"

    # 默认比较看涨看跌
    if bullish_count > bearish_count:
        return "bullish"
    if bearish_count > bullish_count:
        return "bearish"
    return "neutral"


def _guess_doc_type(title: str, text: str) -> str:
    """判断文档类型（研报类型细分）"""
    blob = f"{title or ''} {text or ''}".lower()

    # 新覆盖报告
    if any(token in blob for token in ("initiation", "initiating", "coverage start", "first coverage", "launch report")):
        return "initiation"
    # 财报预告
    if any(token in blob for token in ("preview", "previewing", "earnings preview", "results preview", "pre-earnings")):
        return "earnings_preview"
    # 财报回顾
    if any(token in blob for token in ("post results", "after results", "results review", "post-earnings", "earnings review")):
        return "earnings_review"
    # 宏观观点
    if any(token in blob for token in ("macro", "strategy", "policy", "economy", "economic outlook", "gdp", "cpi")):
        return "macro_view"
    # 行业观点
    if any(token in blob for token in ("sector", "industry", "theme", "thematic", "sector update")):
        return "sector_view"
    # 公司深度报告
    if any(token in blob for token in ("deep dive", "in-depth", "comprehensive", "company report")):
        return "deep_dive"
    # 事件驱动
    if any(token in blob for token in ("merger", "acquisition", "ipo", "spin-off", "restructuring", "announcement")):
        return "event_driven"
    return "equity_view"


def _guess_tags(title: str, text: str) -> List[str]:
    """提取主题标签（扩充关键词字典）"""
    blob = f"{title or ''} {text or ''}".lower()
    tags = []

    # 扩充关键词字典（三级细分）
    keyword_map = {
        # 一级：宏观主题
        "macro": ["macro", "macroeconomic", "policy", "rates", "liquidity", "fx", "growth", "inflation", "china", "economy", "gdp", "cpi", "pmi"],
        "policy": ["policy", "policies", "regulation", "stimulus", "tariff", "export control", "fiscal", "monetary"],
        "monetary": ["monetary", "rate", "interest", "rate cut", "rate hike", "pboc", "central bank"],
        "trade": ["trade", "tariff", "export", "import", "supply chain", "logistics"],
        "growth": ["growth", "gdp", "expansion", "recovery", "stimulus"],
        "inflation": ["inflation", "cpi", "ppi", "deflation", "price"],
        # 一级：行业主题
        "industry": ["sector", "industry", "industries"],
        "technology": ["technology", "tech", "digital", "ai", "internet", "software", "semiconductor", "chip"],
        "finance": ["bank", "banking", "financials", "insurance", "fintech"],
        "consumer": ["consumer", "consumption", "retail", "e-commerce", "luxury"],
        "healthcare": ["healthcare", "pharma", "biotech", "drug", "medical", "hospital"],
        "energy": ["energy", "oil", "gas", "renewable", "solar", "wind", "power"],
        "property": ["property", "real estate", "housing", "developer"],
        "auto": ["auto", "automotive", "ev", "electric vehicle", "battery"],
        # 一级：股票主题
        "equity": ["stock", "stocks", "equity", "share", "valuation"],
        "hk_market": ["hk", "hong kong", "h-share", "hk-listed"],
        "a_share": ["a-share", "ashare", "china a", "mainland"],
        "us_listing": ["us", "adr", "american depositary", "nasdaq", "nyse"],
        # 一级：投资主题
        "earnings": ["earnings", "results", "preview", "guidance", "revenue", "eps", "profit"],
        "valuation": ["valuation", "pe", "pb", "price target", "multiple", "dcf"],
        "rating": ["buy", "sell", "overweight", "underweight", "rating", "upgrade", "downgrade"],
        "risk": ["risk", "downside", "threat", "challenge", "concern"],
    }

    for tag, keywords in keyword_map.items():
        if any(keyword in blob for keyword in keywords):
            tags.append(tag)

    return sorted(set(tags))


def _extract_json_block(content: str) -> Dict[str, Any]:
    text = (content or "").strip()
    if not text:
        return {}
    fenced = re.search(r"```json\s*(\{.*\})\s*```", text, flags=re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except Exception:
                return {}
    return {}


def _chunk_text(text: str, chunk_size: int = 4000) -> List[str]:
    if not text:
        return []
    chunks: List[str] = []
    current = []
    length = 0
    for paragraph in re.split(r"\n\s*\n", text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        if length + len(paragraph) > chunk_size and current:
            chunks.append("\n\n".join(current))
            current = [paragraph]
            length = len(paragraph)
        else:
            current.append(paragraph)
            length += len(paragraph)
    if current:
        chunks.append("\n\n".join(current))
    return chunks


@dataclass
class IngestResult:
    document_id: int
    external_id: str
    storage_path: str
    file_hash: str
    status: str


class ForeignResearchService:
    """海外研报采集、分析和检索服务"""

    def __init__(self, db: AsyncSession):
        self.db = db
        # Task-05: 统一从 settings 读取配置，不再混用 os.getenv()
        self.root = Path(settings.foreign_research_root)
        self.raw_dir = Path(settings.foreign_research_raw_path)
        self.text_dir = Path(settings.foreign_research_text_path)
        self.analysis_dir = Path(settings.foreign_research_analysis_path)
        self.manifest_dir = Path(settings.foreign_research_manifest_path)
        self.retention_days = settings.foreign_research_retention_days
        self.api_key = (settings.bailian_api_key or "").strip()
        self.base_url = (settings.bailian_base_url or "https://coding.dashscope.aliyuncs.com/v1").rstrip("/")
        self.model = (settings.foreign_research_model or "glm-5").strip()
        self.timeout_seconds = settings.foreign_research_timeout_seconds
        # Task-06: 可配置的SSL验证（默认开启）
        self.verify_ssl = settings.http_verify_ssl
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        for path in (self.root, self.raw_dir, self.text_dir, self.analysis_dir, self.manifest_dir):
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _guess_language(title: str, text: str) -> str:
        blob = f"{title or ''} {text or ''}"
        chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", blob))
        return "zh" if chinese_chars > 20 else "en"

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        value = value.strip()
        candidates = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y年%m月%d日")
        for fmt in candidates:
            try:
                return datetime.strptime(value[:20], fmt).date()
            except Exception:
                continue
        match = re.search(r"(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})", value)
        if match:
            try:
                y, m, d = (int(match.group(i)) for i in range(1, 4))
                return date(y, m, d)
            except Exception:
                return None
        return None

    @staticmethod
    def _today_lookback_cutoff(days: Optional[int] = None) -> date:
        return date.today() - timedelta(days=int(days or DEFAULT_LOOKBACK_DAYS))

    async def create_batch(
        self,
        batch_type: str,
        source_scope: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        notes: Optional[str] = None,
    ) -> ForeignResearchBatch:
        batch = ForeignResearchBatch(
            batch_type=batch_type,
            source_scope=source_scope,
            date_from=date_from,
            date_to=date_to,
            notes=notes,
            status="running",
        )
        self.db.add(batch)
        await self.db.flush()
        return batch

    async def finish_batch(self, batch: ForeignResearchBatch, document_count: int, success_count: int, fail_count: int, status: str = "completed") -> None:
        batch.document_count = document_count
        batch.success_count = success_count
        batch.fail_count = fail_count
        batch.status = status
        batch.finished_at = datetime.now()
        await self.db.flush()

    async def upsert_source(self, payload: Dict[str, Any]) -> ForeignResearchSource:
        name = (payload.get("source_name") or "").strip()
        if not name:
            raise ValueError("source_name 不能为空")

        result = await self.db.execute(
            select(ForeignResearchSource).where(ForeignResearchSource.source_name == name)
        )
        source = result.scalar_one_or_none()
        if not source:
            source = ForeignResearchSource(source_name=name)
            self.db.add(source)

        source.institution_name = payload.get("institution_name")
        source.source_type = payload.get("source_type") or "public_web"
        source.base_url = payload.get("base_url")
        source.list_url = payload.get("list_url")
        source.login_required = bool(payload.get("login_required", False))
        source.enabled = bool(payload.get("enabled", True))
        source.crawl_frequency_minutes = int(payload.get("crawl_frequency_minutes") or 1440)
        source.config_json = _to_json(payload.get("config_json") or payload.get("config") or {})
        source.notes = payload.get("notes")
        await self.db.flush()
        return source

    async def list_sources(self) -> List[Dict[str, Any]]:
        result = await self.db.execute(
            select(ForeignResearchSource).order_by(desc(ForeignResearchSource.updated_at))
        )
        items = result.scalars().all()
        return [self._source_to_dict(item) for item in items]

    def _source_to_dict(self, source: ForeignResearchSource) -> Dict[str, Any]:
        return {
            "id": source.id,
            "source_name": source.source_name,
            "institution_name": source.institution_name,
            "source_type": source.source_type,
            "base_url": source.base_url,
            "list_url": source.list_url,
            "login_required": source.login_required,
            "enabled": source.enabled,
            "crawl_frequency_minutes": source.crawl_frequency_minutes,
            "config_json": _from_json(source.config_json, {}),
            "notes": source.notes,
            "created_at": source.created_at.isoformat() if source.created_at else None,
            "updated_at": source.updated_at.isoformat() if source.updated_at else None,
        }

    async def list_documents(
        self,
        page: int = 1,
        page_size: int = 20,
        keyword: Optional[str] = None,
        institution: Optional[str] = None,
        market_scope: Optional[str] = None,
        stance: Optional[str] = None,
        source_name: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        filters = []
        if keyword:
            like = f"%{keyword}%"
            filters.append(
                (
                    ForeignResearchDocument.title_original.ilike(like)
                    | ForeignResearchDocument.title_cn.ilike(like)
                    | ForeignResearchDocument.institution_name.ilike(like)
                    | ForeignResearchDocument.raw_excerpt.ilike(like)
                )
            )
        if institution:
            filters.append(ForeignResearchDocument.institution_name.ilike(f"%{institution}%"))
        if market_scope:
            filters.append(ForeignResearchDocument.market_scope == market_scope)
        if source_name:
            filters.append(ForeignResearchDocument.source_name == source_name)
        if start_date:
            filters.append(ForeignResearchDocument.publish_date >= start_date)
        if end_date:
            filters.append(ForeignResearchDocument.publish_date <= end_date)

        count_stmt = select(func.count(ForeignResearchDocument.id))
        stmt = select(ForeignResearchDocument)
        if filters:
            stmt = stmt.where(*filters)
            count_stmt = count_stmt.where(*filters)
        if stance:
            stance_stmt = select(ForeignResearchAnalysis.document_id).where(ForeignResearchAnalysis.stance == stance)
            stmt = stmt.where(ForeignResearchDocument.id.in_(stance_stmt))
            count_stmt = count_stmt.where(ForeignResearchDocument.id.in_(stance_stmt))

        stmt = stmt.order_by(desc(ForeignResearchDocument.publish_date), desc(ForeignResearchDocument.id))
        stmt = stmt.offset((max(1, page) - 1) * max(1, page_size)).limit(max(1, page_size))

        rows = (await self.db.execute(stmt)).scalars().all()
        total = int((await self.db.execute(count_stmt)).scalar() or 0)

        # Task-09: 避免 N+1 查询，一次性获取所有文档的最新分析
        if rows:
            doc_ids = [doc.id for doc in rows]
            # 使用子查询获取每个文档的最新分析（按 created_at 排序）
            latest_analysis_subq = (
                select(
                    ForeignResearchAnalysis,
                    func.row_number()
                    .over(
                        partition_by=ForeignResearchAnalysis.document_id,
                        order_by=[desc(ForeignResearchAnalysis.created_at), desc(ForeignResearchAnalysis.id)]
                    )
                    .label("rn")
                )
                .where(ForeignResearchAnalysis.document_id.in_(doc_ids))
                .subquery()
            )
            # 只取每个文档的第一条（即最新）
            analyses_result = await self.db.execute(
                select(ForeignResearchAnalysis)
                .from_statement(
                    select(latest_analysis_subq)
                    .where(latest_analysis_subq.c.rn == 1)
                )
            )
            # 构建 document_id -> analysis 的映射
            analysis_map = {}
            for analysis in analyses_result.scalars().all():
                analysis_map[analysis.document_id] = analysis

            docs = []
            for doc in rows:
                latest = analysis_map.get(doc.id)
                docs.append(self._document_to_dict(doc, latest))
        else:
            docs = []

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "documents": docs,
        }

    def _document_to_dict(
        self,
        doc: ForeignResearchDocument,
        analysis: Optional[ForeignResearchAnalysis] = None,
    ) -> Dict[str, Any]:
        return {
            "id": doc.id,
            "source_id": doc.source_id,
            "external_id": doc.external_id,
            "source_name": doc.source_name,
            "institution_name": doc.institution_name,
            "title_original": doc.title_original,
            "title_cn": doc.title_cn,
            "title_en": doc.title_en,
            "author": doc.author,
            "publish_date": doc.publish_date.isoformat() if doc.publish_date else None,
            "language": doc.language,
            "region": doc.region,
            "market_scope": doc.market_scope,
            "doc_type": doc.doc_type,
            "source_url": doc.source_url,
            "pdf_url": doc.pdf_url,
            "html_url": doc.html_url,
            "storage_path": doc.storage_path,
            "file_hash": doc.file_hash,
            "file_size": doc.file_size,
            "page_count": doc.page_count,
            "status": doc.status,
            "retention_expires_at": doc.retention_expires_at.isoformat() if doc.retention_expires_at else None,
            "raw_excerpt": doc.raw_excerpt,
            "analysis": self._analysis_to_dict(analysis) if analysis else None,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
        }

    def _analysis_to_dict(self, analysis: Optional[ForeignResearchAnalysis]) -> Optional[Dict[str, Any]]:
        if not analysis:
            return None
        return {
            "id": analysis.id,
            "document_id": analysis.document_id,
            "analysis_version": analysis.analysis_version,
            "model_name": analysis.model_name,
            "prompt_version": analysis.prompt_version,
            "translation_cn": analysis.translation_cn,
            "summary_cn": analysis.summary_cn,
            "summary_en": analysis.summary_en,
            "stance": analysis.stance,
            "confidence_score": analysis.confidence_score,
            "key_points": _from_json(analysis.key_points_json, []),
            "drivers": _from_json(analysis.drivers_json, []),
            "risks": _from_json(analysis.risks_json, []),
            "invalid_conditions": _from_json(analysis.invalid_conditions_json, []),
            "price_targets": _from_json(analysis.price_targets_json, []),
            "rating_change": analysis.rating_change,
            "macro_conclusion": analysis.macro_conclusion,
            "industry_conclusion": analysis.industry_conclusion,
            "equity_conclusion": analysis.equity_conclusion,
            "topic_tags": _from_json(analysis.topic_tags_json, []),
            "entity_mentions": _from_json(analysis.entity_mentions_json, []),
            "created_at": analysis.created_at.isoformat() if analysis.created_at else None,
            "updated_at": analysis.updated_at.isoformat() if analysis.updated_at else None,
        }

    async def get_document(self, document_id: int) -> Optional[Dict[str, Any]]:
        result = await self.db.execute(select(ForeignResearchDocument).where(ForeignResearchDocument.id == document_id))
        doc = result.scalar_one_or_none()
        if not doc:
            return None
        analysis = await self.get_latest_analysis(document_id)
        return self._document_to_dict(doc, analysis)

    async def get_latest_analysis(self, document_id: int) -> Optional[ForeignResearchAnalysis]:
        result = await self.db.execute(
            select(ForeignResearchAnalysis)
            .where(ForeignResearchAnalysis.document_id == document_id)
            .order_by(desc(ForeignResearchAnalysis.created_at), desc(ForeignResearchAnalysis.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_or_create_tag(self, tag_type: str, tag_name: str, tag_name_cn: Optional[str] = None) -> ForeignResearchTag:
        clean_name = (tag_name or "").strip()
        if not clean_name:
            raise ValueError("tag_name 不能为空")
        result = await self.db.execute(select(ForeignResearchTag).where(ForeignResearchTag.tag_name == clean_name))
        tag = result.scalar_one_or_none()
        if not tag:
            tag = ForeignResearchTag(tag_type=tag_type, tag_name=clean_name, tag_name_cn=tag_name_cn or clean_name)
            self.db.add(tag)
            await self.db.flush()
        return tag

    async def _save_document_tags(self, document_id: int, tag_types: Sequence[str]) -> None:
        for tag_name in tag_types:
            tag = await self.get_or_create_tag("topic", tag_name, tag_name)
            existing = await self.db.execute(
                select(ForeignResearchDocumentTag).where(
                    ForeignResearchDocumentTag.document_id == document_id,
                    ForeignResearchDocumentTag.tag_id == tag.id,
                )
            )
            if existing.scalar_one_or_none():
                continue
            self.db.add(
                ForeignResearchDocumentTag(
                    document_id=document_id,
                    tag_id=tag.id,
                    confidence=0.8,
                    source="model",
                )
            )

    def _storage_target(self, publish_date: Optional[date], source_name: str, title: str, file_ext: str) -> Path:
        pub = publish_date or date.today()
        folder = self.raw_dir / f"{pub.year:04d}" / f"{pub.month:02d}" / _safe_filename(source_name)
        folder.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{timestamp}_{_safe_filename(title)}{file_ext}"
        return folder / filename

    @staticmethod
    def _hash_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    async def _fetch_bytes(self, url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, str, str]:
        merged_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        if headers:
            merged_headers.update(headers)
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self.timeout_seconds,
            headers=merged_headers,
            trust_env=False,
            verify=self.verify_ssl,  # Task-06: 可配置的SSL验证
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            ctype = resp.headers.get("content-type", "").lower()
            return resp.content, ctype, str(resp.url)

    @staticmethod
    def _extract_html_text(html: str) -> Tuple[str, str]:
        """提取 HTML 标题和正文，去除噪声元素"""
        soup = BeautifulSoup(html or "", "html.parser")

        # 移除噪声标签（扩展列表）
        noise_tags = [
            "script", "style", "noscript",  # 技术噪声
            "nav", "header", "footer", "aside",  # 导航元素
            "iframe", "svg", "canvas",  # 嵌入内容
            "form", "button", "input",  # 表单元素
            "advertisement", "ad", "ads",  # 广告（自定义标签）
        ]
        for tag in soup(noise_tags):
            tag.decompose()

        # 移除常见广告类名
        ad_classes = ["ad", "ads", "advertisement", "banner", "promo", "sidebar", "navigation", "menu", "footer", "header", "cookie", "popup"]
        for cls in ad_classes:
            for tag in soup.find_all(class_=lambda x: x and any(c in str(x).lower() for c in [cls])):
                tag.decompose()

        # 提取标题
        title = ""
        if soup.title and soup.title.text:
            title = soup.title.text.strip()

        # 清洗标题：移除站点后缀
        title_suffixes = [
            "| Goldman Sachs", "| J.P. Morgan", "| Morgan Stanley", "| HSBC", "| UBS",
            "| Reuters", "| Bloomberg", "| Financial Times", "| WSJ", "| SCMP",
            "- Goldman Sachs", "- J.P. Morgan", "- Morgan Stanley", "- HSBC", "- UBS",
            "®", "™", " - ", " | ",
        ]
        for suffix in title_suffixes:
            if title.endswith(suffix):
                title = title[:-len(suffix)].strip()

        # 提取正文
        text = soup.get_text("\n", strip=True)

        # 清理多余空白行
        lines = []
        for line in text.split("\n"):
            line = line.strip()
            if line and len(line) > 2:  # 过滤单字符行
                lines.append(line)
        text = "\n".join(lines)

        return title, text

    @staticmethod
    def _extract_html_publish_date(html: str) -> Optional[date]:
        """从 HTML 元数据中提取发布日期"""
        soup = BeautifulSoup(html or "", "html.parser")

        # 尝试多种元数据格式
        date_selectors = [
            # Open Graph
            ("meta[property='article:published_time']", "content"),
            ("meta[property='article:published']", "content"),
            # Dublin Core
            ("meta[name='DC.date']", "content"),
            ("meta[name='dc.date']", "content"),
            # 常见格式
            ("meta[name='publish-date']", "content"),
            ("meta[name='publishdate']", "content"),
            ("meta[name='published']", "content"),
            ("meta[name='date']", "content"),
            ("meta[itemprop='datePublished']", "content"),
            # JSON-LD 中的日期
            ("script[type='application/ld+json']", None),
        ]

        for selector, attr in date_selectors:
            try:
                tag = soup.select_one(selector)
                if tag:
                    if selector == "script[type='application/ld+json']":
                        # 解析 JSON-LD
                        import json
                        data = json.loads(tag.string or "{}")
                        date_str = (
                            data.get("datePublished") or
                            data.get("dateCreated") or
                            (isinstance(data.get("@graph"), list) and
                             next((g.get("datePublished") for g in data["@graph"] if g.get("datePublished")), None))
                        )
                        if date_str:
                            parsed = ForeignResearchService._parse_date(str(date_str))
                            if parsed:
                                return parsed
                    elif attr and tag.get(attr):
                        parsed = ForeignResearchService._parse_date(tag.get(attr))
                        if parsed:
                            return parsed
            except Exception:
                continue

        # 尝试从可见文本中提取日期模式
        text = soup.get_text(" ", strip=True)[:5000]
        patterns = [
            r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})",  # 2024-01-15 or 2024/01/15
            r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",  # 15 Jan 2024
            r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4})",  # Jan 15, 2024
            r"Published[:\s]+(\d{4}[-/]\d{1,2}[-/]\d{1,2})",
            r"Published[:\s]+(\d{1,2}\s+\w+\s+\d{4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                parsed = ForeignResearchService._parse_date(match.group(1))
                if parsed:
                    return parsed

        return None

    @staticmethod
    def _extract_pdf_text_bytes(data: bytes) -> Tuple[str, int]:
        text = ""
        pages = 0
        if fitz is not None:
            try:
                doc = fitz.open(stream=data, filetype="pdf")
                pages = len(doc)
                parts = []
                for page in doc:
                    parts.append(page.get_text("text"))
                text = "\n".join(parts)
                doc.close()
                return text, pages
            except Exception:
                pass
        if PdfReader is not None:
            try:
                reader = PdfReader(io.BytesIO(data))
                pages = len(reader.pages)
                parts = []
                for page in reader.pages:
                    try:
                        parts.append(page.extract_text() or "")
                    except Exception:
                        continue
                return "\n".join(parts), pages
            except Exception:
                pass
        return "", pages

    def _rule_analysis(self, title: str, text: str, language: str) -> Dict[str, Any]:
        """规则引擎分析（无模型时的降级方案）"""
        blob = f"{title}\n{text}"[:120000]
        tags = _guess_tags(title, blob)
        market_scope = _guess_market_scope(blob)
        stance = _guess_stance(blob)
        doc_type = _guess_doc_type(title, blob)

        # 置信度基于文本长度和关键词匹配
        confidence = 0.55 if len(blob) > 2000 else 0.42
        if len(tags) >= 3:
            confidence = min(confidence + 0.15, 0.75)

        # 生成结构化关键点
        key_points = [
            f"文档类型判断: {doc_type}",
            f"主题分类: {market_scope}",
            f"观点倾向: {stance}",
            "系统基于规则模式生成，建议接入模型进一步分析。",
        ]

        # 驱动因素（基于标签提取）
        drivers = []
        if "growth" in tags:
            drivers.append("经济增长预期")
        if "policy" in tags:
            drivers.append("政策支持")
        if "monetary" in tags:
            drivers.append("货币政策变化")
        if "technology" in tags:
            drivers.append("科技行业发展")
        if "earnings" in tags:
            drivers.append("业绩表现")
        if "valuation" in tags:
            drivers.append("估值水平")
        if not drivers:
            drivers.append(f"分类命中: {', '.join(tags) if tags else 'general'}")

        # 风险因素
        risks = []
        if "risk" in tags:
            risks.append("文档提及风险因素")
        if stance == "bearish":
            risks.append("观点偏悲观，需关注下行风险")
        if market_scope == "macro":
            risks.append("宏观环境不确定性")
        if not risks:
            risks.append("当前为规则分析，需人工复核具体风险")

        # 失效条件
        invalid_conditions = [
            "文本抽取不完整时结论可靠性下降",
            "关键词匹配可能存在偏差",
            "未调用模型深度分析",
        ]

        # 结构化结论模板
        macro_conclusion = None
        industry_conclusion = None
        equity_conclusion = None

        if market_scope == "macro":
            macro_conclusion = (
                f"【宏观观点】立场={stance}，置信度={confidence:.0%}。\n"
                f"主要标签: {', '.join(tags)}。\n"
                f"文本长度: {len(blob)}字符。\n"
                f"建议: 模型深度分析以获取具体宏观指标预测。"
            )
        elif market_scope == "industry":
            industry_conclusion = (
                f"【行业观点】立场={stance}，置信度={confidence:.0%}。\n"
                f"相关行业: {', '.join([t for t in tags if t in ['technology', 'finance', 'consumer', 'healthcare', 'energy', 'property', 'auto']])}。\n"
                f"建议: 模型分析以获取行业景气度判断。"
            )
        elif market_scope == "equity":
            equity_conclusion = (
                f"【个股观点】立场={stance}，置信度={confidence:.0%}。\n"
                f"相关标签: {', '.join(tags)}。\n"
                f"建议: 模型分析以获取目标价和评级信息。"
            )

        return {
            "analysis_version": FOREIGN_RESEARCH_SNAPSHOT_VERSION,
            "model_name": "rule-engine-v2",
            "prompt_version": "rule-v2",
            "translation_cn": title if language == "zh" else (title or "未命名研报"),
            "summary_cn": (text[:600] if text else title) or "无可提取文本",
            "summary_en": title if language == "en" else "",
            "stance": stance,
            "confidence_score": confidence,
            "key_points_json": _to_json(key_points),
            "drivers_json": _to_json(drivers),
            "risks_json": _to_json(risks),
            "invalid_conditions_json": _to_json(invalid_conditions),
            "price_targets_json": _to_json([]),
            "rating_change": None,
            "macro_conclusion": macro_conclusion,
            "industry_conclusion": industry_conclusion,
            "equity_conclusion": equity_conclusion,
            "topic_tags_json": _to_json(tags),
            "entity_mentions_json": _to_json([]),
        }

    def _call_bailian(self, title: str, text: str, language: str, source_name: str, institution_name: Optional[str]) -> Dict[str, Any]:
        if not self.api_key:
            return self._rule_analysis(title, text, language)

        chunks = _chunk_text(text, chunk_size=3500)
        excerpt = "\n\n".join(chunks[:3]) if chunks else text[:8000]
        prompt = (
            "你是海外券商研报分析助手。请仅输出严格JSON，不要输出额外解释。"
            "请围绕中国宏观、A股、港股研报输出以下字段："
            "translation_cn, summary_cn, summary_en, stance, confidence_score, key_points, drivers, risks, "
            "invalid_conditions, price_targets, rating_change, macro_conclusion, industry_conclusion, "
            "equity_conclusion, topic_tags, entity_mentions。"
            "其中 key_points/drivers/risks/invalid_conditions/price_targets/topic_tags/entity_mentions 必须是数组。"
            "stance 只能是 bullish / neutral / bearish 之一。"
        )
        payload = {
            "model": self.model,
            "temperature": 0.2,
            "max_tokens": 1800,
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "source_name": source_name,
                            "institution_name": institution_name,
                            "language": language,
                            "title": title,
                            "excerpt": excerpt,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds, trust_env=False, verify=self.verify_ssl) as client:
                resp = client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
                parsed = _extract_json_block(content)
                if not parsed:
                    return self._rule_analysis(title, text, language)
                return {
                    "analysis_version": FOREIGN_RESEARCH_SNAPSHOT_VERSION,
                    "model_name": self.model,
                    "prompt_version": "bailian-v1",
                    "translation_cn": parsed.get("translation_cn"),
                    "summary_cn": parsed.get("summary_cn"),
                    "summary_en": parsed.get("summary_en"),
                    "stance": parsed.get("stance") or "neutral",
                    "confidence_score": float(parsed.get("confidence_score") or 0.0),
                    "key_points_json": _to_json(parsed.get("key_points") or []),
                    "drivers_json": _to_json(parsed.get("drivers") or []),
                    "risks_json": _to_json(parsed.get("risks") or []),
                    "invalid_conditions_json": _to_json(parsed.get("invalid_conditions") or []),
                    "price_targets_json": _to_json(parsed.get("price_targets") or []),
                    "rating_change": parsed.get("rating_change"),
                    "macro_conclusion": parsed.get("macro_conclusion"),
                    "industry_conclusion": parsed.get("industry_conclusion"),
                    "equity_conclusion": parsed.get("equity_conclusion"),
                    "topic_tags_json": _to_json(parsed.get("topic_tags") or []),
                    "entity_mentions_json": _to_json(parsed.get("entity_mentions") or []),
                }
        except Exception as exc:
            fallback = self._rule_analysis(title, text, language)
            fallback["summary_cn"] = f"百炼调用失败，已降级规则分析：{str(exc)[:200]}"
            return fallback

    async def ingest_from_url(
        self,
        url: str,
        source_name: str,
        institution_name: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        publish_date: Optional[date] = None,
        external_id: Optional[str] = None,
        market_scope: Optional[str] = None,
        doc_type: Optional[str] = None,
        auto_analyze: bool = True,
        source_id: Optional[int] = None,
        region: str = "global",
        headers: Optional[Dict[str, str]] = None,
        skip_if_outside_window: bool = False,  # Task-03: 旧日期是否跳过
    ) -> Dict[str, Any]:
        if not url or not url.startswith(("http://", "https://")):
            raise ValueError("url 必须是有效的 http/https 链接")

        source_result = None
        if source_id is None:
            source_result = await self.db.execute(
                select(ForeignResearchSource).where(ForeignResearchSource.source_name == source_name)
            )
            source = source_result.scalar_one_or_none()
        else:
            source_result = await self.db.execute(
                select(ForeignResearchSource).where(ForeignResearchSource.id == source_id)
            )
            source = source_result.scalar_one_or_none()

        if source and not institution_name:
            institution_name = source.institution_name or institution_name
        if source and not source_id:
            source_id = source.id

        content, ctype, resolved_url = await self._fetch_bytes(url, headers=headers)
        file_hash = self._hash_bytes(content)
        existing = await self.db.execute(
            select(ForeignResearchDocument).where(
                (ForeignResearchDocument.file_hash == file_hash)
                | ((ForeignResearchDocument.source_url == resolved_url) & (ForeignResearchDocument.external_id == (external_id or resolved_url)))
            )
        )
        current = existing.scalar_one_or_none()
        if current:
            # Task-08: 统一返回结构，标记为重复
            return {
                "document_id": current.id,
                "status": current.status,
                "is_duplicate": True,
                "document": self._document_to_dict(current, await self.get_latest_analysis(current.id)),
            }

        inferred_title = title
        inferred_text = ""
        storage_ext = ".bin"
        page_count = 0
        html_text = ""  # Task-03: 保存 HTML 用于日期提取

        if "pdf" in ctype or resolved_url.lower().endswith(".pdf"):
            storage_ext = ".pdf"
            inferred_text, page_count = self._extract_pdf_text_bytes(content)
            if not inferred_title:
                inferred_title = _safe_filename(Path(urlparse(resolved_url).path).stem or "report")
        else:
            storage_ext = ".html"
            html_text = content.decode("utf-8", errors="ignore")
            html_title, inferred_text = self._extract_html_text(html_text)
            inferred_title = inferred_title or html_title or _safe_filename(Path(urlparse(resolved_url).path).stem or "report")
        if not inferred_title:
            inferred_title = "foreign_research_report"

        # Task-03: 如果没有传入 publish_date，尝试从 HTML 提取
        inferred_publish_date = publish_date
        if not inferred_publish_date and html_text:
            inferred_publish_date = self._extract_html_publish_date(html_text)

        language = self._guess_language(inferred_title, inferred_text)
        inferred_market_scope = market_scope or _guess_market_scope(f"{inferred_title}\n{inferred_text}")
        inferred_doc_type = doc_type or _guess_doc_type(inferred_title, inferred_text)

        # Task-03: 检查日期窗口
        cutoff = self._today_lookback_cutoff()
        final_publish_date = inferred_publish_date or date.today()

        # Task-03: 如果日期早于窗口起点，按配置决定是否跳过
        if final_publish_date < cutoff:
            if skip_if_outside_window:
                # 删除已写入的文件，不入库
                storage_path = self._storage_target(final_publish_date, source_name, inferred_title, storage_ext)
                if storage_path.exists():
                    storage_path.unlink()
                # Task-08: 统一返回结构
                return {
                    "document_id": None,
                    "status": "skipped_outside_window",
                    "is_duplicate": False,
                    "document": None,
                    "publish_date": final_publish_date.isoformat(),
                    "cutoff": cutoff.isoformat(),
                }
            # 否则使用窗口起点日期
            final_publish_date = cutoff

        storage_path = self._storage_target(final_publish_date, source_name, inferred_title, storage_ext)
        storage_path.write_bytes(content)

        document = ForeignResearchDocument(
            source_id=source_id,
            external_id=external_id or file_hash,
            source_name=source_name,
            institution_name=institution_name or source_name,
            title_original=inferred_title,
            title_cn=inferred_title if language == "zh" else None,
            title_en=inferred_title if language == "en" else None,
            author=author,
            publish_date=final_publish_date,
            language=language,
            region=region,
            market_scope=inferred_market_scope,
            doc_type=inferred_doc_type,
            source_url=resolved_url,
            pdf_url=resolved_url if "pdf" in ctype or resolved_url.lower().endswith(".pdf") else None,
            html_url=resolved_url if "pdf" not in ctype and not resolved_url.lower().endswith(".pdf") else None,
            storage_path=str(storage_path),
            file_hash=file_hash,
            file_size=len(content),
            page_count=page_count,
            # 状态判断：无文本=stored，短文本=low_content，正常=downloaded
            status="low_content" if (inferred_text and len(inferred_text) < 500) else ("downloaded" if inferred_text else "stored"),
            retention_expires_at=datetime.combine(date.today() + timedelta(days=self.retention_days), datetime.min.time()),
            raw_excerpt=(inferred_text[:2000] if inferred_text else None),
        )
        self.db.add(document)
        await self.db.flush()

        await self._save_document_tags(document.id, _guess_tags(inferred_title, inferred_text))

        if inferred_text:
            text_path = self.text_dir / f"{document.id:08d}.txt"
            text_path.write_text(inferred_text, encoding="utf-8")

        if auto_analyze:
            analysis_payload = self._call_bailian(
                title=inferred_title,
                text=inferred_text or (document.raw_excerpt or ""),
                language=language,
                source_name=source_name,
                institution_name=institution_name,
            )
            analysis = ForeignResearchAnalysis(
                document_id=document.id,
                analysis_version=analysis_payload.get("analysis_version") or FOREIGN_RESEARCH_SNAPSHOT_VERSION,
                model_name=analysis_payload.get("model_name") or self.model,
                prompt_version=analysis_payload.get("prompt_version") or "v1",
                translation_cn=analysis_payload.get("translation_cn"),
                summary_cn=analysis_payload.get("summary_cn"),
                summary_en=analysis_payload.get("summary_en"),
                stance=analysis_payload.get("stance") or "neutral",
                confidence_score=float(analysis_payload.get("confidence_score") or 0.0),
                key_points_json=analysis_payload.get("key_points_json") or _to_json([]),
                drivers_json=analysis_payload.get("drivers_json") or _to_json([]),
                risks_json=analysis_payload.get("risks_json") or _to_json([]),
                invalid_conditions_json=analysis_payload.get("invalid_conditions_json") or _to_json([]),
                price_targets_json=analysis_payload.get("price_targets_json") or _to_json([]),
                rating_change=analysis_payload.get("rating_change"),
                macro_conclusion=analysis_payload.get("macro_conclusion"),
                industry_conclusion=analysis_payload.get("industry_conclusion"),
                equity_conclusion=analysis_payload.get("equity_conclusion"),
                topic_tags_json=analysis_payload.get("topic_tags_json") or _to_json([]),
                entity_mentions_json=analysis_payload.get("entity_mentions_json") or _to_json([]),
            )
            self.db.add(analysis)
            document.status = "analyzed"
        else:
            document.status = "parsed" if inferred_text else "stored"

        await self.db.flush()
        # Task-08: 统一返回结构
        return {
            "document_id": document.id,
            "status": document.status,
            "is_duplicate": False,
            "document": self._document_to_dict(document, await self.get_latest_analysis(document.id)),
        }

    async def ingest_from_local_file(
        self,
        file_path: str,
        source_name: str,
        institution_name: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        publish_date: Optional[date] = None,
        external_id: Optional[str] = None,
        market_scope: Optional[str] = None,
        doc_type: Optional[str] = None,
        auto_analyze: bool = True,
        region: str = "global",
    ) -> Dict[str, Any]:
        """从本地文件导入文档，支持 PDF/HTML/TXT/MD 格式"""
        src = Path(file_path)
        if not src.exists():
            raise FileNotFoundError(file_path)

        content = src.read_bytes()
        file_hash = self._hash_bytes(content)

        # Task-08: 添加重复检测逻辑
        existing = await self.db.execute(
            select(ForeignResearchDocument).where(ForeignResearchDocument.file_hash == file_hash)
        )
        current = existing.scalar_one_or_none()
        if current:
            # Task-08: 统一返回结构，标记为重复
            return {
                "document_id": current.id,
                "status": current.status,
                "is_duplicate": True,
                "document": self._document_to_dict(current, await self.get_latest_analysis(current.id)),
            }

        ext = src.suffix.lower() or ".bin"
        storage_path = self._storage_target(publish_date, source_name, title or src.stem, ext)
        storage_path.write_bytes(content)

        inferred_title = title or src.stem

        # Task-02: 按扩展名提取正文
        inferred_text = ""
        page_count = 0

        if ext == ".pdf":
            inferred_text, page_count = self._extract_pdf_text_bytes(content)
        elif ext in (".html", ".htm"):
            html_text = content.decode("utf-8", errors="ignore")
            _, inferred_text = self._extract_html_text(html_text)
        elif ext in (".txt", ".md"):
            inferred_text = content.decode("utf-8", errors="ignore")

        language = self._guess_language(inferred_title, inferred_text)

        # Task-02: 根据正文长度决定状态
        if inferred_text and len(inferred_text) >= 500:
            initial_status = "parsed"
        elif inferred_text and len(inferred_text) > 0:
            initial_status = "low_content"
        else:
            initial_status = "stored"

        document = ForeignResearchDocument(
            source_name=source_name,
            institution_name=institution_name or source_name,
            external_id=external_id or file_hash,
            title_original=inferred_title,
            title_cn=inferred_title if language == "zh" else None,
            title_en=inferred_title if language == "en" else None,
            author=author,
            publish_date=publish_date or date.today(),
            language=language,
            region=region,
            market_scope=market_scope or _guess_market_scope(f"{inferred_title}\n{inferred_text}"),
            doc_type=doc_type or _guess_doc_type(inferred_title, inferred_text),
            source_url=None,
            pdf_url=None,
            html_url=None,
            storage_path=str(storage_path),
            file_hash=file_hash,
            file_size=len(content),
            page_count=page_count,
            status=initial_status,
            retention_expires_at=datetime.combine(date.today() + timedelta(days=self.retention_days), datetime.min.time()),
            raw_excerpt=inferred_text[:2000] if inferred_text else None,
        )
        self.db.add(document)
        await self.db.flush()

        # 保存标签
        await self._save_document_tags(document.id, _guess_tags(inferred_title, inferred_text))

        # Task-02: 写入文本缓存
        if inferred_text:
            text_path = self.text_dir / f"{document.id:08d}.txt"
            text_path.write_text(inferred_text, encoding="utf-8")

        # Task-01: 按 auto_analyze 分支处理
        if auto_analyze:
            await self.analyze_document(document.id, force_refresh=True)
            await self.db.flush()
            # analyze_document 会更新状态为 analyzed

        # Task-08: 统一返回结构
        return {
            "document_id": document.id,
            "status": document.status,
            "is_duplicate": False,
            "document": self._document_to_dict(document, await self.get_latest_analysis(document.id)),
        }

    async def analyze_document(self, document_id: int, force_refresh: bool = False) -> Dict[str, Any]:
        result = await self.db.execute(
            select(ForeignResearchDocument).where(ForeignResearchDocument.id == document_id)
        )
        document = result.scalar_one_or_none()
        if not document:
            raise KeyError(f"document {document_id} not found")

        if not force_refresh:
            latest = await self.get_latest_analysis(document_id)
            if latest:
                return self._analysis_to_dict(latest) or {}

        text = document.raw_excerpt or ""
        text_path = self.text_dir / f"{document.id:08d}.txt"
        if text_path.exists():
            try:
                text = text_path.read_text(encoding="utf-8")
            except Exception:
                pass
        if not text and document.storage_path and Path(document.storage_path).exists():
            stored = Path(document.storage_path)
            if stored.suffix.lower() == ".pdf":
                try:
                    binary = stored.read_bytes()
                    text, _ = self._extract_pdf_text_bytes(binary)
                except Exception:
                    text = document.raw_excerpt or ""

        analysis_payload = self._call_bailian(
            title=document.title_original,
            text=text or document.raw_excerpt or "",
            language=document.language or "en",
            source_name=document.source_name,
            institution_name=document.institution_name,
        )

        analysis = ForeignResearchAnalysis(
            document_id=document.id,
            analysis_version=analysis_payload.get("analysis_version") or FOREIGN_RESEARCH_SNAPSHOT_VERSION,
            model_name=analysis_payload.get("model_name") or self.model,
            prompt_version=analysis_payload.get("prompt_version") or "v1",
            translation_cn=analysis_payload.get("translation_cn"),
            summary_cn=analysis_payload.get("summary_cn"),
            summary_en=analysis_payload.get("summary_en"),
            stance=analysis_payload.get("stance") or "neutral",
            confidence_score=float(analysis_payload.get("confidence_score") or 0.0),
            key_points_json=analysis_payload.get("key_points_json") or _to_json([]),
            drivers_json=analysis_payload.get("drivers_json") or _to_json([]),
            risks_json=analysis_payload.get("risks_json") or _to_json([]),
            invalid_conditions_json=analysis_payload.get("invalid_conditions_json") or _to_json([]),
            price_targets_json=analysis_payload.get("price_targets_json") or _to_json([]),
            rating_change=analysis_payload.get("rating_change"),
            macro_conclusion=analysis_payload.get("macro_conclusion"),
            industry_conclusion=analysis_payload.get("industry_conclusion"),
            equity_conclusion=analysis_payload.get("equity_conclusion"),
            topic_tags_json=analysis_payload.get("topic_tags_json") or _to_json([]),
            entity_mentions_json=analysis_payload.get("entity_mentions_json") or _to_json([]),
        )
        self.db.add(analysis)
        document.status = "analyzed"
        await self.db.flush()
        return self._analysis_to_dict(analysis) or {}

    async def crawl_source(
        self,
        source_id: int,
        lookback_days: Optional[int] = None,
        limit: int = 30,
        auto_analyze: bool = True,
    ) -> Dict[str, Any]:
        result = await self.db.execute(select(ForeignResearchSource).where(ForeignResearchSource.id == source_id))
        source = result.scalar_one_or_none()
        if not source:
            raise KeyError(f"source {source_id} not found")
        if not source.enabled:
            return {"source_id": source_id, "status": "disabled", "documents": 0}
        if not source.list_url:
            raise ValueError("source.list_url 不能为空")

        cutoff = self._today_lookback_cutoff(lookback_days)
        html, _, _ = await self._fetch_bytes(source.list_url)
        soup = BeautifulSoup(html.decode("utf-8", errors="ignore"), "html.parser")
        config = _from_json(source.config_json, {})
        include_patterns = config.get("include_patterns") or ["report", "research", "pdf", "strategy", "china"]
        exclude_patterns = config.get("exclude_patterns") or ["javascript:", "mailto:"]
        seen: List[str] = []
        ingested = 0
        skipped = 0
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            text = a.get_text(" ", strip=True) or ""
            href_l = href.lower()
            candidate_url = urljoin(source.list_url, href)
            if any(pat in href_l for pat in exclude_patterns):
                continue
            if include_patterns and not any(pat in (href_l + " " + text.lower()) for pat in include_patterns):
                continue
            if candidate_url in seen:
                continue
            seen.append(candidate_url)
            if href_l.endswith(".pdf") or "pdf" in href_l or "report" in href_l or text:
                try:
                    # Task-03: 不传递标题和日期，让 ingest_from_url 自己提取
                    # 传递 skip_if_outside_window=True 以跳过旧文档
                    res = await self.ingest_from_url(
                        url=candidate_url,
                        source_name=source.source_name,
                        institution_name=source.institution_name,
                        title=None,  # 让目标页面自己决定标题
                        publish_date=None,  # 让目标页面自己决定日期
                        external_id=None,
                        market_scope=None,
                        doc_type=None,
                        auto_analyze=auto_analyze,
                        source_id=source.id,
                        skip_if_outside_window=True,  # Task-03: 跳过窗口外的旧文档
                    )
                    if res:
                        # Task-03: 检查是否跳过
                        if res.get("status") == "skipped_outside_window":
                            skipped += 1
                        else:
                            ingested += 1
                except Exception:
                    continue
            if ingested >= limit:
                break

        return {
            "source_id": source.id,
            "source_name": source.source_name,
            "status": "completed",
            "documents": ingested,
            "skipped_outside_window": skipped,
            "lookback_days": lookback_days or self.retention_days,
        }

    async def cleanup_expired_documents(self) -> Dict[str, Any]:
        now = datetime.now()
        result = await self.db.execute(
            select(ForeignResearchDocument).where(
                ForeignResearchDocument.retention_expires_at.isnot(None),
                ForeignResearchDocument.retention_expires_at < now,
            )
        )
        items = result.scalars().all()
        deleted = 0
        for doc in items:
            if doc.storage_path and Path(doc.storage_path).exists():
                try:
                    Path(doc.storage_path).unlink()
                except Exception:
                    pass
            text_path = self.text_dir / f"{doc.id:08d}.txt"
            if text_path.exists():
                try:
                    text_path.unlink()
                except Exception:
                    pass
            doc.status = "expired"
            deleted += 1
        await self.db.flush()
        return {"expired": deleted}
