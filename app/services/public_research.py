"""
公开国际研究聚合服务

只展示官方公开可访问内容，用于在 investment 页面里补足
“国际研究 / 中国宏观 / 中国公司观察”的入口层。
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import time
from typing import Any, Dict, List, Optional


class PublicResearchService:
    """聚合公开国际研究入口和示例内容。"""

    CACHE_TTL_SECONDS = 1800

    SOURCE_CARDS: List[Dict[str, Any]] = [
        {
            "id": "goldman-sachs",
            "name": "Goldman Sachs",
            "group": "bank",
            "official_url": "https://www.goldmansachs.com/worldwide/greater-china/insights",
            "coverage": "Greater China 宏观、科技、政策与主题研究",
            "note": "公开观点页，不等同于付费卖方终端。",
        },
        {
            "id": "jpmorgan",
            "name": "JPMorgan",
            "group": "bank",
            "official_url": "https://www.jpmorgan.com/insights/international/china",
            "coverage": "China 宏观、配置与跨境业务观察",
            "note": "公开 Insights 与公开 PDF 入口为主。",
        },
        {
            "id": "ubs",
            "name": "UBS",
            "group": "bank",
            "official_url": "https://www.ubs.com/global/en/investment-bank/insights-and-data/spotlight-on-china.html",
            "coverage": "中国宏观、策略、投资观点专题页",
            "note": "部分深度内容可能只开放摘要或专题入口。",
        },
        {
            "id": "morgan-stanley",
            "name": "Morgan Stanley",
            "group": "bank",
            "official_url": "https://www.morganstanley.com/ideas",
            "coverage": "Ideas / Insights 公开文章与部分 PDF",
            "note": "公开页面可见，但全量卖方报告并不公开。",
        },
        {
            "id": "imf",
            "name": "IMF",
            "group": "macro",
            "official_url": "https://www.imf.org/en/Countries/CHN",
            "coverage": "中国 Article IV、工作论文、宏观评估 PDF",
            "note": "宏观 PDF 稳定，是公开源的核心补充。",
        },
        {
            "id": "world-bank",
            "name": "World Bank",
            "group": "macro",
            "official_url": "https://www.worldbank.org/en/country/china",
            "coverage": "中国专题、经济备忘录、东亚更新 PDF",
            "note": "偏中长期结构性研究，不是卖方节奏。",
        },
        {
            "id": "oecd",
            "name": "OECD",
            "group": "macro",
            "official_url": "https://www.oecd.org/china/",
            "coverage": "中国经济调查、结构改革与政策研究 PDF",
            "note": "适合补政策框架与中长期判断。",
        },
        {
            "id": "bis",
            "name": "BIS",
            "group": "macro",
            "official_url": "https://www.bis.org/topic/china.htm",
            "coverage": "跨境流动、货币、金融周期与亚洲区域专题",
            "note": "偏利率、流动性、金融条件框架。",
        },
    ]

    CURATED_ITEMS: List[Dict[str, Any]] = [
        {
            "id": "gs-china-economy-2026",
            "source_id": "goldman-sachs",
            "source": "Goldman Sachs",
            "source_group": "bank",
            "category": "macro",
            "title": "China's Economy Is Expected to Grow in 2026 Amid Surging Exports",
            "summary": "公开文章，适合跟踪高盛对中国增长、出口和政策节奏的框架判断。",
            "url": "https://www.goldmansachs.com/insights/articles/chinas-economy-expected-to-grow-in-2026-amid-surging-exports",
            "pdf_url": None,
            "format": "article",
            "published_at": "2026-01-08",
            "tags": ["China", "Macro", "Exports"],
        },
        {
            "id": "gs-ai-china-outlook",
            "source_id": "goldman-sachs",
            "source": "Goldman Sachs",
            "source_group": "bank",
            "category": "thematic",
            "title": "What Advanced AI Means for China's Economic Outlook",
            "summary": "公开主题文章，偏中国 AI 对增长与产业结构的影响。",
            "url": "https://www.goldmansachs.com/insights/articles/what-advanced-ai-means-for-chinas-economic-outlook",
            "pdf_url": None,
            "format": "article",
            "published_at": "2025-03-06",
            "tags": ["China", "AI", "Technology"],
        },
        {
            "id": "gs-china-net-zero",
            "source_id": "goldman-sachs",
            "source": "Goldman Sachs",
            "source_group": "bank",
            "category": "sector",
            "title": "China Net Zero: Official Report PDF",
            "summary": "高盛中国净零转型主题 PDF，适合作为新能源和产业政策参考材料。",
            "url": "https://www.goldmansachs.com/worldwide/greater-china/insights/china-net-zero-f",
            "pdf_url": "https://www.goldmansachs.com/worldwide/greater-china/insights/china-net-zero-f/report.pdf",
            "format": "pdf",
            "published_at": None,
            "tags": ["China", "Energy Transition", "Policy"],
        },
        {
            "id": "jpm-china-hub",
            "source_id": "jpmorgan",
            "source": "JPMorgan",
            "source_group": "bank",
            "category": "macro",
            "title": "China Insights Hub",
            "summary": "JPMorgan 官方 China 入口，适合跟踪公开中国观点和专题文章。",
            "url": "https://www.jpmorgan.com/insights/international/china",
            "pdf_url": None,
            "format": "hub",
            "published_at": None,
            "tags": ["China", "Macro", "Hub"],
        },
        {
            "id": "jpm-guide-to-china",
            "source_id": "jpmorgan",
            "source": "JPMorgan",
            "source_group": "bank",
            "category": "strategy",
            "title": "Guide to China",
            "summary": "J.P. Morgan Asset Management 公开 PDF，偏中国资产配置与市场理解。",
            "url": "https://am.jpmorgan.com/content/dam/jpm-am-aem/asiapacific/regional/en/insights/market-insights/guide-to-china.pdf",
            "pdf_url": "https://am.jpmorgan.com/content/dam/jpm-am-aem/asiapacific/regional/en/insights/market-insights/guide-to-china.pdf",
            "format": "pdf",
            "published_at": None,
            "tags": ["China", "Allocation", "Guide"],
        },
        {
            "id": "ubs-spotlight-on-china",
            "source_id": "ubs",
            "source": "UBS",
            "source_group": "bank",
            "category": "macro",
            "title": "Spotlight on China",
            "summary": "UBS 官方 China 专题页，汇总中国宏观与投资相关公开材料。",
            "url": "https://www.ubs.com/global/en/investment-bank/insights-and-data/spotlight-on-china.html",
            "pdf_url": None,
            "format": "hub",
            "published_at": None,
            "tags": ["China", "Macro", "Hub"],
        },
        {
            "id": "ubs-china-outlook",
            "source_id": "ubs",
            "source": "UBS",
            "source_group": "bank",
            "category": "strategy",
            "title": "China Outlook",
            "summary": "UBS 官方 China Outlook 文章入口，适合做中国市场年度/阶段判断补充。",
            "url": "https://www.ubs.com/global/en/investment-bank/insights-and-data/articles/china-outlook.html",
            "pdf_url": None,
            "format": "article",
            "published_at": None,
            "tags": ["China", "Outlook", "Strategy"],
        },
        {
            "id": "ms-china-wealth",
            "source_id": "morgan-stanley",
            "source": "Morgan Stanley",
            "source_group": "bank",
            "category": "strategy",
            "title": "China Wealth Management",
            "summary": "Morgan Stanley 公开文章，偏中国财富管理和资本市场结构主题。",
            "url": "https://www.morganstanley.com/ideas/china-wealth-management",
            "pdf_url": None,
            "format": "article",
            "published_at": None,
            "tags": ["China", "Capital Markets", "Wealth"],
        },
        {
            "id": "ms-china-ai",
            "source_id": "morgan-stanley",
            "source": "Morgan Stanley",
            "source_group": "bank",
            "category": "thematic",
            "title": "Why China Could Become an AI Leader",
            "summary": "Morgan Stanley 公开文章，偏中国 AI 产业和全球竞争格局。",
            "url": "https://www.morganstanley.com/insights/articles/china-ai-becoming-global-leader",
            "pdf_url": None,
            "format": "article",
            "published_at": None,
            "tags": ["China", "AI", "Technology"],
        },
        {
            "id": "ms-focus-list-china-hk",
            "source_id": "morgan-stanley",
            "source": "Morgan Stanley",
            "source_group": "bank",
            "category": "company",
            "title": "Focus List Changes: China / Hong Kong",
            "summary": "Morgan Stanley 官方 PDF，属于最接近你要求的公开可直达大行 PDF 之一。",
            "url": "https://www.morganstanley.com/content/dam/msdotcom/en/assets/pdfs/Focus_List_Changes-China_HK.pdf",
            "pdf_url": "https://www.morganstanley.com/content/dam/msdotcom/en/assets/pdfs/Focus_List_Changes-China_HK.pdf",
            "format": "pdf",
            "published_at": None,
            "tags": ["China", "Hong Kong", "Company"],
        },
        {
            "id": "imf-china-article-iv-2026",
            "source_id": "imf",
            "source": "IMF",
            "source_group": "macro",
            "category": "macro",
            "title": "People's Republic of China: 2026 Article IV Consultation",
            "summary": "IMF 对中国宏观、金融稳定、增长和政策的标准框架 PDF。",
            "url": "https://www.imf.org/en/Countries/CHN",
            "pdf_url": "https://www.imf.org/-/media/files/publications/cr/2026/english/1chnea2026001-source-pdf.pdf",
            "format": "pdf",
            "published_at": "2026-01-17",
            "tags": ["China", "Macro", "Policy"],
        },
        {
            "id": "world-bank-china-cem",
            "source_id": "world-bank",
            "source": "World Bank",
            "source_group": "macro",
            "category": "macro",
            "title": "China Country Economic Memorandum",
            "summary": "世界银行中国经济备忘录 PDF，偏增长结构、改革和中长期框架。",
            "url": "https://www.worldbank.org/en/country/china",
            "pdf_url": "https://documents1.worldbank.org/curated/en/099742112172513711/pdf/IDU-da604b39-c3d2-45a2-a326-52290df0d071.pdf",
            "format": "pdf",
            "published_at": "2025-12-17",
            "tags": ["China", "Macro", "Reform"],
        },
        {
            "id": "oecd-economic-survey-china",
            "source_id": "oecd",
            "source": "OECD",
            "source_group": "macro",
            "category": "macro",
            "title": "OECD Economic Surveys: China 2022",
            "summary": "OECD 中国经济调查 PDF，适合补结构改革与制度层面的长期视角。",
            "url": "https://www.oecd.org/china/",
            "pdf_url": "https://www.oecd.org/content/dam/oecd/en/publications/reports/2022/03/oecd-economic-surveys-china-2022_29d035e3/b0e499cf-en.pdf",
            "format": "pdf",
            "published_at": "2022-03-17",
            "tags": ["China", "Macro", "OECD"],
        },
        {
            "id": "bis-asia-financial-conditions",
            "source_id": "bis",
            "source": "BIS",
            "source_group": "macro",
            "category": "policy",
            "title": "BIS Papers: Asia Financial Conditions and Policy Transmission",
            "summary": "BIS 亚洲金融条件 PDF，适合补利率、流动性与跨境金融环境框架。",
            "url": "https://www.bis.org/topic/china.htm",
            "pdf_url": "https://www.bis.org/publ/bppdf/bispap148_e.pdf",
            "format": "pdf",
            "published_at": "2025-10-10",
            "tags": ["Asia", "Liquidity", "Rates"],
        },
    ]

    def __init__(self) -> None:
        self._cached_payload: Optional[Dict[str, Any]] = None
        self._cached_at: float = 0.0

    def _is_cache_fresh(self) -> bool:
        return (time.time() - self._cached_at) < self.CACHE_TTL_SECONDS

    def _parse_date(self, value: Optional[str]) -> datetime:
        if not value:
            return datetime.min
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return datetime.min

    def _build_summary(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        pdf_items = [item for item in items if item.get("format") == "pdf"]
        bank_items = [item for item in items if item.get("source_group") == "bank"]
        macro_items = [item for item in items if item.get("source_group") == "macro"]
        company_items = [item for item in items if item.get("category") == "company"]
        latest = max((self._parse_date(item.get("published_at")) for item in items), default=datetime.min)

        return {
            "total_items": len(items),
            "pdf_items": len(pdf_items),
            "bank_items": len(bank_items),
            "macro_items": len(macro_items),
            "company_items": len(company_items),
            "latest_date": latest.strftime("%Y-%m-%d") if latest != datetime.min else None,
            "source_count": len(self.SOURCE_CARDS),
        }

    def _build_payload(self) -> Dict[str, Any]:
        items = deepcopy(self.CURATED_ITEMS)
        items.sort(
            key=lambda item: (
                self._parse_date(item.get("published_at")),
                1 if item.get("format") == "pdf" else 0,
                item.get("source", ""),
            ),
            reverse=True,
        )

        payload = {
            "update_time": datetime.now().isoformat(),
            "public_only": True,
            "disclaimer": (
                "当前板块只展示官方公开可访问内容。它能补足国际大行和宏观机构的公开研究入口，"
                "但不能替代 Bloomberg、FactSet、慧博或卖方终端里的全量付费研报。"
            ),
            "summary": self._build_summary(items),
            "sources": deepcopy(self.SOURCE_CARDS),
            "items": items,
            "macro_pdfs": [item for item in items if item.get("source_group") == "macro" and item.get("format") == "pdf"],
            "bank_highlights": [item for item in items if item.get("source_group") == "bank"][:8],
        }
        return payload

    def get_public_research_hub(self) -> Dict[str, Any]:
        if self._cached_payload and self._is_cache_fresh():
            return deepcopy(self._cached_payload)

        payload = self._build_payload()
        self._cached_payload = payload
        self._cached_at = time.time()
        return deepcopy(payload)
