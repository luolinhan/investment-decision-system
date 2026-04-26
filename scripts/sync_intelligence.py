# -*- coding: utf-8 -*-
"""Continuously collect high-impact AI and market intelligence into SQLite.

The website reads these tables only. This script may run on Windows or on an
overseas collector; the storage target remains the Windows SQLite database.
"""

import argparse
import hashlib
import json
import mimetypes
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree as ET

import requests
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.db import get_sqlite_connection  # noqa: E402
    from app.services.intelligence_service import IntelligenceService  # noqa: E402
    from app.services.public_research import PublicResearchService  # noqa: E402
except Exception:
    get_sqlite_connection = None
    IntelligenceService = None
    PublicResearchService = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("INVESTMENT_DB_PATH", os.path.join(BASE_DIR, "data", "investment.db"))

USER_AGENT = os.getenv(
    "INTELLIGENCE_USER_AGENT",
    "InvestmentHubIntelligence/1.0 (+https://github.com/luolinhan/investment-decision-system)",
)
REQUEST_TIMEOUT = int(os.getenv("INTELLIGENCE_TIMEOUT_SECONDS", "25"))
BOOTSTRAP_SEED = os.getenv("INTELLIGENCE_BOOTSTRAP_SEED", "1") != "0"
COLLECTOR_MODE = os.getenv("INTELLIGENCE_COLLECTOR_MODE", "windows").strip().lower()
SEARCH_PROXY_URL = os.getenv("INTELLIGENCE_SEARCH_PROXY_URL", "").rstrip("/")


SOURCES: List[Dict[str, Any]] = [
    {
        "source_key": "openai_gpt55_release",
        "name": "OpenAI GPT-5.5 Release",
        "source_type": "official",
        "url": "https://openai.com/index/introducing-gpt-5-5/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "enabled": 0,
        "cadence_minutes": 15,
        "notes": "Official launch page for GPT-5.5. Direct fetch is blocked; use search signal and bootstrap facts.",
    },
    {
        "source_key": "openai_gpt55_system_card",
        "name": "OpenAI GPT-5.5 System Card",
        "source_type": "official",
        "url": "https://openai.com/index/gpt-5-5-system-card/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "enabled": 0,
        "cadence_minutes": 30,
        "notes": "Official safety and capability system card. Direct fetch is blocked; use search signal and bootstrap facts.",
    },
    {
        "source_key": "deepseek_api_docs",
        "name": "DeepSeek API Docs",
        "source_type": "official",
        "url": "https://api-docs.deepseek.com/",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official",
        "collection_method": "fixed_html",
        "cadence_minutes": 15,
        "notes": "DeepSeek API documentation and deprecation notices.",
    },
    {
        "source_key": "deepseek_hf_flash",
        "name": "DeepSeek V4 Flash on Hugging Face",
        "source_type": "model_repo",
        "url": "https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash/tree/main",
        "category": "ai_model",
        "priority": 0,
        "credibility": "primary_repo",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "Model repository for DeepSeek-V4-Flash.",
    },
    {
        "source_key": "huggingface_deepseek_models",
        "name": "Hugging Face DeepSeek Models API",
        "source_type": "model_repo",
        "url": "https://huggingface.co/api/models?author=deepseek-ai&sort=lastModified&direction=-1&limit=20",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 20,
        "notes": "Detect new DeepSeek model repositories.",
    },
    {
        "source_key": "github_deepseek_repos",
        "name": "DeepSeek GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/deepseek-ai/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "cadence_minutes": 30,
        "notes": "Detect official DeepSeek repository updates.",
    },
    {
        "source_key": "ap_deepseek_v4",
        "name": "AP DeepSeek V4 Coverage",
        "source_type": "credible_media",
        "url": "https://apnews.com/article/deepseek-ai-china-gpt-v4-d2ed33f2521917193616e061674d5f92",
        "category": "ai_model",
        "priority": 1,
        "credibility": "credible_media",
        "collection_method": "fixed_html",
        "cadence_minutes": 60,
        "notes": "Credible media confirmation for market context.",
    },
    {
        "source_key": "openai_gpt55_search",
        "name": "OpenAI GPT-5.5 Search Signal",
        "source_type": "official_search",
        "url": "search://site:openai.com/index/introducing-gpt-5-5 GPT-5.5",
        "query": "site:openai.com/index/introducing-gpt-5-5 GPT-5.5",
        "category": "ai_model",
        "priority": 0,
        "credibility": "official_search",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "OpenAI blocks direct fetch; search-proxy captures official page title/snippet.",
    },
    {
        "source_key": "anthropic_news",
        "name": "Anthropic News",
        "source_type": "official",
        "url": "https://www.anthropic.com/news",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "Official Anthropic model and product news.",
    },
    {
        "source_key": "google_ai_blog_rss",
        "name": "Google AI Blog RSS",
        "source_type": "official_feed",
        "url": "https://blog.google/technology/ai/rss/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "Official Google AI announcements.",
    },
    {
        "source_key": "google_deepmind_blog",
        "name": "Google DeepMind Blog",
        "source_type": "official",
        "url": "https://deepmind.google/discover/blog/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Google DeepMind research and model announcements.",
    },
    {
        "source_key": "meta_ai_blog",
        "name": "Meta AI Blog",
        "source_type": "official",
        "url": "https://ai.meta.com/blog/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official Meta AI research and product announcements.",
    },
    {
        "source_key": "mistral_news",
        "name": "Mistral AI News",
        "source_type": "official",
        "url": "https://mistral.ai/news/",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official Mistral AI announcements.",
    },
    {
        "source_key": "xai_news",
        "name": "xAI News",
        "source_type": "official",
        "url": "https://x.ai/news",
        "category": "ai_model",
        "priority": 1,
        "credibility": "official",
        "collection_method": "fixed_html",
        "collector": "aliyun",
        "enabled": 0,
        "cadence_minutes": 45,
        "notes": "Official xAI announcements. Direct fetch currently returns 403; disabled until a stable feed is available.",
    },
    {
        "source_key": "huggingface_trending_models",
        "name": "Hugging Face Most Liked Models",
        "source_type": "model_repo",
        "url": "https://huggingface.co/api/models?sort=likes&direction=-1&limit=25",
        "category": "ai_model",
        "priority": 1,
        "credibility": "model_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 30,
        "notes": "High-attention model repositories for model-cycle detection.",
    },
    {
        "source_key": "github_openai_repos",
        "name": "OpenAI GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/openai/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Official OpenAI repository updates.",
    },
    {
        "source_key": "github_modelcontextprotocol_repos",
        "name": "Model Context Protocol GitHub Repositories",
        "source_type": "code_repo",
        "url": "https://api.github.com/orgs/modelcontextprotocol/repos?sort=updated&per_page=50",
        "category": "ai_model",
        "priority": 1,
        "credibility": "primary_repo",
        "collection_method": "json_list",
        "collector": "aliyun",
        "cadence_minutes": 45,
        "notes": "Agent/tooling ecosystem signal.",
    },
    {
        "source_key": "arxiv_ai_recent",
        "name": "arXiv AI Recent",
        "source_type": "research_feed",
        "url": "https://export.arxiv.org/api/query?search_query=cat:cs.AI+OR+cat:cs.LG&sortBy=submittedDate&sortOrder=descending&max_results=25",
        "category": "ai_research",
        "priority": 2,
        "credibility": "research_preprint",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 120,
        "notes": "Recent AI/ML papers; P2 research signal only.",
    },
    {
        "source_key": "stanford_hai_ai_index",
        "name": "Stanford HAI AI Index",
        "source_type": "research_search",
        "url": "search://site:hai.stanford.edu/ai-index/2026-ai-index-report (\"AI Index\" OR \"Research and Development\" OR \"Technical Performance\" OR \"Policy and Governance\")",
        "query": "site:hai.stanford.edu/ai-index/2026-ai-index-report (\"AI Index\" OR \"Research and Development\" OR \"Technical Performance\" OR \"Policy and Governance\")",
        "category": "ai_research",
        "priority": 1,
        "credibility": "research_institute",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 8,
        "expand_fetch": 1,
        "fetch_max_length": 9000,
        "include_terms": ["ai index", "artificial intelligence", "research and development", "technical performance", "policy and governance"],
        "exclude_terms": ["privacy", "cookies", "newsletter", "careers"],
        "report_type": "research_report",
        "investment_relevance": "Use as a baseline for model capability, compute intensity, policy, adoption and capex assumptions across AI infrastructure and applications.",
        "investment_relevance_zh": "作为 AI 基础设施与应用链条的底层基准材料，用于校准模型能力、算力强度、政策、采用率和资本开支假设。",
        "thesis_template": "Stanford AI Index provides benchmark-grade evidence for model progress, compute demand, policy and adoption; use it to anchor medium-term AI cycle assumptions.",
        "thesis_template_zh": "Stanford AI Index 提供模型进展、算力需求、政策与采用率的基准证据，可作为中期 AI 周期判断的底层锚点。",
        "notes": "Curated US institutional research source for AI cycle baseline evidence.",
    },
    {
        "source_key": "brookings_ai_research",
        "name": "Brookings AI Research",
        "source_type": "research_search",
        "url": "search://site:brookings.edu/articles (AI OR \"artificial intelligence\") (research OR report OR analysis) Brookings",
        "query": "site:brookings.edu/articles (AI OR \"artificial intelligence\") (research OR report OR analysis) Brookings",
        "category": "ai_research",
        "priority": 1,
        "credibility": "think_tank",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 6,
        "expand_fetch": 1,
        "fetch_max_length": 8000,
        "include_terms": ["ai", "artificial intelligence", "labor", "data center", "regulatory", "adoption"],
        "exclude_terms": ["newsletter", "event", "podcast", "donate"],
        "report_type": "policy_analysis",
        "investment_relevance": "Brookings research helps frame AI adoption, labor market effects, regulation and energy/data-center externalities that matter for medium-term sector allocation.",
        "investment_relevance_zh": "Brookings 研究适合校准 AI 采用、劳动力影响、监管和能源/数据中心外部性，对中期行业配置有参考价值。",
        "thesis_template": "Brookings analysis is useful for validating medium-term AI adoption, labor and regulation narratives before mapping them to cloud, software and power-demand exposures.",
        "thesis_template_zh": "Brookings 分析适合验证 AI 采用、劳动力与监管叙事，再映射到云、软件和电力需求等资产链条。",
        "notes": "US think-tank analysis for AI adoption, labor and policy.",
    },
    {
        "source_key": "cset_ai_publications",
        "name": "CSET AI Publications",
        "source_type": "research_search",
        "url": "search://site:cset.georgetown.edu/publication (AI OR \"artificial intelligence\") (report OR paper OR publication)",
        "query": "site:cset.georgetown.edu/publication (AI OR \"artificial intelligence\") (report OR paper OR publication)",
        "category": "ai_research",
        "priority": 1,
        "credibility": "research_institute",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 6,
        "expand_fetch": 1,
        "fetch_max_length": 8000,
        "include_terms": ["ai", "artificial intelligence", "compute", "governance", "robotics", "frontier"],
        "exclude_terms": ["events", "newsletter", "jobs", "team"],
        "report_type": "policy_research",
        "investment_relevance": "CSET publications help validate frontier governance, physical AI, compute and China-US competition narratives relevant to semis, robotics and cyber-AI chains.",
        "investment_relevance_zh": "CSET 材料适合验证前沿治理、物理 AI、算力和中美竞争叙事，可映射到半导体、机器人和网络安全链条。",
        "thesis_template": "CSET publications are useful for tracking frontier AI governance, physical AI and compute competition; use them as validation inputs rather than standalone trade signals.",
        "thesis_template_zh": "CSET 研究适合跟踪前沿 AI 治理、物理 AI 和算力竞争，更多作为验证输入而非独立交易信号。",
        "notes": "Georgetown CSET research and policy publications.",
    },
    {
        "source_key": "rand_ai_research",
        "name": "RAND AI Research",
        "source_type": "research_search",
        "url": "search://site:rand.org/pubs/research_reports (AI OR \"artificial intelligence\") RAND",
        "query": "site:rand.org/pubs/research_reports (AI OR \"artificial intelligence\") RAND",
        "category": "ai_research",
        "priority": 1,
        "credibility": "think_tank",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 360,
        "result_limit": 6,
        "expand_fetch": 1,
        "fetch_max_length": 7000,
        "include_terms": ["ai", "artificial intelligence", "r&d", "compute", "security", "scenario"],
        "exclude_terms": ["jobs", "career", "newsletter", "podcast"],
        "report_type": "strategic_research",
        "investment_relevance": "RAND research is useful for stress-testing AI R&D, defense demand, national strategy and downside scenarios around compute, safety and deployment.",
        "investment_relevance_zh": "RAND 研究适合做 AI 研发、国防需求、国家战略以及算力/安全/部署风险情景的压力测试。",
        "thesis_template": "RAND work is best used to stress-test AI infrastructure and policy path assumptions rather than as a short-term catalyst feed.",
        "thesis_template_zh": "RAND 更适合用于压力测试 AI 基础设施和政策路径假设，而不是短期催化剂流。",
        "notes": "RAND public research reports on AI strategy and R&D.",
    },
    {
        "source_key": "nber_ai_working_papers",
        "name": "NBER AI Working Papers",
        "source_type": "research_search",
        "url": "search://site:nber.org/papers (\"artificial intelligence\" OR \"generative AI\" OR \"AI agents\" OR \"AI in science\")",
        "query": "site:nber.org/papers (\"artificial intelligence\" OR \"generative AI\" OR \"AI agents\" OR \"AI in science\")",
        "category": "ai_research",
        "priority": 1,
        "credibility": "research_institute",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 8,
        "expand_fetch": 1,
        "fetch_max_length": 9000,
        "include_terms": ["artificial intelligence", "generative ai", "ai agents", "ai in science", "labor market", "productivity"],
        "exclude_terms": ["newsletter", "conference", "summer institute", "podcast"],
        "report_type": "economic_research",
        "investment_relevance": "NBER working papers help quantify AI effects on productivity, labor demand, competition and R&D, which is useful for medium-term macro and sector allocation work.",
        "investment_relevance_zh": "NBER 工作论文适合量化 AI 对生产率、劳动需求、竞争格局和研发的影响，可用于中期宏观和行业配置校准。",
        "thesis_template": "Use NBER AI papers to validate medium-term productivity, labor and competition narratives before mapping them to software, cloud, semis and enterprise demand.",
        "thesis_template_zh": "NBER AI 论文适合验证中期生产率、劳动力和竞争叙事，再映射到软件、云、半导体和企业需求链条。",
        "notes": "Primary academic economics working papers on AI, labor, productivity and R&D.",
    },
    {
        "source_key": "oecd_ai_papers",
        "name": "OECD AI Publications",
        "source_type": "research_search",
        "url": "search://site:oecd.org/en/publications (\"artificial intelligence\" OR \"agentic AI\" OR \"OECD.AI Index\" OR \"AI capability indicators\")",
        "query": "site:oecd.org/en/publications (\"artificial intelligence\" OR \"agentic AI\" OR \"OECD.AI Index\" OR \"AI capability indicators\")",
        "category": "ai_research",
        "priority": 1,
        "credibility": "multilateral_research",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 8,
        "expand_fetch": 1,
        "fetch_max_length": 9000,
        "include_terms": ["artificial intelligence", "agentic ai", "oecd.ai index", "capability indicators", "ai in science", "ai in health"],
        "exclude_terms": ["newsletter", "careers", "event", "video"],
        "report_type": "policy_report",
        "investment_relevance": "OECD publications are useful for tracking AI adoption, capability measurement, governance and sector diffusion across health, education, SMEs and the public sector.",
        "investment_relevance_zh": "OECD 出版物适合跟踪 AI 采用、能力测度、治理以及在医疗、教育、中小企业和公共部门的扩散路径。",
        "thesis_template": "OECD AI reports help anchor cross-country adoption, capability and policy diffusion scenarios that feed into medium-term software, services and infrastructure views.",
        "thesis_template_zh": "OECD AI 报告适合锚定跨国采用率、能力与政策扩散情景，并映射到中期软件、服务和基础设施判断。",
        "notes": "OECD AI paper series, AI capability indicators and sector deployment reports.",
    },
    {
        "source_key": "imf_ai_publications",
        "name": "IMF AI Publications",
        "source_type": "research_search",
        "url": "search://site:imf.org/en/publications (\"artificial intelligence\" OR \"agentic AI\" OR \"AI\") (\"working paper\" OR \"IMF Notes\" OR \"staff discussion note\")",
        "query": "site:imf.org/en/publications (\"artificial intelligence\" OR \"agentic AI\" OR \"AI\") (\"working paper\" OR \"IMF Notes\" OR \"staff discussion note\")",
        "category": "ai_research",
        "priority": 1,
        "credibility": "multilateral_research",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 8,
        "expand_fetch": 1,
        "fetch_max_length": 9000,
        "include_terms": ["artificial intelligence", "agentic ai", "future of work", "productivity", "payments", "global impact of ai"],
        "exclude_terms": ["country report", "article iv", "newsletter", "capacity development"],
        "report_type": "macro_research",
        "investment_relevance": "IMF AI notes and working papers are useful for framing AI productivity, labor, payments and cross-country readiness scenarios relevant to macro, banks and software.",
        "investment_relevance_zh": "IMF 关于 AI 的 notes 和工作论文适合框定生产率、劳动力、支付体系和国家准备度情景，可映射到宏观、银行和软件链条。",
        "thesis_template": "Use IMF AI publications to stress-test global productivity, labor-market and financial-system scenarios rather than to source short-term catalysts.",
        "thesis_template_zh": "IMF AI 出版物更适合用于压力测试全球生产率、劳动力市场和金融系统情景，而不是获取短期催化。",
        "notes": "IMF Notes and working papers on AI, productivity, payments and global macro transmission.",
    },
    {
        "source_key": "bis_ai_research",
        "name": "BIS AI Research",
        "source_type": "research_search",
        "url": "search://site:bis.org/publ (\"artificial intelligence\" OR \"generative AI\" OR \"AI supply chain\" OR \"AI adoption\")",
        "query": "site:bis.org/publ (\"artificial intelligence\" OR \"generative AI\" OR \"AI supply chain\" OR \"AI adoption\")",
        "category": "ai_research",
        "priority": 1,
        "credibility": "central_bank_research",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 240,
        "result_limit": 8,
        "expand_fetch": 1,
        "fetch_max_length": 9000,
        "include_terms": ["artificial intelligence", "generative ai", "ai supply chain", "ai adoption", "productivity", "financial markets"],
        "exclude_terms": ["speech", "event", "newsletter", "press release"],
        "report_type": "financial_research",
        "investment_relevance": "BIS AI work helps map AI supply-chain concentration, adoption, productivity and financial-market implications relevant to semis, cloud, fintech and banks.",
        "investment_relevance_zh": "BIS 的 AI 研究适合刻画 AI 供应链集中度、采用率、生产率与金融市场影响，可映射到半导体、云、金融科技和银行。",
        "thesis_template": "Use BIS AI papers to validate AI supply-chain structure, financial transmission and adoption evidence before extrapolating to infrastructure and financial exposures.",
        "thesis_template_zh": "BIS AI 论文适合验证 AI 供应链结构、金融传导和采用证据，再外推到基础设施和金融资产暴露。",
        "notes": "BIS working papers and papers on AI supply chains, adoption and financial transmission.",
    },
    {
        "source_key": "anthropic_research_search",
        "name": "Anthropic Research",
        "source_type": "research_search",
        "url": "search://site:anthropic.com (research OR \"system card\" OR interpretability OR alignment) Anthropic",
        "query": "site:anthropic.com (research OR \"system card\" OR interpretability OR alignment) Anthropic",
        "category": "ai_research",
        "priority": 1,
        "credibility": "official_research",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 180,
        "result_limit": 6,
        "expand_fetch": 1,
        "fetch_max_length": 8000,
        "include_terms": ["anthropic", "system card", "alignment", "interpretability", "research"],
        "exclude_terms": ["careers", "policy", "events", "newsroom"],
        "report_type": "official_research",
        "investment_relevance": "Anthropic research and system-card style materials help track frontier model capability, safety posture and enterprise deployment implications.",
        "investment_relevance_zh": "Anthropic 的研究和 system card 类材料适合跟踪前沿模型能力、安全取向和企业部署含义。",
        "thesis_template": "Use Anthropic research outputs as primary-source validation for frontier capability and safety claims before mapping them to enterprise AI spending and infrastructure demand.",
        "thesis_template_zh": "Anthropic 研究材料可作为前沿能力与安全主张的第一手验证来源，再映射到企业 AI 开支和基础设施需求。",
        "notes": "Official Anthropic research and safety materials beyond the generic newsroom page.",
    },
    {
        "source_key": "openai_research_search",
        "name": "OpenAI Research Materials",
        "source_type": "research_search",
        "url": "search://site:openai.com/index (\"system card\" OR research OR safety OR model) OpenAI",
        "query": "site:openai.com/index (\"system card\" OR research OR safety OR model) OpenAI",
        "category": "ai_research",
        "priority": 1,
        "credibility": "official_research",
        "collection_method": "search_proxy",
        "collector": "aliyun",
        "cadence_minutes": 180,
        "result_limit": 6,
        "expand_fetch": 1,
        "fetch_max_length": 8000,
        "include_terms": ["openai", "system card", "safety", "research", "model"],
        "exclude_terms": ["careers", "jobs", "pricing", "api reference"],
        "report_type": "official_research",
        "investment_relevance": "OpenAI primary materials help verify frontier capability, safety and commercialization claims that shape AI software and infrastructure expectations.",
        "investment_relevance_zh": "OpenAI 的第一手材料适合验证前沿能力、安全和商业化主张，这些主张会影响 AI 软件和基础设施预期。",
        "thesis_template": "Use OpenAI research and system-card materials as primary-source checkpoints for frontier capability and commercialization narratives.",
        "thesis_template_zh": "OpenAI 的研究和 system card 材料适合作为前沿能力与商业化叙事的第一手校验点。",
        "notes": "OpenAI research and system-card style materials captured via search proxy.",
    },
    {
        "source_key": "sec_current_8k",
        "name": "SEC Current 8-K Feed",
        "source_type": "regulatory_feed",
        "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&owner=include&count=40&output=atom",
        "category": "company_filing",
        "priority": 2,
        "credibility": "regulatory",
        "collection_method": "feed",
        "collector": "aliyun",
        "enabled": 0,
        "cadence_minutes": 60,
        "notes": "US company current reports for cross-market shocks. Disabled until SEC user-agent/access policy is handled.",
    },
    {
        "source_key": "fda_press_rss",
        "name": "FDA Press Announcements",
        "source_type": "regulatory_feed",
        "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
        "category": "biotech",
        "priority": 2,
        "credibility": "regulatory",
        "collection_method": "feed",
        "collector": "aliyun",
        "cadence_minutes": 120,
        "notes": "FDA press releases for global biotech and approval signals.",
    },
    {
        "source_key": "bootstrap_verified_events",
        "name": "Verified Bootstrap Events",
        "source_type": "manual_seed",
        "url": "local://verified-intelligence-bootstrap",
        "category": "ai_model",
        "priority": 0,
        "credibility": "verified_seed",
        "collection_method": "bootstrap",
        "cadence_minutes": 1440,
        "notes": "Prevents empty pages when foreign network collection is temporarily unavailable.",
    },
]

PUBLIC_RESEARCH_SOURCE_CATALOG: List[Dict[str, Any]] = [
    {
        "source_key": "public_goldman_china",
        "public_source_id": "goldman-sachs",
        "name": "Goldman Sachs Greater China Insights",
        "source_type": "public_bank",
        "url": "https://www.goldmansachs.com/worldwide/greater-china/insights",
        "category": "research_library",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "sellside",
        "cadence_minutes": 720,
        "notes": "Public China and sector insight articles/PDFs from Goldman Sachs.",
    },
    {
        "source_key": "public_jpmorgan_china",
        "public_source_id": "jpmorgan",
        "name": "JPMorgan China Insights",
        "source_type": "public_bank",
        "url": "https://www.jpmorgan.com/insights/international/china",
        "category": "research_library",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "sellside",
        "cadence_minutes": 720,
        "notes": "Public China insights and PDF guide material from JPMorgan.",
    },
    {
        "source_key": "public_ubs_china",
        "public_source_id": "ubs",
        "name": "UBS Spotlight on China",
        "source_type": "public_bank",
        "url": "https://www.ubs.com/global/en/investment-bank/insights-and-data/spotlight-on-china.html",
        "category": "research_library",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "sellside",
        "cadence_minutes": 720,
        "notes": "Public China thematic insights from UBS.",
    },
    {
        "source_key": "public_morgan_stanley_china",
        "public_source_id": "morgan-stanley",
        "name": "Morgan Stanley Ideas / China",
        "source_type": "public_bank",
        "url": "https://www.morganstanley.com/ideas",
        "category": "research_library",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "sellside",
        "cadence_minutes": 720,
        "notes": "Public ideas articles and selected PDF material from Morgan Stanley.",
    },
    {
        "source_key": "public_imf_china",
        "public_source_id": "imf",
        "name": "IMF China Country Research",
        "source_type": "macro_publication",
        "url": "https://www.imf.org/en/Countries/CHN",
        "category": "research_library",
        "priority": 1,
        "credibility": "official",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "official",
        "cadence_minutes": 1440,
        "notes": "Public IMF China macro reports and Article IV materials.",
    },
    {
        "source_key": "public_world_bank_china",
        "public_source_id": "world-bank",
        "name": "World Bank China Research",
        "source_type": "macro_publication",
        "url": "https://www.worldbank.org/en/country/china",
        "category": "research_library",
        "priority": 1,
        "credibility": "official",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "official",
        "cadence_minutes": 1440,
        "notes": "Public World Bank China research and economic memorandum materials.",
    },
    {
        "source_key": "public_oecd_china",
        "public_source_id": "oecd",
        "name": "OECD China Research",
        "source_type": "macro_publication",
        "url": "https://www.oecd.org/china/",
        "category": "research_library",
        "priority": 1,
        "credibility": "official",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "official",
        "cadence_minutes": 1440,
        "notes": "Public OECD China economic survey materials.",
    },
    {
        "source_key": "public_bis_china",
        "public_source_id": "bis",
        "name": "BIS China / Asia Financial Research",
        "source_type": "macro_publication",
        "url": "https://www.bis.org/topic/china.htm",
        "category": "research_library",
        "priority": 1,
        "credibility": "official",
        "collection_method": "curated_public_research",
        "publisher_region": "overseas",
        "source_tier": "official",
        "cadence_minutes": 1440,
        "notes": "Public BIS papers relevant to China and Asia financial conditions.",
    },
]

DOMESTIC_RESEARCH_FOCUS_SOURCES: List[Dict[str, Any]] = [
    {
        "source_key": "eastmoney_focus_semi",
        "name": "Eastmoney Public Research - 芯片半导体",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["688981", "603501", "603986"],
        "query_keyword": "半导体",
        "focus_area": "芯片半导体",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side research around semiconductor and chip themes.",
    },
    {
        "source_key": "eastmoney_focus_ai",
        "name": "Eastmoney Public Research - AI",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["002230", "688111", "300308"],
        "query_keyword": "人工智能",
        "focus_area": "AI",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side AI research.",
    },
    {
        "source_key": "eastmoney_focus_robotics",
        "name": "Eastmoney Public Research - 机器人",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["300024", "002747", "002050"],
        "query_keyword": "机器人",
        "focus_area": "机器人",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side robotics research.",
    },
    {
        "source_key": "eastmoney_focus_biotech",
        "name": "Eastmoney Public Research - 创新药",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["600276", "688235", "600196"],
        "query_keyword": "创新药",
        "focus_area": "创新药",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side innovative drug research.",
    },
    {
        "source_key": "eastmoney_focus_pv",
        "name": "Eastmoney Public Research - 光伏",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["601012", "600438", "002459"],
        "query_keyword": "光伏",
        "focus_area": "光伏",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side PV research.",
    },
    {
        "source_key": "eastmoney_focus_nuclear",
        "name": "Eastmoney Public Research - 核电",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["601985", "601611", "002130"],
        "query_keyword": "核电",
        "focus_area": "核电",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side nuclear power research.",
    },
    {
        "source_key": "eastmoney_focus_hog",
        "name": "Eastmoney Public Research - 养猪",
        "source_type": "broker_feed",
        "url": "https://reportapi.eastmoney.com/report/list",
        "category": "domestic_research",
        "priority": 1,
        "credibility": "sellside_public",
        "collection_method": "akshare_stock_bucket",
        "symbols": ["002714", "002567", "002840"],
        "query_keyword": "生猪养殖",
        "focus_area": "养猪",
        "publisher_region": "domestic",
        "source_tier": "sellside",
        "target_scope": "industry",
        "cadence_minutes": 240,
        "notes": "Public domestic sell-side hog cycle research.",
    },
]

SOURCES.extend(PUBLIC_RESEARCH_SOURCE_CATALOG)
SOURCES.extend(DOMESTIC_RESEARCH_FOCUS_SOURCES)


BOOTSTRAP_DOCUMENTS: List[Dict[str, Any]] = [
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://openai.com/index/introducing-gpt-5-5/",
        "title": "Introducing GPT-5.5",
        "published_at": "2026-04-24",
        "summary": "OpenAI released GPT-5.5 for complex coding, research, document/spreadsheet work, tool use and real-world workflows.",
        "raw_text": (
            "GPT-5.5 is OpenAI's frontier model for complex real-world work. "
            "It is available in ChatGPT and Codex, with API availability expected soon. "
            "Official materials describe stronger coding, research, document, spreadsheet, tool-use and scientific workflow capabilities. "
            "The release page and pricing page indicate GPT-5.5 pricing of $5 per million input tokens and $30 per million output tokens, "
            "with GPT-5.5 Pro at higher pricing. The context window is described as 1M tokens."
        ),
        "metadata": {"language": "en", "bootstrap": True},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://openai.com/index/gpt-5-5-system-card/",
        "title": "GPT-5.5 System Card",
        "published_at": "2026-04-24",
        "summary": "OpenAI published the GPT-5.5 system card as a primary safety and capability document.",
        "raw_text": (
            "OpenAI published the GPT-5.5 system card. This document is a primary source for capability, safety and deployment notes. "
            "For investors it should be linked to AI agent commercialization, cloud workloads, enterprise productivity and data-center demand."
        ),
        "metadata": {"language": "en", "bootstrap": True, "report_type": "system_card"},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://api-docs.deepseek.com/",
        "title": "DeepSeek API Docs list DeepSeek V4 models",
        "published_at": "2026-04-24",
        "summary": "DeepSeek API documentation lists deepseek-v4-flash and deepseek-v4-pro, with old chat/reasoner model deprecation on 2026/07/24.",
        "raw_text": (
            "DeepSeek API docs list deepseek-v4-flash and deepseek-v4-pro. "
            "The docs state that old deepseek-chat and deepseek-reasoner models will be deprecated on 2026/07/24. "
            "DeepSeek V4 materials reference 1M context and agentic capabilities."
        ),
        "metadata": {"language": "en", "bootstrap": True},
    },
    {
        "source_key": "bootstrap_verified_events",
        "url": "https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash/tree/main",
        "title": "DeepSeek-V4-Flash model repository",
        "published_at": "2026-04-24",
        "summary": "DeepSeek-V4-Flash appears on Hugging Face under the deepseek-ai namespace.",
        "raw_text": (
            "Hugging Face hosts DeepSeek-V4-Flash under deepseek-ai. "
            "The model repository is a primary distribution signal and should be tied to open model availability, inference cost and China AI competitiveness."
        ),
        "metadata": {"language": "en", "bootstrap": True, "report_type": "model_card"},
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize_text(text)


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


FOCUS_AREA_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "芯片半导体": ("半导体", "芯片", "semiconductor", "chip", "foundry", "wafer", "asml", "tsmc", "nvidia", "gpu"),
    "AI": ("ai", "artificial intelligence", "llm", "model", "agent", "inference", "gpu", "datacenter", "data center", "cloud"),
    "机器人": ("机器人", "robot", "robotics", "automation", "humanoid", "industrial robot", "physical ai"),
    "创新药": ("创新药", "biotech", "drug", "therapy", "clinical", "fda", "nda", "bla", "bd deal", "licensing"),
    "光伏": ("光伏", "solar", "pv", "photovoltaic", "module", "wafer", "cell"),
    "核电": ("核电", "nuclear", "uranium", "reactor", "small modular reactor", "smr"),
    "养猪": ("养猪", "生猪", "母猪", "猪价", "hog", "swine", "pork"),
}

MACRO_SCOPE_TERMS = ("macro", "economy", "article iv", "country economic memorandum", "economic survey", "financial conditions")
COMPANY_SCOPE_TERMS = ("focus list", "stock", "company", "holdings", "评级", "buy", "overweight")
POLICY_SCOPE_TERMS = ("policy", "governance", "regulation", "regulatory", "article iv", "survey")


def content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False) if value not in (None, "") else ""


def dedupe_strings(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        item = normalize_text(value)
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def detect_focus_areas(*parts: Any) -> List[str]:
    haystack = normalize_text(" ".join(str(part or "") for part in parts)).lower()
    hits: List[str] = []
    for focus_area, keywords in FOCUS_AREA_KEYWORDS.items():
        if any(keyword.lower() in haystack for keyword in keywords):
            hits.append(focus_area)
    return dedupe_strings(hits)


def infer_target_scope(*parts: Any) -> str:
    haystack = normalize_text(" ".join(str(part or "") for part in parts)).lower()
    if any(term in haystack for term in MACRO_SCOPE_TERMS):
        return "macro"
    if any(term in haystack for term in POLICY_SCOPE_TERMS):
        return "policy"
    if any(term in haystack for term in COMPANY_SCOPE_TERMS):
        return "company"
    return "industry"


def sanitize_filename(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", normalize_text(value or "")).strip("-._")
    return cleaned[:96] or fallback


def extract_date_from_text(text: str) -> str:
    raw = normalize_text(text)
    if not raw:
        return ""
    month_formats = ("%b %d, %Y", "%B %d, %Y")
    patterns = [
        r"\b([A-Z][a-z]{2,8}\s+\d{1,2},\s+\d{4})\b",
        r"\b(\d{4}-\d{2}-\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw)
        if not match:
            continue
        value = match.group(1)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return value
        for fmt in month_formats:
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                continue
    return ""


def get_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en,zh-CN;q=0.8,zh;q=0.7"})
    proxy = (os.getenv("INTELLIGENCE_PROXY") or "").strip()
    if proxy and proxy.lower() not in {"direct", "none", "0", "false"}:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def ensure_sources(conn: sqlite3.Connection) -> None:
    for source in SOURCES:
        conn.execute(
            """
            INSERT INTO source_registry
            (source_key, name, source_type, url, category, priority, credibility,
             collection_method, enabled, cadence_minutes, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                name=excluded.name,
                source_type=excluded.source_type,
                url=excluded.url,
                category=excluded.category,
                priority=excluded.priority,
                credibility=excluded.credibility,
                collection_method=excluded.collection_method,
                enabled=excluded.enabled,
                cadence_minutes=excluded.cadence_minutes,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            (
                source["source_key"],
                source["name"],
                source["source_type"],
                source["url"],
                source.get("category"),
                source.get("priority", 2),
                source.get("credibility", "primary"),
                source.get("collection_method", "http"),
                int(source.get("enabled", 1)),
                source.get("cadence_minutes", 30),
                source.get("notes"),
                now_iso(),
            ),
        )


def html_to_document(source: Dict[str, Any], url: str, html: str) -> Dict[str, Any]:
    if BeautifulSoup is None:
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html or "", flags=re.IGNORECASE | re.DOTALL)
        title = normalize_text(re.sub(r"<[^>]+>", " ", title_match.group(1))) if title_match else source["name"]
        desc_match = re.search(
            r"<meta[^>]+(?:name|property)=[\"'](?:description|og:description)[\"'][^>]+content=[\"'](.*?)[\"']",
            html or "",
            flags=re.IGNORECASE | re.DOTALL,
        )
        description = normalize_text(desc_match.group(1)) if desc_match else ""
        text = normalize_text(re.sub(r"<[^>]+>", " ", html or ""))
        return {
            "source_key": source["source_key"],
            "url": url,
            "title": title,
            "published_at": "",
            "summary": description or text[:500],
            "raw_text": text[:30000],
            "metadata": {"language": "en", "content_type": "html"},
        }

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = ""
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title = og_title["content"]
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else source["name"]

    description = ""
    for selector in ({"name": "description"}, {"property": "og:description"}):
        meta = soup.find("meta", attrs=selector)
        if meta and meta.get("content"):
            description = meta["content"]
            break

    published_at = ""
    for selector in ({"property": "article:published_time"}, {"name": "date"}, {"name": "pubdate"}):
        meta = soup.find("meta", attrs=selector)
        if meta and meta.get("content"):
            published_at = meta["content"]
            break

    text = soup.get_text(" ", strip=True)
    return {
        "source_key": source["source_key"],
        "url": url,
        "title": normalize_text(title),
        "published_at": published_at,
        "summary": normalize_text(description or text[:500]),
        "raw_text": normalize_text(text)[:30000],
        "metadata": {"language": "en", "content_type": "html"},
    }


def fetch_fixed_html(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return [html_to_document(source, source["url"], resp.text)]


def fetch_json_list(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    docs: List[Dict[str, Any]] = []
    if isinstance(data, list):
        items = data
    else:
        items = data.get("items") or data.get("models") or []

    for item in items[:30]:
        if not isinstance(item, dict):
            continue
        model_id = item.get("modelId") or item.get("full_name") or item.get("name") or item.get("id") or ""
        html_url = item.get("html_url") or item.get("url") or ""
        if source["source_key"].startswith("huggingface") and model_id:
            html_url = f"https://huggingface.co/{model_id}"
        title = model_id or item.get("name") or html_url or source["name"]
        raw_text = json.dumps(item, ensure_ascii=False, sort_keys=True)
        docs.append(
            {
                "source_key": source["source_key"],
                "url": html_url or f"{source['url']}#{content_hash(raw_text)[:12]}",
                "title": title,
                "published_at": (
                    item.get("lastModified")
                    or item.get("last_modified")
                    or item.get("pushedAt")
                    or item.get("pushed_at")
                    or item.get("updated_at")
                    or item.get("createdAt")
                    or item.get("created_at")
                    or ""
                ),
                "summary": strip_html_tags(item.get("description") or item.get("pipeline_tag") or title),
                "raw_text": raw_text[:30000],
                "metadata": {"language": "en", "content_type": "json", "raw": item},
            }
        )
    return docs


def fetch_feed(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = session.get(source["url"], timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    docs: List[Dict[str, Any]] = []

    channel_items = root.findall(".//item")
    atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    entries = channel_items or atom_entries

    def child_text(node: ET.Element, names: List[str]) -> str:
        for name in names:
            child = node.find(name)
            if child is not None and child.text:
                return normalize_text(child.text)
            child = node.find("{http://www.w3.org/2005/Atom}" + name)
            if child is not None and child.text:
                return normalize_text(child.text)
        return ""

    for entry in entries[:40]:
        title = child_text(entry, ["title"]) or source["name"]
        link = child_text(entry, ["link"])
        if not link:
            atom_link = entry.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href", "")
        published = child_text(entry, ["pubDate", "published", "updated"])
        summary = child_text(entry, ["description", "summary", "content"])
        raw_text = normalize_text(" ".join(part for part in (title, summary, published, link) if part))
        docs.append(
            {
                "source_key": source["source_key"],
                "url": link or f"{source['url']}#{content_hash(raw_text)[:12]}",
                "title": title,
                "published_at": published,
                "summary": strip_html_tags(summary)[:1200],
                "raw_text": strip_html_tags(raw_text)[:30000],
                "metadata": {"language": "en", "content_type": "feed"},
            }
        )
    return docs


def fetch_search_proxy_body(session: requests.Session, url: str, max_length: int) -> Dict[str, Any]:
    resp = session.get(
        f"{SEARCH_PROXY_URL}/fetch",
        params={"url": url, "max_length": max(500, min(int(max_length or 6000), 30000))},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        return {}
    return payload


def search_result_allowed(source: Dict[str, Any], item: Dict[str, Any]) -> bool:
    haystack = normalize_text(
        f"{item.get('title') or ''} {item.get('snippet') or ''} {item.get('url') or ''}"
    ).lower()
    include_terms = [str(term).lower() for term in source.get("include_terms") or [] if str(term).strip()]
    exclude_terms = [str(term).lower() for term in source.get("exclude_terms") or [] if str(term).strip()]
    if include_terms and not any(term in haystack for term in include_terms):
        return False
    if exclude_terms and any(term in haystack for term in exclude_terms):
        return False
    return True


def fetch_search_proxy(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not SEARCH_PROXY_URL:
        raise RuntimeError("INTELLIGENCE_SEARCH_PROXY_URL is not configured")
    query = source.get("query") or source["url"].replace("search://", "")
    result_limit = max(1, min(int(source.get("result_limit", 6) or 6), 10))
    expand_fetch = bool(int(source.get("expand_fetch", 0) or 0))
    fetch_max_length = int(source.get("fetch_max_length", 8000) or 8000)
    resp = session.post(
        f"{SEARCH_PROXY_URL}/search",
        json={"q": query, "num_results": result_limit, "nocache": 1},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    docs: List[Dict[str, Any]] = []
    for item in (payload.get("results") or [])[:result_limit]:
        if not isinstance(item, dict):
            continue
        if not search_result_allowed(source, item):
            continue
        title = normalize_text(item.get("title") or source["name"])
        url = item.get("url") or f"{source['url']}#{content_hash(title)[:12]}"
        snippet = strip_html_tags(item.get("snippet") or "")
        published_at = extract_date_from_text(snippet)
        fetched_payload: Dict[str, Any] = {}
        raw_text = normalize_text(f"{title} {snippet} {url}")[:30000]
        if expand_fetch and url.startswith(("http://", "https://")):
            try:
                fetched_payload = fetch_search_proxy_body(session, url, fetch_max_length)
                fetched_content = strip_html_tags(fetched_payload.get("content") or "")
                if fetched_content:
                    raw_text = fetched_content[:30000]
            except Exception:
                fetched_payload = {}
        docs.append(
            {
                "source_key": source["source_key"],
                "url": url,
                "title": title,
                "published_at": published_at,
                "summary": snippet or normalize_text(raw_text[:500]),
                "raw_text": raw_text,
                "metadata": {
                    "language": "en",
                    "content_type": "search_result_expanded" if raw_text != normalize_text(f"{title} {snippet} {url}")[:30000] else "search_result",
                    "query": query,
                    "engine": item.get("engine"),
                    "search_snippet": snippet,
                    "fetch_length": fetched_payload.get("length"),
                },
            }
        )
    return docs


def fetch_curated_public_research(_session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    if PublicResearchService is None:
        return []
    service = PublicResearchService()
    payload = service.get_public_research_hub()
    source_id = source.get("public_source_id") or ""
    docs: List[Dict[str, Any]] = []
    for item in payload.get("items") or []:
        if source_id and item.get("source_id") != source_id:
            continue
        title = normalize_text(item.get("title") or "")
        if not title:
            continue
        tags = [str(tag) for tag in item.get("tags") or [] if str(tag).strip()]
        original_url = item.get("url") or ""
        asset_url = item.get("pdf_url") or original_url
        summary = normalize_text(item.get("summary") or "")
        raw_text = normalize_text(" ".join([title, summary, " ".join(tags)]))
        docs.append(
            {
                "source_key": source["source_key"],
                "url": asset_url or original_url,
                "canonical_url": original_url or asset_url,
                "title": title,
                "published_at": item.get("published_at") or "",
                "summary": summary,
                "raw_text": raw_text,
                "metadata": {
                    "language": "en",
                    "content_type": item.get("format") or "article",
                    "original_url": original_url,
                    "asset_url": asset_url,
                    "public_source_id": source_id,
                    "tags": tags,
                    "source_group": item.get("source_group"),
                    "category": item.get("category"),
                },
            }
        )
    return docs


def fetch_eastmoney_keyword(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    params = {
        "cb": "datatable",
        "industryCode": "*",
        "pageSize": int(source.get("page_size", 20) or 20),
        "industry": "*",
        "rating": "*",
        "ratingChange": "*",
        "beginTime": "",
        "endTime": "",
        "pageNo": 1,
        "fields": "",
        "qType": "0",
        "orgCode": "",
        "code": "",
        "rcode": "10",
        "keywords": source.get("query_keyword") or "",
        "_": int(datetime.now().timestamp() * 1000),
    }
    resp = session.get(source["url"], params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    text = resp.text.strip()
    if text.startswith("datatable(") and text.endswith(");"):
        text = text[len("datatable("):-2]
    elif text.startswith("datatable(") and text.endswith(")"):
        text = text[len("datatable("):-1]
    payload = json.loads(text)
    docs: List[Dict[str, Any]] = []
    for item in (payload.get("data") or [])[: int(source.get("result_limit", 16) or 16)]:
        if not isinstance(item, dict):
            continue
        info_code = str(item.get("infoCode") or "").strip()
        title = normalize_text(item.get("title") or "")
        if not title:
            continue
        summary = normalize_text(item.get("abstract") or "")
        pdf_url = f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else ""
        stock_code = normalize_text(item.get("stockCode") or "")
        stock_name = normalize_text(item.get("stockName") or "")
        institution = normalize_text(item.get("orgSName") or "")
        author = normalize_text(item.get("researcher") or "")
        rating = normalize_text(item.get("emRatingName") or "")
        report_page = f"https://data.eastmoney.com/report/info/{info_code}.html" if info_code else pdf_url
        docs.append(
            {
                "source_key": source["source_key"],
                "url": pdf_url or report_page,
                "canonical_url": report_page or pdf_url,
                "title": title,
                "published_at": str(item.get("publishDate") or "")[:10],
                "summary": summary,
                "raw_text": normalize_text(" ".join([title, summary, stock_name, institution, author, rating, source.get("focus_area") or ""])),
                "metadata": {
                    "language": "zh",
                    "content_type": "pdf" if pdf_url else "article",
                    "original_url": report_page or pdf_url,
                    "asset_url": pdf_url or report_page,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "institution": institution,
                    "author": author,
                    "rating": rating,
                    "focus_area": source.get("focus_area"),
                    "raw": item,
                },
            }
        )
    return docs


def fetch_akshare_stock_bucket(_session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        return []

    docs: List[Dict[str, Any]] = []
    seen = set()
    per_symbol_limit = int(source.get("per_symbol_limit", 8) or 8)
    for symbol in source.get("symbols") or []:
        try:
            frame = ak.stock_research_report_em(symbol=str(symbol))
        except Exception:
            continue
        if frame is None or getattr(frame, "empty", True):
            continue
        columns = set(str(col) for col in frame.columns)
        for _, row in frame.head(per_symbol_limit).iterrows():
            title = normalize_text(str(row.get("报告名称") or row.get("研报标题") or row.get("标题") or ""))
            pdf_url = normalize_text(str(row.get("报告PDF链接") or row.get("PDF链接") or ""))
            trade_date = normalize_text(str(row.get("日期") or row.get("发布时间") or ""))
            stock_code = normalize_text(str(row.get("股票代码") or symbol))
            stock_name = normalize_text(str(row.get("股票简称") or row.get("股票名称") or ""))
            institution = normalize_text(str(row.get("机构") or row.get("研究机构") or ""))
            rating = normalize_text(str(row.get("东财评级") or row.get("评级") or ""))
            industry = normalize_text(str(row.get("行业") or source.get("focus_area") or ""))
            if not title:
                continue
            unique_key = pdf_url or f"{stock_code}|{trade_date}|{title}"
            if unique_key in seen:
                continue
            seen.add(unique_key)
            docs.append(
                {
                    "source_key": source["source_key"],
                    "url": pdf_url or f"https://data.eastmoney.com/report/{stock_code}.html",
                    "canonical_url": pdf_url or f"https://data.eastmoney.com/report/{stock_code}.html",
                    "title": title,
                    "published_at": trade_date[:10],
                    "summary": normalize_text(" ".join(filter(None, [stock_name, institution, rating, industry]))),
                    "raw_text": normalize_text(
                        " ".join(
                            filter(
                                None,
                                [
                                    title,
                                    stock_code,
                                    stock_name,
                                    institution,
                                    rating,
                                    industry,
                                    str(row.get("2025-盈利预测-收益") or ""),
                                    str(row.get("2026-盈利预测-收益") or ""),
                                ],
                            )
                        )
                    ),
                    "metadata": {
                        "language": "zh",
                        "content_type": "pdf" if pdf_url else "table_row",
                        "asset_url": pdf_url,
                        "original_url": pdf_url,
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "institution": institution,
                        "rating": rating,
                        "industry": industry,
                        "focus_area": source.get("focus_area"),
                        "columns": sorted(columns),
                    },
                }
            )
    return docs


def source_runs_on_current_collector(source: Dict[str, Any]) -> bool:
    if not int(source.get("enabled", 1)):
        return False
    collector = (source.get("collector") or "all").lower()
    return collector in {"all", "both", COLLECTOR_MODE}


def upsert_raw_document(conn: sqlite3.Connection, doc: Dict[str, Any]) -> Tuple[int, bool, bool]:
    doc_hash = content_hash((doc.get("title") or "") + "\n" + (doc.get("raw_text") or ""))
    existing = conn.execute("SELECT id, content_hash FROM raw_documents WHERE url = ?", (doc["url"],)).fetchone()
    fetched_at = now_iso()
    if existing:
        changed = existing[1] != doc_hash
        conn.execute(
            """
            UPDATE raw_documents
            SET source_key=?, title=?, title_zh=?, published_at=?, fetched_at=?, language=?,
                content_hash=?, summary=?, summary_zh=?, raw_text=?, metadata_json=?, status='active'
            WHERE id=?
            """,
            (
                doc["source_key"],
                doc.get("title"),
                doc.get("title_zh"),
                doc.get("published_at"),
                fetched_at,
                (doc.get("metadata") or {}).get("language", "en"),
                doc_hash,
                doc.get("summary"),
                doc.get("summary_zh"),
                doc.get("raw_text"),
                json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
                existing[0],
            ),
        )
        return int(existing[0]), False, changed

    cursor = conn.execute(
        """
        INSERT INTO raw_documents
        (source_key, url, canonical_url, title, title_zh, published_at, fetched_at, language,
         content_hash, summary, summary_zh, raw_text, metadata_json, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        """,
        (
            doc["source_key"],
            doc["url"],
            doc.get("canonical_url") or doc["url"],
            doc.get("title"),
            doc.get("title_zh"),
            doc.get("published_at"),
            fetched_at,
            (doc.get("metadata") or {}).get("language", "en"),
            doc_hash,
            doc.get("summary"),
            doc.get("summary_zh"),
            doc.get("raw_text"),
            json.dumps(doc.get("metadata") or {}, ensure_ascii=False),
        ),
    )
    return int(cursor.lastrowid), True, False


OFFICIAL_AI_SIGNAL_SOURCES = {
    "openai_gpt55_search",
    "anthropic_news",
    "google_ai_blog_rss",
    "google_deepmind_blog",
    "meta_ai_blog",
    "mistral_news",
}

MODEL_REPO_SIGNAL_SOURCES = {
    "deepseek_hf_flash",
    "huggingface_deepseek_models",
    "huggingface_trending_models",
    "github_deepseek_repos",
    "github_openai_repos",
    "github_modelcontextprotocol_repos",
}

EVENT_PROMOTION_LIMITS = {
    "huggingface_deepseek_models": 8,
    "github_deepseek_repos": 6,
    "huggingface_trending_models": 6,
    "github_openai_repos": 6,
    "github_modelcontextprotocol_repos": 6,
    "google_ai_blog_rss": 8,
    "arxiv_ai_recent": 8,
    "fda_press_rss": 8,
}

AI_SOURCE_ENTITIES = {
    "openai_gpt55_search": "OpenAI",
    "anthropic_news": "Anthropic",
    "google_ai_blog_rss": "Google",
    "google_deepmind_blog": "Google DeepMind",
    "meta_ai_blog": "Meta AI",
    "mistral_news": "Mistral AI",
    "deepseek_hf_flash": "DeepSeek",
    "huggingface_deepseek_models": "DeepSeek",
    "github_deepseek_repos": "DeepSeek",
    "github_openai_repos": "OpenAI",
    "github_modelcontextprotocol_repos": "Model Context Protocol",
}

OFFICIAL_AI_TERMS = (
    "introducing",
    "announce",
    "announcing",
    "launch",
    "release",
    "available",
    "general availability",
    "new model",
    "frontier model",
    "reasoning model",
    "agent",
    "agents",
    "inference",
    "benchmark",
    "claude",
    "gemini",
    "llama",
    "mistral",
    "gpt",
    "deepseek",
    "grok",
    "alphafold",
    "alphago",
    "vertex ai",
    "cloud tpu",
    "data center",
)

REPO_SIGNAL_TERMS = (
    "deepseek",
    "gpt",
    "openai",
    "qwen",
    "llama",
    "mistral",
    "gemma",
    "phi",
    "claude",
    "gemini",
    "reasoning",
    "agent",
    "agents",
    "inference",
    "multimodal",
    "vision",
    "audio",
    "vl",
    "v4",
    "r1",
    "moe",
    "diffusion",
    "embedding",
    "reranker",
    "mcp",
    "codex",
    "eval",
    "sdk",
    "server",
)

RESEARCH_SIGNAL_TERMS = (
    "large language model",
    "llm",
    "reasoning",
    "agent",
    "agents",
    "inference",
    "benchmark",
    "multimodal",
    "diffusion",
    "reinforcement learning",
    "transformer",
    "mixture of experts",
    "gpu",
    "研报",
    "评级",
    "盈利预测",
    "景气",
    "拐点",
)

FDA_SIGNAL_TERMS = (
    "fda approves",
    "approval",
    "approves",
    "authorizes",
    "clearance",
    "clinical",
    "drug",
    "therapy",
    "biologics",
    "vaccine",
)

GENERIC_NOISE_TERMS = (
    "privacy policy",
    "terms of use",
    "careers",
    "cookie",
    "press kit",
    "newsletter",
    "podcast",
)


def source_meta_for_key(source_key: str) -> Dict[str, Any]:
    for source in SOURCES:
        if source["source_key"] == source_key:
            return source
    return {"source_key": source_key, "name": source_key, "category": "intelligence", "source_type": "unknown"}


def compact_event_slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")
    slug = slug[:max_len].strip("_")
    return slug or content_hash(value)[:12]


def contains_any(text: str, terms: Iterable[str]) -> bool:
    return any(term in text for term in terms)


def generic_event_key(category: str, source_key: str, title: str, url: str) -> str:
    return f"{category}_{source_key}_{compact_event_slug(title)}_{content_hash(url or title)[:8]}"


def clean_doc_summary(doc: Dict[str, Any], title: str, max_len: int = 520) -> str:
    summary = strip_html_tags(doc.get("summary") or "")
    if not summary or summary == title:
        summary = strip_html_tags(doc.get("raw_text") or "")[:max_len]
    return summary[:max_len] if summary else title


def metadata_raw(doc: Dict[str, Any]) -> Dict[str, Any]:
    raw = (doc.get("metadata") or {}).get("raw") or {}
    return raw if isinstance(raw, dict) else {}


def repository_metric_facts(doc: Dict[str, Any], start_order: int) -> List[Tuple[str, str, str, str, str, str, int, float]]:
    raw = metadata_raw(doc)
    facts: List[Tuple[str, str, str, str, str, str, int, float]] = []
    metric_candidates = [
        ("stars", "Stars", "星标数", raw.get("stargazers_count")),
        ("forks", "Forks", "Fork 数", raw.get("forks_count")),
        ("likes", "Likes", "点赞数", raw.get("likes")),
        ("downloads", "Downloads", "下载量", raw.get("downloads")),
        ("pipeline", "Pipeline", "模型任务", raw.get("pipeline_tag")),
    ]
    order = start_order
    for fact_type, label, label_zh, value in metric_candidates:
        if value is None or value == "":
            continue
        facts.append((fact_type, label, label_zh, str(value), str(value), "", order, 0.58))
        order += 1
    return facts


def material_repo_signal(source_key: str, lower: str, title: str) -> bool:
    title_lower = title.lower()
    if source_key == "huggingface_deepseek_models":
        return contains_any(title_lower, ("v4", "v3.2", "r1", "ocr", "prover", "math", "vl", "coder", "moe"))
    if source_key == "github_deepseek_repos":
        return contains_any(
            title_lower,
            ("v4", "v3", "r1", "ocr", "vl", "coder", "moe", "deepgemm", "deepep", "flashmla", "tilekernels", "3fs"),
        )
    if source_key == "huggingface_trending_models":
        return contains_any(
            title_lower,
            ("deepseek", "qwen", "llama", "mistral", "gemma", "phi", "whisper", "stable-diffusion", "flux", "gpt", "vl"),
        )
    if source_key == "github_openai_repos":
        return contains_any(
            title_lower,
            ("codex", "agents", "gpt", "eval", "openai-python", "openai-node", "realtime", "responses", "skills"),
        )
    if source_key == "github_modelcontextprotocol_repos":
        return contains_any(
            title_lower,
            ("sdk", "servers", "inspector", "registry", "specification", "conformance", "experimental", "auth"),
        )
    return contains_any(lower, REPO_SIGNAL_TERMS)


def generic_signal_event_payload(doc: Dict[str, Any], title: str, text: str, lower: str) -> Optional[Dict[str, Any]]:
    source_key = doc.get("source_key") or ""
    source = source_meta_for_key(source_key)
    source_name = source.get("name") or source_key
    url = doc.get("url") or ""
    event_time = doc.get("published_at") or now_iso()
    summary = clean_doc_summary(doc, title)

    has_specific_signal = contains_any(lower, OFFICIAL_AI_TERMS + REPO_SIGNAL_TERMS + RESEARCH_SIGNAL_TERMS)
    if contains_any(lower, GENERIC_NOISE_TERMS) and not has_specific_signal:
        return None

    if source_key in OFFICIAL_AI_SIGNAL_SOURCES and contains_any(lower, OFFICIAL_AI_TERMS):
        company = AI_SOURCE_ENTITIES.get(source_key, source_name)
        return {
            "event_key": generic_event_key("ai_official", source_key, title, url),
            "title": f"{company} official AI signal: {title}",
            "title_zh": f"{company} 官方 AI 信号：{title}",
            "category": "ai_model",
            "priority": "P1",
            "confidence": 0.68,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"官方源出现 AI 模型、产品或基础设施相关信号：{summary}",
            "impact_summary": (
                "Treat as a pre-confirmation signal for the AI cycle. Verify model capability, availability, pricing, "
                "developer adoption and A/H supply-chain read-through before raising exposure."
            ),
            "impact_summary_zh": "作为 AI 周期预确认信号处理，后续验证模型能力、可用性、定价、开发者采用和 A/H 供应链映射。",
            "impact_score": 68,
            "verification_status": "official_signal",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.85),
                ("signal", "Signal title", "信号标题", title, title, "", 2, 0.72),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.56),
                (
                    "market",
                    "A/H watch chain",
                    "A/H 观察链条",
                    "AI applications, cloud, servers, optical modules, PCB, memory, edge devices",
                    "AI 应用、云、服务器、光模块、PCB、存储、端侧设备",
                    "",
                    4,
                    0.64,
                ),
            ],
            "entities": [
                ("company", company, "", "", "issuer", 0.82),
                ("sector", "AI infrastructure", "", "A/H", "impact_chain", 0.72),
                ("sector", "AI applications", "", "A/H", "impact_chain", 0.66),
            ],
            "research": {
                "report_type": "official_signal",
                "thesis": "Official AI updates can reprice model-cycle expectations; track follow-on benchmark, API, pricing and capex evidence.",
                "thesis_zh": "官方 AI 更新可能重估模型周期预期，后续跟踪 benchmark、API、定价和资本开支证据。",
                "relevance": "AI models, cloud, semiconductors, server supply chain, software applications",
                "relevance_zh": "AI 模型、云、半导体、服务器供应链、软件应用",
            },
        }

    if source_key in MODEL_REPO_SIGNAL_SOURCES and material_repo_signal(source_key, lower, title):
        company = AI_SOURCE_ENTITIES.get(source_key, "AI ecosystem")
        facts = [
            ("source", "Source", "来源", source_name, source_name, "", 1, 0.72),
            ("repo", "Repository/model", "仓库或模型", title, title, "", 2, 0.7),
            ("time", "Created or updated", "创建或更新时间", event_time, event_time, "", 3, 0.52),
            (
                "market",
                "Validation path",
                "验证路径",
                "watch downloads, stars, API references, inference cost and downstream app integration",
                "跟踪下载、星标、API 引用、推理成本和下游应用集成",
                "",
                4,
                0.58,
            ),
        ]
        facts.extend(repository_metric_facts(doc, 5))
        return {
            "event_key": generic_event_key("ai_repo", source_key, title, url),
            "title": f"AI repository signal: {title}",
            "title_zh": f"AI 仓库信号：{title}",
            "category": "ai_tooling" if "mcp" in lower or "sdk" in lower or "codex" in lower else "ai_model",
            "priority": "P2",
            "confidence": 0.52,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"模型或代码仓库出现更新信号：{summary}",
            "impact_summary": (
                "Repository activity is an early but noisy adoption signal. Promote only after official announcement, benchmark, "
                "download, API or downstream order evidence improves."
            ),
            "impact_summary_zh": "仓库活跃度是早期但噪声较高的采用信号，需结合官方公告、benchmark、下载、API 或下游订单证据再升级。",
            "impact_score": 52,
            "verification_status": "repo_signal",
            "facts": facts,
            "entities": [
                ("company", company, "", "", "maintainer", 0.7),
                ("platform", "GitHub" if "github" in source_key else "Hugging Face", "", "global", "distribution", 0.66),
                ("sector", "AI developer ecosystem", "", "global", "impact_chain", 0.62),
            ],
            "research": {
                "report_type": "repo_signal",
                "thesis": "Repository updates help detect early AI tooling and model adoption, but need external validation before becoming a trade thesis.",
                "thesis_zh": "仓库更新可帮助发现 AI 工具和模型采用早期变化，但成为交易线索前需要外部验证。",
                "relevance": "AI developer tools, model distribution, inference stack",
                "relevance_zh": "AI 开发工具、模型分发、推理栈",
            },
        }

    if source_key == "arxiv_ai_recent" and contains_any(lower, RESEARCH_SIGNAL_TERMS):
        return {
            "event_key": generic_event_key("ai_research", source_key, title, url),
            "title": f"AI research signal: {title}",
            "title_zh": f"AI 研究信号：{title}",
            "category": "ai_research",
            "priority": "P2",
            "confidence": 0.42,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"arXiv 出现 AI 研究前沿信号：{summary}",
            "impact_summary": "Use as a technology radar input, not a standalone trading signal; verify whether it enters product releases, benchmarks or capex plans.",
            "impact_summary_zh": "作为技术雷达输入，而非独立交易信号；需验证是否进入产品发布、评测榜单或资本开支计划。",
            "impact_score": 42,
            "verification_status": "research_preprint",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.66),
                ("paper", "Paper title", "论文标题", title, title, "", 2, 0.66),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.5),
                (
                    "market",
                    "Follow-up validation",
                    "后续验证",
                    "model release, benchmark adoption, cloud/GPU demand, enterprise productization",
                    "模型发布、评测采用、云/GPU 需求、企业产品化",
                    "",
                    4,
                    0.48,
                ),
            ],
            "entities": [
                ("sector", "AI research", "", "global", "technology_signal", 0.7),
                ("sector", "AI infrastructure", "", "A/H", "impact_chain", 0.5),
            ],
            "research": {
                "report_type": "preprint",
                "thesis": "Recent AI papers can foreshadow model capability shifts; keep them in the radar until product or benchmark confirmation appears.",
                "thesis_zh": "近期 AI 论文可能提前反映模型能力变化，在产品或 benchmark 确认前作为雷达信号保留。",
                "relevance": "AI research, model capability, compute demand",
                "relevance_zh": "AI 研究、模型能力、算力需求",
            },
        }

    if source_key == "fda_press_rss" and contains_any(lower, FDA_SIGNAL_TERMS):
        is_approval = "approves" in lower or "approval" in lower or "fda approves" in lower
        return {
            "event_key": generic_event_key("biotech_regulatory", source_key, title, url),
            "title": f"FDA biotech signal: {title}",
            "title_zh": f"FDA 生物医药信号：{title}",
            "category": "biotech",
            "priority": "P1" if is_approval else "P2",
            "confidence": 0.66 if is_approval else 0.48,
            "event_time": event_time,
            "summary": summary,
            "summary_zh": f"FDA 监管源出现药品、疗法或临床相关信号：{summary}",
            "impact_summary": "Map to HK/A biotech peers by indication, modality and partner pipeline; verify licensing, milestones and clinical read-through.",
            "impact_summary_zh": "按适应症、技术路线和合作管线映射港股/A股创新药标的，后续验证 BD、里程碑和临床读出传导。",
            "impact_score": 64 if is_approval else 48,
            "verification_status": "regulatory_release",
            "facts": [
                ("source", "Source", "来源", source_name, source_name, "", 1, 0.82),
                ("signal", "Regulatory signal", "监管信号", title, title, "", 2, 0.72),
                ("time", "Published or updated", "发布或更新时间", event_time, event_time, "", 3, 0.54),
                (
                    "market",
                    "A/H watch chain",
                    "A/H 观察链条",
                    "innovative drugs, CRO/CDMO, licensing, clinical catalysts",
                    "创新药、CRO/CDMO、授权交易、临床催化",
                    "",
                    4,
                    0.58,
                ),
            ],
            "entities": [
                ("regulator", "FDA", "", "US", "source", 0.92),
                ("sector", "Biotech", "", "A/H", "impact_chain", 0.68),
            ],
            "research": {
                "report_type": "regulatory_release",
                "thesis": "FDA regulatory releases can validate modalities and indications relevant to HK/A biotech pipelines.",
                "thesis_zh": "FDA 监管公告可验证与港股/A股创新药管线相关的技术路线和适应症。",
                "relevance": "Biotech, innovative drugs, CRO/CDMO, licensing",
                "relevance_zh": "生物医药、创新药、CRO/CDMO、授权交易",
            },
        }

    return None


RESEARCH_ONLY_SOURCE_KEYS = {
    "stanford_hai_ai_index",
    "brookings_ai_research",
    "cset_ai_publications",
    "rand_ai_research",
    "public_goldman_china",
    "public_jpmorgan_china",
    "public_ubs_china",
    "public_morgan_stanley_china",
    "public_imf_china",
    "public_world_bank_china",
    "public_oecd_china",
    "public_bis_china",
    "eastmoney_focus_semi",
    "eastmoney_focus_ai",
    "eastmoney_focus_robotics",
    "eastmoney_focus_biotech",
    "eastmoney_focus_pv",
    "eastmoney_focus_nuclear",
    "eastmoney_focus_hog",
}

RESEARCH_CONTEXT_TERMS = (
    "artificial intelligence",
    "agi",
    "physical ai",
    "embodied ai",
    "policy",
    "governance",
    "adoption",
    "labor",
    "labor market",
    "compute",
    "data center",
    "robotics",
    "economy",
    "sovereignty",
    "competition",
    "safety",
    "risk",
    "strategy",
    "半导体",
    "芯片",
    "人工智能",
    "机器人",
    "创新药",
    "光伏",
    "核电",
    "生猪",
    "养殖",
)

RESEARCH_ASSET_DIR = os.getenv(
    "INTELLIGENCE_RESEARCH_ASSET_DIR",
    os.path.join(BASE_DIR, "data", "research_assets"),
)
RESEARCH_ARCHIVE_MAX_PER_RUN = int(os.getenv("INTELLIGENCE_RESEARCH_ARCHIVE_MAX_PER_RUN", "6"))
RESEARCH_ARCHIVE_TIMEOUT_SECONDS = int(os.getenv("INTELLIGENCE_RESEARCH_ARCHIVE_TIMEOUT_SECONDS", "8"))
RESEARCH_ARCHIVE_ATTEMPTS = 0


def default_publisher_region(source: Dict[str, Any]) -> str:
    return str(source.get("publisher_region") or ("domestic" if "eastmoney" in source.get("source_key", "") else "overseas"))


def default_source_tier(source: Dict[str, Any]) -> str:
    value = normalize_text(str(source.get("source_tier") or ""))
    if value:
        return value
    credibility = str(source.get("credibility") or "")
    if "sellside" in credibility:
        return "sellside"
    if credibility in {"official", "regulatory"}:
        return "official"
    return "research"


def extract_key_points(*parts: Any, max_points: int = 4) -> List[Dict[str, str]]:
    text = normalize_text(" ".join(str(part or "") for part in parts))
    if not text:
        return []
    segments = re.split(r"(?<=[\.\!\?。；;])\s+", text)
    points: List[Dict[str, str]] = []
    seen = set()
    for segment in segments:
        item = normalize_text(segment).strip(" -")
        if len(item) < 18 or item.lower() in seen:
            continue
        seen.add(item.lower())
        points.append({"zh": "", "en": item[:220]})
        if len(points) >= max_points:
            break
    return points


def normalize_key_points(value: Any, max_points: int = 6) -> List[Dict[str, str]]:
    points: List[Dict[str, str]] = []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = []
    if not isinstance(value, list):
        return points
    for item in value:
        if isinstance(item, dict):
            zh = normalize_text(str(item.get("zh") or ""))
            en = normalize_text(str(item.get("en") or ""))
        else:
            zh = ""
            en = normalize_text(str(item or ""))
        if zh or en:
            points.append({"zh": zh, "en": en})
        if len(points) >= max_points:
            break
    return points


def merge_key_points(existing: Any, incoming: Any, max_points: int = 6) -> List[Dict[str, str]]:
    existing_points = normalize_key_points(existing, max_points=max_points)
    incoming_points = normalize_key_points(incoming, max_points=max_points)
    if not existing_points:
        return incoming_points
    merged: List[Dict[str, str]] = []
    seen = set()
    for idx, old_point in enumerate(existing_points):
        new_point = incoming_points[idx] if idx < len(incoming_points) else {}
        zh = old_point.get("zh") or new_point.get("zh") or ""
        en = old_point.get("en") or new_point.get("en") or ""
        if not zh and not en:
            continue
        dedupe_key = f"{zh.lower()}|{en.lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append({"zh": zh, "en": en})
        if len(merged) >= max_points:
            return merged
    for point in incoming_points:
        zh = point.get("zh") or ""
        en = point.get("en") or ""
        if not zh and not en:
            continue
        dedupe_key = f"{zh.lower()}|{en.lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append({"zh": zh, "en": en})
        if len(merged) >= max_points:
            break
    return merged


def archive_research_asset(report: Dict[str, Any], existing_path: str = "") -> Dict[str, str]:
    global RESEARCH_ARCHIVE_ATTEMPTS
    asset_url = normalize_text(report.get("original_url") or report.get("url") or "")
    if existing_path and os.path.exists(existing_path) and os.path.getsize(existing_path) > 0:
        suffix = Path(existing_path).suffix.lower()
        return {
            "original_url": asset_url,
            "original_asset_path": existing_path,
            "original_asset_type": "pdf" if suffix == ".pdf" else ("snapshot" if suffix in {".md", ".txt"} else suffix.lstrip(".")),
            "original_asset_status": "archived",
            "original_downloaded_at": now_iso(),
        }
    if not asset_url.startswith(("http://", "https://")):
        return {
            "original_url": asset_url,
            "original_asset_path": existing_path or "",
            "original_asset_type": "",
            "original_asset_status": "missing_url",
            "original_downloaded_at": "",
        }
    if RESEARCH_ARCHIVE_MAX_PER_RUN >= 0 and RESEARCH_ARCHIVE_ATTEMPTS >= RESEARCH_ARCHIVE_MAX_PER_RUN:
        return {
            "original_url": asset_url,
            "original_asset_path": existing_path or "",
            "original_asset_type": "",
            "original_asset_status": "queued",
            "original_downloaded_at": "",
        }

    source_key = sanitize_filename(report.get("source_key") or "unknown", "unknown")
    month_key = (report.get("published_at") or report.get("fetched_at") or now_iso())[:7] or "undated"
    base_name = sanitize_filename(report.get("report_key") or report.get("title") or content_hash(asset_url)[:12], "research")
    target_dir = Path(RESEARCH_ASSET_DIR) / source_key / month_key
    target_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()
    RESEARCH_ARCHIVE_ATTEMPTS += 1

    def _write_bytes(ext: str, content: bytes) -> str:
        target_path = target_dir / f"{base_name}{ext}"
        target_path.write_bytes(content)
        return str(target_path)

    def _write_text(ext: str, text: str) -> str:
        target_path = target_dir / f"{base_name}{ext}"
        target_path.write_text(text, encoding="utf-8")
        return str(target_path)

    try:
        resp = session.get(asset_url, timeout=RESEARCH_ARCHIVE_TIMEOUT_SECONDS)
        resp.raise_for_status()
        content_type = (resp.headers.get("Content-Type") or "").lower()
        path_ext = Path(unquote(urlparse(asset_url).path)).suffix.lower()
        guessed_ext = mimetypes.guess_extension(content_type.split(";")[0].strip()) or path_ext or ""
        if "pdf" in content_type or path_ext == ".pdf" or resp.content[:4] == b"%PDF":
            local_path = _write_bytes(".pdf", resp.content)
            asset_type = "pdf"
        else:
            if guessed_ext not in {".html", ".htm", ".txt", ".md"}:
                guessed_ext = ".html"
            local_path = _write_text(guessed_ext, resp.text)
            asset_type = "snapshot"
        return {
            "original_url": asset_url,
            "original_asset_path": local_path,
            "original_asset_type": asset_type,
            "original_asset_status": "archived",
            "original_downloaded_at": now_iso(),
        }
    except Exception as exc:
        if SEARCH_PROXY_URL:
            try:
                payload = fetch_search_proxy_body(session, asset_url, 18000)
                content = normalize_text(payload.get("content") or "")
                if content:
                    snapshot_text = "\n".join(
                        [
                            f"# {report.get('title') or report.get('report_key') or 'Research Snapshot'}",
                            "",
                            f"Source URL: {asset_url}",
                            "",
                            content,
                        ]
                    )
                    local_path = _write_text(".md", snapshot_text)
                    return {
                        "original_url": asset_url,
                        "original_asset_path": local_path,
                        "original_asset_type": "snapshot",
                        "original_asset_status": "archived_via_proxy",
                        "original_downloaded_at": now_iso(),
                    }
            except Exception:
                pass
        return {
            "original_url": asset_url,
            "original_asset_path": existing_path or "",
            "original_asset_type": "",
            "original_asset_status": f"failed:{str(exc)[:80]}",
            "original_downloaded_at": "",
        }


def research_payload_for_document(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_key = doc.get("source_key") or ""
    source = source_meta_for_key(source_key)
    metadata = doc.get("metadata") or {}
    if source_key not in RESEARCH_ONLY_SOURCE_KEYS and source.get("category") != "ai_research":
        return None

    title = normalize_text(doc.get("title") or "")
    if not title:
        return None

    text = normalize_text(f"{title} {doc.get('summary') or ''} {doc.get('raw_text') or ''}")
    lower = text.lower()
    has_signal = contains_any(lower, RESEARCH_SIGNAL_TERMS + RESEARCH_CONTEXT_TERMS)
    if contains_any(lower, GENERIC_NOISE_TERMS) and not has_signal:
        return None
    if not has_signal:
        return None

    source_name = source.get("name") or source_key
    published_at = doc.get("published_at") or extract_date_from_text(doc.get("summary") or "") or extract_date_from_text(doc.get("raw_text") or "")
    summary = clean_doc_summary(doc, title, max_len=640)
    focus_areas = detect_focus_areas(
        source.get("focus_area"),
        title,
        summary,
        doc.get("raw_text"),
        " ".join(metadata.get("tags") or []),
    )
    if source.get("focus_area") and source.get("focus_area") not in focus_areas:
        focus_areas.insert(0, str(source.get("focus_area")))
    tags = dedupe_strings(
        list(metadata.get("tags") or [])
        + [source.get("category") or "", metadata.get("category") or "", metadata.get("source_group") or ""]
        + focus_areas
    )
    target_scope = str(source.get("target_scope") or infer_target_scope(title, summary, doc.get("raw_text"), metadata.get("category")))
    publisher_region = default_publisher_region(source)
    source_tier = default_source_tier(source)
    best_link = doc.get("canonical_url") or doc.get("url") or ""
    original_asset_url = metadata.get("asset_url") or metadata.get("original_url") or doc.get("url") or best_link
    if source_key.startswith("eastmoney_focus_"):
        focus_line = focus_areas[0] if focus_areas else (source.get("focus_area") or "行业")
        thesis = source.get("thesis_template") or f"Use as public domestic sell-side tracking for {focus_line}; focus on earnings revisions, rating changes and catalyst validation."
        thesis_zh = source.get("thesis_template_zh") or f"作为 {focus_line} 的公开卖方跟踪材料，重点看盈利预测、评级变化和催化验证。"
        relevance = source.get("investment_relevance") or f"{focus_line}, earnings revision, valuation rerating, leading companies and catalyst calendar"
        relevance_zh = source.get("investment_relevance_zh") or f"{focus_line}、盈利预测修正、估值重估、龙头公司与催化日历"
    elif source.get("category") == "research_library":
        thesis = source.get("thesis_template") or "Use as a public macro and thematic validation input for China assets, policy path and sector rotation."
        thesis_zh = source.get("thesis_template_zh") or "作为中国资产、政策路径和行业轮动的公开宏观/主题验证材料。"
        relevance = source.get("investment_relevance") or "China macro, policy, cross-border liquidity, sector rotation, valuation context"
        relevance_zh = source.get("investment_relevance_zh") or "中国宏观、政策、跨境流动性、行业轮动与估值背景"
    else:
        thesis = source.get("thesis_template") or "Use as a medium-term validation input for AI capability, adoption, compute demand and policy path."
        thesis_zh = source.get("thesis_template_zh") or "作为 AI 能力、采用率、算力需求和政策路径的中期验证输入。"
        relevance = source.get("investment_relevance") or "AI infrastructure, cloud, semiconductors, software, data center power"
        relevance_zh = source.get("investment_relevance_zh") or "AI 基础设施、云、半导体、软件、数据中心电力"

    evidence: List[Tuple[str, str, str]] = [
        ("Source", source_name, doc.get("url") or ""),
        ("Topic", title, doc.get("url") or ""),
    ]
    if published_at:
        evidence.append(("Published", published_at, doc.get("url") or ""))

    return {
        "report_key": content_hash(f"research|{source_key}|{doc.get('url') or title}")[:24],
        "title": title,
        "title_zh": doc.get("title_zh") or "",
        "source_key": source_key,
        "source_name": source_name,
        "url": best_link,
        "report_type": source.get("report_type") or "research",
        "publisher_region": publisher_region,
        "source_tier": source_tier,
        "target_scope": target_scope,
        "published_at": published_at,
        "language": (doc.get("metadata") or {}).get("language", "en"),
        "summary": summary,
        "summary_zh": doc.get("summary_zh") or "",
        "thesis": thesis,
        "thesis_zh": thesis_zh,
        "relevance": relevance,
        "relevance_zh": relevance_zh,
        "focus_areas": focus_areas,
        "tags": tags,
        "tickers": dedupe_strings([metadata.get("stock_code") or ""]),
        "key_points": extract_key_points(summary, thesis, relevance),
        "original_url": original_asset_url,
        "evidence": evidence,
    }


def event_payload_for_document(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = normalize_text(doc.get("title") or "")
    text = normalize_text(f"{title} {doc.get('url') or ''} {doc.get('summary') or ''} {doc.get('raw_text') or ''}")
    lower = text.lower()

    if re.search(r"gpt[- ]?5\.5", lower):
        return {
            "event_key": "openai_gpt_5_5_release_2026_04_24",
            "title": "OpenAI releases GPT-5.5",
            "title_zh": "OpenAI 发布 GPT-5.5",
            "category": "ai_model",
            "priority": "P0",
            "confidence": 0.92,
            "event_time": doc.get("published_at") or "2026-04-24",
            "summary": "OpenAI released GPT-5.5 for complex coding, research, document/spreadsheet work, tool use and real-world workflows.",
            "summary_zh": "OpenAI 发布 GPT-5.5，重点指向复杂代码、研究、文档表格、工具协作和现实工作流。",
            "impact_summary": "Raises the bar for AI agent capability and sustains demand expectations for cloud, data-center capex, inference infrastructure and enterprise AI software.",
            "impact_summary_zh": "提高 AI Agent 能力门槛，并继续支撑云、数据中心资本开支、推理基础设施和企业 AI 软件的需求预期。",
            "impact_score": 88,
            "verification_status": "official_confirmed",
            "facts": [
                ("model", "Model", "模型", "GPT-5.5", "GPT-5.5", "", 1, 0.95),
                ("capability", "Core capability", "核心能力", "complex coding, research, documents, spreadsheets, tool use", "复杂代码、研究、文档、表格和工具调用", "", 2, 0.85),
                ("context", "Context window", "上下文窗口", "1M", "100 万", "tokens", 3, 0.8),
                ("availability", "Availability", "可用性", "ChatGPT and Codex; API availability expected soon", "ChatGPT 与 Codex 已可用；API 预计随后开放", "", 4, 0.82),
                ("pricing", "GPT-5.5 pricing", "GPT-5.5 定价", "$5/M input, $30/M output", "输入 5 美元/百万 token，输出 30 美元/百万 token", "USD tokens", 5, 0.78),
                ("market", "A/H watch chain", "A/H 观察链条", "cloud, data center, servers, optical modules, PCB, storage, AI applications", "云、数据中心、服务器、光模块、PCB、存储、AI 应用", "", 6, 0.76),
            ],
            "entities": [
                ("company", "OpenAI", "", "", "issuer", 0.98),
                ("product", "GPT-5.5", "", "", "model", 0.98),
                ("product", "Codex", "", "", "developer workflow", 0.76),
                ("sector", "AI infrastructure", "", "global", "impact_chain", 0.86),
                ("sector", "AI applications", "", "global", "impact_chain", 0.78),
            ],
            "research": {
                "report_type": "system_card" if "system card" in lower else "official_release",
                "thesis": "GPT-5.5 strengthens the AI agent and enterprise workflow cycle; track inference cost, cloud capex and software adoption.",
                "thesis_zh": "GPT-5.5 强化 AI Agent 与企业工作流周期，后续重点跟踪推理成本、云资本开支和软件采用率。",
                "relevance": "AI, cloud, semiconductors, server supply chain, enterprise software",
                "relevance_zh": "AI、云、半导体、服务器供应链、企业软件",
            },
        }

    if "deepseek-v4" in lower or "deepseek v4" in lower or "deepseek_v4" in lower or (
        "deepseek" in lower and re.search(r"\bgpt[- ]?v4\b|\bv4\b", lower)
    ):
        return {
            "event_key": "deepseek_v4_release_2026_04_24",
            "title": "DeepSeek V4 models appear across official docs and model repositories",
            "title_zh": "DeepSeek V4 模型在官方文档和模型仓库出现",
            "category": "ai_model",
            "priority": "P0",
            "confidence": 0.9,
            "event_time": doc.get("published_at") or "2026-04-24",
            "summary": "DeepSeek V4 Pro and Flash signals point to a new China model cycle, with API docs, Hugging Face distribution and deprecation notices for older models.",
            "summary_zh": "DeepSeek V4 Pro/Flash 指向新一轮国产大模型周期，官方 API 文档、Hugging Face 仓库和旧模型弃用节奏构成关键证据。",
            "impact_summary": "Strengthens China AI competitiveness narrative and should be tied to domestic inference stack, model application, cloud and compute supply-chain validation.",
            "impact_summary_zh": "强化中国 AI 竞争力叙事，需联动国产推理栈、模型应用、云和算力供应链订单验证。",
            "impact_score": 86,
            "verification_status": "primary_sources_seen",
            "facts": [
                ("model", "Model family", "模型族", "DeepSeek V4", "DeepSeek V4", "", 1, 0.95),
                ("model", "API model", "API 模型", "deepseek-v4-flash", "deepseek-v4-flash", "", 2, 0.92),
                ("model", "API model", "API 模型", "deepseek-v4-pro", "deepseek-v4-pro", "", 3, 0.9),
                ("lifecycle", "Old model deprecation", "旧模型弃用", "deepseek-chat and deepseek-reasoner deprecated on 2026/07/24", "deepseek-chat 与 deepseek-reasoner 将于 2026/07/24 弃用", "", 4, 0.85),
                ("context", "Context window", "上下文窗口", "1M", "100 万", "tokens", 5, 0.78),
                ("market", "A/H watch chain", "A/H 观察链条", "domestic AI apps, inference hardware, servers, cloud, optical modules, memory", "国产 AI 应用、推理硬件、服务器、云、光模块、存储", "", 6, 0.76),
            ],
            "entities": [
                ("company", "DeepSeek", "", "China", "issuer", 0.98),
                ("product", "DeepSeek V4", "", "", "model", 0.98),
                ("product", "DeepSeek-V4-Flash", "", "", "model", 0.9),
                ("platform", "Hugging Face", "", "global", "distribution", 0.68),
                ("sector", "China AI infrastructure", "", "A/H", "impact_chain", 0.86),
            ],
            "research": {
                "report_type": "model_card" if "huggingface" in lower else "official_docs",
                "thesis": "DeepSeek V4 improves the China AI prepositioning case; verify model usage, API pricing, open-weight adoption and downstream order signals.",
                "thesis_zh": "DeepSeek V4 强化中国 AI 提前布局逻辑，需验证模型调用、API 定价、开源权重采用和下游订单信号。",
                "relevance": "China AI, cloud, semiconductors, inference, software",
                "relevance_zh": "中国 AI、云、半导体、推理、软件",
            },
        }

    return generic_signal_event_payload(doc, title, text, lower)


def upsert_research_report(conn: sqlite3.Connection, doc: Dict[str, Any], report: Dict[str, Any]) -> bool:
    existing = conn.execute(
        """
        SELECT id, original_asset_path, original_asset_status,
               title_zh, summary_zh, thesis_zh, relevance_zh, key_points_json
        FROM research_reports
        WHERE report_key = ?
        """,
        (report["report_key"],),
    ).fetchone()
    existing_path = existing[1] if existing and len(existing) > 1 else ""
    existing_title_zh = existing[3] if existing and len(existing) > 3 else ""
    existing_summary_zh = existing[4] if existing and len(existing) > 4 else ""
    existing_thesis_zh = existing[5] if existing and len(existing) > 5 else ""
    existing_relevance_zh = existing[6] if existing and len(existing) > 6 else ""
    existing_key_points_json = existing[7] if existing and len(existing) > 7 else ""
    merged_key_points = merge_key_points(existing_key_points_json, report.get("key_points") or [])
    archive_info = archive_research_asset(report, existing_path=existing_path or "")
    report = {
        **report,
        **archive_info,
        "publisher_region": report.get("publisher_region") or "overseas",
        "source_tier": report.get("source_tier") or "research",
        "target_scope": report.get("target_scope") or "industry",
        "title_zh": report.get("title_zh") or existing_title_zh or "",
        "summary_zh": report.get("summary_zh") or existing_summary_zh or "",
        "thesis_zh": report.get("thesis_zh") or existing_thesis_zh or "",
        "relevance_zh": report.get("relevance_zh") or existing_relevance_zh or "",
        "focus_areas_json": safe_json_dumps(report.get("focus_areas") or []),
        "tags_json": safe_json_dumps(report.get("tags") or []),
        "tickers_json": safe_json_dumps(report.get("tickers") or []),
        "key_points_json": safe_json_dumps(merged_key_points),
    }
    if existing:
        report_id = int(existing[0])
        conn.execute(
            """
            UPDATE research_reports
            SET title=?, title_zh=?, source_key=?, source_name=?, url=?, report_type=?,
                publisher_region=?, source_tier=?, target_scope=?,
                published_at=?, fetched_at=?, language=?, summary=?, summary_zh=?,
                thesis=?, thesis_zh=?, relevance=?, relevance_zh=?,
                focus_areas_json=?, tags_json=?, tickers_json=?, key_points_json=?,
                original_url=?, original_asset_path=?, original_asset_type=?,
                original_asset_status=?, original_downloaded_at=?, status='active'
            WHERE id=?
            """,
            (
                report["title"],
                report.get("title_zh"),
                report.get("source_key"),
                report.get("source_name"),
                report.get("url"),
                report.get("report_type", "research"),
                report.get("publisher_region"),
                report.get("source_tier"),
                report.get("target_scope"),
                report.get("published_at"),
                now_iso(),
                report.get("language", "en"),
                report.get("summary"),
                report.get("summary_zh"),
                report.get("thesis"),
                report.get("thesis_zh"),
                report.get("relevance"),
                report.get("relevance_zh"),
                report.get("focus_areas_json"),
                report.get("tags_json"),
                report.get("tickers_json"),
                report.get("key_points_json"),
                report.get("original_url"),
                report.get("original_asset_path"),
                report.get("original_asset_type"),
                report.get("original_asset_status"),
                report.get("original_downloaded_at"),
                report_id,
            ),
        )
        added = False
    else:
        cursor = conn.execute(
            """
            INSERT INTO research_reports
            (report_key, title, title_zh, source_key, source_name, url, report_type,
             publisher_region, source_tier, target_scope,
             published_at, fetched_at, language, summary, summary_zh,
             thesis, thesis_zh, relevance, relevance_zh,
             focus_areas_json, tags_json, tickers_json, key_points_json,
             original_url, original_asset_path, original_asset_type,
             original_asset_status, original_downloaded_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """,
            (
                report["report_key"],
                report["title"],
                report.get("title_zh"),
                report.get("source_key"),
                report.get("source_name"),
                report.get("url"),
                report.get("report_type", "research"),
                report.get("publisher_region"),
                report.get("source_tier"),
                report.get("target_scope"),
                report.get("published_at"),
                now_iso(),
                report.get("language", "en"),
                report.get("summary"),
                report.get("summary_zh"),
                report.get("thesis"),
                report.get("thesis_zh"),
                report.get("relevance"),
                report.get("relevance_zh"),
                report.get("focus_areas_json"),
                report.get("tags_json"),
                report.get("tickers_json"),
                report.get("key_points_json"),
                report.get("original_url"),
                report.get("original_asset_path"),
                report.get("original_asset_type"),
                report.get("original_asset_status"),
                report.get("original_downloaded_at"),
            ),
        )
        report_id = int(cursor.lastrowid)
        added = True

    for sort_order, evidence in enumerate(report.get("evidence") or [], start=1):
        label, value, source_url = (list(evidence) + ["", "", ""])[:3]
        conn.execute(
            """
            INSERT OR IGNORE INTO research_evidence
            (report_id, label, value, source_url, sort_order)
            VALUES (?, ?, ?, ?, ?)
            """,
            (report_id, label, value, source_url or report.get("url"), sort_order),
        )
    return added


def upsert_event_bundle(conn: sqlite3.Connection, doc_id: int, doc: Dict[str, Any], bundle: Dict[str, Any]) -> bool:
    first_seen = now_iso()
    existing = conn.execute("SELECT id, first_seen_at FROM intelligence_events WHERE event_key = ?", (bundle["event_key"],)).fetchone()
    if existing:
        event_id = int(existing[0])
        first_seen = existing[1] or first_seen
        conn.execute(
            """
            UPDATE intelligence_events
            SET title=?, title_zh=?, category=?, priority=?, confidence=?,
                first_seen_at=?, last_seen_at=?, event_time=?, summary=?, summary_zh=?,
                impact_summary=?, impact_summary_zh=?, impact_score=?, verification_status=?,
                primary_source_url=COALESCE(primary_source_url, ?), updated_at=?
            WHERE id=?
            """,
            (
                bundle["title"],
                bundle.get("title_zh"),
                bundle["category"],
                bundle["priority"],
                bundle.get("confidence", 0.5),
                first_seen,
                now_iso(),
                bundle.get("event_time"),
                bundle.get("summary"),
                bundle.get("summary_zh"),
                bundle.get("impact_summary"),
                bundle.get("impact_summary_zh"),
                bundle.get("impact_score", 0),
                bundle.get("verification_status", "watching"),
                doc["url"],
                now_iso(),
                event_id,
            ),
        )
        added = False
    else:
        cursor = conn.execute(
            """
            INSERT INTO intelligence_events
            (event_key, title, title_zh, category, priority, status, confidence,
             first_seen_at, last_seen_at, event_time, summary, summary_zh,
             impact_summary, impact_summary_zh, impact_score, verification_status, primary_source_url, updated_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle["event_key"],
                bundle["title"],
                bundle.get("title_zh"),
                bundle["category"],
                bundle["priority"],
                bundle.get("confidence", 0.5),
                first_seen,
                first_seen,
                bundle.get("event_time"),
                bundle.get("summary"),
                bundle.get("summary_zh"),
                bundle.get("impact_summary"),
                bundle.get("impact_summary_zh"),
                bundle.get("impact_score", 0),
                bundle.get("verification_status", "watching"),
                doc["url"],
                now_iso(),
            ),
        )
        event_id = int(cursor.lastrowid)
        added = True

    for fact in bundle.get("facts") or []:
        if len(fact) == 6:
            fact_type, label, value, unit, sort_order, confidence = fact
            label_zh = None
            value_zh = None
        else:
            fact_type, label, label_zh, value, value_zh, unit, sort_order, confidence = fact
        conn.execute(
            """
            INSERT OR IGNORE INTO event_facts
            (event_id, fact_type, label, label_zh, value, value_zh, unit, source_url, confidence, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, fact_type, label, label_zh, value, value_zh, unit, doc["url"], confidence, sort_order),
        )

    for entity_type, name, ticker, market, role, relevance_score in bundle.get("entities") or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO event_entities
            (event_id, entity_type, name, ticker, market, role, relevance_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, entity_type, name, ticker, market, role, relevance_score),
        )

    conn.execute(
        """
        INSERT OR IGNORE INTO event_updates
        (event_id, raw_document_id, source_key, update_type, title, summary, published_at, url, created_at)
        VALUES (?, ?, ?, 'source_seen', ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            doc_id,
            doc["source_key"],
            doc.get("title"),
            doc.get("summary"),
            doc.get("published_at"),
            doc["url"],
            now_iso(),
        ),
    )

    conn.execute(
        """
        UPDATE intelligence_events
        SET source_count = (
            SELECT COUNT(DISTINCT source_key)
            FROM event_updates
            WHERE event_id = ?
        )
        WHERE id = ?
        """,
        (event_id, event_id),
    )

    research = bundle.get("research")
    if research:
        source = source_meta_for_key(doc["source_key"])
        metadata = doc.get("metadata") or {}
        focus_areas = detect_focus_areas(
            source.get("focus_area"),
            doc.get("title"),
            doc.get("summary"),
            research.get("relevance"),
            research.get("thesis"),
        )
        upsert_research_report(
            conn,
            doc,
            {
                "report_key": content_hash(f"{bundle['event_key']}|{doc['url']}")[:24],
                "title": doc.get("title") or bundle["title"],
                "title_zh": doc.get("title_zh") or "",
                "source_key": doc["source_key"],
                "source_name": source.get("name") or doc["source_key"],
                "url": doc.get("canonical_url") or doc["url"],
                "report_type": research.get("report_type", "research"),
                "publisher_region": default_publisher_region(source),
                "source_tier": default_source_tier(source),
                "target_scope": infer_target_scope(doc.get("title"), research.get("relevance"), research.get("thesis")),
                "published_at": doc.get("published_at"),
                "language": (doc.get("metadata") or {}).get("language", "en"),
                "summary": doc.get("summary") or bundle.get("summary"),
                "summary_zh": doc.get("summary_zh") or "",
                "thesis": research.get("thesis"),
                "thesis_zh": research.get("thesis_zh") or "",
                "relevance": research.get("relevance"),
                "relevance_zh": research.get("relevance_zh") or "",
                "focus_areas": focus_areas,
                "tags": dedupe_strings(focus_areas + [bundle.get("category") or "", source.get("category") or ""]),
                "tickers": dedupe_strings([metadata.get("stock_code") or ""]),
                "key_points": extract_key_points(doc.get("summary"), research.get("thesis"), research.get("relevance")),
                "original_url": metadata.get("asset_url") or metadata.get("original_url") or doc["url"],
                "evidence": [
                    ("Event", bundle["title"], doc["url"]),
                    ("Source", source.get("name") or doc["source_key"], doc["url"]),
                ],
            },
        )
    return added


def source_by_key() -> Dict[str, Dict[str, Any]]:
    return {source["source_key"]: source for source in SOURCES}


def update_source_status(conn: sqlite3.Connection, source_key: str, ok: bool, error: str = "") -> None:
    conn.execute(
        """
        UPDATE source_registry
        SET last_checked_at=?, last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END,
            last_error=?, updated_at=?
        WHERE source_key=?
        """,
        (now_iso(), 1 if ok else 0, now_iso(), error[:500] if error else None, now_iso(), source_key),
    )


def require_db_helpers() -> None:
    if get_sqlite_connection is None or IntelligenceService is None:
        raise RuntimeError("database helpers are unavailable; use --collect-only on lightweight collectors")


def collect_documents(include_bootstrap: bool = BOOTSTRAP_SEED) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], int]:
    session = get_session()
    documents_by_source: Dict[str, List[Dict[str, Any]]] = {}
    source_runs: List[Dict[str, Any]] = []
    failures = 0

    if include_bootstrap:
        for doc in BOOTSTRAP_DOCUMENTS:
            documents_by_source.setdefault(doc["source_key"], []).append(doc)

    for source in SOURCES:
        if source["collection_method"] == "bootstrap":
            continue
        started_at = now_iso()
        run_status = "success"
        found = 0
        error = ""
        if not source_runs_on_current_collector(source):
            reason = "disabled" if not int(source.get("enabled", 1)) else f"collector={source.get('collector')} skipped on {COLLECTOR_MODE}"
            source_runs.append(
                {
                    "source_key": source["source_key"],
                    "started_at": started_at,
                    "finished_at": now_iso(),
                    "status": "skipped",
                    "records_found": 0,
                    "records_added": 0,
                    "records_updated": 0,
                    "error": reason,
                }
            )
            continue
        try:
            if source["collection_method"] == "fixed_html":
                docs = fetch_fixed_html(session, source)
            elif source["collection_method"] == "json_list":
                docs = fetch_json_list(session, source)
            elif source["collection_method"] == "feed":
                docs = fetch_feed(session, source)
            elif source["collection_method"] == "search_proxy":
                docs = fetch_search_proxy(session, source)
            elif source["collection_method"] == "curated_public_research":
                docs = fetch_curated_public_research(session, source)
            elif source["collection_method"] == "eastmoney_keyword":
                docs = fetch_eastmoney_keyword(session, source)
            elif source["collection_method"] == "akshare_stock_bucket":
                docs = fetch_akshare_stock_bucket(session, source)
            else:
                docs = []
            documents_by_source.setdefault(source["source_key"], []).extend(docs)
            found = len(docs)
        except Exception as exc:
            failures += 1
            run_status = "failed"
            error = str(exc)[:500]
            print(f"  {source['source_key']} FAIL: {error}")

        source_runs.append(
            {
                "source_key": source["source_key"],
                "started_at": started_at,
                "finished_at": now_iso(),
                "status": run_status,
                "records_found": found,
                "records_added": 0,
                "records_updated": 0,
                "error": error,
            }
        )
    return documents_by_source, source_runs, failures


def apply_documents_to_db(
    documents_by_source: Dict[str, List[Dict[str, Any]]],
    source_runs: List[Dict[str, Any]],
    failures: int,
) -> Dict[str, int]:
    require_db_helpers()
    service = IntelligenceService(DB_PATH)
    total_found = 0
    total_added_docs = 0
    total_updated_docs = 0
    total_events_added = 0
    total_research_added = 0
    promoted_by_source: Dict[str, int] = {}

    with get_sqlite_connection(DB_PATH, timeout=60, busy_timeout=15000) as conn:
        ensure_sources(conn)
        conn.commit()

        for run in source_runs:
            status = run.get("status") or "unknown"
            if status == "success":
                update_source_status(conn, run["source_key"], ok=True)
            elif status == "failed":
                update_source_status(conn, run["source_key"], ok=False, error=run.get("error") or "")
            conn.execute(
                """
                INSERT INTO collection_runs
                (source_key, started_at, finished_at, status, records_found, records_added, records_updated, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.get("source_key"),
                    run.get("started_at"),
                    run.get("finished_at"),
                    status,
                    int(run.get("records_found") or 0),
                    int(run.get("records_added") or 0),
                    int(run.get("records_updated") or 0),
                    run.get("error") or "",
                ),
            )

        for source_key, docs in documents_by_source.items():
            for doc in docs:
                doc.setdefault("source_key", source_key)
                total_found += 1
                doc_id, added_doc, updated_doc = upsert_raw_document(conn, doc)
                if added_doc:
                    total_added_docs += 1
                if updated_doc:
                    total_updated_docs += 1
                bundle = event_payload_for_document(doc)
                if bundle:
                    priority = bundle.get("priority") or "P2"
                    limit = EVENT_PROMOTION_LIMITS.get(source_key)
                    if priority != "P0" and limit is not None:
                        promoted = promoted_by_source.get(source_key, 0)
                        if promoted >= limit:
                            continue
                        promoted_by_source[source_key] = promoted + 1
                    if upsert_event_bundle(conn, doc_id, doc, bundle):
                        total_events_added += 1
                else:
                    research = research_payload_for_document(doc)
                    if research and upsert_research_report(conn, doc, research):
                        total_research_added += 1
            update_source_status(conn, source_key, ok=True)

        conn.commit()

    _service = service  # keep ensure_tables side effect obvious for linters
    return {
        "records_found": total_found,
        "records_added": total_added_docs,
        "records_updated": total_updated_docs,
        "events_added": total_events_added,
        "research_added": total_research_added,
        "records_failed": failures,
    }


def run_collection() -> Dict[str, int]:
    documents_by_source, source_runs, failures = collect_documents(include_bootstrap=BOOTSTRAP_SEED)
    return apply_documents_to_db(documents_by_source, source_runs, failures)


def export_bundle(output_path: str, include_bootstrap: bool = False) -> Dict[str, int]:
    documents_by_source, source_runs, failures = collect_documents(include_bootstrap=include_bootstrap)
    documents = []
    for _source_key, docs in documents_by_source.items():
        documents.extend(docs)
    bundle = {
        "generated_at": now_iso(),
        "collector_mode": COLLECTOR_MODE,
        "sources": SOURCES,
        "source_runs": source_runs,
        "documents": documents,
        "metrics": {
            "records_found": len(documents),
            "records_failed": failures,
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, ensure_ascii=False, indent=2)
    return {
        "records_found": len(documents),
        "records_added": 0,
        "records_updated": 0,
        "events_added": 0,
        "records_failed": failures,
    }


def import_bundle(input_path: str) -> Dict[str, int]:
    with open(input_path, "r", encoding="utf-8") as fh:
        bundle = json.load(fh)
    documents_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for doc in bundle.get("documents") or []:
        source_key = doc.get("source_key") or "unknown"
        documents_by_source.setdefault(source_key, []).append(doc)
    source_runs = bundle.get("source_runs") or []
    failures = int((bundle.get("metrics") or {}).get("records_failed") or 0)
    return apply_documents_to_db(documents_by_source, source_runs, failures)


def main() -> int:
    parser = argparse.ArgumentParser(description="Investment intelligence collector")
    parser.add_argument("--collect-only", action="store_true", help="collect sources and write a JSON bundle without touching SQLite")
    parser.add_argument("--output", default="", help="bundle output path for --collect-only")
    parser.add_argument("--import-json", default="", help="import a collector JSON bundle into SQLite")
    parser.add_argument("--no-bootstrap", action="store_true", help="skip verified bootstrap documents")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Intelligence Sync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    if args.collect_only:
        output = args.output or os.path.join(BASE_DIR, "data", "intelligence_bundle.json")
        metrics = export_bundle(output, include_bootstrap=not args.no_bootstrap)
        print(f"[OK] Collector bundle written: {output}")
    elif args.import_json:
        metrics = import_bundle(args.import_json)
        print(f"[OK] Collector bundle imported: {args.import_json}")
    else:
        metrics = run_collection()
    print(
        f"[OK] Intelligence sync: found={metrics['records_found']}, "
        f"added_docs={metrics['records_added']}, updated_docs={metrics['records_updated']}, "
        f"new_events={metrics['events_added']}, failed_sources={metrics['records_failed']}"
    )
    print("ETL_METRICS_JSON=" + json.dumps(metrics, ensure_ascii=False))
    # Partial source failures are acceptable because pages read persisted data.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
