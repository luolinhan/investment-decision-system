"""Shortline execution layer for US-led cross-market mapping."""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv
from xml.etree import ElementTree as ET

from app.db import get_sqlite_connection
from app.services.bailian_client import BailianJsonTranslator
from quant_workbench.service import QuantWorkbenchService

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "investment.db"


PLAYBOOKS: List[Dict[str, Any]] = [
    {
        "playbook_key": "adr_hk_gap_repair",
        "name": "ADR/H 股价差修复",
        "market_scope": "HK",
        "theme": "cross_listed",
        "trigger_condition": "ADR 隔夜涨跌幅显著偏离港股前收，且港股竞价跟随不足。",
        "entry_rule": "竞价偏弱但开盘后 15 分钟补涨确认时介入。",
        "timing_rule": "09:20-10:00",
        "risk_rule": "卖空占比恶化或南向转负时降权。",
        "exit_rule": "日内价差修复后分批兑现。",
        "invalid_condition": "H 股直接低开低走且 30 分钟内无量能回补。",
        "source_tier": "T1",
        "event_required": 0,
    },
    {
        "playbook_key": "us_earnings_spillover",
        "name": "美股财报/指引溢出",
        "market_scope": "A/H",
        "theme": "earnings",
        "trigger_condition": "美股龙头财报 beat、指引上修或资本开支上修。",
        "entry_rule": "只做有直接链条证据的中国供应链或同主体映射。",
        "timing_rule": "09:15-10:30",
        "risk_rule": "市场 risk_off 时仓位打折。",
        "exit_rule": "首日强兑现后转入趋势持有或减仓。",
        "invalid_condition": "中国映射标的竞价不强，或板块不共振。",
        "source_tier": "T0",
        "event_required": 1,
    },
    {
        "playbook_key": "us_etf_theme_rotation",
        "name": "美股主题 ETF -> 中国主题映射",
        "market_scope": "A/H/ETF",
        "theme": "sector_rotation",
        "trigger_condition": "SMH/BOTZ/IBB/TAN/NLR 等主题 ETF 显著强于大盘。",
        "entry_rule": "优先 ETF，其次龙头，再到二线映射。",
        "timing_rule": "09:20-10:30",
        "risk_rule": "只追随一阶映射，不做泛概念扩散。",
        "exit_rule": "主题冲高回落时先减 ETF，再减个股。",
        "invalid_condition": "中国对应 ETF 竞价转弱且核心龙头不跟随。",
        "source_tier": "T1",
        "event_required": 0,
    },
    {
        "playbook_key": "cn_dragon_follow",
        "name": "港股先动 / 中国龙头次日跟随",
        "market_scope": "A/H",
        "theme": "follow_through",
        "trigger_condition": "港股强势龙头或中概龙头先行走强。",
        "entry_rule": "只在 A 股同链条 ETF/龙头竞价同步时执行。",
        "timing_rule": "09:20-10:00",
        "risk_rule": "板块共振不足则放弃。",
        "exit_rule": "日内冲高分批，次日弱转强再留。",
        "invalid_condition": "A 股开盘不跟，且北向 30 分钟内明显流出。",
        "source_tier": "T1",
        "event_required": 0,
    },
    {
        "playbook_key": "fda_readout_follow",
        "name": "FDA / 临床读出跟随",
        "market_scope": "A/H",
        "theme": "biotech",
        "trigger_condition": "FDA 批准、临床 readout、NDA/BLA 推进。",
        "entry_rule": "优先直接受益主体，再看 CRO/CXO 扩散。",
        "timing_rule": "09:15-10:30",
        "risk_rule": "仅接受官方事件源；传闻不进入执行层。",
        "exit_rule": "事件兑现后看板块二阶扩散强度。",
        "invalid_condition": "国内映射公司并无对应管线或市场误映射。",
        "source_tier": "T0",
        "event_required": 1,
    },
    {
        "playbook_key": "robotics_chain_follow",
        "name": "机器人/自动化链映射",
        "market_scope": "A/H",
        "theme": "robotics",
        "trigger_condition": "特斯拉机器人、工业自动化或 BOTZ 显著异动。",
        "entry_rule": "优先减速器、伺服、工控、整机龙头。",
        "timing_rule": "09:20-10:30",
        "risk_rule": "区分整机叙事与真实订单链。",
        "exit_rule": "高开冲高不追，分歧回踩确认再介入。",
        "invalid_condition": "仅情绪驱动，无硬事件/ETF/指数配合。",
        "source_tier": "T1",
        "event_required": 0,
    },
    {
        "playbook_key": "energy_policy_follow",
        "name": "能源政策 / 核电光伏映射",
        "market_scope": "A/H",
        "theme": "energy",
        "trigger_condition": "核电、光伏、储能海外政策或龙头大幅异动。",
        "entry_rule": "优先政策直接相关的中国龙头与 ETF。",
        "timing_rule": "09:20-10:30",
        "risk_rule": "注意国内价格链与产能消息反向扰动。",
        "exit_rule": "政策确认后保留强趋势，去掉纯情绪跟风。",
        "invalid_condition": "国内价格链利空覆盖政策利多。",
        "source_tier": "T0/T1",
        "event_required": 0,
    },
    {
        "playbook_key": "agri_cycle_follow",
        "name": "农业 / 养猪链跟随",
        "market_scope": "A",
        "theme": "agriculture",
        "trigger_condition": "美股农业、饲料、生猪相关标的大涨，或 USDA 数据利多。",
        "entry_rule": "结合国内猪价、期货和龙头竞价验证。",
        "timing_rule": "09:20-10:00",
        "risk_rule": "没有期货/现货验证时不提级。",
        "exit_rule": "冲高兑现，回踩再看基本面。",
        "invalid_condition": "国内生猪期货和现货价格未配合。",
        "source_tier": "T0/T1",
        "event_required": 0,
    },
]


SEED_MAPPINGS: List[Dict[str, Any]] = [
    # Direct ADR / HK
    {"us_symbol": "BABA", "us_name": "Alibaba", "cn_symbol": "9988.HK", "cn_name": "阿里巴巴-W", "market": "HK", "relation_type": "adr_to_hk", "theme": "internet", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "JD", "us_name": "JD.com", "cn_symbol": "9618.HK", "cn_name": "京东集团-SW", "market": "HK", "relation_type": "adr_to_hk", "theme": "internet", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "NTES", "us_name": "NetEase", "cn_symbol": "9999.HK", "cn_name": "网易-S", "market": "HK", "relation_type": "adr_to_hk", "theme": "gaming", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "PDD", "us_name": "PDD", "cn_symbol": "PDD_CHAIN", "cn_name": "拼多多映射池", "market": "A", "relation_type": "thematic_proxy", "theme": "ecommerce", "strength_score": 68, "evidence_source": "platform_peer"},
    {"us_symbol": "TCEHY", "us_name": "Tencent", "cn_symbol": "0700.HK", "cn_name": "腾讯控股", "market": "HK", "relation_type": "same_entity", "theme": "internet", "strength_score": 99, "evidence_source": "same_entity"},
    {"us_symbol": "NIO", "us_name": "NIO", "cn_symbol": "9866.HK", "cn_name": "蔚来-SW", "market": "HK", "relation_type": "adr_to_hk", "theme": "ev", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "XPEV", "us_name": "XPeng", "cn_symbol": "9868.HK", "cn_name": "小鹏汽车-W", "market": "HK", "relation_type": "adr_to_hk", "theme": "ev", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "LI", "us_name": "Li Auto", "cn_symbol": "2015.HK", "cn_name": "理想汽车-W", "market": "HK", "relation_type": "adr_to_hk", "theme": "ev", "strength_score": 98, "evidence_source": "same_entity"},
    {"us_symbol": "BIDU", "us_name": "Baidu", "cn_symbol": "9888.HK", "cn_name": "百度集团-SW", "market": "HK", "relation_type": "adr_to_hk", "theme": "ai", "strength_score": 98, "evidence_source": "same_entity"},
    # AI / infra
    {"us_symbol": "NVDA", "us_name": "NVIDIA", "cn_symbol": "300308.SZ", "cn_name": "中际旭创", "market": "A", "relation_type": "supplier", "theme": "ai", "strength_score": 90, "evidence_source": "optical_interconnect"},
    {"us_symbol": "NVDA", "us_name": "NVIDIA", "cn_symbol": "300502.SZ", "cn_name": "新易盛", "market": "A", "relation_type": "supplier", "theme": "ai", "strength_score": 89, "evidence_source": "optical_interconnect"},
    {"us_symbol": "NVDA", "us_name": "NVIDIA", "cn_symbol": "002463.SZ", "cn_name": "沪电股份", "market": "A", "relation_type": "supplier", "theme": "ai", "strength_score": 86, "evidence_source": "pcb"},
    {"us_symbol": "NVDA", "us_name": "NVIDIA", "cn_symbol": "601138.SH", "cn_name": "工业富联", "market": "A", "relation_type": "supplier", "theme": "ai", "strength_score": 84, "evidence_source": "server_assembly"},
    {"us_symbol": "AMD", "us_name": "AMD", "cn_symbol": "688041.SH", "cn_name": "海光信息", "market": "A", "relation_type": "direct_peer", "theme": "ai", "strength_score": 85, "evidence_source": "cpu_gpu_peer"},
    {"us_symbol": "SMCI", "us_name": "Super Micro", "cn_symbol": "603019.SH", "cn_name": "中科曙光", "market": "A", "relation_type": "direct_peer", "theme": "ai", "strength_score": 80, "evidence_source": "server_peer"},
    {"us_symbol": "MSFT", "us_name": "Microsoft", "cn_symbol": "9888.HK", "cn_name": "百度集团-SW", "market": "HK", "relation_type": "direct_peer", "theme": "ai", "strength_score": 73, "evidence_source": "cloud_ai_peer"},
    {"us_symbol": "META", "us_name": "Meta", "cn_symbol": "1024.HK", "cn_name": "快手-W", "market": "HK", "relation_type": "direct_peer", "theme": "ai", "strength_score": 64, "evidence_source": "ad_ai_peer"},
    {"us_symbol": "GOOGL", "us_name": "Alphabet", "cn_symbol": "3690.HK", "cn_name": "美团-W", "market": "HK", "relation_type": "thematic_proxy", "theme": "ai", "strength_score": 55, "evidence_source": "internet_ai_proxy"},
    {"us_symbol": "BOTZ", "us_name": "Global X Robotics ETF", "cn_symbol": "159551.SZ", "cn_name": "机器人ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "robotics", "strength_score": 86, "evidence_source": "theme_etf"},
    {"us_symbol": "BOTZ", "us_name": "Global X Robotics ETF", "cn_symbol": "002747.SZ", "cn_name": "埃斯顿", "market": "A", "relation_type": "direct_peer", "theme": "robotics", "strength_score": 79, "evidence_source": "automation_peer"},
    {"us_symbol": "TSLA", "us_name": "Tesla", "cn_symbol": "603596.SH", "cn_name": "伯特利", "market": "A", "relation_type": "supplier", "theme": "robotics", "strength_score": 74, "evidence_source": "auto_robotics_supply"},
    {"us_symbol": "TSLA", "us_name": "Tesla", "cn_symbol": "300124.SZ", "cn_name": "汇川技术", "market": "A", "relation_type": "thematic_proxy", "theme": "robotics", "strength_score": 76, "evidence_source": "servo_control"},
    {"us_symbol": "TSLA", "us_name": "Tesla", "cn_symbol": "688017.SH", "cn_name": "绿的谐波", "market": "A", "relation_type": "supplier", "theme": "robotics", "strength_score": 82, "evidence_source": "harmonic_drive"},
    # Semis
    {"us_symbol": "TSM", "us_name": "TSMC", "cn_symbol": "0981.HK", "cn_name": "中芯国际", "market": "HK", "relation_type": "direct_peer", "theme": "semiconductor", "strength_score": 88, "evidence_source": "foundry_peer"},
    {"us_symbol": "ASML", "us_name": "ASML", "cn_symbol": "688012.SH", "cn_name": "中微公司", "market": "A", "relation_type": "direct_peer", "theme": "semiconductor", "strength_score": 85, "evidence_source": "equipment_peer"},
    {"us_symbol": "AMAT", "us_name": "Applied Materials", "cn_symbol": "002371.SZ", "cn_name": "北方华创", "market": "A", "relation_type": "direct_peer", "theme": "semiconductor", "strength_score": 89, "evidence_source": "equipment_peer"},
    {"us_symbol": "AMAT", "us_name": "Applied Materials", "cn_symbol": "688072.SH", "cn_name": "拓荆科技", "market": "A", "relation_type": "direct_peer", "theme": "semiconductor", "strength_score": 81, "evidence_source": "deposition_peer"},
    {"us_symbol": "LRCX", "us_name": "Lam Research", "cn_symbol": "688037.SH", "cn_name": "芯源微", "market": "A", "relation_type": "direct_peer", "theme": "semiconductor", "strength_score": 74, "evidence_source": "process_peer"},
    {"us_symbol": "SMH", "us_name": "VanEck Semiconductor ETF", "cn_symbol": "159995.SZ", "cn_name": "芯片ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "semiconductor", "strength_score": 91, "evidence_source": "theme_etf"},
    {"us_symbol": "SOXX", "us_name": "iShares Semiconductor ETF", "cn_symbol": "516640.SH", "cn_name": "半导体设备ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "semiconductor", "strength_score": 84, "evidence_source": "theme_etf"},
    # Biotech
    {"us_symbol": "LLY", "us_name": "Eli Lilly", "cn_symbol": "1801.HK", "cn_name": "信达生物", "market": "HK", "relation_type": "direct_peer", "theme": "biotech", "strength_score": 79, "evidence_source": "glp1_peer"},
    {"us_symbol": "LLY", "us_name": "Eli Lilly", "cn_symbol": "600276.SH", "cn_name": "恒瑞医药", "market": "A", "relation_type": "direct_peer", "theme": "biotech", "strength_score": 76, "evidence_source": "innovative_drug_peer"},
    {"us_symbol": "NVO", "us_name": "Novo Nordisk", "cn_symbol": "1801.HK", "cn_name": "信达生物", "market": "HK", "relation_type": "direct_peer", "theme": "biotech", "strength_score": 76, "evidence_source": "glp1_peer"},
    {"us_symbol": "MRNA", "us_name": "Moderna", "cn_symbol": "2269.HK", "cn_name": "药明生物", "market": "HK", "relation_type": "customer", "theme": "biotech", "strength_score": 67, "evidence_source": "cxo_proxy"},
    {"us_symbol": "BNTX", "us_name": "BioNTech", "cn_symbol": "603259.SH", "cn_name": "药明康德", "market": "A", "relation_type": "customer", "theme": "biotech", "strength_score": 65, "evidence_source": "cxo_proxy"},
    {"us_symbol": "XBI", "us_name": "SPDR Biotech ETF", "cn_symbol": "512290.SH", "cn_name": "生物医药ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "biotech", "strength_score": 88, "evidence_source": "theme_etf"},
    {"us_symbol": "IBB", "us_name": "iShares Biotech ETF", "cn_symbol": "9926.HK", "cn_name": "康方生物", "market": "HK", "relation_type": "thematic_proxy", "theme": "biotech", "strength_score": 72, "evidence_source": "biotech_theme"},
    # Solar / energy
    {"us_symbol": "FSLR", "us_name": "First Solar", "cn_symbol": "601012.SH", "cn_name": "隆基绿能", "market": "A", "relation_type": "direct_peer", "theme": "solar", "strength_score": 78, "evidence_source": "solar_peer"},
    {"us_symbol": "ENPH", "us_name": "Enphase", "cn_symbol": "300274.SZ", "cn_name": "阳光电源", "market": "A", "relation_type": "direct_peer", "theme": "solar", "strength_score": 77, "evidence_source": "inverter_peer"},
    {"us_symbol": "TAN", "us_name": "Invesco Solar ETF", "cn_symbol": "159857.SZ", "cn_name": "光伏ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "solar", "strength_score": 89, "evidence_source": "theme_etf"},
    {"us_symbol": "TAN", "us_name": "Invesco Solar ETF", "cn_symbol": "600438.SH", "cn_name": "通威股份", "market": "A", "relation_type": "thematic_proxy", "theme": "solar", "strength_score": 68, "evidence_source": "solar_leader"},
    {"us_symbol": "NLR", "us_name": "VanEck Uranium+Nuclear ETF", "cn_symbol": "601985.SH", "cn_name": "中国核电", "market": "A", "relation_type": "etf_proxy", "theme": "nuclear", "strength_score": 90, "evidence_source": "nuclear_theme"},
    {"us_symbol": "URA", "us_name": "Global X Uranium ETF", "cn_symbol": "1816.HK", "cn_name": "中广核电力", "market": "HK", "relation_type": "etf_proxy", "theme": "nuclear", "strength_score": 84, "evidence_source": "nuclear_theme"},
    {"us_symbol": "OKLO", "us_name": "Oklo", "cn_symbol": "603308.SH", "cn_name": "应流股份", "market": "A", "relation_type": "thematic_proxy", "theme": "nuclear", "strength_score": 63, "evidence_source": "smr_proxy"},
    # Agriculture / hog
    {"us_symbol": "ADM", "us_name": "ADM", "cn_symbol": "002714.SZ", "cn_name": "牧原股份", "market": "A", "relation_type": "thematic_proxy", "theme": "hog", "strength_score": 63, "evidence_source": "agri_cycle"},
    {"us_symbol": "TSN", "us_name": "Tyson Foods", "cn_symbol": "300498.SZ", "cn_name": "温氏股份", "market": "A", "relation_type": "direct_peer", "theme": "hog", "strength_score": 71, "evidence_source": "hog_peer"},
    {"us_symbol": "MOO", "us_name": "VanEck Agribusiness ETF", "cn_symbol": "000876.SZ", "cn_name": "新希望", "market": "A", "relation_type": "etf_proxy", "theme": "hog", "strength_score": 66, "evidence_source": "agri_theme"},
    # Dividend / financials
    {"us_symbol": "DIA", "us_name": "SPDR Dow Jones ETF", "cn_symbol": "510880.SH", "cn_name": "红利ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "dividend", "strength_score": 74, "evidence_source": "dividend_proxy"},
    {"us_symbol": "XLF", "us_name": "Financial Select Sector SPDR", "cn_symbol": "512800.SH", "cn_name": "银行ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "dividend", "strength_score": 72, "evidence_source": "financial_proxy"},
    {"us_symbol": "KWEB", "us_name": "KraneShares CSI China Internet ETF", "cn_symbol": "3067.HK", "cn_name": "恒生科技ETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "internet", "strength_score": 88, "evidence_source": "china_internet_etf"},
    {"us_symbol": "CQQQ", "us_name": "Invesco China Technology ETF", "cn_symbol": "159998.SZ", "cn_name": "TMTETF", "market": "ETF", "relation_type": "etf_proxy", "theme": "ai", "strength_score": 73, "evidence_source": "china_tech_etf"},
]


US_WATCHLIST: List[Dict[str, Any]] = [
    {"symbol": "NVDA", "name": "NVIDIA", "sector": "ai", "kind": "leader"},
    {"symbol": "AMD", "name": "AMD", "sector": "ai", "kind": "leader"},
    {"symbol": "AVGO", "name": "Broadcom", "sector": "ai", "kind": "leader"},
    {"symbol": "SMCI", "name": "Super Micro", "sector": "ai", "kind": "leader"},
    {"symbol": "MSFT", "name": "Microsoft", "sector": "ai", "kind": "leader"},
    {"symbol": "META", "name": "Meta", "sector": "ai", "kind": "leader"},
    {"symbol": "GOOGL", "name": "Alphabet", "sector": "ai", "kind": "leader"},
    {"symbol": "TSM", "name": "TSMC", "sector": "semiconductor", "kind": "leader"},
    {"symbol": "ASML", "name": "ASML", "sector": "semiconductor", "kind": "leader"},
    {"symbol": "AMAT", "name": "Applied Materials", "sector": "semiconductor", "kind": "leader"},
    {"symbol": "LRCX", "name": "Lam Research", "sector": "semiconductor", "kind": "leader"},
    {"symbol": "TSLA", "name": "Tesla", "sector": "robotics", "kind": "leader"},
    {"symbol": "ISRG", "name": "Intuitive Surgical", "sector": "biotech", "kind": "leader"},
    {"symbol": "LLY", "name": "Eli Lilly", "sector": "biotech", "kind": "leader"},
    {"symbol": "NVO", "name": "Novo Nordisk", "sector": "biotech", "kind": "leader"},
    {"symbol": "MRNA", "name": "Moderna", "sector": "biotech", "kind": "leader"},
    {"symbol": "BNTX", "name": "BioNTech", "sector": "biotech", "kind": "leader"},
    {"symbol": "FSLR", "name": "First Solar", "sector": "solar", "kind": "leader"},
    {"symbol": "ENPH", "name": "Enphase", "sector": "solar", "kind": "leader"},
    {"symbol": "OKLO", "name": "Oklo", "sector": "nuclear", "kind": "leader"},
    {"symbol": "ADM", "name": "ADM", "sector": "hog", "kind": "leader"},
    {"symbol": "TSN", "name": "Tyson Foods", "sector": "hog", "kind": "leader"},
    {"symbol": "BABA", "name": "Alibaba", "sector": "internet", "kind": "adr"},
    {"symbol": "JD", "name": "JD.com", "sector": "internet", "kind": "adr"},
    {"symbol": "NTES", "name": "NetEase", "sector": "gaming", "kind": "adr"},
    {"symbol": "PDD", "name": "PDD", "sector": "ecommerce", "kind": "adr"},
    {"symbol": "BIDU", "name": "Baidu", "sector": "ai", "kind": "adr"},
    {"symbol": "NIO", "name": "NIO", "sector": "ev", "kind": "adr"},
    {"symbol": "XPEV", "name": "XPeng", "sector": "ev", "kind": "adr"},
    {"symbol": "LI", "name": "Li Auto", "sector": "ev", "kind": "adr"},
    {"symbol": "SMH", "name": "VanEck Semiconductor ETF", "sector": "semiconductor", "kind": "etf"},
    {"symbol": "SOXX", "name": "iShares Semiconductor ETF", "sector": "semiconductor", "kind": "etf"},
    {"symbol": "BOTZ", "name": "Global X Robotics ETF", "sector": "robotics", "kind": "etf"},
    {"symbol": "IBB", "name": "iShares Biotechnology ETF", "sector": "biotech", "kind": "etf"},
    {"symbol": "XBI", "name": "SPDR Biotech ETF", "sector": "biotech", "kind": "etf"},
    {"symbol": "TAN", "name": "Invesco Solar ETF", "sector": "solar", "kind": "etf"},
    {"symbol": "NLR", "name": "VanEck Uranium+Nuclear ETF", "sector": "nuclear", "kind": "etf"},
    {"symbol": "URA", "name": "Global X Uranium ETF", "sector": "nuclear", "kind": "etf"},
    {"symbol": "MOO", "name": "VanEck Agribusiness ETF", "sector": "hog", "kind": "etf"},
    {"symbol": "KWEB", "name": "KraneShares CSI China Internet ETF", "sector": "internet", "kind": "etf"},
    {"symbol": "CQQQ", "name": "Invesco China Technology ETF", "sector": "ai", "kind": "etf"},
    {"symbol": "XLF", "name": "Financial Select Sector SPDR", "sector": "dividend", "kind": "etf"},
    {"symbol": "DIA", "name": "SPDR Dow Jones ETF", "sector": "dividend", "kind": "etf"},
]


THEME_LABELS = {
    "ai": "AI",
    "semiconductor": "芯片半导体",
    "robotics": "机器人",
    "biotech": "创新药",
    "solar": "光伏",
    "nuclear": "核电",
    "hog": "养猪",
    "dividend": "红利/高股息",
    "internet": "互联网",
    "gaming": "游戏",
    "ecommerce": "电商",
    "ev": "新能源车",
}

SEC_RELEVANT_FORMS = {"8-K", "6-K", "10-Q", "10-K", "20-F"}
SEC_CATALYST_KEYWORDS = (
    "earnings",
    "financial results",
    "results of operations",
    "guidance",
    "outlook",
    "quarterly report",
    "annual report",
    "repurchase",
    "buyback",
    "acquisition",
    "merger",
    "agreement",
    "partnership",
    "approval",
    "launch",
    "production",
    "expansion",
    "capex",
)

FDA_SPONSOR_ALIASES: Dict[str, List[str]] = {
    "LLY": ["ELI LILLY", "LILLY"],
    "NVO": ["NOVO NORDISK"],
    "MRNA": ["MODERNA"],
    "BNTX": ["BIONTECH"],
}

CLINICAL_TRIAL_SPONSOR_ALIASES: Dict[str, List[str]] = {
    "LLY": ["Eli Lilly", "Lilly"],
    "NVO": ["Novo Nordisk"],
    "MRNA": ["Moderna"],
    "BNTX": ["BioNTech"],
}

COMPANY_IR_SOURCES: List[Dict[str, str]] = [
    {
        "symbol": "NVDA",
        "name": "NVIDIA",
        "sector": "ai",
        "url": "https://nvidianews.nvidia.com/releases.xml",
        "label": "NVIDIA Newsroom",
    },
    {
        "symbol": "AMD",
        "name": "AMD",
        "sector": "ai",
        "url": "https://ir.amd.com/news-events/press-releases/rss",
        "label": "AMD IR",
    },
    {
        "symbol": "META",
        "name": "Meta",
        "sector": "ai",
        "url": "https://about.fb.com/feed/",
        "label": "Meta Newsroom",
    },
    {
        "symbol": "MRNA",
        "name": "Moderna",
        "sector": "biotech",
        "url": "https://investors.modernatx.com/feed/rss2",
        "label": "Moderna IR",
    },
]

COMPANY_IR_MATERIAL_KEYWORDS = (
    "earnings",
    "results",
    "fiscal",
    "quarter",
    "guidance",
    "outlook",
    "partnership",
    "collaboration",
    "approval",
    "launch",
    "unveil",
    "data center",
    "blackwell",
    "mi300",
    "trial",
    "phase 3",
)


def _utcnow_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _normalize_openai_base_url(value: str, default: str) -> str:
    base_url = (value or default).strip().rstrip("/")
    if base_url.endswith("/chat/completions"):
        base_url = base_url[: -len("/chat/completions")]
    return base_url


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, "", "-", "--"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ShortlineService:
    """Cross-market shortline service."""

    def __init__(self, db_path: str | Path = DB_PATH):
        load_dotenv(BASE_DIR / ".env.local", override=False)
        load_dotenv(BASE_DIR / ".env", override=False)
        self.db_path = str(db_path)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
                )
            }
        )
        self.sec_session = requests.Session()
        self.sec_session.headers.update(
            {
                "User-Agent": os.getenv(
                    "SHORTLINE_SEC_USER_AGENT",
                    "InvestmentShortline/1.0 research@example.com",
                ),
                "Accept": "application/json",
            }
        )
        self.bailian_translator = BailianJsonTranslator(
            repo_root=BASE_DIR.parent,
            default_model="qwen3.6-plus",
            timeout_default=60,
        )
        self.bailian_api_key = self.bailian_translator.api_key
        self.bailian_base_url = self.bailian_translator.base_url
        self.bailian_model = self.bailian_translator.model
        self.bailian_timeout = self.bailian_translator.timeout
        self._sec_ticker_cache: Optional[Dict[str, Dict[str, str]]] = None
        self._reference_seed_lock = threading.Lock()
        self.workbench = QuantWorkbenchService()
        self.ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = get_sqlite_connection(self.db_path, timeout=60, busy_timeout=60000)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_table_columns(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [str(row[1]) for row in rows]

    def ensure_tables(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS cross_market_mapping_master (
                    mapping_id TEXT PRIMARY KEY,
                    us_symbol TEXT NOT NULL,
                    us_name TEXT,
                    cn_symbol TEXT NOT NULL,
                    cn_name TEXT,
                    market TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    theme TEXT,
                    strength_score REAL,
                    evidence_source TEXT,
                    manual_verified INTEGER DEFAULT 1,
                    updated_at TEXT NOT NULL,
                    UNIQUE(us_symbol, cn_symbol, relation_type)
                );

                CREATE TABLE IF NOT EXISTS cross_market_signal_events (
                    event_id TEXT PRIMARY KEY,
                    source_market TEXT NOT NULL,
                    source_symbol TEXT NOT NULL,
                    source_name TEXT,
                    sector TEXT,
                    event_type TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    headline_zh TEXT,
                    summary TEXT,
                    summary_zh TEXT,
                    facts_json TEXT,
                    impact_direction TEXT,
                    urgency TEXT,
                    source_url TEXT,
                    source_tier TEXT,
                    status TEXT DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cross_market_signal_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    source_symbol TEXT NOT NULL,
                    source_name TEXT,
                    event_time TEXT NOT NULL,
                    cn_symbol TEXT NOT NULL,
                    cn_name TEXT,
                    market TEXT NOT NULL,
                    theme TEXT,
                    relation_type TEXT NOT NULL,
                    mapping_strength_score REAL,
                    us_event_score REAL,
                    china_follow_through_score REAL,
                    execution_priority REAL,
                    priority TEXT,
                    playbook_key TEXT,
                    trade_playbook TEXT,
                    thesis TEXT,
                    entry_window TEXT,
                    invalid_condition TEXT,
                    status TEXT DEFAULT 'active',
                    source_url TEXT,
                    evidence_links_json TEXT,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(event_id, cn_symbol)
                );

                CREATE TABLE IF NOT EXISTS cross_market_playbooks (
                    playbook_key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    market_scope TEXT,
                    theme TEXT,
                    trigger_condition TEXT,
                    entry_rule TEXT,
                    timing_rule TEXT,
                    risk_rule TEXT,
                    exit_rule TEXT,
                    invalid_condition TEXT,
                    source_tier TEXT,
                    event_required INTEGER DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_xmap_us_symbol ON cross_market_mapping_master(us_symbol);
                CREATE INDEX IF NOT EXISTS idx_xmap_theme ON cross_market_mapping_master(theme);
                CREATE INDEX IF NOT EXISTS idx_xevent_symbol_time ON cross_market_signal_events(source_symbol, event_time DESC);
                CREATE INDEX IF NOT EXISTS idx_xevent_status_time ON cross_market_signal_events(status, event_time DESC);
                CREATE INDEX IF NOT EXISTS idx_xcandidate_priority ON cross_market_signal_candidates(priority, execution_priority DESC);
                CREATE INDEX IF NOT EXISTS idx_xcandidate_theme ON cross_market_signal_candidates(theme, market);
                """
            )
            columns = set(self._get_table_columns(conn, "cross_market_signal_events"))
            if "summary_zh" not in columns:
                conn.execute("ALTER TABLE cross_market_signal_events ADD COLUMN summary_zh TEXT")

    def ensure_reference_data(self, allow_locked: bool = False) -> Dict[str, int]:
        with self._reference_seed_lock:
            with self._connect() as conn:
                mapping_count = int(
                    conn.execute("SELECT COUNT(1) FROM cross_market_mapping_master").fetchone()[0] or 0
                )
                playbook_count = int(
                    conn.execute("SELECT COUNT(1) FROM cross_market_playbooks").fetchone()[0] or 0
                )

            result = {"mappings_seeded": 0, "playbooks_seeded": 0}
            if mapping_count == 0:
                try:
                    result["mappings_seeded"] = self.seed_mappings()
                except sqlite3.OperationalError:
                    if not allow_locked:
                        raise
            if playbook_count == 0:
                try:
                    result["playbooks_seeded"] = self.seed_playbooks()
                except sqlite3.OperationalError:
                    if not allow_locked:
                        raise
            return result

    @staticmethod
    def _mapping_id(item: Dict[str, Any]) -> str:
        raw = f"{item['us_symbol']}::{item['cn_symbol']}::{item['relation_type']}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

    def seed_mappings(self) -> int:
        now = _utcnow_iso()
        inserted = 0
        with self._connect() as conn:
            for row in SEED_MAPPINGS:
                payload = dict(row)
                payload["mapping_id"] = self._mapping_id(payload)
                payload["updated_at"] = now
                conn.execute(
                    """
                    INSERT OR IGNORE INTO cross_market_mapping_master (
                        mapping_id, us_symbol, us_name, cn_symbol, cn_name, market,
                        relation_type, theme, strength_score, evidence_source,
                        manual_verified, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["mapping_id"],
                        payload["us_symbol"],
                        payload["us_name"],
                        payload["cn_symbol"],
                        payload["cn_name"],
                        payload["market"],
                        payload["relation_type"],
                        payload["theme"],
                        payload["strength_score"],
                        payload["evidence_source"],
                        1,
                        payload["updated_at"],
                    ),
                )
                inserted += int(conn.total_changes > 0)
        return inserted

    def seed_playbooks(self) -> int:
        now = _utcnow_iso()
        inserted = 0
        with self._connect() as conn:
            for row in PLAYBOOKS:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cross_market_playbooks (
                        playbook_key, name, market_scope, theme, trigger_condition,
                        entry_rule, timing_rule, risk_rule, exit_rule,
                        invalid_condition, source_tier, event_required, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["playbook_key"],
                        row["name"],
                        row["market_scope"],
                        row["theme"],
                        row["trigger_condition"],
                        row["entry_rule"],
                        row["timing_rule"],
                        row["risk_rule"],
                        row["exit_rule"],
                        row["invalid_condition"],
                        row["source_tier"],
                        int(bool(row["event_required"])),
                        now,
                    ),
                )
                inserted += 1
        return inserted

    def _fetch_quote_series(self, symbol: str, range_value: str = "5d", interval: str = "1d") -> Optional[Dict[str, Any]]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        try:
            resp = self.session.get(
                url,
                params={"range": range_value, "interval": interval, "includePrePost": "true"},
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()
            result = ((payload.get("chart") or {}).get("result") or [None])[0]
            if not result:
                return None
            meta = result.get("meta") or {}
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []

            history = []
            for ts, close, volume in zip(timestamps, closes, volumes):
                close_value = _safe_float(close)
                if close_value is None:
                    continue
                point = {
                    "date": datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d"),
                    "close": close_value,
                    "volume": _safe_float(volume, 0.0) or 0.0,
                }
                history.append(point)
            if len(history) < 2:
                return None
            latest = history[-1]
            previous = history[-2]
            prev_close = previous["close"]
            latest_close = latest["close"]
            change_pct = ((latest_close - prev_close) / prev_close * 100.0) if prev_close else 0.0
            avg_volume = (
                sum(item["volume"] for item in history[:-1]) / max(1, len(history[:-1]))
                if len(history) > 1
                else 0.0
            )
            volume_ratio = (latest["volume"] / avg_volume) if avg_volume else 1.0
            return {
                "symbol": symbol,
                "regular_market_price": _safe_float(meta.get("regularMarketPrice"), latest_close) or latest_close,
                "prev_close": _safe_float(meta.get("chartPreviousClose"), prev_close) or prev_close,
                "quote_time": datetime.fromtimestamp(int(meta.get("regularMarketTime") or timestamps[-1])).strftime("%Y-%m-%d %H:%M:%S"),
                "change_pct": round(change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
                "history": history,
            }
        except Exception:
            pass

        try:
            import yfinance as yf  # type: ignore

            df = yf.download(symbol, period=range_value, interval=interval, progress=False, auto_adjust=False)
            if df is None or df.empty:
                return None
            history = []
            for date_idx, row in df.tail(5).iterrows():
                close_value = row["Close"]
                volume_value = row.get("Volume")
                if hasattr(close_value, "iloc"):
                    close_value = close_value.iloc[0]
                if hasattr(volume_value, "iloc"):
                    volume_value = volume_value.iloc[0]
                close_float = _safe_float(close_value)
                if close_float is None:
                    continue
                history.append(
                    {
                        "date": date_idx.strftime("%Y-%m-%d"),
                        "close": close_float,
                        "volume": _safe_float(volume_value, 0.0) or 0.0,
                    }
                )
            if len(history) < 2:
                return None
            latest = history[-1]
            previous = history[-2]
            prev_close = previous["close"]
            latest_close = latest["close"]
            change_pct = ((latest_close - prev_close) / prev_close * 100.0) if prev_close else 0.0
            avg_volume = sum(item["volume"] for item in history[:-1]) / max(1, len(history[:-1]))
            volume_ratio = (latest["volume"] / avg_volume) if avg_volume else 1.0
            return {
                "symbol": symbol,
                "regular_market_price": latest_close,
                "prev_close": prev_close,
                "quote_time": f"{latest['date']} 16:00:00",
                "change_pct": round(change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
                "history": history,
            }
        except Exception:
            return None

    def _upsert_event(
        self,
        conn: sqlite3.Connection,
        *,
        event_id: str,
        source_symbol: str,
        source_name: str,
        sector: str,
        event_type: str,
        event_time: str,
        headline: str,
        summary: str,
        facts: Dict[str, Any],
        impact_direction: str,
        urgency: str,
        source_url: str,
        source_tier: str,
        headline_zh: Optional[str] = None,
        summary_zh: Optional[str] = None,
    ) -> None:
        now = _utcnow_iso()
        conn.execute(
            """
            INSERT OR REPLACE INTO cross_market_signal_events (
                event_id, source_market, source_symbol, source_name, sector,
                event_type, event_time, headline, headline_zh, summary, summary_zh,
                facts_json, impact_direction, urgency, source_url,
                source_tier, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                "US",
                source_symbol,
                source_name,
                sector,
                event_type,
                event_time,
                headline,
                headline_zh or headline,
                summary,
                summary_zh,
                json.dumps(facts, ensure_ascii=False),
                impact_direction,
                urgency,
                source_url,
                source_tier,
                "active",
                now,
                now,
            ),
        )

    @staticmethod
    def _event_type(item: Dict[str, Any], change_pct: float) -> str:
        if item["kind"] == "etf":
            return "etf_breakout"
        if item["kind"] == "adr":
            return "adr_signal"
        if abs(change_pct) >= 8:
            return "earnings_spillover"
        if abs(change_pct) >= 5:
            return "price_breakout"
        return "sector_rotation"

    @staticmethod
    def _urgency(change_pct: float, kind: str) -> str:
        abs_change = abs(change_pct)
        if kind == "adr" and abs_change >= 5:
            return "P0"
        if abs_change >= 7:
            return "P0"
        if abs_change >= 4:
            return "P1"
        return "P2"

    def sync_us_market_events(self, symbols: Optional[Iterable[str]] = None, max_items: int = 24) -> Dict[str, Any]:
        watch = [item for item in US_WATCHLIST if not symbols or item["symbol"] in set(symbols)]
        created = 0
        touched = 0
        failed: List[str] = []
        now = _utcnow_iso()
        events: List[Dict[str, Any]] = []

        with self._connect() as conn:
            for item in watch[: max(1, int(max_items or len(watch)))]:
                series = self._fetch_quote_series(item["symbol"])
                if not series:
                    failed.append(item["symbol"])
                    continue
                change_pct = _safe_float(series.get("change_pct"), 0.0) or 0.0
                volume_ratio = _safe_float(series.get("volume_ratio"), 1.0) or 1.0
                if abs(change_pct) < 2.0 and volume_ratio < 1.35:
                    continue

                event_type = self._event_type(item, change_pct)
                urgency = self._urgency(change_pct, item["kind"])
                source_url = f"https://finance.yahoo.com/quote/{item['symbol']}"
                headline = f"{item['symbol']} 隔夜{change_pct:+.1f}%"
                if event_type == "etf_breakout":
                    headline = f"{item['name']} 主题 ETF 隔夜{change_pct:+.1f}%"
                elif event_type == "earnings_spillover":
                    headline = f"{item['name']} 高强度异动，关注财报/指引溢出"
                summary = (
                    f"{item['name']} 最新收盘 {series['regular_market_price']:.2f}，"
                    f"较前收 {change_pct:+.2f}%，量能比 {volume_ratio:.2f}，"
                    f"对应中国主题 {THEME_LABELS.get(item['sector'], item['sector'])}。"
                )
                event_date = series["quote_time"][:10]
                event_id = hashlib.md5(f"{event_date}:{item['symbol']}:{event_type}".encode("utf-8")).hexdigest()[:20]
                facts = {
                    "close": series["regular_market_price"],
                    "prev_close": series["prev_close"],
                    "change_pct": round(change_pct, 2),
                    "volume_ratio": round(volume_ratio, 2),
                    "quote_time": series["quote_time"],
                    "kind": item["kind"],
                    "theme": item["sector"],
                    "history": series["history"][-3:],
                }
                self._upsert_event(
                    conn,
                    event_id=event_id,
                    source_symbol=item["symbol"],
                    source_name=item["name"],
                    sector=item["sector"],
                    event_type=event_type,
                    event_time=series["quote_time"],
                    headline=headline,
                    summary=summary,
                    facts=facts,
                    impact_direction="bullish" if change_pct >= 0 else "bearish",
                    urgency=urgency,
                    source_url=source_url,
                    source_tier="T1",
                )
                touched += 1
                created += 1
                events.append({"event_id": event_id, "symbol": item["symbol"], "headline": headline})

            conn.execute(
                """
                UPDATE cross_market_signal_events
                SET status='archived', updated_at=?
                WHERE status='active' AND datetime(event_time) < datetime(?)
                """,
                (now, (datetime.now() - timedelta(days=7)).replace(microsecond=0).isoformat()),
            )

        return {
            "ok": True,
            "synced": touched,
            "created": created,
            "failed": failed,
            "sample_events": events[:8],
            "generated_at": now,
        }

    def _fetch_sec_ticker_map(self) -> Dict[str, Dict[str, str]]:
        if self._sec_ticker_cache is not None:
            return self._sec_ticker_cache
        resp = self.sec_session.get("https://www.sec.gov/files/company_tickers.json", timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        items: Iterable[Dict[str, Any]]
        if isinstance(payload, dict):
            items = payload.values()
        else:
            items = payload
        mapping: Dict[str, Dict[str, str]] = {}
        for item in items:
            ticker = str(item.get("ticker") or "").upper()
            cik = str(item.get("cik_str") or "").strip()
            if not ticker or not cik:
                continue
            mapping[ticker] = {
                "cik": cik.zfill(10),
                "title": str(item.get("title") or ticker),
            }
        self._sec_ticker_cache = mapping
        return mapping

    def _fetch_sec_submissions(self, cik: str) -> Dict[str, Any]:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = self.sec_session.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _is_relevant_sec_filing(form: str, description: str, items: str) -> bool:
        if form not in SEC_RELEVANT_FORMS:
            return False
        if form in {"10-Q", "10-K", "20-F"}:
            return True
        text = f"{description} {items}".lower()
        if any(keyword in text for keyword in SEC_CATALYST_KEYWORDS):
            return True
        return any(code in text for code in ("2.02", "7.01", "8.01", "1.01"))

    @staticmethod
    def _sec_urgency(form: str, description: str, items: str) -> str:
        text = f"{description} {items}".lower()
        if form in {"8-K", "6-K"} and (
            "earnings" in text
            or "financial results" in text
            or "guidance" in text
            or "2.02" in text
        ):
            return "P0"
        if form in {"10-Q", "10-K", "20-F"}:
            return "P1"
        return "P2"

    @staticmethod
    def _sec_event_type(form: str, description: str) -> str:
        text = description.lower()
        if "earnings" in text or "financial results" in text or form in {"10-Q", "10-K", "20-F"}:
            return "official_filing"
        if "guidance" in text or "outlook" in text:
            return "guidance_update"
        if "agreement" in text or "partnership" in text:
            return "strategic_update"
        return "official_filing"

    @staticmethod
    def _sec_source_url(cik: str, accession: str, primary_document: str) -> str:
        accession_nodash = accession.replace("-", "")
        primary = primary_document or ""
        return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}/{primary}"

    def sync_sec_filings(self, symbols: Optional[Iterable[str]] = None, days: int = 14, max_companies: int = 12) -> Dict[str, Any]:
        watch_map = {
            item["symbol"]: item
            for item in US_WATCHLIST
            if item.get("kind") != "etf"
        }
        symbol_list = [symbol for symbol in watch_map if not symbols or symbol in set(symbols)]
        cutoff_date = (datetime.now() - timedelta(days=max(1, int(days or 14)))).date()
        created = 0
        failed: List[str] = []
        sample_events: List[Dict[str, Any]] = []
        ticker_map = self._fetch_sec_ticker_map()

        with self._connect() as conn:
            for symbol in symbol_list[: max(1, int(max_companies or len(symbol_list)))]:
                sec_meta = ticker_map.get(symbol)
                if not sec_meta:
                    failed.append(symbol)
                    continue
                try:
                    payload = self._fetch_sec_submissions(sec_meta["cik"])
                except Exception:
                    failed.append(symbol)
                    continue
                recent = ((payload.get("filings") or {}).get("recent") or {})
                forms = recent.get("form") or []
                filing_dates = recent.get("filingDate") or []
                accession_numbers = recent.get("accessionNumber") or []
                documents = recent.get("primaryDocument") or []
                descriptions = recent.get("primaryDocDescription") or []
                items_list = recent.get("items") or []
                acceptance_list = recent.get("acceptanceDateTime") or []

                for idx, form in enumerate(forms):
                    filing_date = str(filing_dates[idx]) if idx < len(filing_dates) else ""
                    try:
                        filing_dt = datetime.strptime(filing_date[:10], "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if filing_dt < cutoff_date:
                        continue
                    description = str(descriptions[idx]) if idx < len(descriptions) else ""
                    items = str(items_list[idx]) if idx < len(items_list) else ""
                    if not self._is_relevant_sec_filing(str(form), description, items):
                        continue
                    accession = str(accession_numbers[idx]) if idx < len(accession_numbers) else ""
                    primary_document = str(documents[idx]) if idx < len(documents) else ""
                    acceptance = str(acceptance_list[idx]) if idx < len(acceptance_list) else ""
                    event_time = f"{filing_date[:10]} 00:00:00"
                    if len(acceptance) >= 14 and acceptance.isdigit():
                        event_time = datetime.strptime(acceptance[:14], "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                    event_type = self._sec_event_type(str(form), description)
                    urgency = self._sec_urgency(str(form), description, items)
                    headline = f"{symbol} {form}: {description or 'SEC filing update'}"
                    summary = (
                        f"{watch_map[symbol]['name']} 披露 {form}。"
                        f"{description or '请查看原始 filing 细节'}"
                    )
                    source_url = self._sec_source_url(sec_meta["cik"], accession, primary_document)
                    event_id = hashlib.md5(f"sec:{symbol}:{accession}".encode("utf-8")).hexdigest()[:20]
                    facts = {
                        "form": form,
                        "filing_date": filing_date[:10],
                        "acceptance_time": acceptance,
                        "description": description,
                        "items": items,
                        "accession": accession,
                        "primary_document": primary_document,
                        "theme": watch_map[symbol]["sector"],
                    }
                    self._upsert_event(
                        conn,
                        event_id=event_id,
                        source_symbol=symbol,
                        source_name=watch_map[symbol]["name"],
                        sector=watch_map[symbol]["sector"],
                        event_type=event_type,
                        event_time=event_time,
                        headline=headline,
                        summary=summary,
                        facts=facts,
                        impact_direction="context",
                        urgency=urgency,
                        source_url=source_url,
                        source_tier="T0",
                    )
                    created += 1
                    if len(sample_events) < 8:
                        sample_events.append({"symbol": symbol, "form": form, "headline": headline})

        return {
            "ok": True,
            "created": created,
            "failed": failed,
            "sample_events": sample_events,
            "generated_at": _utcnow_iso(),
        }

    def _watch_meta(self) -> Dict[str, Dict[str, Any]]:
        return {item["symbol"]: item for item in US_WATCHLIST}

    @staticmethod
    def _normalize_text(value: str) -> str:
        text = html.unescape(value or "")
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _parse_datetime_value(value: Any) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text[: len(fmt)], fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
        try:
            return parsedate_to_datetime(text).replace(tzinfo=None)
        except Exception:
            return None

    def _fetch_openfda_recent_results(self, days: int = 30, limit: int = 100) -> List[Dict[str, Any]]:
        cutoff = (datetime.now() - timedelta(days=max(1, int(days or 30)))).strftime("%Y%m%d")
        params = {
            "search": f"submissions.submission_status:AP AND submissions.submission_status_date:[{cutoff} TO *]",
            "limit": max(20, min(int(limit or 100), 100)),
        }
        resp = self.session.get("https://api.fda.gov/drug/drugsfda.json", params=params, timeout=20)
        resp.raise_for_status()
        return list((resp.json().get("results") or []))

    @staticmethod
    def _match_symbol_by_alias(text: str, alias_map: Dict[str, List[str]]) -> Optional[str]:
        haystack = (text or "").upper()
        for symbol, aliases in alias_map.items():
            if any(alias.upper() in haystack for alias in aliases):
                return symbol
        return None

    @staticmethod
    def _fda_event_meta(submission: Dict[str, Any]) -> tuple[str, str]:
        submission_type = str(submission.get("submission_type") or "").upper()
        class_desc = str(submission.get("submission_class_code_description") or "").lower()
        if submission_type == "ORIG":
            return "fda_approval", "P0"
        if "priority" in class_desc or "efficacy" in class_desc:
            return "fda_supplement_approval", "P0"
        return "fda_supplement_approval", "P1"

    def sync_fda_events(self, days: int = 30) -> Dict[str, Any]:
        created = 0
        failed: List[str] = []
        sample_events: List[Dict[str, Any]] = []
        watch_map = self._watch_meta()
        cutoff = (datetime.now() - timedelta(days=max(1, int(days or 30)))).date()
        try:
            results = self._fetch_openfda_recent_results(days=days)
        except Exception as exc:
            return {"ok": False, "created": 0, "failed": [f"openfda:{exc}"], "sample_events": [], "generated_at": _utcnow_iso()}

        with self._connect() as conn:
            for record in results:
                sponsor_name = str(record.get("sponsor_name") or "")
                symbol = self._match_symbol_by_alias(sponsor_name, FDA_SPONSOR_ALIASES)
                if not symbol or symbol not in watch_map:
                    continue
                product = (record.get("products") or [{}])[0]
                product_name = str(product.get("brand_name") or product.get("generic_name") or record.get("application_number") or symbol)
                for submission in record.get("submissions") or []:
                    raw_date = str(submission.get("submission_status_date") or "")
                    event_dt = self._parse_datetime_value(raw_date)
                    if not event_dt or event_dt.date() < cutoff:
                        continue
                    if str(submission.get("submission_status") or "").upper() != "AP":
                        continue
                    event_type, urgency = self._fda_event_meta(submission)
                    application_number = str(record.get("application_number") or "unknown")
                    event_id = hashlib.md5(
                        f"fda:{symbol}:{application_number}:{submission.get('submission_number')}:{raw_date}".encode("utf-8")
                    ).hexdigest()[:20]
                    source_url = (
                        "https://api.fda.gov/drug/drugsfda.json?search="
                        + quote_plus(f"application_number:{application_number}")
                    )
                    headline = f"FDA {symbol} {product_name} 审批进展"
                    summary = (
                        f"{watch_map[symbol]['name']} 对应 sponsor {sponsor_name} 出现 FDA 批准事件，"
                        f"产品 {product_name}，类型 {submission.get('submission_type') or 'N/A'}。"
                    )
                    facts = {
                        "application_number": application_number,
                        "submission_number": submission.get("submission_number"),
                        "submission_type": submission.get("submission_type"),
                        "submission_status": submission.get("submission_status"),
                        "submission_status_date": raw_date,
                        "product_name": product_name,
                        "sponsor_name": sponsor_name,
                    }
                    self._upsert_event(
                        conn,
                        event_id=event_id,
                        source_symbol=symbol,
                        source_name=watch_map[symbol]["name"],
                        sector=watch_map[symbol]["sector"],
                        event_type=event_type,
                        event_time=event_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        headline=headline,
                        summary=summary,
                        facts=facts,
                        impact_direction="context",
                        urgency=urgency,
                        source_url=source_url,
                        source_tier="T0",
                    )
                    created += 1
                    if len(sample_events) < 8:
                        sample_events.append({"symbol": symbol, "headline": headline})
        return {"ok": True, "created": created, "failed": failed, "sample_events": sample_events, "generated_at": _utcnow_iso()}

    def _fetch_clinical_trials_studies(self, sponsor_alias: str, page_size: int = 20) -> List[Dict[str, Any]]:
        params = {"query.spons": sponsor_alias, "pageSize": max(1, min(int(page_size or 20), 100)), "format": "json"}
        resp = self.session.get("https://clinicaltrials.gov/api/v2/studies", params=params, timeout=20)
        resp.raise_for_status()
        return list((resp.json().get("studies") or []))

    def _clinical_trial_event_from_study(self, symbol: str, study: Dict[str, Any], cutoff: datetime.date) -> Optional[Dict[str, Any]]:
        protocol = study.get("protocolSection") or {}
        ident = protocol.get("identificationModule") or {}
        sponsor_module = protocol.get("sponsorCollaboratorsModule") or {}
        status_module = protocol.get("statusModule") or {}
        lead_sponsor = ((sponsor_module.get("leadSponsor") or {}).get("name") or "")
        org_name = ((ident.get("organization") or {}).get("fullName") or "")
        alias_text = f"{lead_sponsor} {org_name}"
        if symbol not in CLINICAL_TRIAL_SPONSOR_ALIASES:
            return None
        if not any(alias.lower() in alias_text.lower() for alias in CLINICAL_TRIAL_SPONSOR_ALIASES[symbol]):
            return None

        first_post = self._parse_datetime_value(((status_module.get("studyFirstPostDateStruct") or {}).get("date") or ""))
        last_update = self._parse_datetime_value(((status_module.get("lastUpdatePostDateStruct") or {}).get("date") or ""))
        event_type = None
        event_dt = None
        if first_post and first_post.date() >= cutoff:
            event_type = "clinical_trial_new_study"
            event_dt = first_post
        elif last_update and last_update.date() >= cutoff:
            event_type = "clinical_trial_update"
            event_dt = last_update
        if not event_type or not event_dt:
            return None

        phases = (protocol.get("designModule") or {}).get("phases") or []
        phase_blob = " ".join(phases).upper()
        overall_status = str(status_module.get("overallStatus") or "")
        urgency = "P1"
        if "PHASE3" in phase_blob or "PHASE4" in phase_blob:
            urgency = "P0"
        elif "PHASE2" in phase_blob:
            urgency = "P1"
        else:
            urgency = "P2"
        nct_id = str(ident.get("nctId") or "UNKNOWN")
        brief_title = str(ident.get("briefTitle") or nct_id)
        return {
            "event_id": hashlib.md5(f"ct:{symbol}:{nct_id}:{event_type}:{event_dt.date().isoformat()}".encode("utf-8")).hexdigest()[:20],
            "event_type": event_type,
            "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "headline": f"ClinicalTrials {symbol} {brief_title}",
            "summary": f"{lead_sponsor or symbol} 临床项目 {nct_id} 出现 {event_type}，当前状态 {overall_status or 'N/A'}。",
            "facts": {
                "nct_id": nct_id,
                "brief_title": brief_title,
                "lead_sponsor": lead_sponsor,
                "overall_status": overall_status,
                "phases": phases,
            },
            "source_url": f"https://clinicaltrials.gov/study/{nct_id}",
            "urgency": urgency,
        }

    def sync_clinical_trials_events(self, days: int = 30) -> Dict[str, Any]:
        created = 0
        failed: List[str] = []
        sample_events: List[Dict[str, Any]] = []
        watch_map = self._watch_meta()
        cutoff = (datetime.now() - timedelta(days=max(1, int(days or 30)))).date()
        seen_ids: set[str] = set()

        with self._connect() as conn:
            for symbol in CLINICAL_TRIAL_SPONSOR_ALIASES:
                if symbol not in watch_map:
                    continue
                alias = CLINICAL_TRIAL_SPONSOR_ALIASES[symbol][0]
                try:
                    studies = self._fetch_clinical_trials_studies(alias)
                except Exception:
                    failed.append(symbol)
                    continue
                for study in studies:
                    event = self._clinical_trial_event_from_study(symbol, study, cutoff)
                    if not event or event["event_id"] in seen_ids:
                        continue
                    seen_ids.add(event["event_id"])
                    self._upsert_event(
                        conn,
                        event_id=event["event_id"],
                        source_symbol=symbol,
                        source_name=watch_map[symbol]["name"],
                        sector=watch_map[symbol]["sector"],
                        event_type=event["event_type"],
                        event_time=event["event_time"],
                        headline=event["headline"],
                        summary=event["summary"],
                        facts=event["facts"],
                        impact_direction="context",
                        urgency=event["urgency"],
                        source_url=event["source_url"],
                        source_tier="T0",
                    )
                    created += 1
                    if len(sample_events) < 8:
                        sample_events.append({"symbol": symbol, "headline": event["headline"]})
        return {"ok": True, "created": created, "failed": failed, "sample_events": sample_events, "generated_at": _utcnow_iso()}

    def _fetch_company_ir_feed(self, url: str) -> bytes:
        resp = self.session.get(url, timeout=20)
        resp.raise_for_status()
        return resp.content

    def _parse_company_ir_feed(self, payload: bytes, source: Dict[str, str], cutoff: datetime) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        try:
            root = ET.fromstring(payload)
        except Exception:
            return results

        def local_name(tag: str) -> str:
            return tag.split("}", 1)[-1].lower()

        def find_text(node: ET.Element, names: Iterable[str]) -> str:
            wanted = {name.lower() for name in names}
            for child in node.iter():
                if local_name(child.tag) in wanted:
                    return self._normalize_text(child.text or "")
            return ""

        entries = [node for node in root.iter() if local_name(node.tag) in {"item", "entry"}]
        for entry in entries:
            title = find_text(entry, {"title"})
            summary = find_text(entry, {"description", "summary", "content"})
            published = find_text(entry, {"pubdate", "published", "updated"})
            link = ""
            for child in entry.iter():
                if local_name(child.tag) != "link":
                    continue
                link = child.get("href") or self._normalize_text(child.text or "")
                if link:
                    break
            published_dt = self._parse_datetime_value(published)
            if not title or not link or not published_dt or published_dt < cutoff:
                continue
            text_blob = f"{title} {summary}".lower()
            if not any(keyword in text_blob for keyword in COMPANY_IR_MATERIAL_KEYWORDS):
                continue
            event_type = "company_ir_release"
            urgency = "P1"
            if any(token in text_blob for token in ("earnings", "results", "fiscal", "quarter", "guidance", "outlook")):
                event_type = "company_ir_earnings"
                urgency = "P0"
            elif any(token in text_blob for token in ("partnership", "collaboration", "approval")):
                event_type = "company_ir_strategic"
            event_id = hashlib.md5(
                f"ir:{source['symbol']}:{link}:{published_dt.date().isoformat()}".encode("utf-8")
            ).hexdigest()[:20]
            results.append(
                {
                    "event_id": event_id,
                    "source_symbol": source["symbol"],
                    "source_name": source["name"],
                    "sector": source["sector"],
                    "event_type": event_type,
                    "event_time": published_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "headline": title,
                    "summary": summary or title,
                    "facts": {"label": source["label"], "feed_url": source["url"], "published_at": published_dt.isoformat()},
                    "impact_direction": "context",
                    "urgency": urgency,
                    "source_url": link,
                    "source_tier": "T0",
                }
            )
        return results

    def sync_company_ir_events(self, lookback_hours: int = 120, max_items_per_source: int = 6) -> Dict[str, Any]:
        created = 0
        failed: List[str] = []
        sample_events: List[Dict[str, Any]] = []
        cutoff = datetime.now() - timedelta(hours=max(24, int(lookback_hours or 120)))

        with self._connect() as conn:
            for source in COMPANY_IR_SOURCES:
                try:
                    payload = self._fetch_company_ir_feed(source["url"])
                    events = self._parse_company_ir_feed(payload, source, cutoff)
                except Exception:
                    failed.append(source["symbol"])
                    continue
                for event in events[: max(1, int(max_items_per_source or 6))]:
                    self._upsert_event(conn, **event)
                    created += 1
                    if len(sample_events) < 8:
                        sample_events.append({"symbol": event["source_symbol"], "headline": event["headline"]})
        return {"ok": True, "created": created, "failed": failed, "sample_events": sample_events, "generated_at": _utcnow_iso()}

    def sync_official_events(
        self,
        days: int = 30,
        lookback_hours: int = 120,
        max_sec_companies: int = 12,
        max_company_ir_items: int = 6,
    ) -> Dict[str, Any]:
        sec = self.sync_sec_filings(days=days, max_companies=max_sec_companies)
        fda = self.sync_fda_events(days=days)
        clinical_trials = self.sync_clinical_trials_events(days=days)
        company_ir = self.sync_company_ir_events(lookback_hours=lookback_hours, max_items_per_source=max_company_ir_items)
        return {
            "ok": all(item.get("ok", True) for item in [sec, fda, clinical_trials, company_ir]),
            "generated_at": _utcnow_iso(),
            "sec": sec,
            "fda": fda,
            "clinical_trials": clinical_trials,
            "company_ir": company_ir,
            "created_total": sum(int(item.get("created", 0) or 0) for item in [sec, fda, clinical_trials, company_ir]),
        }

    def translate_recent_events(self, limit: int = 20, source_tier: str = "T0") -> Dict[str, Any]:
        if not self.bailian_api_key:
            return {"ok": False, "translated": 0, "reason": "missing_bailian_api_key"}

        updated = 0
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT event_id, headline, headline_zh, summary, summary_zh
                FROM cross_market_signal_events
                WHERE status='active' AND source_tier = ?
                  AND (
                    headline_zh IS NULL OR headline_zh = '' OR headline_zh = headline
                    OR summary_zh IS NULL OR summary_zh = ''
                  )
                ORDER BY event_time DESC
                LIMIT ?
                """,
                (source_tier, max(1, min(int(limit or 20), 100))),
            ).fetchall()
            for row in rows:
                translated = self._translate_payload(
                    {
                        "headline_zh": row["headline"] or "",
                        "summary_zh": row["summary"] or "",
                    }
                )
                if not translated:
                    continue
                conn.execute(
                    """
                    UPDATE cross_market_signal_events
                    SET headline_zh = ?, summary_zh = ?, updated_at = ?
                    WHERE event_id = ?
                    """,
                    (
                        translated.get("headline_zh") or row["headline"],
                        translated.get("summary_zh") or row["summary"],
                        _utcnow_iso(),
                        row["event_id"],
                    ),
                )
                conn.commit()
                updated += 1
        return {"ok": True, "translated": updated, "generated_at": _utcnow_iso()}

    def _translate_payload(self, payload: Dict[str, Any]) -> Dict[str, str]:
        try:
            return self.bailian_translator.translate_payload(
                payload,
                system_prompt=(
                    "你是面向 A股和港股短线研究的双语事件翻译助手。"
                    "请把输入英文翻译为简洁专业的中文。"
                    "只输出严格 JSON，字段名与输入保持一致。"
                ),
                max_tokens=400,
            )
        except Exception:
            return {}

    def _event_score(self, event: Dict[str, Any]) -> float:
        facts = event.get("facts") or {}
        change_pct = abs(_safe_float(facts.get("change_pct"), 0.0) or 0.0)
        volume_ratio = _safe_float(facts.get("volume_ratio"), 1.0) or 1.0
        base = min(change_pct * 7.5, 55.0)
        volume_bonus = min(max(volume_ratio - 1.0, 0.0) * 12.0, 15.0)
        type_bonus_map = {
            "earnings_spillover": 18.0,
            "price_breakout": 12.0,
            "etf_breakout": 10.0,
            "adr_signal": 14.0,
            "sector_rotation": 8.0,
            "official_filing": 14.0,
            "guidance_update": 16.0,
            "strategic_update": 14.0,
            "fda_approval": 22.0,
            "fda_supplement_approval": 16.0,
            "clinical_trial_new_study": 16.0,
            "clinical_trial_update": 12.0,
            "company_ir_earnings": 18.0,
            "company_ir_strategic": 16.0,
            "company_ir_release": 12.0,
        }
        urgency_bonus_map = {"P0": 12.0, "P1": 6.0, "P2": 0.0}
        return min(100.0, base + volume_bonus + type_bonus_map.get(event.get("event_type"), 0.0) + urgency_bonus_map.get(event.get("urgency"), 0.0))

    def _choose_playbook(self, relation_type: str, event_type: str, theme: str, market: str) -> Dict[str, Any]:
        key = "us_etf_theme_rotation"
        if relation_type in {"adr_to_hk", "same_entity"} and market == "HK":
            key = "adr_hk_gap_repair"
        elif event_type in {"earnings_spillover", "company_ir_earnings", "guidance_update"}:
            key = "us_earnings_spillover"
        elif theme == "biotech" or event_type in {"fda_approval", "fda_supplement_approval", "clinical_trial_new_study", "clinical_trial_update"}:
            key = "fda_readout_follow"
        elif theme == "robotics":
            key = "robotics_chain_follow"
        elif theme in {"solar", "nuclear"}:
            key = "energy_policy_follow"
        elif theme == "hog":
            key = "agri_cycle_follow"
        elif relation_type in {"adr_to_hk", "same_entity"} and market == "A":
            key = "cn_dragon_follow"
        return next((item for item in PLAYBOOKS if item["playbook_key"] == key), PLAYBOOKS[0])

    def _china_follow_score(self, theme: str, market: str, urgency: str, relation_type: str) -> float:
        try:
            regime = self.workbench.get_market_regime()
        except Exception:
            regime = {"label": "neutral", "score": 0, "reasons": []}
        score = 52.0
        if regime.get("label") == "risk_on":
            score += 12.0
        elif regime.get("label") == "neutral":
            score += 4.0
        else:
            score -= 10.0
        if market == "HK":
            score += 6.0
        if relation_type in {"adr_to_hk", "same_entity"}:
            score += 12.0
        if theme in {"ai", "semiconductor", "robotics"}:
            score += 6.0
        if urgency == "P0":
            score += 8.0
        elif urgency == "P1":
            score += 4.0
        return max(20.0, min(score, 100.0))

    @staticmethod
    def _priority_label(score: float) -> str:
        if score >= 80:
            return "P0"
        if score >= 68:
            return "P1"
        return "P2"

    def build_candidates(self, max_age_hours: int = 36) -> Dict[str, Any]:
        self.ensure_reference_data()
        cutoff = (datetime.now() - timedelta(hours=max(1, int(max_age_hours or 36)))).replace(microsecond=0).isoformat()
        now = _utcnow_iso()
        created = 0
        with self._connect() as conn:
            event_rows = conn.execute(
                """
                SELECT *
                FROM cross_market_signal_events
                WHERE status='active' AND datetime(event_time) >= datetime(?)
                ORDER BY event_time DESC
                """,
                (cutoff,),
            ).fetchall()
            mappings = conn.execute(
                "SELECT * FROM cross_market_mapping_master WHERE manual_verified = 1"
            ).fetchall()
            mapping_by_symbol: Dict[str, List[sqlite3.Row]] = {}
            for row in mappings:
                mapping_by_symbol.setdefault(row["us_symbol"], []).append(row)

            for event_row in event_rows:
                event = dict(event_row)
                event["facts"] = json.loads(event.get("facts_json") or "{}")
                related = mapping_by_symbol.get(event["source_symbol"], [])
                if not related:
                    continue
                event_score = self._event_score(event)
                for mapping in related:
                    mapping_score = _safe_float(mapping["strength_score"], 0.0) or 0.0
                    follow_score = self._china_follow_score(
                        mapping["theme"], mapping["market"], event.get("urgency") or "P2", mapping["relation_type"]
                    )
                    execution_priority = round(event_score * 0.35 + mapping_score * 0.35 + follow_score * 0.30, 1)
                    priority = self._priority_label(execution_priority)
                    playbook = self._choose_playbook(mapping["relation_type"], event["event_type"], mapping["theme"], mapping["market"])
                    candidate_id = hashlib.md5(f"{event['event_id']}::{mapping['cn_symbol']}".encode("utf-8")).hexdigest()[:24]
                    thesis = (
                        f"{event['source_symbol']} 隔夜异动，映射到 {mapping['cn_name']} "
                        f"({mapping['relation_type']})，关注 {playbook['name']}。"
                    )
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO cross_market_signal_candidates (
                            candidate_id, event_id, source_symbol, source_name, event_time,
                            cn_symbol, cn_name, market, theme, relation_type,
                            mapping_strength_score, us_event_score, china_follow_through_score,
                            execution_priority, priority, playbook_key, trade_playbook,
                            thesis, entry_window, invalid_condition, status, source_url,
                            evidence_links_json, note, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            candidate_id,
                            event["event_id"],
                            event["source_symbol"],
                            event.get("source_name"),
                            event["event_time"],
                            mapping["cn_symbol"],
                            mapping["cn_name"],
                            mapping["market"],
                            mapping["theme"],
                            mapping["relation_type"],
                            round(mapping_score, 1),
                            round(event_score, 1),
                            round(follow_score, 1),
                            execution_priority,
                            priority,
                            playbook["playbook_key"],
                            playbook["name"],
                            thesis,
                            playbook["timing_rule"],
                            playbook["invalid_condition"],
                            "active",
                            event.get("source_url"),
                            json.dumps([event.get("source_url")], ensure_ascii=False),
                            playbook["trigger_condition"],
                            now,
                            now,
                        ),
                    )
                    created += 1
            conn.execute(
                """
                UPDATE cross_market_signal_candidates
                SET status='archived', updated_at=?
                WHERE status='active' AND datetime(event_time) < datetime(?)
                """,
                (now, cutoff),
            )

        return {"ok": True, "candidates_upserted": created, "generated_at": now}

    def refresh_pipeline(self, include_official: bool = True, translate: bool = False) -> Dict[str, Any]:
        sync_result = self.sync_us_market_events()
        official_result = self.sync_official_events() if include_official else {"ok": True, "skipped": True}
        translate_result = self.translate_recent_events(limit=20) if translate else {"ok": True, "skipped": True}
        candidate_result = self.build_candidates()
        return {
            "ok": True,
            "generated_at": _utcnow_iso(),
            "sync": sync_result,
            "official": official_result,
            "translate": translate_result,
            "candidates": candidate_result,
        }

    def ensure_fresh_data(self, max_age_hours: int = 8) -> Dict[str, Any]:
        latest = self._latest_event_time()
        if latest and latest >= datetime.now() - timedelta(hours=max_age_hours):
            return {"ok": True, "refreshed": False, "latest_event_time": latest.isoformat()}
        result = self.refresh_pipeline()
        result["refreshed"] = True
        return result

    def _latest_event_time(self) -> Optional[datetime]:
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(event_time) AS latest FROM cross_market_signal_events").fetchone()
        if not row or not row["latest"]:
            return None
        try:
            return datetime.fromisoformat(str(row["latest"]).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return {key: row[key] for key in row.keys()}

    def list_playbooks(self) -> List[Dict[str, Any]]:
        self.ensure_reference_data(allow_locked=True)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM cross_market_playbooks ORDER BY playbook_key"
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_events(self, limit: int = 30, theme: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = [
            "SELECT * FROM cross_market_signal_events WHERE status='active'"
        ]
        params: List[Any] = []
        if theme:
            sql.append("AND sector = ?")
            params.append(theme)
        sql.append("ORDER BY urgency ASC, event_time DESC LIMIT ?")
        params.append(max(1, min(int(limit or 30), 200)))
        with self._connect() as conn:
            rows = conn.execute(" ".join(sql), params).fetchall()
        result = []
        for row in rows:
            item = self._row_to_dict(row)
            item["facts"] = json.loads(item.get("facts_json") or "{}")
            item["theme_label"] = THEME_LABELS.get(item.get("sector") or "", item.get("sector"))
            item["summary_en"] = item.get("summary")
            item["summary"] = item.get("summary_zh") or item.get("summary")
            result.append(item)
        return result

    def list_candidates(
        self,
        limit: int = 60,
        priority: Optional[str] = None,
        theme: Optional[str] = None,
        market: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = [
            """
            SELECT c.*, e.headline, e.headline_zh, e.summary, e.summary_zh, e.urgency AS event_urgency, e.event_type, e.impact_direction, e.source_tier
            FROM cross_market_signal_candidates c
            JOIN cross_market_signal_events e ON e.event_id = c.event_id
            WHERE c.status='active'
            """
        ]
        params: List[Any] = []
        if priority:
            sql.append("AND c.priority = ?")
            params.append(priority)
        if theme:
            sql.append("AND c.theme = ?")
            params.append(theme)
        if market:
            sql.append("AND c.market = ?")
            params.append(market)
        sql.append("ORDER BY c.execution_priority DESC, c.event_time DESC LIMIT ?")
        params.append(max(1, min(int(limit or 60), 300)))
        with self._connect() as conn:
            rows = conn.execute(" ".join(sql), params).fetchall()
        result = []
        for row in rows:
            item = self._row_to_dict(row)
            item["evidence_links"] = json.loads(item.get("evidence_links_json") or "[]")
            item["theme_label"] = THEME_LABELS.get(item.get("theme") or "", item.get("theme"))
            item["summary_en"] = item.get("summary")
            item["summary"] = item.get("summary_zh") or item.get("summary")
            item["headline_display"] = item.get("headline_zh") or item.get("headline")
            result.append(item)
        return result

    def get_overview(self) -> Dict[str, Any]:
        self.ensure_reference_data(allow_locked=True)
        with self._connect() as conn:
            event_stats = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN urgency='P0' THEN 1 ELSE 0 END) AS p0,
                       SUM(CASE WHEN urgency='P1' THEN 1 ELSE 0 END) AS p1,
                       SUM(CASE WHEN source_tier='T0' THEN 1 ELSE 0 END) AS t0_total,
                       MAX(updated_at) AS latest_refresh
                FROM cross_market_signal_events
                WHERE status='active'
                """
            ).fetchone()
            candidate_stats = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN priority='P0' THEN 1 ELSE 0 END) AS p0,
                       SUM(CASE WHEN priority='P1' THEN 1 ELSE 0 END) AS p1
                FROM cross_market_signal_candidates
                WHERE status='active'
                """
            ).fetchone()
            theme_rows = conn.execute(
                """
                SELECT theme, COUNT(*) AS count
                FROM cross_market_signal_candidates
                WHERE status='active'
                GROUP BY theme
                ORDER BY count DESC, theme
                """
            ).fetchall()
            mapping_count = conn.execute(
                "SELECT COUNT(*) AS total FROM cross_market_mapping_master WHERE manual_verified = 1"
            ).fetchone()
        themes = [{"theme": row["theme"], "label": THEME_LABELS.get(row["theme"], row["theme"]), "count": row["count"]} for row in theme_rows]
        coverage = len(themes)
        top_candidates = self.list_candidates(limit=8)
        top_events = self.list_events(limit=8)
        latest_refresh = (event_stats["latest_refresh"] if event_stats else None) or _utcnow_iso()
        return {
            "generated_at": _utcnow_iso(),
            "metrics": {
                "overnight_events": int(event_stats["total"] or 0) if event_stats else 0,
                "candidate_total": int(candidate_stats["total"] or 0) if candidate_stats else 0,
                "candidate_p0": int(candidate_stats["p0"] or 0) if candidate_stats else 0,
                "candidate_p1": int(candidate_stats["p1"] or 0) if candidate_stats else 0,
                "event_p0": int(event_stats["p0"] or 0) if event_stats else 0,
                "event_p1": int(event_stats["p1"] or 0) if event_stats else 0,
                "official_events": int(event_stats["t0_total"] or 0) if event_stats else 0,
                "mapping_total": int(mapping_count["total"] or 0) if mapping_count else 0,
                "sector_coverage": coverage,
                "latest_refresh": latest_refresh,
            },
            "themes": themes,
            "top_candidates": top_candidates,
            "top_events": top_events,
            "playbooks": self.list_playbooks(),
        }
