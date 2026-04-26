"""Lead-lag alpha engine.

The service is import-safe and read-mostly. It starts from the structured sample
bundle under ``sample_data/lead_lag`` and, when available, fuses live local
evidence from Radar snapshots plus Intelligence / Research SQLite tables.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from app.services.lead_lag_diagnostics import build_replay_diagnostics, build_transmission_workspace
from app.services.lead_lag_events import build_event_relevance, filter_event_relevance
from app.services.lead_lag_macro import build_macro_bridge
from app.services.lead_lag_memory_actions import build_research_memory_actions
from app.services.lead_lag_v3 import BLOCKED_SOURCE_CLASSES, LeadLagV3Projector
from app.services.lead_lag_schema import apply_generation_status, compact_strings
from app.services.lead_lag_scoring import clamp_score, load_lead_lag_v2_config, opportunity_decision_score
from app.services.lead_lag_sector_evidence import build_sector_deep_evidence
from app.services.obsidian_memory_service import ObsidianMemoryService
try:
    from app.services.evidence_vault import EvidenceVaultService
except Exception:  # pragma: no cover - optional until V3 migration is run.
    EvidenceVaultService = None
try:
    from app.services.opportunity_universe import OpportunityUniverseRegistry
except Exception:  # pragma: no cover - optional until V3 migration is run.
    OpportunityUniverseRegistry = None


STAGES = ("latent", "pre_trigger", "triggered", "validating", "crowded", "decaying", "invalidated")
STAGE_ORDER = {stage: index for index, stage in enumerate(STAGES)}
BATON_ORDER = ("first_baton", "second_baton", "next_baton")
CANONICAL_RELATIONS = {
    "supplier": "spills_over_to",
    "pcb_supplier": "affects_demand_of",
    "assembly_peer": "maps_to",
    "direct_peer": "maps_to",
    "foundry_cycle": "affects_capex_of",
    "equipment_peer": "affects_capex_of",
    "inverter_peer": "maps_to",
    "cycle_peer": "validates",
}
SECTOR_ALIASES = {
    "ai": "ai",
    "AI": "ai",
    "人工智能": "ai",
    "算力": "ai",
    "semi": "semis",
    "semis": "semis",
    "semiconductor": "semis",
    "半导体": "semis",
    "芯片半导体": "semis",
    "pharma": "innovative_pharma",
    "biotech": "innovative_pharma",
    "innovative_pharma": "innovative_pharma",
    "创新药": "innovative_pharma",
    "pv": "solar",
    "solar": "solar",
    "光伏": "solar",
    "hog": "hog_cycle",
    "hog_cycle": "hog_cycle",
    "猪周期": "hog_cycle",
}
SECTOR_DISPLAY_NAMES = {
    "ai": "AI 算力与应用",
    "semis": "芯片半导体",
    "innovative_pharma": "创新药",
    "solar": "光伏与逆变器",
    "hog_cycle": "猪周期",
    "cross_market": "跨市场",
}
MODEL_DISPLAY = {
    "leadership_breadth": {
        "name_zh": "龙头扩散模型",
        "explain_zh": "观察海外或行业龙头的上涨是否扩散到二线、本地映射和 ETF。",
    },
    "event_spillover": {
        "name_zh": "事件溢出模型",
        "explain_zh": "把官方事件、财报、审批、资本开支等催化映射到 A 股、港股和 ETF。",
    },
    "transmission_graph": {
        "name_zh": "传导图谱模型",
        "explain_zh": "沿领先资产、桥接资产、本地映射和验证资产判断第几棒正在生效。",
    },
    "liquidity_dispersion": {
        "name_zh": "流动性与拥挤模型",
        "explain_zh": "识别资金是否过度集中，以及是否已经进入不宜追高的阶段。",
    },
    "earnings_revision": {
        "name_zh": "业绩修正模型",
        "explain_zh": "跟踪指引、利润预期、订单和价格信号是否正在上修或下修。",
    },
    "policy_surprise": {
        "name_zh": "政策意外模型",
        "explain_zh": "识别监管、产业政策、审批或财政信用变化带来的重估机会。",
    },
    "valuation_gap": {
        "name_zh": "估值差模型",
        "explain_zh": "比较海外龙头、本地映射和 ETF 的估值差是否足以支持补涨。",
    },
    "replay_validation": {
        "name_zh": "历史回放模型",
        "explain_zh": "用历史样本检查当前传导路径是否曾经有效，以及常见失败模式。",
    },
    "breadth_thrust": {
        "name_zh": "市场宽度模型",
        "explain_zh": "判断行情是单点噪音，还是已经出现足够多资产共同确认。",
    },
    "memory_alignment": {
        "name_zh": "研究记忆模型",
        "explain_zh": "把新信号和 Obsidian 中的旧 thesis、胜负案例、陷阱经验做对照。",
    },
}
BATON_DISPLAY = {
    "pre_trigger": "预触发",
    "first_baton": "第一棒",
    "second_baton": "第二棒",
    "third_baton": "第三棒",
    "validation_baton": "验证棒",
    "crowded": "拥挤",
    "invalidated": "已失效",
}
STATUS_DISPLAY = {
    "actionable": "可执行",
    "watch_only": "观察",
    "insufficient_evidence": "证据不足",
    "invalidated": "已失效",
}
MARKET_DISPLAY = {
    "A": "A股",
    "HK": "港股",
    "US": "美股",
    "ETF": "ETF",
    "CN": "中国资产",
    "CrossMarket": "跨市场",
}
ROLE_DISPLAY = {
    "leader": "领先资产",
    "bridge": "桥接资产",
    "local_mapping": "本地映射",
    "proxy": "同赛道代理",
}
DEFAULT_ASSET_NAMES = {
    "NVDA": ("NVIDIA", "US"),
    "SMCI": ("Super Micro", "US"),
    "AMD": ("AMD", "US"),
    "AMAT": ("Applied Materials", "US"),
    "ASML": ("ASML", "US"),
    "TSM": ("TSMC", "US"),
    "LLY": ("Eli Lilly", "US"),
    "FSLR": ("First Solar", "US"),
    "ENPH": ("Enphase", "US"),
    "TSN": ("Tyson Foods", "US"),
    "ADM": ("ADM", "US"),
    "300502.SZ": ("新易盛", "A"),
    "300308.SZ": ("中际旭创", "A"),
    "002371.SZ": ("北方华创", "A"),
    "688012.SH": ("中微公司", "A"),
    "600276.SH": ("恒瑞医药", "A"),
    "601012.SH": ("隆基绿能", "A"),
    "300274.SZ": ("阳光电源", "A"),
    "300498.SZ": ("温氏股份", "A"),
    "002714.SZ": ("牧原股份", "A"),
    "000876.SZ": ("新希望", "A"),
    "002157.SZ": ("正邦科技", "A"),
    "603477.SH": ("巨星农牧", "A"),
    "688981.SH": ("中芯国际", "A"),
    "603501.SH": ("韦尔股份", "A"),
    "688072.SH": ("拓荆科技", "A"),
    "688037.SH": ("芯源微", "A"),
    "600438.SH": ("通威股份", "A"),
    "688235.SH": ("百济神州", "A"),
    "603259.SH": ("药明康德", "A"),
    "688111.SH": ("金山办公", "A"),
    "002230.SZ": ("科大讯飞", "A"),
    "002463.SZ": ("沪电股份", "A"),
    "601138.SH": ("工业富联", "A"),
    "01801.HK": ("信达生物", "HK"),
    "06160.HK": ("百济神州", "HK"),
    "02269.HK": ("药明生物", "HK"),
    "00981.HK": ("中芯国际", "HK"),
    "09988.HK": ("阿里巴巴-W", "HK"),
    "00700.HK": ("腾讯控股", "HK"),
    "01024.HK": ("快手-W", "HK"),
    "512720.SH": ("计算机ETF", "ETF"),
    "515980.SH": ("人工智能ETF", "ETF"),
    "159865.SZ": ("畜牧养殖ETF", "ETF"),
    "512480.SH": ("半导体ETF", "ETF"),
    "159995.SZ": ("芯片ETF", "ETF"),
    "516180.SH": ("光伏ETF", "ETF"),
    "159857.SZ": ("光伏ETF", "ETF"),
    "159992.SZ": ("创新药ETF", "ETF"),
    "512290.SH": ("生物医药ETF", "ETF"),
}
FREE_RELIABLE_DOMAINS = {
    "anthropic.com",
    "ai.meta.com",
    "apnews.com",
    "api-docs.deepseek.com",
    "arxiv.org",
    "bis.org",
    "blog.google",
    "brookings.edu",
    "cninfo.com.cn",
    "cset.georgetown.edu",
    "deepmind.google",
    "dfcfw.com",
    "eastmoney.com",
    "export.arxiv.org",
    "fda.gov",
    "georgetown.edu",
    "github.com",
    "hai.stanford.edu",
    "hkexnews.hk",
    "huggingface.co",
    "imf.org",
    "mistral.ai",
    "nber.org",
    "nmpa.gov.cn",
    "nvidia.com",
    "oecd.org",
    "openai.com",
    "pdf.dfcfw.com",
    "rand.org",
    "sec.gov",
    "stanford.edu",
    "sse.com.cn",
    "szse.cn",
    "x.ai",
}
RELIABLE_VERIFICATION_STATUSES = {
    "official_confirmed",
    "primary_sources_seen",
    "official_signal",
    "regulatory_release",
    "exchange_filing",
    "company_release",
    "public_research",
}


class LeadLagService:
    def __init__(
        self,
        data_dir: Optional[str | Path] = None,
        obsidian_vault: Optional[str | Path] = None,
        db_path: Optional[str | Path] = None,
        radar_snapshot_path: Optional[str | Path] = None,
        live_enabled: Optional[bool] = None,
    ):
        self.repo_root = Path(__file__).resolve().parents[2]
        self.data_dir = Path(data_dir) if data_dir else self.repo_root / "sample_data" / "lead_lag"
        self.db_path = Path(db_path) if db_path else self.repo_root / "data" / "investment.db"
        self.radar_snapshot_path = (
            Path(radar_snapshot_path)
            if radar_snapshot_path
            else self.repo_root / "data" / "radar" / "cache" / "overview.json"
        )
        if live_enabled is None:
            live_enabled = str(os.getenv("LEAD_LAG_LIVE_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
        self.live_enabled = bool(live_enabled)
        self.bundle = self._load_bundle()
        self.asset_lookup = self._build_asset_lookup()
        self.v2_config = load_lead_lag_v2_config(self.repo_root)
        self._factor_snapshot_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self.live_evidence = self._load_live_evidence() if self.live_enabled else self._empty_live_evidence()
        self.memory_service: Optional[ObsidianMemoryService]
        if obsidian_vault is None:
            self.memory_service = ObsidianMemoryService()
        else:
            vault_path = Path(obsidian_vault)
            self.memory_service = ObsidianMemoryService(vault_path=str(vault_path)) if vault_path.exists() else None

    @staticmethod
    def _empty_bundle() -> Dict[str, Any]:
        return {
            "version": "v1",
            "as_of": None,
            "model_families": [],
            "sector_theses": [],
            "transmission_graph": {"nodes": [], "edges": []},
            "events": [],
            "replay_stats": [],
            "watchlists": {},
            "source_health": {},
            "opportunities": [],
            "liquidity": {},
            "cross_market_map": {},
            "memory_seed": {},
        }

    @staticmethod
    def _empty_live_evidence() -> Dict[str, Any]:
        return {
            "enabled": False,
            "loaded_at": None,
            "radar": {"cards": [], "liquidity": {}, "macro": {}, "external": {}, "hk": {}, "source_health": {}},
            "intelligence": {"events": [], "source_health": {}},
            "research": {"reports": [], "source_health": {}},
            "errors": [],
        }

    def _load_bundle(self) -> Dict[str, Any]:
        bundle = self._empty_bundle()
        if not self.data_dir.exists():
            return bundle

        json_files = sorted(path for path in self.data_dir.glob("*.json") if path.is_file())
        for path in json_files:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            self._merge_bundle(bundle, payload)

        self._dedupe_bundle(bundle)
        return bundle

    @staticmethod
    def _merge_bundle(bundle: Dict[str, Any], payload: Dict[str, Any]) -> None:
        for key, value in payload.items():
            if key in {"transmission_graph", "liquidity", "cross_market_map", "memory_seed"} and isinstance(value, dict):
                target = bundle.setdefault(key, {})
                if isinstance(target, dict):
                    for inner_key, inner_value in value.items():
                        target[inner_key] = inner_value
                else:
                    bundle[key] = value
                continue

            if isinstance(value, list):
                target = bundle.setdefault(key, [])
                if isinstance(target, list):
                    target.extend(value)
                else:
                    bundle[key] = list(value)
                continue

            if isinstance(value, dict):
                target = bundle.setdefault(key, {})
                if isinstance(target, dict):
                    target.update(value)
                else:
                    bundle[key] = dict(value)
                continue

            bundle[key] = value

    @staticmethod
    def _dedupe_bundle(bundle: Dict[str, Any]) -> None:
        def dedupe_list(items: Iterable[Dict[str, Any]], key_name: str) -> List[Dict[str, Any]]:
            seen = set()
            unique: List[Dict[str, Any]] = []
            for item in items:
                marker = item.get(key_name)
                if marker is None:
                    unique.append(item)
                    continue
                if marker in seen:
                    continue
                seen.add(marker)
                unique.append(item)
            return unique

        for key_name in ("model_families", "sector_theses", "events", "replay_stats", "opportunities"):
            if isinstance(bundle.get(key_name), list):
                bundle[key_name] = dedupe_list(bundle[key_name], {
                    "model_families": "family_id",
                    "sector_theses": "sector_key",
                    "events": "event_id",
                    "replay_stats": "scenario_key",
                    "opportunities": "opportunity_id",
                }[key_name])

        graph = bundle.get("transmission_graph")
        if isinstance(graph, dict):
            if isinstance(graph.get("nodes"), list):
                graph["nodes"] = dedupe_list(graph["nodes"], "node_id")
            if isinstance(graph.get("edges"), list):
                seen_edges = set()
                edges: List[Dict[str, Any]] = []
                for edge in graph["edges"]:
                    marker = (
                        edge.get("source"),
                        edge.get("target"),
                        edge.get("relation"),
                    )
                    if marker in seen_edges:
                        continue
                    seen_edges.add(marker)
                    edges.append(edge)
                graph["edges"] = edges

    @staticmethod
    def _normalize_sector(value: Any) -> str:
        if value is None:
            return "cross_market"
        text = str(value).strip()
        if not text:
            return "cross_market"
        return SECTOR_ALIASES.get(text, SECTOR_ALIASES.get(text.lower(), text.lower()))

    @staticmethod
    def _infer_market(code: str) -> str:
        normalized = str(code or "").upper()
        if normalized.endswith(".HK") or normalized.startswith("HK:"):
            return "HK"
        if normalized.endswith(".SH") or normalized.endswith(".SZ"):
            prefix = normalized.split(".", 1)[0]
            if prefix.startswith(("159", "511", "512", "513", "515", "516", "517", "588")):
                return "ETF"
            return "A"
        if re.fullmatch(r"\d{6}", normalized):
            if normalized.startswith(("159", "511", "512", "513", "515", "516", "517", "588")):
                return "ETF"
            return "A"
        if normalized in {"", "-", "N/A"}:
            return ""
        return "US"

    @classmethod
    def _normalize_asset_code(cls, value: Any, market: Optional[str] = None) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return ""
        text = text.replace("HK:", "").replace("SH:", "").replace("SZ:", "")
        text = text.replace("sh", "SH").replace("sz", "SZ")
        if text.startswith("SH") and len(text) == 8:
            return f"{text[2:]}.SH"
        if text.startswith("SZ") and len(text) == 8:
            return f"{text[2:]}.SZ"
        if text.startswith("HK") and len(text) in {6, 7}:
            return f"{text[2:].zfill(5)}.HK"
        if text.endswith(".HK"):
            prefix = text[:-3]
            return f"{prefix.zfill(5)}.HK" if prefix.isdigit() else text
        if text.endswith(".SH") or text.endswith(".SZ"):
            return text
        if re.fullmatch(r"\d{6}", text):
            if market and str(market).upper() == "ETF":
                return f"{text}.SH" if text.startswith("5") else f"{text}.SZ"
            return f"{text}.SH" if text.startswith(("5", "6", "9")) else f"{text}.SZ"
        if re.fullmatch(r"\d{4,5}", text):
            return f"{text.zfill(5)}.HK"
        return text

    @staticmethod
    def _json_loads(value: Any, fallback: Any) -> Any:
        if value in (None, ""):
            return fallback
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(str(value))
        except Exception:
            return fallback

    @staticmethod
    def _source_domain(url: Any) -> str:
        try:
            parsed = urlparse(str(url or "").strip())
        except Exception:
            return ""
        return parsed.netloc.lower().removeprefix("www.")

    @staticmethod
    def _domain_allowed(domain: str) -> bool:
        if not domain:
            return False
        return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in FREE_RELIABLE_DOMAINS)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d")
        except Exception:
            return None

    @classmethod
    def _is_future_dated(cls, value: Any, grace_days: int = 1) -> bool:
        parsed = cls._parse_datetime(value)
        if parsed is None:
            return False
        return parsed.date() > datetime.now().date() and (parsed.date() - datetime.now().date()).days > grace_days

    @classmethod
    def _source_quality(cls, url: Any, verification_status: Any = None, source_count: Any = None) -> Dict[str, Any]:
        domain = cls._source_domain(url)
        status = str(verification_status or "").strip().lower()
        try:
            count = int(source_count or 0)
        except Exception:
            count = 0
        return {
            "domain": domain,
            "free_public": cls._domain_allowed(domain),
            "verification_status": status,
            "source_count": count,
        }

    @classmethod
    def _is_reliable_event(cls, event: Dict[str, Any]) -> bool:
        url = event.get("primary_source_url")
        if not url:
            return False
        quality = cls._source_quality(url, event.get("verification_status"), event.get("source_count"))
        confidence = float(event.get("confidence") or 0.0)
        if cls._is_future_dated(event.get("event_time") or event.get("last_seen_at") or event.get("first_seen_at")):
            return False
        if quality["free_public"] and confidence >= 0.48:
            return True
        return quality["verification_status"] in RELIABLE_VERIFICATION_STATUSES and confidence >= 0.60

    @classmethod
    def _is_reliable_report(cls, report: Dict[str, Any]) -> bool:
        url = report.get("original_url") or report.get("url")
        if not url:
            return False
        quality = cls._source_quality(url, report.get("source_tier") or report.get("source_key"), 1)
        if cls._is_future_dated(report.get("published_at"), grace_days=30):
            return False
        return bool(quality["free_public"])

    def _build_asset_lookup(self) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}

        def add_asset(code: Any, name: Any = None, market: Any = None, sector: Any = None, role: Any = None) -> None:
            normalized = self._normalize_asset_code(code, market=market)
            if not normalized:
                return
            default_name, default_market = DEFAULT_ASSET_NAMES.get(normalized, (None, None))
            lookup[normalized] = {
                "code": normalized,
                "name": str(name or default_name or normalized),
                "market": str(market or default_market or self._infer_market(normalized)),
                "sector": self._normalize_sector(sector),
                "role": role or "",
            }

        for code, (name, market) in DEFAULT_ASSET_NAMES.items():
            add_asset(code, name=name, market=market)

        graph = self.bundle.get("transmission_graph") or {}
        if isinstance(graph, dict):
            for node in graph.get("nodes", []):
                if isinstance(node, dict):
                    add_asset(
                        node.get("symbol"),
                        name=node.get("label"),
                        market=node.get("market"),
                        sector=node.get("sector"),
                        role=node.get("role"),
                    )

        for group in (self.bundle.get("watchlists") or {}).values():
            if not isinstance(group, list):
                continue
            for row in group:
                if isinstance(row, dict):
                    add_asset(row.get("symbol"), row.get("name"), row.get("market"), row.get("sector"), row.get("role"))

        for opportunity in self.bundle.get("opportunities", []):
            if isinstance(opportunity, dict):
                add_asset(
                    opportunity.get("symbol"),
                    opportunity.get("asset_name"),
                    opportunity.get("market"),
                    opportunity.get("sector_key"),
                    opportunity.get("baton"),
                )

        if self.db_path.exists():
            try:
                with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5) as conn:
                    conn.row_factory = sqlite3.Row
                    table_exists = conn.execute(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stocks'"
                    ).fetchone()[0]
                    if table_exists:
                        for row in conn.execute("SELECT code, name, market, category FROM stocks"):
                            add_asset(row["code"], row["name"], row["market"], row["category"])
            except Exception:
                pass
        return lookup

    def _asset_for(self, code: Any, name: Any = None, market: Any = None, sector: Any = None) -> Dict[str, Any]:
        normalized = self._normalize_asset_code(code, market=market)
        if not normalized:
            return {"code": "", "name": str(name or ""), "market": str(market or ""), "sector": self._normalize_sector(sector)}
        asset = dict(self.asset_lookup.get(normalized) or {})
        default_name, default_market = DEFAULT_ASSET_NAMES.get(normalized, (None, None))
        asset.setdefault("code", normalized)
        asset["code"] = normalized
        asset["name"] = str(name or asset.get("name") or default_name or normalized)
        asset["market"] = str(market or asset.get("market") or default_market or self._infer_market(normalized))
        asset["sector"] = self._normalize_sector(sector or asset.get("sector"))
        return asset

    def _load_live_evidence(self) -> Dict[str, Any]:
        evidence = self._empty_live_evidence()
        evidence["enabled"] = True
        evidence["loaded_at"] = datetime.now().replace(microsecond=0).isoformat()

        try:
            evidence["radar"] = self._load_radar_evidence()
        except Exception as exc:
            evidence["errors"].append({"surface": "radar", "error": str(exc)})

        try:
            db_payload = self._load_sqlite_evidence()
            evidence["intelligence"] = db_payload.get("intelligence", evidence["intelligence"])
            evidence["research"] = db_payload.get("research", evidence["research"])
        except Exception as exc:
            evidence["errors"].append({"surface": "sqlite", "error": str(exc)})

        return evidence

    def _load_radar_evidence(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"cards": [], "liquidity": {}, "macro": {}, "external": {}, "hk": {}, "source_health": {"surface": "radar", "status": "missing"}}
        if not self.radar_snapshot_path.exists():
            return payload
        snapshot = json.loads(self.radar_snapshot_path.read_text(encoding="utf-8"))
        sectors = ((snapshot.get("sectors") or {}).get("cards") or []) if isinstance(snapshot, dict) else []
        cards = []
        for card in sectors:
            if not isinstance(card, dict):
                continue
            watchlist = []
            for code in card.get("watchlist", []) or []:
                asset = self._asset_for(code, sector=card.get("key"))
                watchlist.append(asset)
            cards.append(
                {
                    "sector_key": self._normalize_sector(card.get("key") or card.get("name")),
                    "sector_name": card.get("name") or card.get("key"),
                    "score": self._as_float(card.get("score")) / 100.0,
                    "confidence": self._as_float(card.get("confidence"), 0.5),
                    "leading_variables": card.get("leading_variables") or [],
                    "validation_signals": card.get("confirmed_variables") or [],
                    "invalidation_rules": card.get("invalid_conditions") or [],
                    "watchlist": watchlist,
                    "evidence": card.get("evidence") or {},
                    "source_url": self.radar_snapshot_path.as_posix(),
                    "updated_at": snapshot.get("generated_at") or (snapshot.get("summary") or {}).get("last_data_sync"),
                }
            )
        summary = snapshot.get("summary") or {}
        payload["cards"] = cards
        payload["liquidity"] = {
            "external_risk_score": summary.get("external_risk_score"),
            "hk_liquidity_score": summary.get("hk_liquidity_score"),
            "macro_regime": summary.get("macro_regime"),
            "sector_preposition_score": summary.get("sector_preposition_score"),
            "data_coverage": summary.get("data_coverage"),
            "last_data_sync": summary.get("last_data_sync"),
        }
        payload["macro"] = snapshot.get("macro") if isinstance(snapshot.get("macro"), dict) else {}
        payload["external"] = snapshot.get("external") if isinstance(snapshot.get("external"), dict) else {}
        payload["hk"] = snapshot.get("hk") if isinstance(snapshot.get("hk"), dict) else {}
        payload["source_health"] = {
            "surface": "radar",
            "status": "healthy" if cards else "empty",
            "source": "data/radar/cache/overview.json",
            "updated_at": snapshot.get("generated_at"),
            "served_from": (snapshot.get("meta") or {}).get("served_from"),
            "data_coverage": summary.get("data_coverage"),
        }
        return payload

    def _load_sqlite_evidence(self) -> Dict[str, Any]:
        payload = {
            "intelligence": {"events": [], "source_health": {"surface": "intelligence", "status": "missing"}},
            "research": {"reports": [], "source_health": {"surface": "research", "status": "missing"}},
        }
        if not self.db_path.exists():
            return payload
        with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            tables = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if {"intelligence_events", "event_entities"}.issubset(tables):
                payload["intelligence"] = self._load_intelligence_events(conn)
            if "research_reports" in tables:
                payload["research"] = self._load_research_reports(conn)
        return payload

    def _load_intelligence_events(self, conn: sqlite3.Connection, limit: int = 30) -> Dict[str, Any]:
        rows = conn.execute(
            """
            SELECT id, event_key, title, title_zh, category, priority, confidence,
                   first_seen_at, last_seen_at, event_time, summary, summary_zh,
                   impact_summary, impact_summary_zh, impact_score, verification_status,
                   source_count, primary_source_url
            FROM intelligence_events
            WHERE status = 'active'
            ORDER BY
                CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 ELSE 2 END,
                COALESCE(event_time, last_seen_at, first_seen_at) DESC,
                id DESC
            LIMIT ?
            """,
            (limit * 4,),
        ).fetchall()
        events = []
        for row in rows:
            event = {key: row[key] for key in row.keys()}
            if not self._is_reliable_event(event):
                continue
            entities = conn.execute(
                """
                SELECT entity_type, name, name_zh, ticker, market, role, role_zh, relevance_score
                FROM event_entities
                WHERE event_id = ?
                ORDER BY relevance_score DESC, rowid ASC
                LIMIT 12
                """,
                (row["id"],),
            ).fetchall()
            assets = []
            for entity in entities:
                ticker = entity["ticker"]
                if not ticker:
                    continue
                asset = self._asset_for(ticker, name=entity["name_zh"] or entity["name"], market=entity["market"], sector=event["category"])
                asset["entity_type"] = entity["entity_type"]
                asset["role"] = entity["role_zh"] or entity["role"]
                asset["relevance_score"] = self._as_float(entity["relevance_score"], 0.0)
                assets.append(asset)
            event["assets"] = assets
            event["sector_key"] = self._normalize_sector(event.get("category"))
            event["source_quality"] = self._source_quality(
                event.get("primary_source_url"),
                event.get("verification_status"),
                event.get("source_count"),
            )
            events.append(event)
            if len(events) >= limit:
                break
        return {
            "events": events,
            "source_health": {
                "surface": "intelligence",
                "status": "healthy" if events else "empty",
                "source": "data/investment.db:intelligence_events",
                "updated_at": max((event.get("last_seen_at") or "" for event in events), default=None),
                "count": len(events),
                "quality_filter": "free_public_reliable",
            },
        }

    def _load_research_reports(self, conn: sqlite3.Connection, limit: int = 80) -> Dict[str, Any]:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(research_reports)").fetchall()}
        select_cols = [
            "report_key", "title", "title_zh", "source_key", "source_name", "url",
            "report_type", "published_at", "fetched_at", "language", "summary",
            "summary_zh", "thesis", "thesis_zh", "relevance", "relevance_zh",
        ]
        for optional in ("source_tier", "target_scope", "publisher_region", "tickers_json", "focus_areas_json", "tags_json", "original_url"):
            if optional in columns:
                select_cols.append(optional)
        rows = conn.execute(
            f"""
            SELECT {', '.join(select_cols)}
            FROM research_reports
            WHERE status = 'active'
            ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
            LIMIT ?
            """,
            (limit * 3,),
        ).fetchall()
        reports = []
        for row in rows:
            report = {key: row[key] for key in row.keys()}
            if not self._is_reliable_report(report):
                continue
            tickers = self._json_loads(report.get("tickers_json"), [])
            focus_areas = self._json_loads(report.get("focus_areas_json"), [])
            assets = []
            for code in tickers if isinstance(tickers, list) else []:
                asset = self._asset_for(code)
                if asset.get("code"):
                    assets.append(asset)
            report["assets"] = assets
            report["focus_areas"] = [self._normalize_sector(item) for item in focus_areas] if isinstance(focus_areas, list) else []
            report["source_url"] = report.get("original_url") or report.get("url")
            report["source_quality"] = self._source_quality(
                report.get("source_url"),
                report.get("source_tier") or report.get("source_key"),
                1,
            )
            reports.append(report)
            if len(reports) >= limit:
                break
        return {
            "reports": reports,
            "source_health": {
                "surface": "research",
                "status": "healthy" if reports else "empty",
                "source": "data/investment.db:research_reports",
                "updated_at": max((report.get("fetched_at") or report.get("published_at") or "" for report in reports), default=None),
                "count": len(reports),
                "quality_filter": "free_public_reliable",
            },
        }

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
        return max(minimum, min(maximum, value))

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    def _candidate_stage(self, item: Dict[str, Any]) -> str:
        if item.get("invalidated") is True or str(item.get("stage_hint", "")).lower() == "invalidated":
            return "invalidated"

        if self._as_float(item.get("decay_rate")) >= 0.55 or self._as_int(item.get("days_since_event")) >= 21:
            return "decaying"

        if self._as_float(item.get("crowding")) >= 0.70 or str(item.get("stage_hint", "")).lower() == "crowded":
            return "crowded"

        validation_status = str(item.get("validation_status", "")).lower()
        replay_hit_rate = self._as_float(item.get("replay_hit_rate", item.get("validation_hit_rate")))
        signal_strength = self._as_float(item.get("signal_strength"))
        evidence_count = self._as_int(item.get("evidence_count"))
        if validation_status in {"validating", "replaying"} or (
            signal_strength >= 0.65 and evidence_count >= 2 and 0.45 <= replay_hit_rate < 0.70
        ):
            return "validating"

        if signal_strength >= 0.70 and evidence_count >= 2:
            return "triggered"

        if signal_strength >= 0.35 or evidence_count >= 1:
            return "pre_trigger"

        return "latent"

    def _candidate_score(self, item: Dict[str, Any]) -> float:
        base_score = self._as_float(item.get("base_score"))
        signal_strength = self._as_float(item.get("signal_strength")) * 100.0
        evidence_count = min(self._as_int(item.get("evidence_count")), 8) * 4.0
        liquidity_score = self._as_float(item.get("liquidity_score", item.get("liquidity"))) * 100.0
        replay_hit_rate = self._as_float(item.get("replay_hit_rate", item.get("validation_hit_rate", 0.5))) * 100.0
        crowding_penalty = self._as_float(item.get("crowding")) * 35.0
        decay_penalty = self._as_float(item.get("decay_rate")) * 20.0

        score = (
            base_score * 0.30
            + signal_strength * 0.22
            + evidence_count * 0.10
            + liquidity_score * 0.16
            + replay_hit_rate * 0.14
            + (100.0 - crowding_penalty) * 0.05
            + (100.0 - decay_penalty) * 0.03
        )
        if item.get("invalidated") is True:
            score -= 15.0
        return round(self._clamp(score), 2)

    @staticmethod
    def _rank_key(item: Dict[str, Any]) -> tuple:
        return (
            -float(item.get("score", 0.0)),
            STAGE_ORDER.get(str(item.get("stage", "")).lower(), len(STAGES)),
            str(item.get("name") or item.get("title") or item.get("symbol") or item.get("opportunity_id") or ""),
        )

    def _enrich_opportunities(self) -> List[Dict[str, Any]]:
        enriched: List[Dict[str, Any]] = []
        for raw in self.bundle.get("opportunities", []):
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            asset = self._asset_for(item.get("symbol"), name=item.get("asset_name"), market=item.get("market"), sector=item.get("sector_key"))
            item["symbol"] = asset.get("code") or item.get("symbol")
            item["asset_code"] = asset.get("code")
            item["asset_name"] = asset.get("name")
            item["market"] = asset.get("market") or item.get("market")
            item["stage"] = self._candidate_stage(item)
            item["score"] = self._candidate_score(item)
            item["evidence_sources"] = item.get("evidence_sources") or ["sample_data"]
            enriched.append(item)
        enriched.extend(self._live_opportunities())
        enriched.sort(key=self._rank_key)
        return enriched

    def _live_opportunities(self) -> List[Dict[str, Any]]:
        if not self.live_enabled:
            return []
        live_items: List[Dict[str, Any]] = []
        radar_cards = self.live_evidence.get("radar", {}).get("cards", [])
        latest_events = self.live_evidence.get("intelligence", {}).get("events", [])
        reports = self.live_evidence.get("research", {}).get("reports", [])
        event_by_sector: Dict[str, List[Dict[str, Any]]] = {}
        report_by_sector: Dict[str, List[Dict[str, Any]]] = {}
        for event in latest_events:
            sector = self._normalize_sector(event.get("sector_key") or event.get("category"))
            event_by_sector.setdefault(sector, []).append(event)
        for report in reports:
            sectors = report.get("focus_areas") or [self._normalize_sector(report.get("report_type"))]
            for sector in sectors:
                report_by_sector.setdefault(self._normalize_sector(sector), []).append(report)

        for card in radar_cards:
            if not isinstance(card, dict):
                continue
            sector = self._normalize_sector(card.get("sector_key"))
            watchlist = [item for item in card.get("watchlist", []) if isinstance(item, dict) and item.get("code")]
            if not watchlist:
                continue
            sector_events = event_by_sector.get(sector, [])
            sector_reports = report_by_sector.get(sector, [])
            evidence_count = len(sector_events[:5]) + len(sector_reports[:5]) + int(bool(card.get("evidence")))
            signal_strength = self._clamp(self._as_float(card.get("score"), 0.0), 0.0, 1.0)
            confidence = self._clamp(self._as_float(card.get("confidence"), 0.5), 0.0, 1.0)
            for index, asset in enumerate(watchlist[:3]):
                event = sector_events[0] if sector_events else {}
                report = sector_reports[0] if sector_reports else {}
                item = {
                    "opportunity_id": f"live_{sector}_{asset.get('code', '').replace('.', '_').lower()}",
                    "name": f"{card.get('sector_name') or sector} live baton",
                    "title": f"{asset.get('name')} ({asset.get('code')})",
                    "sector_key": sector,
                    "symbol": asset.get("code"),
                    "asset_code": asset.get("code"),
                    "asset_name": asset.get("name"),
                    "market": asset.get("market"),
                    "base_score": round(signal_strength * 100, 1),
                    "signal_strength": signal_strength,
                    "evidence_count": evidence_count,
                    "liquidity_score": self._clamp(
                        self._as_float(self.live_evidence.get("radar", {}).get("liquidity", {}).get("hk_liquidity_score"), 50) / 100.0,
                        0.0,
                        1.0,
                    ),
                    "replay_hit_rate": max(0.45, min(0.78, confidence)),
                    "crowding": 0.62 if signal_strength >= 0.70 and index == 0 else 0.28,
                    "decay_rate": 0.08,
                    "days_since_event": 1 if event else 5,
                    "validation_status": "validating" if evidence_count >= 2 else "watching",
                    "source_url": event.get("primary_source_url") or report.get("source_url") or card.get("source_url"),
                    "updated_at": event.get("last_seen_at") or report.get("fetched_at") or card.get("updated_at"),
                    "evidence_sources": [
                        source
                        for source in ("radar_snapshot", "intelligence_events" if sector_events else "", "research_reports" if sector_reports else "")
                        if source
                    ],
                    "evidence_items": self._compact_evidence_items(card, sector_events, sector_reports),
                    "notes": event.get("title_zh") or event.get("title") or report.get("title_zh") or report.get("title") or "Live radar watchlist candidate.",
                }
                item["stage"] = self._candidate_stage(item)
                item["score"] = self._candidate_score(item)
                live_items.append(item)
        return live_items

    @staticmethod
    def _compact_evidence_items(card: Dict[str, Any], events: List[Dict[str, Any]], reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if card:
            items.append(
                {
                    "source_surface": "radar",
                    "title": card.get("sector_name"),
                    "confidence": card.get("confidence"),
                    "updated_at": card.get("updated_at"),
                    "source_url": card.get("source_url"),
                }
            )
        for event in events[:2]:
            items.append(
                {
                    "source_surface": "intelligence",
                    "title": event.get("title_zh") or event.get("title"),
                    "confidence": event.get("confidence"),
                    "updated_at": event.get("last_seen_at") or event.get("event_time"),
                    "source_url": event.get("primary_source_url"),
                }
            )
        for report in reports[:2]:
            items.append(
                {
                    "source_surface": "research",
                    "title": report.get("title_zh") or report.get("title"),
                    "confidence": report.get("source_tier") or report.get("source_key"),
                    "updated_at": report.get("published_at") or report.get("fetched_at"),
                    "source_url": report.get("source_url"),
                }
            )
        return items

    def _enrich_model_family(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(raw)
        sector_defaults = {
            "leadership_breadth": ["ai", "semis", "innovative_pharma"],
            "event_spillover": ["ai", "innovative_pharma", "semis"],
            "transmission_graph": ["ai", "semis", "solar", "hog_cycle"],
            "liquidity_dispersion": ["ai", "semis", "solar"],
            "earnings_revision": ["ai", "innovative_pharma", "semis"],
            "policy_surprise": ["solar", "hog_cycle", "innovative_pharma"],
            "valuation_gap": ["ai", "semis"],
            "replay_validation": ["ai", "innovative_pharma", "semis", "solar", "hog_cycle"],
            "breadth_thrust": ["ai", "semis", "solar"],
            "memory_alignment": ["ai", "innovative_pharma", "semis", "solar", "hog_cycle"],
        }
        item["model_id"] = item.get("model_id") or item.get("family_id")
        item["model_family"] = item.get("model_family") or item.get("name")
        item["thesis"] = item.get("thesis") or item.get("description")
        item["description"] = item.get("description") or item.get("thesis") or ""
        item["applicable_assets"] = item.get("applicable_assets") or ["A-share", "HK", "US ADR", "Global lead assets"]
        item["applicable_sectors"] = item.get("applicable_sectors") or sector_defaults.get(str(item.get("family_id") or ""), ["ai", "semis"])
        item["lead_signals"] = item.get("lead_signals") or [item.get("description") or item.get("name")]
        item["follower_tiers"] = item.get("follower_tiers") or ["first_baton", "second_baton", "next_baton"]
        item["lag_windows"] = item.get("lag_windows") or {"min_days": 1, "max_days": 20}
        item["validation_signals"] = item.get("validation_signals") or ["replay_validation", "source_health", "event_confirmation"]
        item["crowding_signals"] = item.get("crowding_signals") or ["liquidity_dispersion", "crowding", "stage=crowded"]
        item["invalidation_rules"] = item.get("invalidation_rules") or ["evidence weakens", "decay rate rises", "stage becomes invalidated"]
        item["evidence_sources"] = item.get("evidence_sources") or list((self.bundle.get("source_health") or {}).keys()) or ["sample_data"]
        item["refresh_frequency"] = item.get("refresh_frequency") or "daily"
        item["confidence_formula"] = item.get("confidence_formula") or "0.65 * signal_strength + 0.35 * confidence"
        item["stage_machine"] = item.get("stage_machine") or list(STAGES)
        item["notes_links"] = item.get("notes_links") or []
        item["score"] = round(
            self._as_float(item.get("signal_strength", item.get("signal", 0.0))) * 100.0 * 0.65
            + self._as_float(item.get("confidence", 0.0)) * 100.0 * 0.35,
            2,
        )
        return item

    def _normalized_graph(self) -> Dict[str, Any]:
        graph = self.bundle.get("transmission_graph", {})
        nodes = []
        edges = []
        for raw_node in graph.get("nodes", []) if isinstance(graph, dict) else []:
            if not isinstance(raw_node, dict):
                continue
            node = dict(raw_node)
            node["node_type"] = node.get("node_type") or ("asset" if node.get("symbol") else "sector")
            nodes.append(node)
        for raw_edge in graph.get("edges", []) if isinstance(graph, dict) else []:
            if not isinstance(raw_edge, dict):
                continue
            edge = dict(raw_edge)
            detail_relation = edge.get("relation", "maps_to")
            lag_days = self._as_int(edge.get("lag_days"), 3)
            edge["from_id"] = edge.get("from_id") or edge.get("source")
            edge["to_id"] = edge.get("to_id") or edge.get("target")
            edge["relation_detail"] = detail_relation
            edge["relation"] = edge.get("relation") if edge.get("relation") in {
                "leads", "validates", "crowds", "invalidates", "maps_to", "spills_over_to", "affects_margin_of", "affects_demand_of", "affects_capex_of"
            } else CANONICAL_RELATIONS.get(str(detail_relation), "maps_to")
            edge["sign"] = edge.get("sign", 1)
            edge["strength"] = self._as_float(edge.get("strength"), 0.5)
            edge["lag_min_days"] = edge.get("lag_min_days", max(1, lag_days))
            edge["lag_max_days"] = edge.get("lag_max_days", max(2, lag_days + 2))
            edge["evidence_type"] = edge.get("evidence_type") or detail_relation
            edge["confidence"] = edge.get("confidence", round(self._as_float(edge.get("strength"), 0.5), 2))
            edge["last_verified_at"] = edge.get("last_verified_at") or self.bundle.get("as_of")
            edges.append(edge)
        node_ids = {node.get("node_id") for node in nodes if isinstance(node, dict)}
        for card in self.live_evidence.get("radar", {}).get("cards", []):
            if not isinstance(card, dict):
                continue
            sector = self._normalize_sector(card.get("sector_key"))
            sector_node = f"live_sector_{sector}"
            if sector_node not in node_ids:
                nodes.append(
                    {
                        "node_id": sector_node,
                        "label": card.get("sector_name") or sector,
                        "node_type": "sector",
                        "market": "CN",
                        "sector": sector,
                        "role": "live_radar_sector",
                        "score": round(self._as_float(card.get("score"), 0.0) * 100, 1),
                    }
                )
                node_ids.add(sector_node)
            for asset in (card.get("watchlist") or [])[:4]:
                if not isinstance(asset, dict) or not asset.get("code"):
                    continue
                asset_node = f"live_asset_{asset['code'].replace('.', '_').lower()}"
                if asset_node not in node_ids:
                    nodes.append(
                        {
                            "node_id": asset_node,
                            "label": asset.get("name"),
                            "symbol": asset.get("code"),
                            "node_type": "asset",
                            "market": asset.get("market"),
                            "sector": sector,
                            "role": "live_watchlist",
                            "score": round(self._as_float(card.get("score"), 0.0) * 100, 1),
                        }
                    )
                    node_ids.add(asset_node)
                edges.append(
                    {
                        "source": sector_node,
                        "target": asset_node,
                        "from_id": sector_node,
                        "to_id": asset_node,
                        "relation": "maps_to",
                        "relation_detail": "live_watchlist",
                        "sign": 1,
                        "strength": round(self._as_float(card.get("confidence"), 0.5), 2),
                        "lag_min_days": 1,
                        "lag_max_days": 10,
                        "sector": sector,
                        "evidence_type": "radar_snapshot_watchlist",
                        "confidence": round(self._as_float(card.get("confidence"), 0.5), 2),
                        "last_verified_at": card.get("updated_at") or self.live_evidence.get("loaded_at"),
                    }
                )
        return {"nodes": nodes, "edges": edges}

    @staticmethod
    def _split_batons(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        buckets: Dict[str, List[Dict[str, Any]]] = {name: [] for name in BATON_ORDER}
        buckets["invalidated"] = []
        baton_slots = ("first_baton", "second_baton", "next_baton")
        non_invalidated_rank = 0
        for item in items:
            if item.get("stage") == "invalidated":
                item["baton"] = "blocked"
                buckets["invalidated"].append(item)
                continue

            if non_invalidated_rank == 0:
                baton = baton_slots[0]
            elif non_invalidated_rank < 3:
                baton = baton_slots[1]
            else:
                baton = baton_slots[2]
            item["baton"] = baton.replace("_", "-")
            buckets[baton].append(item)
            non_invalidated_rank += 1
        return buckets

    @staticmethod
    def _section_summary(items: List[Dict[str, Any]], key: str) -> Dict[str, int]:
        summary: Dict[str, int] = {}
        for item in items:
            value = item.get(key)
            if value is None:
                continue
            summary[str(value)] = summary.get(str(value), 0) + 1
        return summary

    def _sector_thesis_map(self) -> Dict[str, Dict[str, Any]]:
        return {
            self._normalize_sector(item.get("sector_key")): item
            for item in self.bundle.get("sector_theses", [])
            if isinstance(item, dict)
        }

    def _sector_assets(self, sector: str) -> List[Dict[str, Any]]:
        normalized_sector = self._normalize_sector(sector)
        thesis = self._sector_thesis_map().get(normalized_sector, {})
        symbols: List[Any] = []
        baton_map = thesis.get("baton_map") if isinstance(thesis, dict) else {}
        if isinstance(baton_map, dict):
            for key in ("first_baton", "second_baton", "next_baton"):
                symbols.extend(baton_map.get(key) or [])
        symbols.extend(thesis.get("anchors") or [])
        for group in (self.bundle.get("watchlists") or {}).values():
            if not isinstance(group, list):
                continue
            for row in group:
                if isinstance(row, dict) and self._normalize_sector(row.get("sector")) == normalized_sector:
                    symbols.append(row.get("symbol"))
        seen = set()
        assets: List[Dict[str, Any]] = []
        for symbol in symbols:
            asset = self._asset_for(symbol, sector=normalized_sector)
            code = asset.get("code")
            if not code or code in seen:
                continue
            seen.add(code)
            assets.append(asset)
        return assets

    @staticmethod
    def _with_asset_role(asset: Dict[str, Any], role: str) -> Dict[str, Any]:
        payload = {
            "code": asset.get("code", ""),
            "name": asset.get("name", ""),
            "market": asset.get("market", ""),
            "role": role,
        }
        if asset.get("sector"):
            payload["sector"] = asset.get("sector")
        return payload

    def _select_v2_assets(self, item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        current = self._asset_for(item.get("symbol") or item.get("asset_code"), item.get("asset_name"), item.get("market"), sector)
        sector_assets = self._sector_assets(sector)
        if current.get("code") and all(asset.get("code") != current.get("code") for asset in sector_assets):
            sector_assets.insert(0, current)

        leader = next((asset for asset in sector_assets if str(asset.get("market")) == "US"), None) or current
        local = (
            current
            if str(current.get("market")) in {"A", "HK", "ETF"}
            else next((asset for asset in sector_assets if str(asset.get("market")) in {"A", "HK", "ETF"}), {})
        )
        bridge = next(
            (
                asset
                for asset in sector_assets
                if asset.get("code") not in {leader.get("code"), local.get("code")}
                and str(asset.get("market")) in {"US", "HK", "A", "ETF"}
            ),
            {},
        )
        if not bridge:
            bridge = local if local.get("code") != current.get("code") else leader

        return {
            "leader_asset": self._with_asset_role(leader, "leader") if leader else {},
            "bridge_asset": self._with_asset_role(bridge, "bridge") if bridge else {},
            "local_asset": self._with_asset_role(local, "local_mapping") if local else {},
        }

    @staticmethod
    def _baton_stage_v2(stage: str, baton: Any = None) -> str:
        normalized = str(stage or "").lower()
        if normalized == "invalidated":
            return "invalidated"
        if normalized == "crowded":
            return "crowded"
        if normalized == "validating":
            return "validation_baton"
        if normalized == "pre_trigger":
            return "pre_trigger"
        if normalized == "triggered":
            return "first_baton"
        baton_text = str(baton or "").replace("-", "_")
        if baton_text in {"first_baton", "second_baton", "third_baton"}:
            return baton_text
        return "second_baton" if normalized == "decaying" else "pre_trigger"

    def _lag_window_for(self, item: Dict[str, Any]) -> Dict[str, int]:
        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        symbol = self._normalize_asset_code(item.get("symbol") or item.get("asset_code"), item.get("market"))
        min_days = 1
        max_days = 5
        graph = self._normalized_graph()
        for edge in graph.get("edges", []):
            if not isinstance(edge, dict):
                continue
            target = str(edge.get("target") or edge.get("to_id") or "")
            if edge.get("sector") == sector or symbol.replace(".", "_").lower() in target.lower() or symbol == target:
                min_days = min(min_days, self._as_int(edge.get("lag_min_days"), min_days))
                max_days = max(max_days, self._as_int(edge.get("lag_max_days"), max_days))
        return {"min": max(0, min_days), "max": max(max_days, min_days)}

    def _expected_review_times(self, last_update: Any) -> List[str]:
        parsed = self._parse_datetime(last_update) or datetime.now().replace(microsecond=0)
        return [
            parsed.replace(hour=11, minute=40, second=0, microsecond=0).isoformat(),
            parsed.replace(hour=15, minute=15, second=0, microsecond=0).isoformat(),
        ]

    def _replay_summary_for_sector(self, sector: str) -> Dict[str, Any]:
        normalized = self._normalize_sector(sector)
        best: Dict[str, Any] = {}
        for item in self.bundle.get("replay_stats", []):
            if isinstance(item, dict) and self._normalize_sector(item.get("sector_key")) == normalized:
                best = item
                break
        hit_rate = self._as_float(best.get("hit_rate"), 0.0)
        avg_lead = self._as_float(best.get("avg_lead_days"), 0.0)
        return {
            "hit_rate": round(hit_rate, 3),
            "best_horizon": f"{avg_lead:.1f}d" if avg_lead else "待回放",
            "worst_failure_mode": best.get("failure_mode") or "验证滞后或拥挤度快速上升",
            "stage_note": "历史回放支持" if best.get("validated") else "历史回放仍需验证",
        }

    @staticmethod
    def _state_label(score: float, labels: tuple[str, str, str, str]) -> str:
        if score >= 80:
            return labels[3]
        if score >= 60:
            return labels[2]
        if score >= 40:
            return labels[1]
        return labels[0]

    def _source_quality_v2(self, item: Dict[str, Any]) -> Dict[str, str]:
        source_count = self._as_int(item.get("evidence_count"), len(item.get("evidence_sources") or []))
        if source_count >= 4:
            label = "T1"
        elif source_count >= 2:
            label = "T2"
        elif source_count >= 1:
            label = "T3"
        else:
            label = "mixed"
        return {"label": label, "explanation": f"{source_count} 条证据源，来自 {', '.join(item.get('evidence_sources') or ['sample_data'])}"}

    @staticmethod
    def _display(mapping: Dict[str, str], value: Any) -> str:
        text = str(value or "")
        return mapping.get(text, text)

    @classmethod
    def _sector_display(cls, sector: Any) -> str:
        normalized = cls._normalize_sector(sector)
        return SECTOR_DISPLAY_NAMES.get(normalized, str(sector or normalized))

    @staticmethod
    def _model_display(model_id: Any) -> Dict[str, str]:
        key = str(model_id or "leadership_breadth")
        payload = MODEL_DISPLAY.get(key, {})
        return {
            "model_id": key,
            "model_name_zh": payload.get("name_zh") or key,
            "model_explain_zh": payload.get("explain_zh") or "模型说明待补充。",
        }

    def _factor_snapshots(self) -> Dict[str, Dict[str, Any]]:
        if self._factor_snapshot_cache is not None:
            return self._factor_snapshot_cache
        cache: Dict[str, Dict[str, Any]] = {}
        self._factor_snapshot_cache = cache
        if not self.db_path.exists():
            return cache
        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                table_exists = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_factor_snapshot'"
                ).fetchone()[0]
                if not table_exists:
                    return cache
                rows = conn.execute(
                    """
                    SELECT trade_date, code, model, quality, growth, valuation, flow, technical, risk, total
                    FROM stock_factor_snapshot
                    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_factor_snapshot)
                    ORDER BY model = 'conservative' DESC, total DESC
                    """
                ).fetchall()
        except Exception:
            return cache

        for row in rows:
            raw_code = str(row["code"] or "").strip()
            normalized = self._normalize_asset_code(raw_code)
            if not normalized or normalized in cache:
                continue
            cache[normalized] = {
                "trade_date": row["trade_date"],
                "model": row["model"],
                "quality": round(self._as_float(row["quality"]), 1),
                "growth": round(self._as_float(row["growth"]), 1),
                "valuation": round(self._as_float(row["valuation"]), 1),
                "flow": round(self._as_float(row["flow"]), 1),
                "technical": round(self._as_float(row["technical"]), 1),
                "risk": round(self._as_float(row["risk"]), 1),
                "total": round(self._as_float(row["total"]), 1),
            }
        return cache

    def _factor_snapshot_for_asset(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        code = self._normalize_asset_code(asset.get("code"), asset.get("market"))
        if not code:
            return {}
        snapshots = self._factor_snapshots()
        if code in snapshots:
            return snapshots[code]
        bare_code = code.split(".", 1)[0]
        for key, value in snapshots.items():
            if key.split(".", 1)[0] == bare_code:
                return value
        return {}

    def _stock_pool_item(
        self,
        asset: Dict[str, Any],
        role: str,
        card: Dict[str, Any],
        reason: str,
    ) -> Dict[str, Any]:
        normalized = self._asset_for(asset.get("code"), asset.get("name"), asset.get("market"), asset.get("sector") or card.get("sector"))
        normalized["role"] = role
        factor = self._factor_snapshot_for_asset(normalized)
        score = self._as_float(factor.get("total"), self._as_float(card.get("decision_priority_score")))
        risk = factor.get("risk")
        data_quality = "有本地因子快照" if factor else "暂无本地因子快照，先用 Lead-Lag 证据下钻"
        return {
            "code": normalized.get("code", ""),
            "name": normalized.get("name", ""),
            "market": normalized.get("market", ""),
            "market_zh": MARKET_DISPLAY.get(str(normalized.get("market") or ""), normalized.get("market", "")),
            "sector": normalized.get("sector") or card.get("sector"),
            "sector_zh": self._sector_display(normalized.get("sector") or card.get("sector")),
            "role": role,
            "role_zh": ROLE_DISPLAY.get(role, role),
            "reason": reason,
            "opportunity_id": card.get("id"),
            "opportunity_thesis": card.get("thesis"),
            "baton_stage": card.get("baton_stage"),
            "baton_stage_zh": BATON_DISPLAY.get(str(card.get("baton_stage") or ""), card.get("baton_stage")),
            "generation_status": card.get("generation_status"),
            "generation_status_zh": STATUS_DISPLAY.get(str(card.get("generation_status") or ""), card.get("generation_status")),
            "decision_priority_score": card.get("decision_priority_score"),
            "actionability_score": card.get("actionability_score"),
            "tradability_score": card.get("tradability_score"),
            "evidence_completeness": card.get("evidence_completeness"),
            "freshness_score": card.get("freshness_score"),
            "source_count": card.get("source_count"),
            "next_review_time": (card.get("expected_review_times") or [card.get("last_update")])[0],
            "invalidation_rules": card.get("invalidation_rules", []),
            "factor_snapshot": factor,
            "basic_info": {
                "market": MARKET_DISPLAY.get(str(normalized.get("market") or ""), normalized.get("market", "")),
                "sector": self._sector_display(normalized.get("sector") or card.get("sector")),
                "role": ROLE_DISPLAY.get(role, role),
                "data_quality": data_quality,
                "factor_total": score if factor else None,
                "factor_risk": risk,
            },
        }

    def _stock_pool_for_card(self, card: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates: List[tuple[Dict[str, Any], str, str]] = [
            (card.get("leader_asset") or {}, "leader", "领先资产：先反映订单、资本开支、流动性或风险偏好。"),
            (card.get("bridge_asset") or {}, "bridge", "桥接资产：验证海外信号能否跨市场传导。"),
            (card.get("local_asset") or {}, "local_mapping", "本地映射：主要下钻和跟踪对象。"),
        ]
        for asset in card.get("local_proxy_assets") or []:
            if isinstance(asset, dict):
                candidates.append((asset, "proxy", asset.get("reason") or "同赛道代理：用于比较强弱和补涨可能。"))

        seen = set()
        rows: List[Dict[str, Any]] = []
        for asset, role, reason in candidates:
            if not isinstance(asset, dict):
                continue
            code = self._normalize_asset_code(asset.get("code"), asset.get("market"))
            if not code or code in seen:
                continue
            seen.add(code)
            rows.append(self._stock_pool_item(asset, role, card, reason))
        return rows

    def _decision_chain_for_card(self, card: Dict[str, Any]) -> Dict[str, Any]:
        status = STATUS_DISPLAY.get(str(card.get("generation_status") or ""), card.get("generation_status"))
        baton = BATON_DISPLAY.get(str(card.get("baton_stage") or ""), card.get("baton_stage"))
        sector = self._sector_display(card.get("sector"))
        local_asset = card.get("local_asset") or {}
        stock_name = local_asset.get("name") or local_asset.get("code") or "本地映射资产"
        confirmations = compact_strings(card.get("confirmations"))
        missing = compact_strings(card.get("missing_confirmations"))
        invalidations = compact_strings(card.get("invalidation_rules"))
        data_points = [
            {"label": "决策优先级", "value": card.get("decision_priority_score"), "explain": "综合可执行度、可交易性、证据、新鲜度、回放和噪音惩罚。"},
            {"label": "可执行度", "value": card.get("actionability_score"), "explain": "信号强度叠加宏观/外部/港股桥接后的行动价值。"},
            {"label": "可交易性", "value": card.get("tradability_score"), "explain": "本地资产映射、流动性和交易窗口是否足够清晰。"},
            {"label": "证据完整度", "value": card.get("evidence_completeness"), "explain": "独立证据源数量和来源质量是否支撑结论。"},
            {"label": "新鲜度", "value": card.get("freshness_score"), "explain": "催化距离当前越近，新鲜度越高。"},
            {"label": "噪音惩罚", "value": card.get("noise_penalty"), "explain": "拥挤、衰减或重复信号带来的扣分。"},
        ]
        return {
            "result": f"{status}：{sector} - {stock_name}，当前处于{baton}。",
            "thinking": card.get("why_now") or card.get("driver") or "领先变量进入观察窗口，等待本地映射验证。",
            "strategy": (
                "只做验证仓或继续观察；先等缺口补齐。"
                if card.get("generation_status") == "insufficient_evidence"
                else "围绕本地映射和同赛道代理做强弱比较，按下次检查时间复核。"
            ),
            "evidence": confirmations or missing or ["证据链待补充"],
            "data": data_points,
            "invalidations": invalidations,
        }

    def _model_discoveries_for_card(
        self,
        card: Dict[str, Any],
        item: Dict[str, Any],
        mapped_events: List[Dict[str, Any]],
        bridge_impact: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        discoveries: List[Dict[str, Any]] = []

        def add(model_id: str, role: str, strength: Any, conclusion: str, evidence: List[str]) -> None:
            display = self._model_display(model_id)
            discoveries.append(
                {
                    **display,
                    "role": role,
                    "strength": clamp_score(strength),
                    "conclusion": conclusion,
                    "evidence": compact_strings(evidence),
                    "opportunity_id": card.get("id"),
                    "local_asset": card.get("local_asset"),
                }
            )

        actionability = self._as_float(card.get("actionability_score"))
        tradability = self._as_float(card.get("tradability_score"))
        source_count = self._as_int(card.get("source_count"))
        replay_hit = self._as_float((card.get("historical_replay_summary") or {}).get("hit_rate")) * 100.0
        crowding = self._as_float((card.get("crowding_state") or {}).get("score"))

        if card.get("generation_status") == "invalidated":
            add(
                "replay_validation",
                "风险过滤",
                max(replay_hit, 65.0),
                "当前机会已进入失效状态，历史回放和失效条件优先级高于追涨。",
                card.get("invalidation_rules", []),
            )
        if actionability >= 68 or source_count >= 4:
            add(
                "leadership_breadth",
                "主发现",
                actionability,
                "领先资产和本地映射的强度足以进入机会池。",
                compact_strings([card.get("driver"), f"{source_count} 条证据源"]),
            )
        if mapped_events:
            add(
                "event_spillover",
                "催化发现",
                max([self._as_float(event.get("relevance_score")) for event in mapped_events] + [55.0]),
                "已有事件能映射到该赛道或资产，适合按催化跟踪。",
                compact_strings([event.get("title") for event in mapped_events[:3]]),
            )
        if card.get("leader_asset") and card.get("bridge_asset") and card.get("local_asset"):
            add(
                "transmission_graph",
                "路径验证",
                min(100.0, (actionability + tradability) / 2.0),
                "领先资产、桥接资产、本地映射三段链路可用于观察传导是否成立。",
                compact_strings([card.get("decision_chain", {}).get("thinking"), bridge_impact.get("explanation")]),
            )
        if crowding >= 55 or card.get("crowding_state", {}).get("label") in {"high", "crowded"}:
            add(
                "liquidity_dispersion",
                "拥挤提示",
                crowding,
                "资金集中度偏高，需要降低追涨权重并观察回撤后的验证。",
                compact_strings([card.get("crowding_state", {}).get("explanation")]),
            )
        if replay_hit:
            add(
                "replay_validation",
                "历史验证",
                replay_hit,
                "历史回放给出该赛道的胜率和失败模式，用于决定仓位和复核节奏。",
                compact_strings([
                    f"历史命中率 {replay_hit:.0f}%",
                    (card.get("historical_replay_summary") or {}).get("worst_failure_mode"),
                ]),
            )
        if not discoveries:
            add(
                "valuation_gap",
                "观察发现",
                card.get("decision_priority_score", 0),
                "当前主要是估值差或潜在传导观察，尚未形成完整催化。",
                card.get("missing_confirmations", []),
            )

        discoveries.sort(key=lambda row: -self._as_float(row.get("strength")))
        seen = set()
        unique: List[Dict[str, Any]] = []
        for row in discoveries:
            model_id = row.get("model_id")
            if model_id in seen:
                continue
            seen.add(model_id)
            unique.append(row)
        return unique

    def _model_opportunity_groups(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[str, Dict[str, Any]] = {}
        for card in cards:
            for discovery in card.get("model_discoveries") or []:
                model_id = discovery.get("model_id")
                if not model_id:
                    continue
                group = groups.setdefault(
                    model_id,
                    {
                        **self._model_display(model_id),
                        "count": 0,
                        "opportunities": [],
                    },
                )
                group["count"] += 1
                group["opportunities"].append(
                    {
                        "id": card.get("id"),
                        "thesis": card.get("thesis"),
                        "sector": card.get("sector"),
                        "sector_zh": self._sector_display(card.get("sector")),
                        "local_asset": card.get("local_asset"),
                        "baton_stage": card.get("baton_stage"),
                        "baton_stage_zh": BATON_DISPLAY.get(str(card.get("baton_stage") or ""), card.get("baton_stage")),
                        "generation_status": card.get("generation_status"),
                        "generation_status_zh": STATUS_DISPLAY.get(str(card.get("generation_status") or ""), card.get("generation_status")),
                        "decision_priority_score": card.get("decision_priority_score"),
                        "model_strength": discovery.get("strength"),
                        "model_conclusion": discovery.get("conclusion"),
                    }
                )
        ordered = sorted(
            groups.values(),
            key=lambda group: (
                -self._as_int(group.get("count")),
                str(group.get("model_name_zh") or group.get("model_id")),
            ),
        )
        return ordered

    def _evidence_panels_by_url_for_items(self, items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        urls: List[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("source_url"):
                urls.append(str(item.get("source_url")))
            for evidence in item.get("evidence_items") or []:
                if isinstance(evidence, dict) and evidence.get("source_url"):
                    urls.append(str(evidence.get("source_url")))
        urls = list(dict.fromkeys(url for url in urls if url))
        if not urls or EvidenceVaultService is None or not self.db_path.exists():
            return {}
        try:
            vault = EvidenceVaultService(db_path=self.db_path, archive_root=self.repo_root / "data" / "archive")
            panels = vault.evidence_panel_for_urls(urls, limit=20)
        except Exception:
            return {}
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for panel in panels:
            original_link = str(panel.get("original_link") or "")
            if original_link:
                grouped.setdefault(original_link, []).append(panel)
        return grouped

    def _events_relevance_all(self) -> List[Dict[str, Any]]:
        raw_events = self.events_calendar(limit=100).get("events", [])
        relevance = [
            build_event_relevance(
                event,
                asset_resolver=self._asset_for,
                sector_normalizer=self._normalize_sector,
                config=self.v2_config,
            )
            for event in raw_events
            if isinstance(event, dict)
        ]
        relevance.sort(
            key=lambda event: (
                -self._as_float(event.get("relevance_score")),
                str(event.get("effective_time", "")),
                str(event.get("event_id", "")),
            )
        )
        return relevance

    def _mapped_events_for_item(self, item: Dict[str, Any], events: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        symbol = self._normalize_asset_code(item.get("symbol") or item.get("asset_code"), item.get("market"))
        mapped = []
        for event in events or self._events_relevance_all():
            sectors = event.get("sector_mapping") or []
            assets = event.get("asset_mapping") or []
            sector_match = any(self._normalize_sector(row.get("sector")) == sector for row in sectors if isinstance(row, dict))
            asset_match = any(asset.get("code") == symbol for asset in assets if isinstance(asset, dict))
            if sector_match or asset_match:
                mapped.append(
                    {
                        "event_id": event.get("event_id"),
                        "title": event.get("title"),
                        "event_class": event.get("event_class"),
                        "relevance_score": event.get("relevance_score"),
                    }
                )
        return mapped[:3]

    def _mapped_notes_for_item(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        memory = self.obsidian_memory(limit=8)
        rows = memory.get("theme_matches") or memory.get("recent_notes") or []
        mapped = []
        for index, note in enumerate(rows):
            if not isinstance(note, dict):
                continue
            title = str(note.get("title") or "")
            if sector not in title.lower() and not any(alias in title for alias, normalized in SECTOR_ALIASES.items() if normalized == sector):
                continue
            mapped.append(
                {
                    "note_id": note.get("path") or f"memory_{index}",
                    "title": title,
                    "memory_type": "thesis_summary",
                }
            )
        return mapped[:3]

    def macro_bridge(self, **kwargs: Any) -> Dict[str, Any]:
        return build_macro_bridge(self.bundle, self.live_evidence, self.v2_config, **kwargs)

    def get_macro_bridge(self, **kwargs: Any) -> Dict[str, Any]:
        return self.macro_bridge(**kwargs)

    def _opportunity_cards_for_builder(self, sector: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        queue = self.opportunity_queue(limit=limit, sector=sector)
        return [card for card in queue.get("cards", []) if isinstance(card, dict)]

    def transmission_workspace(self, sector: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        cards = self._opportunity_cards_for_builder(sector=sector)
        return build_transmission_workspace(
            self.bundle,
            cards,
            event_relevance=self._events_relevance_all(),
            macro_bridge=self.macro_bridge(),
            sector=sector,
        )

    def get_transmission_workspace(self, **kwargs: Any) -> Dict[str, Any]:
        return self.transmission_workspace(**kwargs)

    def replay_diagnostics(self, sector: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        cards = self._opportunity_cards_for_builder(sector=sector)
        return build_replay_diagnostics(self.bundle, cards, sector=sector)

    def get_replay_diagnostics(self, **kwargs: Any) -> Dict[str, Any]:
        return self.replay_diagnostics(**kwargs)

    def research_memory_actions(self, sector: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        cards = self._opportunity_cards_for_builder(sector=sector)
        return build_research_memory_actions(self.obsidian_memory(limit=20), opportunity_cards=cards, sector=sector)

    def get_research_memory_actions(self, **kwargs: Any) -> Dict[str, Any]:
        return self.research_memory_actions(**kwargs)

    def sector_deep_evidence(self, sector: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        cards = self._opportunity_cards_for_builder(sector=sector)
        return build_sector_deep_evidence(
            self.bundle,
            cards,
            event_relevance=self._events_relevance_all(),
            macro_bridge=self.macro_bridge(),
            sector=sector,
        )

    def get_sector_deep_evidence(self, **kwargs: Any) -> Dict[str, Any]:
        return self.sector_deep_evidence(**kwargs)

    def _bridge_impact_for_item(
        self,
        item: Dict[str, Any],
        assets: Dict[str, Dict[str, Any]],
        bridge: Dict[str, Any],
    ) -> Dict[str, Any]:
        bridge_config = ((self.v2_config.get("scoring") or {}).get("bridge_impact") or {})
        actionability_weight = self._as_float(bridge_config.get("actionability_weight"), 0.08)
        tradability_weight = self._as_float(bridge_config.get("tradability_weight"), 0.10)
        decision_weight = self._as_float(bridge_config.get("decision_weight"), 0.08)
        high_beta_penalty_threshold = self._as_float(bridge_config.get("high_beta_penalty_threshold"), 45.0)
        high_beta_sectors = set(bridge_config.get("high_beta_sectors") or ["ai", "semis", "innovative_pharma", "solar"])

        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        macro_score = self._as_float((bridge.get("macro_regime") or {}).get("score"), 50.0)
        external_score = self._as_float((bridge.get("external_risk") or {}).get("score"), 50.0)
        hk_score = self._as_float((bridge.get("hk_liquidity") or {}).get("score"), 50.0)
        bridge_score = self._as_float(bridge.get("bridge_score"), 50.0)
        local_market = str((assets.get("local_asset") or {}).get("market") or "")
        bridge_market = str((assets.get("bridge_asset") or {}).get("market") or "")

        actionability_delta = (macro_score - 50.0) * actionability_weight
        if sector in high_beta_sectors and external_score < high_beta_penalty_threshold:
            actionability_delta -= (high_beta_penalty_threshold - external_score) * actionability_weight

        tradability_delta = 0.0
        if "HK" in {local_market, bridge_market} or str(item.get("region")) == "HK":
            tradability_delta += (hk_score - 50.0) * tradability_weight
        else:
            tradability_delta += (hk_score - 50.0) * tradability_weight * 0.35
        tradability_delta += (external_score - 50.0) * tradability_weight * 0.25

        decision_delta = (bridge_score - 50.0) * decision_weight
        explanation = (
            f"Bridge {bridge.get('bridge_state')}: macro {macro_score:.1f}, "
            f"external {external_score:.1f}, HK {hk_score:.1f}; "
            f"actionability {actionability_delta:+.1f}, tradability {tradability_delta:+.1f}."
        )
        return {
            "bridge_state": bridge.get("bridge_state"),
            "bridge_score": round(bridge_score, 2),
            "macro_score": round(macro_score, 2),
            "external_score": round(external_score, 2),
            "hk_score": round(hk_score, 2),
            "actionability_delta": round(actionability_delta, 2),
            "tradability_delta": round(tradability_delta, 2),
            "decision_delta": round(decision_delta, 2),
            "explanation": explanation,
            "cache_status": bridge.get("cache_status"),
        }

    def _opportunity_card_v2(self, item: Dict[str, Any], events: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        sector = self._normalize_sector(item.get("sector_key") or item.get("sector"))
        thesis = self._sector_thesis_map().get(sector, {})
        assets = self._select_v2_assets(item)
        source_count = self._as_int(item.get("evidence_count"), len(item.get("evidence_sources") or []))
        signal_strength = clamp_score(self._as_float(item.get("signal_strength")) * 100.0)
        tradability_score = clamp_score(self._as_float(item.get("liquidity_score"), 0.0) * 100.0 + (12.0 if assets["local_asset"].get("code") else 0.0))
        bridge = self.macro_bridge()
        bridge_impact = self._bridge_impact_for_item(item, assets, bridge)
        signal_strength = clamp_score(signal_strength + bridge_impact["actionability_delta"])
        tradability_score = clamp_score(tradability_score + bridge_impact["tradability_delta"])
        evidence_completeness = clamp_score(min(source_count, 6) / 6.0 * 100.0)
        freshness_score = clamp_score(100.0 - min(self._as_int(item.get("days_since_event"), 5), 30) * 3.0)
        historical_replay = self._replay_summary_for_sector(sector)
        historical_replay_score = clamp_score(self._as_float(historical_replay.get("hit_rate")) * 100.0)
        crowding_score = clamp_score(self._as_float(item.get("crowding")) * 100.0)
        noise_penalty = clamp_score(crowding_score * 0.35 + self._as_float(item.get("decay_rate")) * 100.0 * 0.30)
        score_inputs = {
            "actionability_score": signal_strength,
            "tradability_score": tradability_score,
            "evidence_completeness": evidence_completeness,
            "freshness_score": freshness_score,
            "historical_replay_score": historical_replay_score,
            "noise_penalty": noise_penalty,
            "bridge_adjustment_score": bridge_impact["bridge_score"],
        }
        decision_score = clamp_score(opportunity_decision_score(score_inputs, self.v2_config) + bridge_impact["decision_delta"])
        thresholds = self.v2_config.get("thresholds") or {}
        actionable_min = self._as_float(thresholds.get("opportunity_actionable_min"), 70.0)
        watch_min = self._as_float(thresholds.get("opportunity_watch_min"), 45.0)
        stage = str(item.get("stage") or self._candidate_stage(item))
        if stage == "invalidated":
            generation_status = "invalidated"
        elif decision_score >= actionable_min and source_count >= 2:
            generation_status = "actionable"
        elif source_count < 2 or evidence_completeness < 45:
            generation_status = "insufficient_evidence"
        elif decision_score >= watch_min:
            generation_status = "watch_only"
        else:
            generation_status = "insufficient_evidence"

        confirmations = compact_strings(
            [
                item.get("validation_signal"),
                item.get("confirmation"),
                f"{source_count} 条证据支持" if source_count >= 2 else "",
                f"历史命中率 {historical_replay.get('hit_rate'):.0%}" if self._as_float(historical_replay.get("hit_rate")) else "",
            ]
        )
        missing_confirmations = []
        if source_count < 2:
            missing_confirmations.append("缺少至少两条独立证据确认")
        if not assets["local_asset"].get("code"):
            missing_confirmations.append("缺少本地可交易映射资产")
        if evidence_completeness < 45:
            missing_confirmations.append("证据完整度不足，需补充事件或研报确认")

        last_update = item.get("updated_at") or self.bundle.get("as_of") or datetime.now().replace(microsecond=0).isoformat()
        mapped_events = self._mapped_events_for_item(item, events)
        card = {
            "id": item.get("opportunity_id") or item.get("asset_code") or item.get("symbol"),
            "generation_status": generation_status,
            "thesis": item.get("thesis") or thesis.get("title") or thesis.get("thesis") or item.get("notes") or item.get("name"),
            "region": "CN" if str(item.get("market")) in {"A", "ETF"} else str(item.get("market") or "CrossMarket"),
            "sector": sector,
            "model_family": item.get("model_family") or ("event_spillover" if mapped_events else "leadership_breadth"),
            "leader_asset": assets["leader_asset"],
            "bridge_asset": assets["bridge_asset"],
            "local_asset": assets["local_asset"],
            "local_proxy_assets": [
                {
                    "code": asset.get("code"),
                    "name": asset.get("name"),
                    "market": asset.get("market"),
                    "reason": "同赛道本地代理",
                }
                for asset in self._sector_assets(sector)
                if str(asset.get("market")) in {"A", "HK", "ETF"} and asset.get("code") != assets["local_asset"].get("code")
            ][:4],
            "baton_stage": self._baton_stage_v2(stage, item.get("baton")),
            "why_now": item.get("why_now") or thesis.get("title") or item.get("notes") or thesis.get("notes") or "现有领先变量进入可验证窗口。",
            "driver": item.get("driver") or thesis.get("title") or item.get("name") or f"{sector} lead-lag driver",
            "confirmations": confirmations,
            "missing_confirmations": missing_confirmations,
            "missing_evidence_reason": "; ".join(missing_confirmations),
            "risk": item.get("risk") or item.get("invalidation_rule") or "; ".join(thesis.get("risks") or thesis.get("bear_case") or ["信号拥挤或验证失败"]),
            "invalidation_rules": compact_strings([item.get("invalidation_rule")] + list(thesis.get("bear_case") or [])) or ["领先资产失效或本地映射无跟随"],
            "expected_lag_days": self._lag_window_for(item),
            "expected_review_times": self._expected_review_times(last_update),
            "crowding_state": {
                "label": self._state_label(crowding_score, ("low", "medium", "high", "crowded")),
                "score": crowding_score,
                "explanation": f"crowding={crowding_score:.1f}",
            },
            "liquidity_state": {
                "label": self._state_label(tradability_score, ("poor", "acceptable", "good", "excellent")),
                "score": tradability_score,
                "explanation": "由流动性分数和本地映射完整度合成",
            },
            "actionability_score": signal_strength,
            "tradability_score": tradability_score,
            "evidence_completeness": evidence_completeness,
            "freshness_score": freshness_score,
            "noise_penalty": noise_penalty,
            "decision_priority_score": decision_score,
            "bridge_impact": bridge_impact,
            "historical_replay_summary": historical_replay,
            "mapped_events": mapped_events,
            "mapped_notes": self._mapped_notes_for_item(item),
            "raw_evidence_items": item.get("evidence_items") or [],
            "source_urls": compact_strings(
                [item.get("source_url")]
                + [
                    evidence.get("source_url")
                    for evidence in item.get("evidence_items", [])
                    if isinstance(evidence, dict)
                ]
            ),
            "source_count": source_count,
            "source_quality": self._source_quality_v2(item),
            "confidence": clamp_score(self._as_float(item.get("replay_hit_rate"), 0.5) * 100.0),
            "cache_status": "live" if str(item.get("opportunity_id", "")).startswith("live_") else "sample_fallback",
            "last_update": last_update,
        }
        card = apply_generation_status(card)
        card["sector_zh"] = self._sector_display(card.get("sector"))
        card["baton_stage_zh"] = BATON_DISPLAY.get(str(card.get("baton_stage") or ""), card.get("baton_stage"))
        card["generation_status_zh"] = STATUS_DISPLAY.get(str(card.get("generation_status") or ""), card.get("generation_status"))
        card["decision_chain"] = self._decision_chain_for_card(card)
        card["stock_pool"] = self._stock_pool_for_card(card)
        discoveries = self._model_discoveries_for_card(card, item, mapped_events, bridge_impact)
        if discoveries:
            primary = discoveries[0]
            card["model_family"] = primary.get("model_id")
            card["model_name_zh"] = primary.get("model_name_zh")
            card["model_explain_zh"] = primary.get("model_explain_zh")
        else:
            display = self._model_display(card.get("model_family"))
            card.update(display)
        card["model_discoveries"] = discoveries
        return card

    def opportunity_queue(
        self,
        limit: int = 12,
        region: Optional[str] = None,
        sector: Optional[str] = None,
        family: Optional[str] = None,
        min_tradability: Optional[float] = None,
        include_sample: bool = False,
        live_only: bool = False,
        archived_only: bool = False,
        **_: Any,
    ) -> Dict[str, Any]:
        events = self._events_relevance_all()
        raw_items = self._enrich_opportunities()
        evidence_panels_by_url = self._evidence_panels_by_url_for_items(raw_items)
        projector = LeadLagV3Projector(evidence_panels_by_url=evidence_panels_by_url)
        cards = [
            projector.enrich_card(self._opportunity_card_v2(item, events), item)
            for item in raw_items
        ]
        if region and str(region).lower() not in {"all", ""}:
            cards = [card for card in cards if str(card.get("region", "")).lower() == str(region).lower()]
        if sector and str(sector).lower() not in {"all", ""}:
            normalized_sector = self._normalize_sector(sector)
            cards = [card for card in cards if self._normalize_sector(card.get("sector")) == normalized_sector]
        if family and str(family).lower() not in {"all", ""}:
            normalized_family = str(family).strip()
            cards = [
                card for card in cards
                if normalized_family in set(card.get("opportunity_families") or [card.get("opportunity_family")])
            ]
        if min_tradability is not None:
            cards = [card for card in cards if self._as_float(card.get("tradability_score")) >= self._as_float(min_tradability)]
        if archived_only:
            cards = [card for card in cards if self._as_int(card.get("archived_link_count")) > 0]
        cards.sort(key=lambda card: (-self._as_float(card.get("decision_priority_score")), str(card.get("id"))))
        limit_value = max(1, min(self._as_int(limit, 12), 50))
        blocked_cards = [card for card in cards if card.get("data_source_class") in BLOCKED_SOURCE_CLASSES]
        governed_cards = cards if include_sample else [
            card for card in cards if card.get("data_source_class") not in BLOCKED_SOURCE_CLASSES
        ]
        if live_only:
            governed_cards = [card for card in governed_cards if self._as_int(card.get("live_source_count")) > 0]
        visible = governed_cards[:limit_value]
        parent_cards = projector.parent_thesis_cards(visible, limit=limit_value) if visible else []
        return {
            "as_of": datetime.now().replace(microsecond=0).isoformat(),
            "count": len(governed_cards),
            "raw_count": len(cards),
            "cards": visible,
            "items": visible,
            "parent_thesis_cards": parent_cards,
            "blocked_cards": blocked_cards[:limit_value],
            "sample_cards": blocked_cards[:limit_value],
            "model_groups": self._model_opportunity_groups(visible),
            "source_quality_lineage": projector.quality_lineage_summary(cards, events=events),
            "filters": {
                "include_sample": include_sample,
                "live_only": live_only,
                "archived_only": archived_only,
                "family": family or "all",
                "sector": sector or "all",
                "region": region or "all",
            },
            "source": "lead_lag_v2",
            "scoring_config": self.v2_config.get("scoring", {}),
        }

    def get_opportunity_queue(self, **kwargs: Any) -> Dict[str, Any]:
        return self.opportunity_queue(**kwargs)

    def event_frontline(
        self,
        limit: int = 20,
        event_class: str = "market-facing",
        include_research_facing: bool = False,
        include_sample: bool = False,
        sector: Optional[str] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        events = self._events_relevance_all()
        if sector and str(sector).lower() not in {"all", ""}:
            normalized_sector = self._normalize_sector(sector)
            events = [
                event
                for event in events
                if any(self._normalize_sector(row.get("sector")) == normalized_sector for row in event.get("sector_mapping", []))
            ]
        projector = LeadLagV3Projector()
        events = [projector.enhance_event(event) for event in events]
        if not include_sample:
            events = [event for event in events if event.get("data_source_class") not in BLOCKED_SOURCE_CLASSES]
        events = filter_event_relevance(events, event_class=event_class, include_research_facing=include_research_facing)
        limit_value = max(1, min(self._as_int(limit, 20), 100))
        visible = events[:limit_value]
        return {
            "as_of": datetime.now().replace(microsecond=0).isoformat(),
            "count": len(events),
            "events": visible,
            "items": visible,
            "default_filter": "market-facing" if not include_research_facing else "all",
            "include_sample": include_sample,
            "source": "lead_lag_v2",
        }

    def get_event_frontline(self, **kwargs: Any) -> Dict[str, Any]:
        return self.event_frontline(**kwargs)

    def source_quality_lineage(self, limit: int = 20, **_: Any) -> Dict[str, Any]:
        queue = self.opportunity_queue(limit=max(limit, 20), include_sample=True)
        vault_summary: Dict[str, Any] = {}
        if EvidenceVaultService is not None and self.db_path.exists():
            try:
                vault_summary = EvidenceVaultService(
                    db_path=self.db_path,
                    archive_root=self.repo_root / "data" / "archive",
                ).source_quality_summary()
            except Exception as exc:
                vault_summary = {"error": str(exc)}
        return {
            "as_of": datetime.now().replace(microsecond=0).isoformat(),
            "lineage": queue.get("source_quality_lineage", {}),
            "evidence_vault": vault_summary,
            "filters": {
                "sample_hidden_by_default": True,
                "executable_requires_live_source": True,
                "generated_inference_requires_confirmation": True,
            },
        }

    def get_source_quality_lineage(self, **kwargs: Any) -> Dict[str, Any]:
        return self.source_quality_lineage(**kwargs)

    def report_center(self, q: Optional[str] = None, limit: int = 20, **_: Any) -> Dict[str, Any]:
        if EvidenceVaultService is None:
            return {"as_of": datetime.now().replace(microsecond=0).isoformat(), "reports": [], "count": 0, "status": "unavailable"}
        vault = EvidenceVaultService(db_path=self.db_path, archive_root=self.repo_root / "data" / "archive")
        try:
            vault.ensure_schema()
            if q:
                reports = vault.search_reports(q, limit=limit)
            else:
                with vault.connect() as conn:
                    rows = conn.execute(
                        """
                        SELECT report_id, report_type, title, local_path, generated_at, as_of_date
                        FROM reports
                        ORDER BY generated_at DESC
                        LIMIT ?
                        """,
                        (max(1, min(self._as_int(limit, 20), 100)),),
                    ).fetchall()
                    reports = [dict(row) for row in rows]
            return {
                "as_of": datetime.now().replace(microsecond=0).isoformat(),
                "count": len(reports),
                "reports": reports,
                "query": q or "",
                "full_text_search": True,
                "export_target": "Obsidian 独立目录由报告生成器写入，DB 保存 local_path 与引用关系。",
            }
        except Exception as exc:
            return {
                "as_of": datetime.now().replace(microsecond=0).isoformat(),
                "count": 0,
                "reports": [],
                "query": q or "",
                "status": "error",
                "error": str(exc),
            }

    def get_report_center(self, **kwargs: Any) -> Dict[str, Any]:
        return self.report_center(**kwargs)

    def opportunity_universe_registry(self, **_: Any) -> Dict[str, Any]:
        readonly = self._opportunity_universe_summary_readonly()
        if readonly.get("counts", {}).get("sector_registry"):
            return readonly
        if OpportunityUniverseRegistry is None:
            return {"registry_version": None, "counts": {}, "sectors": [], "status": "unavailable"}
        try:
            registry = OpportunityUniverseRegistry(db_path=self.db_path)
            registry.seed_defaults()
            return self._opportunity_universe_summary_readonly() or registry.registry_summary()
        except Exception as exc:
            return {"registry_version": None, "counts": {}, "sectors": [], "status": "error", "error": str(exc)}

    def get_opportunity_universe_registry(self, **kwargs: Any) -> Dict[str, Any]:
        return self.opportunity_universe_registry(**kwargs)

    def _opportunity_universe_summary_readonly(self) -> Dict[str, Any]:
        if not self.db_path.exists():
            return {"registry_version": "v3.0.0", "counts": {}, "sectors": [], "status": "missing_db"}
        tables = [
            "sector_registry",
            "theme_registry",
            "entity_registry",
            "instrument_registry",
            "mapping_registry",
            "model_registry",
            "thesis_registry",
            "event_template_registry",
        ]
        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                existing = {
                    row["name"]
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                if not set(tables).issubset(existing):
                    return {"registry_version": "v3.0.0", "counts": {}, "sectors": [], "status": "not_initialized"}
                counts = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tables}
                sectors = [
                    dict(row)
                    for row in conn.execute("SELECT sector_id, name_zh, enabled FROM sector_registry ORDER BY sector_id").fetchall()
                ]
            return {"registry_version": "v3.0.0", "counts": counts, "sectors": sectors, "status": "ready"}
        except Exception as exc:
            return {"registry_version": "v3.0.0", "counts": {}, "sectors": [], "status": "error", "error": str(exc)}

    def _registry_table_row(self, table: str, key_field: str, key_value: str) -> Dict[str, Any]:
        if not self.db_path.exists():
            return {}
        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                exists = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()[0]
                if not exists:
                    return {}
                row = conn.execute(f"SELECT * FROM {table} WHERE {key_field} = ?", (key_value,)).fetchone()
                return dict(row) if row else {}
        except Exception:
            return {}

    def _registry_table_rows(self, table: str, where: str = "", params: tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        if not self.db_path.exists():
            return []
        try:
            with sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                exists = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()[0]
                if not exists:
                    return []
                sql = f"SELECT * FROM {table} {where}"
                return [dict(row) for row in conn.execute(sql, params).fetchall()]
        except Exception:
            return []

    def sector_dossier(self, sector_id: str, limit: int = 10, **_: Any) -> Dict[str, Any]:
        registry_payload = self.opportunity_universe_registry()
        sector = self._registry_table_row("sector_registry", "sector_id", sector_id)
        if sector:
            for key in (
                "lead_assets",
                "bridge_assets",
                "local_assets",
                "proxy_assets",
                "upstream_nodes",
                "downstream_nodes",
                "key_metrics",
                "key_events",
                "default_invalidation_rules",
                "replay_horizons",
                "source_requirements",
            ):
                try:
                    sector[key] = json.loads(sector.get(key) or "[]")
                except Exception:
                    sector[key] = []
        queue = self.opportunity_queue(limit=max(limit, 10), sector=sector_id, include_sample=True)
        reports = self.report_center(q=sector.get("name_zh") or sector_id, limit=limit)
        return {
            "sector_id": sector_id,
            "sector": sector,
            "theses": queue.get("parent_thesis_cards", []),
            "key_evidence_checklist": sector.get("source_requirements", []),
            "chain_map": {
                "upstream_nodes": sector.get("upstream_nodes", []),
                "downstream_nodes": sector.get("downstream_nodes", []),
                "lead_assets": sector.get("lead_assets", []),
                "local_assets": sector.get("local_assets", []),
            },
            "current_opportunities": queue.get("cards", []),
            "related_reports": reports.get("reports", []),
            "registry_counts": registry_payload.get("counts", {}),
        }

    def entity_dossier(self, entity_id: str, limit: int = 10, **_: Any) -> Dict[str, Any]:
        if not self._opportunity_universe_summary_readonly().get("counts", {}).get("entity_registry"):
            self.opportunity_universe_registry()
        entity = self._registry_table_row("entity_registry", "entity_id", entity_id)
        instruments = self._registry_table_rows(
            "instrument_registry",
            "WHERE entity_id = ? ORDER BY market, ticker",
            (entity_id,),
        )
        query = entity.get("name_zh") or entity_id
        reports = self.report_center(q=query, limit=limit)
        evidence = []
        if EvidenceVaultService is not None and self.db_path.exists():
            try:
                evidence = EvidenceVaultService(
                    db_path=self.db_path,
                    archive_root=self.repo_root / "data" / "archive",
                ).search_documents(query, limit=limit)
            except Exception:
                evidence = []
        return {
            "entity_id": entity_id,
            "entity": entity,
            "instruments": instruments,
            "recent_event_timeline": self.event_frontline(limit=limit, include_sample=True, include_research_facing=True).get("events", []),
            "related_reports": reports.get("reports", []),
            "related_evidence": evidence,
            "peer_comparison": [],
            "transmission_position": entity.get("sector_ids"),
            "historical_triggers": [],
        }

    def instrument_dossier(self, instrument_id: str, limit: int = 10, **_: Any) -> Dict[str, Any]:
        if not self._opportunity_universe_summary_readonly().get("counts", {}).get("instrument_registry"):
            self.opportunity_universe_registry()
        instrument_rows = self._registry_table_rows(
            "instrument_registry",
            "WHERE instrument_id = ? OR ticker = ? ORDER BY instrument_id LIMIT 1",
            (instrument_id, instrument_id),
        )
        instrument = instrument_rows[0] if instrument_rows else {}
        entity = self._registry_table_row("entity_registry", "entity_id", str(instrument.get("entity_id") or ""))
        ticker = instrument.get("ticker") or instrument_id
        queue = self.opportunity_queue(limit=50, include_sample=True)
        related_cards = []
        for card in queue.get("cards", []):
            if any(asset.get("code") == ticker for asset in card.get("stock_pool", []) if isinstance(asset, dict)):
                related_cards.append(card)
        evidence = []
        if EvidenceVaultService is not None and self.db_path.exists():
            try:
                evidence = EvidenceVaultService(
                    db_path=self.db_path,
                    archive_root=self.repo_root / "data" / "archive",
                ).search_documents(ticker, limit=limit)
            except Exception:
                evidence = []
        return {
            "instrument_id": instrument_id,
            "instrument": instrument,
            "entity": entity,
            "market_liquidity": instrument.get("liquidity_bucket"),
            "factor_snapshot": self._factor_snapshot_for_asset({"code": ticker, "market": instrument.get("market")}),
            "related_opportunity_cards": related_cards[:limit],
            "related_evidence": evidence,
            "related_events": self.event_frontline(limit=limit, include_sample=True, include_research_facing=True).get("events", []),
            "local_archive_links": [item.get("local_archive_path") for item in evidence if item.get("local_archive_path")],
        }

    def _risk_budget_from_bridge(self, actionable: List[Dict[str, Any]], bridge: Dict[str, Any]) -> Dict[str, str]:
        bridge_score = self._as_float(bridge.get("bridge_score"), 50.0)
        external_score = self._as_float((bridge.get("external_risk") or {}).get("score"), 50.0)
        hk_score = self._as_float((bridge.get("hk_liquidity") or {}).get("score"), 50.0)
        bridge_state = str(bridge.get("bridge_state") or "risk_neutral")
        if bridge_score < 42 or external_score < 40:
            return {
                "label": "no_new_risk",
                "reason": f"Macro/External/HK bridge 收紧，external={external_score:.1f}, HK={hk_score:.1f}",
            }
        if not actionable:
            return {
                "label": "no_new_risk",
                "reason": f"无 actionable 机会；bridge={bridge_state}, score={bridge_score:.1f}",
            }
        if bridge_score < 55 or hk_score < 45:
            return {
                "label": "conservative",
                "reason": f"Bridge 只允许小仓位验证，bridge={bridge_state}, score={bridge_score:.1f}",
            }
        if bridge_score >= 70 and len(actionable) >= 2:
            return {
                "label": "aggressive",
                "reason": f"Bridge 支持加风险预算，macro/external/HK 综合分 {bridge_score:.1f}",
            }
        return {
            "label": "balanced",
            "reason": f"存在可执行机会，bridge={bridge_state}, score={bridge_score:.1f}",
        }

    def decision_center(self, limit: int = 5, **kwargs: Any) -> Dict[str, Any]:
        queue = self.opportunity_queue(limit=max(limit, 8), **kwargs)
        cards = queue.get("cards", [])
        actionable = [card for card in cards if card.get("generation_status") == "actionable"]
        top_cards = actionable or cards
        headline = "领先-传导决策中心"
        main = top_cards[0].get("thesis") if top_cards else "当前缺少可执行机会，先补证据。"
        bridge = self.macro_bridge()
        risk_budget = self._risk_budget_from_bridge(actionable, bridge)
        return {
            "as_of": queue.get("as_of"),
            "headline": headline,
            "main_conclusion": main,
            "do_not_do_today": [
                card.get("thesis")
                for card in cards
                if card.get("generation_status") in {"insufficient_evidence", "invalidated"}
            ][:3],
            "top_directions": [
                {
                    "rank": index + 1,
                    "sector": card.get("sector_zh") or card.get("sector"),
                    "thesis": card.get("thesis"),
                    "reason": card.get("why_now"),
                    "opportunity_id": card.get("id"),
                }
                for index, card in enumerate(top_cards[:limit])
            ],
            "baton_summary": {
                "first_baton": [{"opportunity_id": card.get("id")} for card in cards if card.get("baton_stage") == "first_baton"],
                "second_baton": [{"opportunity_id": card.get("id")} for card in cards if card.get("baton_stage") == "second_baton"],
                "pre_trigger": [{"opportunity_id": card.get("id")} for card in cards if card.get("baton_stage") == "pre_trigger"],
            },
            "risk_budget": {
                "label": risk_budget["label"],
                "reason": risk_budget["reason"],
            },
            "macro_bridge_summary": bridge.get("decision_impact"),
            "key_invalidations": [
                rule
                for card in cards[:limit]
                for rule in card.get("invalidation_rules", [])[:1]
            ],
            "next_check_time": (cards[0].get("expected_review_times") or [datetime.now().replace(microsecond=0).isoformat()])[0] if cards else datetime.now().replace(microsecond=0).isoformat(),
            "source_count": sum(self._as_int(card.get("source_count")) for card in cards),
            "cache_status": "live" if any(card.get("cache_status") == "live" for card in cards) else "sample_fallback",
        }

    def get_decision_center(self, **kwargs: Any) -> Dict[str, Any]:
        return self.decision_center(**kwargs)

    def avoid_board(self, limit: int = 8, **kwargs: Any) -> Dict[str, Any]:
        cards = self.opportunity_queue(limit=50, **kwargs).get("cards", [])
        avoid_cards = [
            card for card in cards
            if card.get("generation_status") in {"insufficient_evidence", "invalidated"}
            or card.get("crowding_state", {}).get("label") == "crowded"
        ]
        items = []
        for card in avoid_cards[: max(1, min(self._as_int(limit, 8), 50))]:
            if card.get("generation_status") == "invalidated":
                reason_type = "invalidation_triggered"
            elif card.get("crowding_state", {}).get("label") == "crowded":
                reason_type = "crowded"
            elif card.get("liquidity_state", {}).get("label") == "poor":
                reason_type = "liquidity_mismatch"
            else:
                reason_type = "incomplete_evidence"
            items.append(
                {
                    "id": f"avoid_{card.get('id')}",
                    "thesis": card.get("thesis"),
                    "reason_type": reason_type,
                    "reason": card.get("missing_evidence_reason") or card.get("risk"),
                    "evidence": card.get("missing_confirmations") or card.get("invalidation_rules"),
                    "related_assets": [card.get("local_asset"), card.get("leader_asset")],
                    "next_review_time": (card.get("expected_review_times") or [card.get("last_update")])[0],
                    "source_count": card.get("source_count"),
                    "last_update": card.get("last_update"),
                }
            )
        return {"as_of": datetime.now().replace(microsecond=0).isoformat(), "count": len(avoid_cards), "items": items}

    def get_avoid_board(self, **kwargs: Any) -> Dict[str, Any]:
        return self.avoid_board(**kwargs)

    def what_changed(self, limit: int = 8, **kwargs: Any) -> Dict[str, Any]:
        queue = self.opportunity_queue(limit=limit, **kwargs)
        frontline = self.event_frontline(limit=limit)
        cards = queue.get("cards", [])
        events = frontline.get("events", [])
        return {
            "as_of": queue.get("as_of"),
            "since": self.bundle.get("as_of") or queue.get("as_of"),
            "new_signals": [event.get("title") for event in events[:5]],
            "upgraded_opportunities": [
                {"opportunity_id": card.get("id"), "reason": card.get("why_now")}
                for card in cards
                if card.get("generation_status") == "actionable"
            ][:5],
            "downgraded_or_invalidated": [
                {"opportunity_id": card.get("id"), "reason": card.get("missing_evidence_reason") or card.get("risk")}
                for card in cards
                if card.get("generation_status") in {"insufficient_evidence", "invalidated"}
            ][:5],
            "crowding_up": [
                {"thesis": card.get("thesis"), "reason": card.get("crowding_state", {}).get("explanation")}
                for card in cards
                if card.get("crowding_state", {}).get("label") in {"high", "crowded"}
            ][:5],
            "macro_external_policy_changes": [
                event.get("title") for event in events if event.get("event_type") in {"macro", "policy", "liquidity"}
            ][:5],
        }

    def get_what_changed(self, **kwargs: Any) -> Dict[str, Any]:
        return self.what_changed(**kwargs)

    def models(self, limit: int = 10) -> Dict[str, Any]:
        families = []
        for raw in self.bundle.get("model_families", []):
            if not isinstance(raw, dict):
                continue
            families.append(self._enrich_model_family(raw))
        families.sort(key=self._rank_key)
        families = families[: max(1, min(self._as_int(limit, 10), 50))]
        return {
            "version": self.bundle.get("version", "v1"),
            "count": len(self.bundle.get("model_families", [])),
            "families": families,
            "top_families": [item["family_id"] for item in families[:3]],
        }

    def opportunities(self, limit: int = 12) -> Dict[str, Any]:
        items = self._enrich_opportunities()
        buckets = self._split_batons(items)
        limit_value = max(1, min(self._as_int(limit, 12), 50))
        visible = items[:limit_value]
        return {
            "count": len(items),
            "stage_counts": self._section_summary(items, "stage"),
            "all": visible,
            "baton_buckets": {
                "first_baton": buckets["first_baton"],
                "second_baton": buckets["second_baton"],
                "next_baton": buckets["next_baton"],
                "invalidated": buckets["invalidated"],
            },
        }

    def cross_market_map(self) -> Dict[str, Any]:
        graph = self._normalized_graph()
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        market_counts: Dict[str, int] = {}
        relation_counts: Dict[str, int] = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            market = str(node.get("market", "unknown"))
            market_counts[market] = market_counts.get(market, 0) + 1
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            relation = str(edge.get("relation", "unknown"))
            relation_counts[relation] = relation_counts.get(relation, 0) + 1
        return {
            "markets": market_counts,
            "relation_counts": relation_counts,
            "nodes": nodes,
            "edges": sorted(
                [edge for edge in edges if isinstance(edge, dict)],
                key=lambda edge: (-self._as_float(edge.get("strength")), str(edge.get("source", "")), str(edge.get("target", ""))),
            ),
        }

    def industry_transmission(self) -> Dict[str, Any]:
        graph = self._normalized_graph()
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        sector_paths: Dict[str, List[Dict[str, Any]]] = {}
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            sector = str(edge.get("sector", "unknown"))
            sector_paths.setdefault(sector, []).append(edge)
        return {
            "nodes": nodes,
            "edges": sorted(
                [edge for edge in edges if isinstance(edge, dict)],
                key=lambda edge: (-self._as_float(edge.get("strength")), str(edge.get("source", "")), str(edge.get("target", ""))),
            ),
            "sector_paths": {
                sector: sorted(
                    items,
                    key=lambda edge: (-self._as_float(edge.get("strength")), str(edge.get("source", "")), str(edge.get("target", ""))),
                )
                for sector, items in sector_paths.items()
            },
        }

    def liquidity(self) -> Dict[str, Any]:
        liquidity = self.bundle.get("liquidity", {})
        opportunities = self._enrich_opportunities()
        markets = liquidity.get("markets", []) if isinstance(liquidity, dict) else []
        sectors = liquidity.get("sectors", []) if isinstance(liquidity, dict) else []
        crowding = liquidity.get("crowding", []) if isinstance(liquidity, dict) else []
        live_liquidity = self.live_evidence.get("radar", {}).get("liquidity", {})
        if live_liquidity:
            markets = list(markets) + [
                {
                    "name": "External Risk",
                    "summary": f"Radar external risk score {live_liquidity.get('external_risk_score')}",
                    "tone": "positive" if self._as_float(live_liquidity.get("external_risk_score"), 50) >= 55 else "neutral",
                    "signal": live_liquidity.get("macro_regime") or "live",
                    "value": live_liquidity.get("external_risk_score"),
                    "source": "radar_snapshot",
                    "updated_at": live_liquidity.get("last_data_sync"),
                },
                {
                    "name": "HK Liquidity",
                    "summary": f"Radar HK liquidity score {live_liquidity.get('hk_liquidity_score')}",
                    "tone": "positive" if self._as_float(live_liquidity.get("hk_liquidity_score"), 50) >= 55 else "neutral",
                    "signal": "live",
                    "value": live_liquidity.get("hk_liquidity_score"),
                    "source": "radar_snapshot",
                    "updated_at": live_liquidity.get("last_data_sync"),
                },
            ]
        return {
            "markets": markets,
            "sectors": sectors,
            "crowding": crowding,
            "watchlists": self.bundle.get("watchlists", {}),
            "source_health": self.bundle.get("source_health", {}),
            "live_source_health": {
                "radar": self.live_evidence.get("radar", {}).get("source_health", {}),
                "intelligence": self.live_evidence.get("intelligence", {}).get("source_health", {}),
                "research": self.live_evidence.get("research", {}).get("source_health", {}),
            },
            "opportunity_liquidity": [
                {
                    "opportunity_id": item.get("opportunity_id"),
                    "symbol": item.get("symbol"),
                    "asset_code": item.get("asset_code"),
                    "asset_name": item.get("asset_name"),
                    "sector": item.get("sector"),
                    "score": item.get("score"),
                    "liquidity_score": item.get("liquidity_score"),
                    "crowding": item.get("crowding"),
                }
                for item in opportunities
            ],
        }

    def sector_thesis(self) -> Dict[str, Any]:
        cards = []
        for raw in self.bundle.get("sector_theses", []):
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            item["score"] = round(
                self._as_float(item.get("signal_strength", item.get("score", 0.0))) * 0.7
                + self._as_float(item.get("confidence", 0.0)) * 0.3,
                2,
            )
            cards.append(item)
        existing = {self._normalize_sector(item.get("sector_key")) for item in cards if isinstance(item, dict)}
        for card in self.live_evidence.get("radar", {}).get("cards", []):
            if not isinstance(card, dict):
                continue
            sector = self._normalize_sector(card.get("sector_key"))
            if sector in existing:
                for item in cards:
                    if self._normalize_sector(item.get("sector_key")) == sector:
                        item["live_score"] = round(self._as_float(card.get("score"), 0.0) * 100, 1)
                        item["live_confidence"] = card.get("confidence")
                        item["live_watchlist"] = card.get("watchlist", [])
                        item["live_updated_at"] = card.get("updated_at")
                        item.setdefault("evidence_sources", []).append("radar_snapshot")
                        break
                continue
            cards.append(
                {
                    "sector_key": sector,
                    "sector_name": card.get("sector_name") or sector,
                    "title": f"{card.get('sector_name') or sector} live thesis",
                    "thesis": "Live Radar sector card generated from free local snapshot data.",
                    "signal_strength": card.get("score"),
                    "confidence": card.get("confidence"),
                    "stage_hint": "validating" if self._as_float(card.get("score"), 0.0) >= 0.55 else "pre_trigger",
                    "baton_map": {"next_baton": [asset.get("code") for asset in card.get("watchlist", [])[:5] if isinstance(asset, dict)]},
                    "anchors": [asset.get("code") for asset in card.get("watchlist", [])[:3] if isinstance(asset, dict)],
                    "bull_case": card.get("leading_variables", []),
                    "bear_case": card.get("invalidation_rules", []),
                    "risks": [],
                    "notes": "Live Radar snapshot evidence.",
                    "live_watchlist": card.get("watchlist", []),
                    "live_updated_at": card.get("updated_at"),
                    "evidence_sources": ["radar_snapshot"],
                    "score": round(self._as_float(card.get("score"), 0.0) * 0.7 + self._as_float(card.get("confidence"), 0.0) * 0.3, 2),
                }
            )
        cards.sort(key=self._rank_key)
        return {
            "count": len(cards),
            "cards": cards,
            "coverage": self._section_summary(cards, "sector_key"),
        }

    def events_calendar(self, limit: int = 20) -> Dict[str, Any]:
        events = [dict(item) for item in self.bundle.get("events", []) if isinstance(item, dict)]
        for event in self.live_evidence.get("intelligence", {}).get("events", []):
            if not isinstance(event, dict):
                continue
            assets = event.get("assets") or []
            events.append(
                {
                    "event_id": event.get("event_key"),
                    "event_date": event.get("event_time") or event.get("last_seen_at") or event.get("first_seen_at"),
                    "title": event.get("title_zh") or event.get("title"),
                    "sector_key": self._normalize_sector(event.get("sector_key") or event.get("category")),
                    "market": assets[0].get("market") if assets else "",
                    "importance": event.get("priority"),
                    "stage": "triggered" if event.get("priority") in {"P0", "P1"} else "pre_trigger",
                    "priority_score": event.get("impact_score") or round(self._as_float(event.get("confidence"), 0.0) * 100, 1),
                    "related_symbols": [asset.get("code") for asset in assets if asset.get("code")],
                    "related_assets": assets,
                    "source_url": event.get("primary_source_url"),
                    "source_count": event.get("source_count"),
                    "confidence": event.get("confidence"),
                    "source_surface": "intelligence",
                }
            )
        events.sort(
            key=lambda item: (
                str(item.get("event_date", "")),
                -self._as_float(item.get("priority_score", item.get("score", 0.0))),
                str(item.get("event_id", "")),
            )
        )
        limit_value = max(1, min(self._as_int(limit, 20), 100))
        return {
            "count": len(events),
            "events": events[:limit_value],
            "by_sector": self._section_summary(events, "sector_key"),
            "by_stage": self._section_summary(events, "stage"),
        }

    def replay_validation(self) -> Dict[str, Any]:
        stats = [dict(item) for item in self.bundle.get("replay_stats", []) if isinstance(item, dict)]
        total_cases = sum(self._as_int(item.get("cases")) for item in stats)
        weighted_hit_numerator = sum(self._as_float(item.get("hit_rate")) * self._as_int(item.get("cases")) for item in stats)
        weighted_win_numerator = sum(self._as_float(item.get("win_rate")) * self._as_int(item.get("cases")) for item in stats)
        aggregate_hit_rate = round(weighted_hit_numerator / total_cases, 3) if total_cases else 0.0
        aggregate_win_rate = round(weighted_win_numerator / total_cases, 3) if total_cases else 0.0
        validation_score = round((aggregate_hit_rate * 0.6 + aggregate_win_rate * 0.4) * 100.0, 2)
        return {
            "count": len(stats),
            "cases": total_cases,
            "aggregate_hit_rate": aggregate_hit_rate,
            "aggregate_win_rate": aggregate_win_rate,
            "validation_score": validation_score,
            "scenarios": stats,
        }

    def obsidian_memory(self, limit: int = 6) -> Dict[str, Any]:
        seed = dict(self.bundle.get("memory_seed", {}))
        if self.memory_service and getattr(self.memory_service, "vault_path", None):
            themes = seed.get("themes") if isinstance(seed.get("themes"), list) else ["AI", "创新药", "半导体", "光伏", "养猪"]
            vault_summary = self.memory_service.index_notes(themes=themes, limit=max(1, min(self._as_int(limit, 6), 20)))
            recent_notes = vault_summary.get("recent_notes", [])
            theme_matches = vault_summary.get("theme_matches", [])
            recent_notes = recent_notes if isinstance(recent_notes, list) else []
            theme_matches = theme_matches if isinstance(theme_matches, list) else []
            return {
                "status": vault_summary.get("status", "missing"),
                "source": "obsidian_vault",
                "vault_path": vault_summary.get("vault_path"),
                "tracked_tags": vault_summary.get("tracked_tags", []),
                "note_count": vault_summary.get("note_count", 0),
                "recent_notes": recent_notes[: max(1, min(self._as_int(limit, 6), 20))],
                "theme_matches": theme_matches[: max(1, min(self._as_int(limit, 6), 20))],
                "seed": seed,
            }

        return {
            "status": seed.get("status", "missing"),
            "source": "sample_data",
            "vault_path": None,
            "tracked_tags": seed.get("tracked_tags", []),
            "note_count": seed.get("note_count", 0),
            "recent_notes": seed.get("recent_notes", [])[: max(1, min(self._as_int(limit, 6), 20))],
            "theme_matches": seed.get("theme_matches", [])[: max(1, min(self._as_int(limit, 6), 20))],
            "signals": seed.get("signals", []),
            "themes": seed.get("themes", []),
        }

    def overview(self) -> Dict[str, Any]:
        models = self.models(limit=10)
        opportunities = self.opportunities(limit=10)
        thesis = self.sector_thesis()
        replay = self.replay_validation()
        memory = self.obsidian_memory(limit=4)
        graph = self.cross_market_map()
        return {
            "version": self.bundle.get("version", "v1"),
            "as_of": self.bundle.get("as_of"),
            "live_enabled": self.live_enabled,
            "live_loaded_at": self.live_evidence.get("loaded_at"),
            "model_family_count": models["count"],
            "sector_thesis_count": thesis["count"],
            "opportunity_count": opportunities["count"],
            "event_count": len(self.bundle.get("events", [])),
            "live_event_count": len(self.live_evidence.get("intelligence", {}).get("events", [])),
            "live_research_count": len(self.live_evidence.get("research", {}).get("reports", [])),
            "watchlist_count": sum(len(items) for items in self.bundle.get("watchlists", {}).values() if isinstance(items, list)),
            "stage_counts": opportunities["stage_counts"],
            "top_models": models["top_families"],
            "top_batons": {
                "first_baton": [item.get("opportunity_id") for item in opportunities["baton_buckets"]["first_baton"]],
                "second_baton": [item.get("opportunity_id") for item in opportunities["baton_buckets"]["second_baton"]],
                "next_baton": [item.get("opportunity_id") for item in opportunities["baton_buckets"]["next_baton"]],
            },
            "source_health": self.bundle.get("source_health", {}),
            "live_source_health": {
                "radar": self.live_evidence.get("radar", {}).get("source_health", {}),
                "intelligence": self.live_evidence.get("intelligence", {}).get("source_health", {}),
                "research": self.live_evidence.get("research", {}).get("source_health", {}),
                "errors": self.live_evidence.get("errors", []),
            },
            "replay_validation": {
                "aggregate_hit_rate": replay["aggregate_hit_rate"],
                "aggregate_win_rate": replay["aggregate_win_rate"],
                "validation_score": replay["validation_score"],
            },
            "memory_status": memory.get("status"),
            "cross_market_markets": graph.get("markets", {}),
        }

    @staticmethod
    def _format_percent(value: Any) -> str:
        try:
            return f"{float(value) * 100:.1f}%"
        except Exception:
            return "-"

    @staticmethod
    def _tone_from_stage(stage: str) -> str:
        normalized = str(stage or "").lower()
        if normalized in {"triggered", "validating"}:
            return "positive"
        if normalized in {"crowded", "decaying"}:
            return "warning"
        if normalized == "invalidated":
            return "negative"
        return "neutral"

    def get_overview(self) -> Dict[str, Any]:
        raw = self.overview()
        opportunity_payload = self.opportunities(limit=10)
        validation = raw.get("replay_validation", {})
        first_baton = opportunity_payload.get("baton_buckets", {}).get("first_baton", [])
        first_baton_label = ", ".join(
            item.get("symbol") or item.get("opportunity_id") or "-"
            for item in first_baton[:2]
        ) or "n/a"
        return {
            "headline": "领先-传导 Alpha 引擎 V1",
            "summary": "融合 Radar 快照、Intelligence 事件、Research 研报和样例图谱的领先-传导-验证引擎。",
            "flags": [
                f"Top Model: {raw.get('top_models', ['n/a'])[0]}",
                f"First Baton: {', '.join(raw.get('top_batons', {}).get('first_baton', [])[:2]) or 'n/a'}",
                f"Live Events: {raw.get('live_event_count', 0)}",
                f"Live Research: {raw.get('live_research_count', 0)}",
            ],
            "metrics": [
                {"label": "模型数", "value": raw.get("model_family_count", 0), "note": "10 个模型家族"},
                {"label": "机会数", "value": raw.get("opportunity_count", 0), "note": "含第一棒/第二棒/下一棒"},
                {"label": "事件窗口", "value": raw.get("event_count", 0), "note": "事件日历与审批里程碑"},
                {
                    "label": "验证分",
                    "value": validation.get("validation_score", 0),
                    "note": f"命中率 {self._format_percent(validation.get('aggregate_hit_rate'))}",
                },
            ],
            "regions": ["all", "CN", "HK", "US"],
            "regimes": ["all", "latent", "triggered", "validating", "crowded", "decaying", "invalidated"],
            "as_of": raw.get("as_of"),
            "status_text": f"Live {raw.get('live_loaded_at') or raw.get('as_of')} | 第一棒 {first_baton_label}",
            "source": "live_fusion" if self.live_enabled else "sample_data",
            "live_event_count": raw.get("live_event_count", 0),
            "live_research_count": raw.get("live_research_count", 0),
            "live_source_health": raw.get("live_source_health", {}),
        }

    def list_models(self, limit: int = 10) -> List[Dict[str, Any]]:
        payload = self.models(limit=limit)
        return [
            {
                "model": item.get("family_id"),
                "name": self._model_display(item.get("family_id")).get("model_name_zh") or item.get("name"),
                "summary": self._model_display(item.get("family_id")).get("model_explain_zh") or item.get("description"),
                "status": BATON_DISPLAY.get(str(item.get("preferred_stage") or ""), item.get("preferred_stage", "latent")),
                "lead_window": item.get("lead_window", "1-20d"),
                "universe": item.get("domain"),
                "confidence": self._format_percent(item.get("confidence")),
                "tone": self._tone_from_stage(item.get("preferred_stage", "latent")),
            }
            for item in payload.get("families", [])
        ]

    def list_opportunities(self, limit: int = 12) -> List[Dict[str, Any]]:
        payload = self.opportunities(limit=limit)
        rows = []
        for item in payload.get("all", []):
            rows.append(
                {
                    "title": item.get("title") or item.get("symbol") or item.get("opportunity_id"),
                    "name": item.get("asset_name") or item.get("symbol") or item.get("opportunity_id"),
                    "asset_code": item.get("asset_code") or item.get("symbol"),
                    "asset_name": item.get("asset_name"),
                    "market": item.get("market"),
                    "score": item.get("score"),
                    "rationale": item.get("thesis") or item.get("summary"),
                    "driver": item.get("driver"),
                    "confirmation": item.get("validation_signal"),
                    "risk": item.get("invalidation_rule"),
                    "baton": item.get("baton"),
                    "stage": item.get("stage"),
                    "source_url": item.get("source_url"),
                    "updated_at": item.get("updated_at"),
                    "evidence_sources": item.get("evidence_sources", []),
                }
            )
        return rows

    def get_cross_market_map(self) -> List[Dict[str, Any]]:
        payload = self.cross_market_map()
        rows: List[Dict[str, Any]] = []
        node_map = {item.get("node_id"): item for item in payload.get("nodes", [])}
        for edge in payload.get("edges", [])[:12]:
            source = node_map.get(edge.get("source"), {})
            target = node_map.get(edge.get("target"), {})
            rows.append(
                {
                    "name": f"{source.get('label', edge.get('source'))} -> {target.get('label', edge.get('target'))}",
                    "asset": target.get("symbol") or target.get("label"),
                    "asset_code": target.get("symbol"),
                    "asset_name": target.get("label"),
                    "market": target.get("market"),
                    "summary": edge.get("thesis") or edge.get("evidence_type") or edge.get("relation"),
                    "tone": "positive" if self._as_float(edge.get("sign"), 1.0) >= 0 else "negative",
                    "signal": round(self._as_float(edge.get("strength")) * 100, 1),
                    "lag": f"{edge.get('lag_min_days', '?')}-{edge.get('lag_max_days', '?')}d",
                    "source_url": edge.get("source_url"),
                    "updated_at": edge.get("last_verified_at"),
                }
            )
        return rows

    def get_industry_transmission(self) -> List[Dict[str, Any]]:
        payload = self.industry_transmission()
        rows: List[Dict[str, Any]] = []
        node_map = {item.get("node_id"): item for item in payload.get("nodes", [])}
        for sector, edges in payload.get("sector_paths", {}).items():
            ordered = sorted(edges, key=lambda item: -self._as_float(item.get("strength")))
            steps = []
            for edge in ordered[:4]:
                source = node_map.get(edge.get("source"), {})
                target = node_map.get(edge.get("target"), {})
                steps.append(
                    target.get("label")
                    or target.get("symbol")
                    or edge.get("target")
                    or source.get("label")
                    or edge.get("source")
                )
            head = ordered[0] if ordered else {}
            rows.append(
                {
                    "name": sector,
                    "driver": node_map.get(head.get("source"), {}).get("label") or head.get("source") or sector,
                    "summary": head.get("thesis") or f"{sector} sector transmission graph",
                    "signal": "positive" if self._as_float(head.get("sign"), 1.0) >= 0 else "negative",
                    "steps": steps,
                }
            )
        return rows

    def get_liquidity(self) -> List[Dict[str, Any]]:
        payload = self.liquidity()
        rows: List[Dict[str, Any]] = []
        for item in payload.get("markets", []):
            rows.append(
                {
                    "name": item.get("name") or item.get("market"),
                    "summary": item.get("summary") or item.get("notes"),
                    "tone": item.get("tone") or "neutral",
                    "signal": item.get("signal") or item.get("status") or "watch",
                    "value": item.get("value") or item.get("score") or "-",
                }
            )
        for item in payload.get("sectors", []):
            rows.append(
                {
                    "name": item.get("name") or item.get("sector"),
                    "summary": item.get("summary") or item.get("notes"),
                    "tone": item.get("tone") or "neutral",
                    "signal": item.get("signal") or item.get("status") or "watch",
                    "value": item.get("value") or item.get("score") or "-",
                }
            )
        return rows[:12]

    def list_sector_thesis(self) -> List[Dict[str, Any]]:
        payload = self.sector_thesis()
        return [
            {
                "name": item.get("sector_name"),
                "title": item.get("title"),
                "summary": item.get("thesis"),
                "evidence": item.get("bull_case", []),
                "invalidation": ", ".join(item.get("bear_case", [])),
                "crowding": item.get("stage_hint", "latent"),
                "score": round(float(item.get("score", 0.0)) * 100, 1),
                "watchlist": item.get("live_watchlist") or [
                    self._asset_for(code) for code in item.get("anchors", []) if code
                ],
                "updated_at": item.get("live_updated_at"),
                "evidence_sources": item.get("evidence_sources", []),
            }
            for item in payload.get("cards", [])
        ]

    def get_events_calendar(self, limit: int = 20) -> List[Dict[str, Any]]:
        payload = self.events_calendar(limit=limit)
        return [
            {
                "title": item.get("title"),
                "date": item.get("event_date"),
                "type": item.get("event_type") or item.get("sector_key"),
                "importance": item.get("priority") or item.get("stage"),
                "notes": item.get("notes") or item.get("validation_signal"),
                "related_assets": item.get("related_assets") or [
                    self._asset_for(code) for code in item.get("related_symbols", []) if code
                ],
                "source_url": item.get("source_url"),
                "confidence": item.get("confidence"),
            }
            for item in payload.get("events", [])
        ]

    def get_replay_validation(self) -> List[Dict[str, Any]]:
        payload = self.replay_validation()
        return [
            {
                "title": item.get("scenario_name"),
                "outcome": item.get("outcome"),
                "hit_rate": self._format_percent(item.get("hit_rate")),
                "reason": item.get("notes") or item.get("failure_mode"),
                "reference": item.get("scenario_key"),
            }
            for item in payload.get("scenarios", [])
        ]

    def get_obsidian_memory(self, limit: int = 6) -> List[Dict[str, Any]]:
        payload = self.obsidian_memory(limit=limit)
        rows: List[Dict[str, Any]] = []
        for item in payload.get("theme_matches", []) or payload.get("recent_notes", []):
            rows.append(
                {
                    "title": item.get("title"),
                    "tags": item.get("tracked_tags") or item.get("tags") or [],
                    "notes": item.get("relative_path") or item.get("path") or item.get("summary") or "",
                    "links": [item.get("path")] if item.get("path") else [],
                }
            )
        return rows
