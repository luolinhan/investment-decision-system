"""Opportunity Universe Registry for Lead-Lag V3.

The registry moves sectors, themes, entities, instruments, mappings, models,
theses, and event templates out of hard-coded UI assumptions and into local
SQLite tables that can be extended safely over time.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.db import get_sqlite_connection


DEFAULT_DB_PATH = Path("data/investment.db")
REGISTRY_VERSION = "v3.0.0"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def dumps(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False, sort_keys=True)


def loads(value: Any, fallback: Any = None) -> Any:
    if value in (None, "", "null"):
        return [] if fallback is None else fallback
    try:
        return json.loads(str(value))
    except Exception:
        return [] if fallback is None else fallback


class OpportunityUniverseRegistry:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = get_sqlite_connection(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> Dict[str, Any]:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sector_registry (
                    sector_id TEXT PRIMARY KEY,
                    name_zh TEXT NOT NULL,
                    name_en TEXT,
                    description TEXT,
                    lead_assets TEXT NOT NULL DEFAULT '[]',
                    bridge_assets TEXT NOT NULL DEFAULT '[]',
                    local_assets TEXT NOT NULL DEFAULT '[]',
                    proxy_assets TEXT NOT NULL DEFAULT '[]',
                    upstream_nodes TEXT NOT NULL DEFAULT '[]',
                    downstream_nodes TEXT NOT NULL DEFAULT '[]',
                    key_metrics TEXT NOT NULL DEFAULT '[]',
                    key_events TEXT NOT NULL DEFAULT '[]',
                    default_invalidation_rules TEXT NOT NULL DEFAULT '[]',
                    replay_horizons TEXT NOT NULL DEFAULT '[1,3,5,10,20]',
                    source_requirements TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS theme_registry (
                    theme_id TEXT PRIMARY KEY,
                    sector_id TEXT,
                    name_zh TEXT NOT NULL,
                    name_en TEXT,
                    family_defaults TEXT NOT NULL DEFAULT '[]',
                    evidence_checklist TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS entity_registry (
                    entity_id TEXT PRIMARY KEY,
                    name_zh TEXT NOT NULL,
                    name_en TEXT,
                    entity_type TEXT NOT NULL DEFAULT 'company',
                    sector_ids TEXT NOT NULL DEFAULT '[]',
                    theme_ids TEXT NOT NULL DEFAULT '[]',
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS instrument_registry (
                    instrument_id TEXT PRIMARY KEY,
                    entity_id TEXT,
                    market TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    name_zh TEXT NOT NULL,
                    name_en TEXT,
                    instrument_type TEXT NOT NULL DEFAULT 'stock',
                    role_defaults TEXT NOT NULL DEFAULT '[]',
                    liquidity_bucket TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mapping_registry (
                    mapping_id TEXT PRIMARY KEY,
                    source_entity_id TEXT,
                    target_entity_id TEXT,
                    source_instrument_id TEXT,
                    target_instrument_id TEXT,
                    sector_id TEXT,
                    mapping_type TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0.0,
                    evidence_requirements TEXT NOT NULL DEFAULT '[]',
                    pollution_checks TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS model_registry (
                    model_id TEXT PRIMARY KEY,
                    name_zh TEXT NOT NULL,
                    name_en TEXT,
                    family TEXT NOT NULL,
                    applicable_families TEXT NOT NULL DEFAULT '[]',
                    required_inputs TEXT NOT NULL DEFAULT '[]',
                    output_fields TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS thesis_registry (
                    thesis_id TEXT PRIMARY KEY,
                    sector_id TEXT,
                    theme_id TEXT,
                    family TEXT NOT NULL,
                    title_zh TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    source_requirements TEXT NOT NULL DEFAULT '[]',
                    invalidation_rules TEXT NOT NULL DEFAULT '[]',
                    replay_horizons TEXT NOT NULL DEFAULT '[1,3,5,10,20]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_template_registry (
                    template_id TEXT PRIMARY KEY,
                    sector_id TEXT,
                    family TEXT NOT NULL,
                    event_class TEXT NOT NULL,
                    name_zh TEXT NOT NULL,
                    source_requirements TEXT NOT NULL DEFAULT '[]',
                    mapping_rules TEXT NOT NULL DEFAULT '[]',
                    catalyst_window_days INTEGER NOT NULL DEFAULT 10,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_theme_sector ON theme_registry(sector_id);
                CREATE INDEX IF NOT EXISTS idx_instrument_ticker ON instrument_registry(ticker, market);
                CREATE INDEX IF NOT EXISTS idx_thesis_sector_family ON thesis_registry(sector_id, family);
                CREATE INDEX IF NOT EXISTS idx_event_template_sector ON event_template_registry(sector_id);
                """
            )
            conn.commit()
        return {"registry_version": REGISTRY_VERSION, "db_path": self.db_path.as_posix()}

    def seed_defaults(self) -> Dict[str, int]:
        self.ensure_schema()
        metrics = {
            "sectors": 0,
            "themes": 0,
            "entities": 0,
            "instruments": 0,
            "mappings": 0,
            "models": 0,
            "theses": 0,
            "event_templates": 0,
        }
        with self.connect() as conn:
            for sector in DEFAULT_SECTORS:
                self._upsert_sector(conn, sector)
                metrics["sectors"] += 1
                self._upsert_theme(conn, self._theme_from_sector(sector))
                metrics["themes"] += 1
                self._upsert_thesis(conn, self._thesis_from_sector(sector))
                metrics["theses"] += 1
                self._upsert_event_template(conn, self._event_template_from_sector(sector))
                metrics["event_templates"] += 1
            for entity in DEFAULT_ENTITIES:
                self._upsert_entity(conn, entity)
                metrics["entities"] += 1
            for instrument in DEFAULT_INSTRUMENTS:
                self._upsert_instrument(conn, instrument)
                metrics["instruments"] += 1
            for mapping in DEFAULT_MAPPINGS:
                self._upsert_mapping(conn, mapping)
                metrics["mappings"] += 1
            for model in DEFAULT_MODELS:
                self._upsert_model(conn, model)
                metrics["models"] += 1
            conn.commit()
        return metrics

    def registry_summary(self) -> Dict[str, Any]:
        self.ensure_schema()
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
        with self.connect() as conn:
            counts = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tables}
            sectors = [dict(row) for row in conn.execute("SELECT sector_id, name_zh, enabled FROM sector_registry ORDER BY sector_id").fetchall()]
        return {"registry_version": REGISTRY_VERSION, "counts": counts, "sectors": sectors}

    def list_sectors(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        self.ensure_schema()
        where = "WHERE enabled = 1" if enabled_only else ""
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM sector_registry {where} ORDER BY sector_id").fetchall()
        return [self._decode_json_fields(dict(row)) for row in rows]

    @staticmethod
    def _decode_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in list(row.items()):
            if key.endswith("_assets") or key.endswith("_nodes") or key in {
                "key_metrics",
                "key_events",
                "default_invalidation_rules",
                "replay_horizons",
                "source_requirements",
                "family_defaults",
                "evidence_checklist",
                "sector_ids",
                "theme_ids",
                "role_defaults",
                "evidence_requirements",
                "pollution_checks",
                "applicable_families",
                "required_inputs",
                "output_fields",
                "invalidation_rules",
                "mapping_rules",
            }:
                row[key] = loads(value)
        return row

    @staticmethod
    def _theme_from_sector(sector: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "theme_id": f"{sector['sector_id']}_default",
            "sector_id": sector["sector_id"],
            "name_zh": sector["name_zh"],
            "name_en": sector.get("name_en"),
            "family_defaults": sector.get("families", ["industry_transmission"]),
            "evidence_checklist": sector.get("source_requirements", []),
            "enabled": sector.get("enabled", True),
        }

    @staticmethod
    def _thesis_from_sector(sector: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "thesis_id": f"thesis_{sector['sector_id']}_default",
            "sector_id": sector["sector_id"],
            "theme_id": f"{sector['sector_id']}_default",
            "family": (sector.get("families") or ["industry_transmission"])[0],
            "title_zh": f"{sector['name_zh']}领先-传导默认 thesis",
            "status": "active_template",
            "source_requirements": sector.get("source_requirements", []),
            "invalidation_rules": sector.get("default_invalidation_rules", []),
            "replay_horizons": sector.get("replay_horizons", [1, 3, 5, 10, 20]),
        }

    @staticmethod
    def _event_template_from_sector(sector: Dict[str, Any]) -> Dict[str, Any]:
        family = (sector.get("families") or ["industry_transmission"])[0]
        return {
            "template_id": f"event_{sector['sector_id']}_{family}",
            "sector_id": sector["sector_id"],
            "family": family,
            "event_class": "market-facing",
            "name_zh": f"{sector['name_zh']}可交易催化模板",
            "source_requirements": sector.get("source_requirements", []),
            "mapping_rules": [
                "必须有本地可交易载体",
                "弱中国资产映射不得进入高优先级",
                "样例或占位数据只能进入研究背景层",
            ],
            "catalyst_window_days": 10,
            "enabled": sector.get("enabled", True),
        }

    def _upsert_sector(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        now = now_iso()
        conn.execute(
            """
            INSERT INTO sector_registry (
                sector_id, name_zh, name_en, description, lead_assets, bridge_assets,
                local_assets, proxy_assets, upstream_nodes, downstream_nodes,
                key_metrics, key_events, default_invalidation_rules, replay_horizons,
                source_requirements, enabled, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sector_id) DO UPDATE SET
                name_zh=excluded.name_zh,
                name_en=excluded.name_en,
                description=excluded.description,
                lead_assets=excluded.lead_assets,
                bridge_assets=excluded.bridge_assets,
                local_assets=excluded.local_assets,
                proxy_assets=excluded.proxy_assets,
                upstream_nodes=excluded.upstream_nodes,
                downstream_nodes=excluded.downstream_nodes,
                key_metrics=excluded.key_metrics,
                key_events=excluded.key_events,
                default_invalidation_rules=excluded.default_invalidation_rules,
                replay_horizons=excluded.replay_horizons,
                source_requirements=excluded.source_requirements,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at
            """,
            (
                row["sector_id"],
                row["name_zh"],
                row.get("name_en"),
                row.get("description"),
                dumps(row.get("lead_assets")),
                dumps(row.get("bridge_assets")),
                dumps(row.get("local_assets")),
                dumps(row.get("proxy_assets")),
                dumps(row.get("upstream_nodes")),
                dumps(row.get("downstream_nodes")),
                dumps(row.get("key_metrics")),
                dumps(row.get("key_events")),
                dumps(row.get("default_invalidation_rules")),
                dumps(row.get("replay_horizons", [1, 3, 5, 10, 20])),
                dumps(row.get("source_requirements")),
                1 if row.get("enabled", True) else 0,
                now,
                now,
            ),
        )

    def _upsert_theme(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "theme_registry",
            "theme_id",
            row,
            json_fields=("family_defaults", "evidence_checklist"),
            bool_fields=("enabled",),
        )

    def _upsert_entity(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "entity_registry",
            "entity_id",
            row,
            json_fields=("sector_ids", "theme_ids"),
        )

    def _upsert_instrument(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "instrument_registry",
            "instrument_id",
            row,
            json_fields=("role_defaults",),
            bool_fields=("enabled",),
        )

    def _upsert_mapping(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "mapping_registry",
            "mapping_id",
            row,
            json_fields=("evidence_requirements", "pollution_checks"),
            bool_fields=("enabled",),
        )

    def _upsert_model(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "model_registry",
            "model_id",
            row,
            json_fields=("applicable_families", "required_inputs", "output_fields"),
            bool_fields=("enabled",),
        )

    def _upsert_thesis(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "thesis_registry",
            "thesis_id",
            row,
            json_fields=("source_requirements", "invalidation_rules", "replay_horizons"),
        )

    def _upsert_event_template(self, conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
        self._upsert_generic(
            conn,
            "event_template_registry",
            "template_id",
            row,
            json_fields=("source_requirements", "mapping_rules"),
            bool_fields=("enabled",),
        )

    @staticmethod
    def _upsert_generic(
        conn: sqlite3.Connection,
        table: str,
        pk: str,
        row: Dict[str, Any],
        json_fields: Iterable[str] = (),
        bool_fields: Iterable[str] = (),
    ) -> None:
        now = now_iso()
        payload = dict(row)
        payload["created_at"] = now
        payload["updated_at"] = now
        for field in json_fields:
            payload[field] = dumps(payload.get(field))
        for field in bool_fields:
            payload[field] = 1 if payload.get(field, True) else 0
        columns = list(payload.keys())
        placeholders = ", ".join([f":{column}" for column in columns])
        update_columns = [column for column in columns if column not in {pk, "created_at"}]
        updates = ", ".join([f"{column}=excluded.{column}" for column in update_columns])
        conn.execute(
            f"""
            INSERT INTO {table} ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT({pk}) DO UPDATE SET {updates}
            """,
            payload,
        )


SOURCE_REQUIREMENTS = [
    "至少 2 个独立来源",
    "至少 1 个 live_official/live_public/company filing/exchange 来源",
    "必须可打开原始链接或本地归档",
]

DEFAULT_SECTORS: List[Dict[str, Any]] = [
    {
        "sector_id": "ai_compute_infra",
        "name_zh": "AI 算力基础设施",
        "name_en": "AI Compute Infrastructure",
        "description": "GPU、服务器、光模块、PCB、IDC、电源和液冷链。",
        "lead_assets": ["NVDA", "SMCI", "AVGO"],
        "bridge_assets": ["TSM", "AMD", "0981.HK"],
        "local_assets": ["300502.SZ", "300308.SZ", "002463.SZ", "601138.SH"],
        "proxy_assets": ["515980.SH", "512720.SH"],
        "upstream_nodes": ["GPU", "HBM", "光模块", "PCB"],
        "downstream_nodes": ["云厂商资本开支", "企业 AI 部署"],
        "key_metrics": ["GPU 订单", "云 capex", "光模块价格", "订单能见度"],
        "key_events": ["海外龙头财报", "云厂商 capex 指引", "产业大会"],
        "default_invalidation_rules": ["云 capex 指引下修", "本地光模块订单未验证", "拥挤度过高"],
        "families": ["customer_capex_spillover", "industry_transmission", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "ai_application_software",
        "name_zh": "AI 应用 / 软件",
        "name_en": "AI Application Software",
        "description": "办公、教育、营销、工业软件和智能体应用。",
        "lead_assets": ["MSFT", "ADBE", "CRM"],
        "bridge_assets": ["09988.HK", "00700.HK"],
        "local_assets": ["688111.SH", "002230.SZ"],
        "proxy_assets": ["512720.SH"],
        "upstream_nodes": ["大模型", "云资源", "企业预算"],
        "downstream_nodes": ["软件订阅", "垂直场景转化"],
        "key_metrics": ["ARR", "付费转化", "模型调用成本", "企业 IT 支出"],
        "key_events": ["产品发布", "价格调整", "大型客户订单"],
        "default_invalidation_rules": ["付费转化低于预期", "模型成本未下降"],
        "families": ["earnings_revision", "valuation_gap"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "innovative_pharma",
        "name_zh": "创新药",
        "name_en": "Innovative Pharma",
        "description": "临床、审批、BD、出海和商业化兑现。",
        "lead_assets": ["LLY", "MRK", "VRTX"],
        "bridge_assets": ["06160.HK", "01801.HK"],
        "local_assets": ["600276.SH", "688235.SH"],
        "proxy_assets": ["159992.SZ", "512290.SH"],
        "upstream_nodes": ["靶点", "临床数据", "监管审批"],
        "downstream_nodes": ["医保准入", "商业化销售"],
        "key_metrics": ["临床终点", "NDA/BLA", "BD 金额", "销售爬坡"],
        "key_events": ["临床读出", "NMPA/FDA 审批", "BD 授权"],
        "default_invalidation_rules": ["临床终点失败", "审批延迟", "商业化不达预期"],
        "families": ["clinical_approval_bd", "event_calendar"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "cro_cxo_research_service",
        "name_zh": "CRO / CXO / 科研服务",
        "name_en": "CRO CXO Research Services",
        "description": "研发外包、生产外包、订单和海外生物科技融资。",
        "lead_assets": ["IQV", "TMO"],
        "bridge_assets": ["02269.HK"],
        "local_assets": ["603259.SH", "300759.SZ"],
        "proxy_assets": ["159992.SZ"],
        "upstream_nodes": ["全球 biotech 融资", "药企研发预算"],
        "downstream_nodes": ["订单", "产能利用率"],
        "key_metrics": ["订单", "backlog", "融资额", "产能利用率"],
        "key_events": ["大额订单", "政策豁免", "融资回暖"],
        "default_invalidation_rules": ["订单下修", "政策风险上升"],
        "families": ["customer_capex_spillover", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "semiconductor_equipment",
        "name_zh": "半导体设备",
        "name_en": "Semiconductor Equipment",
        "description": "晶圆厂 capex、设备订单、先进制程与国产替代。",
        "lead_assets": ["ASML", "AMAT", "LRCX"],
        "bridge_assets": ["TSM", "00981.HK"],
        "local_assets": ["002371.SZ", "688012.SH", "688072.SH", "688037.SH"],
        "proxy_assets": ["512480.SH"],
        "upstream_nodes": ["晶圆厂 capex", "设备交付"],
        "downstream_nodes": ["国产设备订单", "验收收入"],
        "key_metrics": ["capex 指引", "订单", "国产化率", "交付周期"],
        "key_events": ["海外设备财报", "晶圆厂扩产", "制裁变动"],
        "default_invalidation_rules": ["capex 下修", "出口管制加码", "订单验收延迟"],
        "families": ["customer_capex_spillover", "policy_credit_fiscal"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "semiconductor_full_chain",
        "name_zh": "半导体材料 / 代工 / 封测 / 设计",
        "name_en": "Semiconductor Full Chain",
        "description": "材料、晶圆代工、封测、IC 设计和周期修复。",
        "lead_assets": ["TSM", "AMD", "MU"],
        "bridge_assets": ["00981.HK"],
        "local_assets": ["688981.SH", "603501.SH", "002185.SZ"],
        "proxy_assets": ["159995.SZ", "512480.SH"],
        "upstream_nodes": ["晶圆价格", "库存", "终端需求"],
        "downstream_nodes": ["设计公司收入", "封测稼动率"],
        "key_metrics": ["库存天数", "稼动率", "ASP", "出货量"],
        "key_events": ["月度营收", "涨价函", "库存拐点"],
        "default_invalidation_rules": ["库存去化失败", "稼动率回落"],
        "families": ["inventory_destocking_cycle", "price_spread_pass_through"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "solar",
        "name_zh": "光伏",
        "name_en": "Solar",
        "description": "硅料、组件、逆变器、海外装机和价格链。",
        "lead_assets": ["FSLR", "ENPH"],
        "bridge_assets": ["968.HK"],
        "local_assets": ["601012.SH", "300274.SZ", "600438.SH"],
        "proxy_assets": ["516180.SH", "159857.SZ"],
        "upstream_nodes": ["硅料价格", "组件排产"],
        "downstream_nodes": ["装机", "逆变器出货"],
        "key_metrics": ["硅料价格", "组件价格", "装机", "库存"],
        "key_events": ["政策招标", "海外需求", "价格拐点"],
        "default_invalidation_rules": ["价格继续下跌", "排产未改善"],
        "families": ["price_spread_pass_through", "inventory_destocking_cycle"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "wind_power",
        "name_zh": "风电",
        "name_en": "Wind Power",
        "description": "海风审批、招标、整机、塔筒、海缆与运营商。",
        "lead_assets": ["VWS.CO", "GEV"],
        "bridge_assets": ["0916.HK"],
        "local_assets": ["002202.SZ", "603606.SH"],
        "proxy_assets": ["516850.SH"],
        "upstream_nodes": ["钢材", "叶片", "海缆"],
        "downstream_nodes": ["运营商装机", "招标"],
        "key_metrics": ["招标量", "审批进度", "中标价格", "装机"],
        "key_events": ["海风审批", "大基地招标", "补贴政策"],
        "default_invalidation_rules": ["审批延迟", "中标价格恶化"],
        "families": ["policy_credit_fiscal", "event_calendar"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "grid_storage_power_equipment",
        "name_zh": "电网 / 储能 / 电力设备",
        "name_en": "Grid Storage Power Equipment",
        "description": "特高压、配网、储能招标、电力设备出口。",
        "lead_assets": ["ETN", "PWR"],
        "bridge_assets": ["VST"],
        "local_assets": ["600406.SH", "300750.SZ", "688063.SH"],
        "proxy_assets": ["516160.SH"],
        "upstream_nodes": ["铜铝", "电芯", "变压器"],
        "downstream_nodes": ["电网投资", "海外数据中心电力"],
        "key_metrics": ["电网投资", "储能招标", "出口订单", "变压器交期"],
        "key_events": ["电网招标", "数据中心电力约束", "政策投资"],
        "default_invalidation_rules": ["招标低于预期", "原材料成本冲击"],
        "families": ["policy_credit_fiscal", "customer_capex_spillover"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "ev_battery",
        "name_zh": "锂电 / 电动车",
        "name_en": "EV Battery",
        "description": "电动车销量、锂电材料、电池、整车和储能。",
        "lead_assets": ["TSLA", "ALB"],
        "bridge_assets": ["1211.HK"],
        "local_assets": ["300750.SZ", "002594.SZ", "603799.SH"],
        "proxy_assets": ["159840.SZ"],
        "upstream_nodes": ["锂价", "材料", "电芯"],
        "downstream_nodes": ["整车销量", "储能需求"],
        "key_metrics": ["锂价", "销量", "排产", "库存"],
        "key_events": ["月度销量", "价格调整", "材料涨价"],
        "default_invalidation_rules": ["销量下修", "价格战加剧", "库存上升"],
        "families": ["price_spread_pass_through", "inventory_destocking_cycle"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "robotics_automation",
        "name_zh": "机器人 / 自动化",
        "name_en": "Robotics Automation",
        "description": "人形机器人、工业自动化、减速器、伺服和传感器。",
        "lead_assets": ["TSLA", "ISRG", "ROK"],
        "bridge_assets": ["0981.HK"],
        "local_assets": ["002472.SZ", "300124.SZ", "688017.SH"],
        "proxy_assets": ["159770.SZ"],
        "upstream_nodes": ["减速器", "伺服", "传感器"],
        "downstream_nodes": ["汽车厂", "工厂自动化"],
        "key_metrics": ["出货量", "订单", "BOM 成本", "客户验证"],
        "key_events": ["产品发布", "量产节点", "大客户订单"],
        "default_invalidation_rules": ["量产延期", "订单未兑现"],
        "families": ["event_calendar", "customer_capex_spillover"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "defense_satellite_aerospace",
        "name_zh": "军工 / 卫星 / 航空航天",
        "name_en": "Defense Satellite Aerospace",
        "description": "装备订单、商业航天、卫星互联网和航空发动机。",
        "lead_assets": ["LMT", "RTX", "NOC"],
        "bridge_assets": ["000768.SZ"],
        "local_assets": ["600893.SH", "002179.SZ", "300034.SZ"],
        "proxy_assets": ["512670.SH"],
        "upstream_nodes": ["军费", "订单", "航天发射"],
        "downstream_nodes": ["装备交付", "卫星应用"],
        "key_metrics": ["订单", "发射计划", "军费预算", "交付节奏"],
        "key_events": ["预算发布", "发射成功", "重大订单"],
        "default_invalidation_rules": ["订单延后", "预算低于预期"],
        "families": ["policy_credit_fiscal", "event_calendar"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "brokerage",
        "name_zh": "券商",
        "name_en": "Brokerage",
        "description": "市场成交、两融、IPO、投行、政策和风险偏好。",
        "lead_assets": ["SCHW", "MS"],
        "bridge_assets": ["0388.HK"],
        "local_assets": ["600030.SH", "300059.SZ", "601688.SH"],
        "proxy_assets": ["512000.SH"],
        "upstream_nodes": ["成交额", "融资余额", "政策"],
        "downstream_nodes": ["经纪佣金", "投行业务"],
        "key_metrics": ["成交额", "两融", "IPO 节奏", "估值"],
        "key_events": ["资本市场政策", "交易活跃度突破"],
        "default_invalidation_rules": ["成交额回落", "政策低于预期"],
        "families": ["policy_credit_fiscal", "external_liquidity_bridge"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "insurance",
        "name_zh": "保险",
        "name_en": "Insurance",
        "description": "长端利率、权益市场、保费、负债成本和投资收益。",
        "lead_assets": ["BRK.B", "MET"],
        "bridge_assets": ["2318.HK"],
        "local_assets": ["601318.SH", "601601.SH"],
        "proxy_assets": ["512070.SH"],
        "upstream_nodes": ["长端利率", "权益市场"],
        "downstream_nodes": ["投资收益", "新业务价值"],
        "key_metrics": ["10Y 国债", "保费", "NBV", "投资收益"],
        "key_events": ["利率拐点", "保费月报", "权益市场转强"],
        "default_invalidation_rules": ["利率下行", "权益市场转弱"],
        "families": ["external_liquidity_bridge", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "bank_high_dividend",
        "name_zh": "银行 / 高股息 / 红利",
        "name_en": "Banks High Dividend",
        "description": "银行、煤电运营商、运营现金流和红利资产。",
        "lead_assets": ["JPM", "XLF"],
        "bridge_assets": ["0939.HK"],
        "local_assets": ["600036.SH", "601398.SH", "600900.SH"],
        "proxy_assets": ["510880.SH"],
        "upstream_nodes": ["利率", "信用", "分红政策"],
        "downstream_nodes": ["股息率", "估值修复"],
        "key_metrics": ["息差", "不良率", "股息率", "信用利差"],
        "key_events": ["分红政策", "利率变化", "业绩快报"],
        "default_invalidation_rules": ["信用风险上升", "分红不及预期"],
        "families": ["valuation_gap", "external_liquidity_bridge"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "metals_gold_copper",
        "name_zh": "黄金 / 铜 / 铝 / 稀土等有色",
        "name_en": "Metals Gold Copper",
        "description": "美元、实际利率、库存、供给扰动和资源股弹性。",
        "lead_assets": ["GC=F", "HG=F", "GLD"],
        "bridge_assets": ["2899.HK"],
        "local_assets": ["601899.SH", "603993.SH", "600111.SH"],
        "proxy_assets": ["512400.SH"],
        "upstream_nodes": ["美元", "利率", "库存", "矿山供给"],
        "downstream_nodes": ["资源股盈利", "冶炼利润"],
        "key_metrics": ["价格", "库存", "实际利率", "TC/RC"],
        "key_events": ["美联储", "矿山扰动", "库存拐点"],
        "default_invalidation_rules": ["美元走强", "库存上升", "需求下修"],
        "families": ["price_spread_pass_through", "external_liquidity_bridge"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "energy_coal_oil_power_nuclear",
        "name_zh": "煤炭 / 油气 / 电力 / 核电",
        "name_en": "Energy Coal Oil Power Nuclear",
        "description": "能源价格、电力需求、装机、核电审批与现金流。",
        "lead_assets": ["XOM", "CVX", "URA"],
        "bridge_assets": ["0883.HK"],
        "local_assets": ["601088.SH", "600938.SH", "601985.SH"],
        "proxy_assets": ["510880.SH"],
        "upstream_nodes": ["煤价", "油价", "铀价"],
        "downstream_nodes": ["发电量", "核电装机"],
        "key_metrics": ["煤价", "油价", "用电量", "核准项目"],
        "key_events": ["能源价格突破", "核电审批", "电价政策"],
        "default_invalidation_rules": ["能源价格回落", "需求不及预期"],
        "families": ["price_spread_pass_through", "policy_credit_fiscal"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "shipping_port_logistics",
        "name_zh": "航运 / 港口 / 物流",
        "name_en": "Shipping Port Logistics",
        "description": "运价、港口吞吐、地缘绕航和跨境物流。",
        "lead_assets": ["BDI", "ZIM"],
        "bridge_assets": ["1919.HK"],
        "local_assets": ["601919.SH", "600018.SH"],
        "proxy_assets": ["159666.SZ"],
        "upstream_nodes": ["运价", "燃油", "港口拥堵"],
        "downstream_nodes": ["航运盈利", "物流订单"],
        "key_metrics": ["BDI", "SCFI", "吞吐量", "燃油价格"],
        "key_events": ["绕航", "旺季", "运价突破"],
        "default_invalidation_rules": ["运价回落", "需求转弱"],
        "families": ["seasonal_calendar", "price_spread_pass_through"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "internet_platform_cloud",
        "name_zh": "互联网平台 / 云",
        "name_en": "Internet Platform Cloud",
        "description": "平台经济、广告、电商、游戏、云和 AI 资本开支。",
        "lead_assets": ["AMZN", "GOOGL", "META"],
        "bridge_assets": ["09988.HK", "00700.HK"],
        "local_assets": ["09988.HK", "00700.HK", "01024.HK"],
        "proxy_assets": ["513180.SH"],
        "upstream_nodes": ["广告预算", "云 capex", "监管"],
        "downstream_nodes": ["平台利润", "云收入"],
        "key_metrics": ["广告增速", "云收入", "take rate", "回购"],
        "key_events": ["财报", "监管政策", "产品发布"],
        "default_invalidation_rules": ["广告预算下修", "监管收紧"],
        "families": ["earnings_revision", "valuation_gap"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "consumer_electronics_apple_chain",
        "name_zh": "消费电子 / 苹果链",
        "name_en": "Consumer Electronics Apple Chain",
        "description": "苹果链、安卓链、MR、AI 终端和零组件。",
        "lead_assets": ["AAPL", "QCOM"],
        "bridge_assets": ["2382.TW"],
        "local_assets": ["002475.SZ", "002241.SZ", "603986.SH"],
        "proxy_assets": ["159732.SZ"],
        "upstream_nodes": ["芯片", "面板", "镜头"],
        "downstream_nodes": ["手机销量", "穿戴设备"],
        "key_metrics": ["出货量", "库存", "新机预期", "订单"],
        "key_events": ["新品发布", "拉货周期", "价格调整"],
        "default_invalidation_rules": ["拉货不及预期", "库存上升"],
        "families": ["seasonal_calendar", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "global_manufacturing_machinery",
        "name_zh": "出海制造 / 工程机械",
        "name_en": "Global Manufacturing Machinery",
        "description": "工程机械、叉车、工具、海外订单和汇率。",
        "lead_assets": ["CAT", "DE"],
        "bridge_assets": ["631.HK"],
        "local_assets": ["600031.SH", "000425.SZ", "000157.SZ"],
        "proxy_assets": ["159667.SZ"],
        "upstream_nodes": ["钢材", "汇率", "海外基建"],
        "downstream_nodes": ["出口订单", "利润率"],
        "key_metrics": ["出口", "订单", "汇率", "开工小时"],
        "key_events": ["海外订单", "基建政策", "汇率变动"],
        "default_invalidation_rules": ["出口订单下修", "汇率不利"],
        "families": ["customer_capex_spillover", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "tourism_hotel_dutyfree",
        "name_zh": "旅游 / 酒店 / 免税",
        "name_en": "Tourism Hotel Duty Free",
        "description": "出行、酒店 RevPAR、免税销售和节假日弹性。",
        "lead_assets": ["BKNG", "MAR"],
        "bridge_assets": ["9961.HK"],
        "local_assets": ["601888.SH", "600754.SH"],
        "proxy_assets": ["159766.SZ"],
        "upstream_nodes": ["客流", "航班", "消费"],
        "downstream_nodes": ["酒店价格", "免税销售"],
        "key_metrics": ["客流", "RevPAR", "免税销售", "航班量"],
        "key_events": ["节假日预订", "政策放松", "客流拐点"],
        "default_invalidation_rules": ["客流不及预期", "价格回落"],
        "families": ["seasonal_calendar", "earnings_revision"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "real_estate_building_materials",
        "name_zh": "房地产 / 建材 / 家居链",
        "name_en": "Real Estate Building Materials",
        "description": "地产销售、竣工、建材、家居和政策信用。",
        "lead_assets": ["VNQ", "XHB"],
        "bridge_assets": ["1109.HK"],
        "local_assets": ["600048.SH", "000002.SZ", "000786.SZ"],
        "proxy_assets": ["512200.SH"],
        "upstream_nodes": ["信用", "土地", "水泥玻璃"],
        "downstream_nodes": ["销售", "竣工", "家居消费"],
        "key_metrics": ["销售面积", "拿地", "竣工", "价格"],
        "key_events": ["地产政策", "销售拐点", "信用支持"],
        "default_invalidation_rules": ["销售未改善", "信用收缩"],
        "families": ["policy_credit_fiscal", "inventory_destocking_cycle"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "hog_agriculture_seed",
        "name_zh": "猪周期 / 农业 / 饲料 / 种业",
        "name_en": "Hog Agriculture Feed Seed",
        "description": "猪价、能繁母猪、饲料、种业政策和农产品价格。",
        "lead_assets": ["TSN", "ADM", "CORN"],
        "bridge_assets": ["159865.SZ"],
        "local_assets": ["002714.SZ", "300498.SZ", "000876.SZ"],
        "proxy_assets": ["159865.SZ"],
        "upstream_nodes": ["玉米豆粕", "能繁母猪"],
        "downstream_nodes": ["养殖利润", "饲料销量"],
        "key_metrics": ["猪价", "能繁母猪", "养殖利润", "饲料价格"],
        "key_events": ["产能去化", "价格突破", "政策收储"],
        "default_invalidation_rules": ["产能去化不足", "猪价回落"],
        "families": ["inventory_destocking_cycle", "seasonal_calendar"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
    {
        "sector_id": "chemical_fertilizer_price_chain",
        "name_zh": "化工 / 农化 / 化肥 / 价格链行业",
        "name_en": "Chemical Fertilizer Price Chain",
        "description": "化工品价格、库存、开工率、化肥和农化需求。",
        "lead_assets": ["DOW", "NTR"],
        "bridge_assets": ["0547.HK"],
        "local_assets": ["600309.SH", "600426.SH", "002601.SZ"],
        "proxy_assets": ["516020.SH"],
        "upstream_nodes": ["油气煤", "库存", "开工率"],
        "downstream_nodes": ["价差", "利润", "出口"],
        "key_metrics": ["产品价格", "价差", "库存", "开工率"],
        "key_events": ["涨价", "装置检修", "出口订单"],
        "default_invalidation_rules": ["价差收窄", "库存上升"],
        "families": ["price_spread_pass_through", "inventory_destocking_cycle"],
        "source_requirements": SOURCE_REQUIREMENTS,
    },
]

DEFAULT_ENTITIES = [
    {"entity_id": "entity_nvidia", "name_zh": "NVIDIA", "name_en": "NVIDIA", "entity_type": "company", "sector_ids": ["ai_compute_infra"], "theme_ids": ["ai_compute_infra_default"], "description": "AI GPU 与加速计算龙头。"},
    {"entity_id": "entity_innlight", "name_zh": "中际旭创", "name_en": "Innolight", "entity_type": "company", "sector_ids": ["ai_compute_infra"], "theme_ids": ["ai_compute_infra_default"], "description": "光模块本地映射载体。"},
    {"entity_id": "entity_naura", "name_zh": "北方华创", "name_en": "NAURA", "entity_type": "company", "sector_ids": ["semiconductor_equipment"], "theme_ids": ["semiconductor_equipment_default"], "description": "半导体设备本地核心载体。"},
    {"entity_id": "entity_hengrui", "name_zh": "恒瑞医药", "name_en": "Hengrui Pharma", "entity_type": "company", "sector_ids": ["innovative_pharma"], "theme_ids": ["innovative_pharma_default"], "description": "创新药本地核心载体。"},
    {"entity_id": "entity_catl", "name_zh": "宁德时代", "name_en": "CATL", "entity_type": "company", "sector_ids": ["ev_battery", "grid_storage_power_equipment"], "theme_ids": ["ev_battery_default"], "description": "动力电池与储能龙头。"},
]

DEFAULT_INSTRUMENTS = [
    {"instrument_id": "inst_nvda_us", "entity_id": "entity_nvidia", "market": "US", "ticker": "NVDA", "name_zh": "NVIDIA", "name_en": "NVIDIA", "instrument_type": "stock", "role_defaults": ["leader"], "liquidity_bucket": "high"},
    {"instrument_id": "inst_300308_sz", "entity_id": "entity_innlight", "market": "A", "ticker": "300308.SZ", "name_zh": "中际旭创", "name_en": "Innolight", "instrument_type": "stock", "role_defaults": ["local", "bridge"], "liquidity_bucket": "high"},
    {"instrument_id": "inst_002371_sz", "entity_id": "entity_naura", "market": "A", "ticker": "002371.SZ", "name_zh": "北方华创", "name_en": "NAURA", "instrument_type": "stock", "role_defaults": ["local"], "liquidity_bucket": "high"},
    {"instrument_id": "inst_600276_sh", "entity_id": "entity_hengrui", "market": "A", "ticker": "600276.SH", "name_zh": "恒瑞医药", "name_en": "Hengrui Pharma", "instrument_type": "stock", "role_defaults": ["local"], "liquidity_bucket": "high"},
    {"instrument_id": "inst_300750_sz", "entity_id": "entity_catl", "market": "A", "ticker": "300750.SZ", "name_zh": "宁德时代", "name_en": "CATL", "instrument_type": "stock", "role_defaults": ["local", "leader"], "liquidity_bucket": "high"},
]

DEFAULT_MAPPINGS = [
    {
        "mapping_id": "map_nvda_innlight_ai_optics",
        "source_entity_id": "entity_nvidia",
        "target_entity_id": "entity_innlight",
        "source_instrument_id": "inst_nvda_us",
        "target_instrument_id": "inst_300308_sz",
        "sector_id": "ai_compute_infra",
        "mapping_type": "customer_capex_spillover",
        "confidence": 0.72,
        "evidence_requirements": SOURCE_REQUIREMENTS,
        "pollution_checks": ["必须验证光模块订单或云 capex", "不能只用海外股价涨跌桥接"],
    },
    {
        "mapping_id": "map_asml_naura_equipment",
        "source_entity_id": None,
        "target_entity_id": "entity_naura",
        "source_instrument_id": None,
        "target_instrument_id": "inst_002371_sz",
        "sector_id": "semiconductor_equipment",
        "mapping_type": "customer_capex_spillover",
        "confidence": 0.64,
        "evidence_requirements": SOURCE_REQUIREMENTS,
        "pollution_checks": ["必须验证本地晶圆厂 capex 或订单", "出口管制负面时不得高优先级"],
    },
]

DEFAULT_MODELS = [
    {
        "model_id": "leadership_breadth",
        "name_zh": "龙头扩散模型",
        "name_en": "Leadership Breadth",
        "family": "industry_transmission",
        "applicable_families": ["industry_transmission", "cross_market_mapping"],
        "required_inputs": ["leader_asset", "bridge_asset", "local_asset", "relative_strength"],
        "output_fields": ["actionability_score", "tradability_score", "mapped_variants"],
    },
    {
        "model_id": "event_spillover",
        "name_zh": "事件溢出模型",
        "name_en": "Event Spillover",
        "family": "event_calendar",
        "applicable_families": ["event_calendar", "clinical_approval_bd", "policy_credit_fiscal"],
        "required_inputs": ["event_class", "source_documents", "mapping_score"],
        "output_fields": ["tradability_class", "catalyst_window", "linked_theses"],
    },
    {
        "model_id": "source_lineage",
        "name_zh": "来源血缘模型",
        "name_en": "Source Lineage",
        "family": "source_quality",
        "applicable_families": ["all"],
        "required_inputs": ["source_catalog", "source_documents", "citations"],
        "output_fields": ["source_count", "live_source_count", "sample_source_count", "evidence_completeness"],
    },
]
