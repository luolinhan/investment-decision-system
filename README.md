# Investment Hub / Lead-Lag Alpha Engine

本仓库是 Investment Hub 的长期控制仓库，当前重点是把“领先-传导 Alpha 引擎”从 V2 研究面板升级为 V3 研究操作系统：证据驱动、可归档、可扩展、可复核。

Windows 生产目录是唯一运行标准代码源：

- 生产目录：`C:\Users\Administrator\research_report_system`
- 生产入口：`http://100.64.93.19:8080/investment/`
- Lead-Lag 页面：`http://100.64.93.19:8080/investment/lead-lag`

## 当前目标

V3 优先交付四件事：

- 证据入库：网页、报告、公告、链接、解析文本进入 Evidence Vault，并落地本地归档文件。
- 机会去重：机会队列支持“母 thesis + 子载体”，避免同一 thesis 被个股、ETF、A/H 载体重复平铺。
- live/sample 硬隔离：`sample_demo` / `fallback_placeholder` 不进入默认主队列，也不能标为可执行。
- 来源可见：机会卡、事件和报告可以看到原始链接、本地归档、摘要、引用片段和数据点。

## 技术栈

- 后端：FastAPI + Python 3.11
- 页面：Jinja2 templates + 静态 JS/CSS
- 主库：SQLite，Windows 本机生产使用
- V3 证据库：SQLite + FTS5，表结构见 `docs/evidence_vault_schema.md`
- V3 行情/回放规划：DuckDB + Parquet，设计见 `docs/v3_architecture.md`
- 测试：pytest
- 部署：Windows 常驻服务 `InvestmentHub8080`

## 启动方式

本地开发：

```bash
pip install -r requirements.txt
python app/main.py
```

Windows 生产：

```powershell
cd C:\Users\Administrator\research_report_system
python scripts\investment_job_runner.py validate
.\start_investment_hub.ps1
```

常用验证：

```bash
python3 -m py_compile app/services/lead_lag_service.py app/routers/investment.py
python3 -m pytest -q tests/test_lead_lag_api.py tests/test_lead_lag_v3.py
```

## 数据架构

V3 采用双存储思路：

- SQLite：证据档案、全文索引、引用关系、entity/instrument registry、事件与报告索引。
- DuckDB + Parquet：行情时间序列、因子快照、模型分数、回放和统计查询。
- 文件归档：原始 HTML/PDF/text/report/screenshot 落在 `data/archive/`，数据库保存路径、hash、抓取时间、URL、parser 版本和解析状态。

当前已落地的迁移脚本：

```bash
python3 scripts/migrate_v3_evidence_vault.py --json
python3 scripts/migrate_v3_opportunity_universe.py --json
```

## Evidence Vault

Evidence Vault 表包括：

- `source_catalog`
- `source_documents`
- `source_chunks`
- `citations`
- `extracted_facts`
- `archived_links`
- `reports`
- `report_sections`

本地归档目录：

- `data/archive/html/`
- `data/archive/pdf/`
- `data/archive/text/`
- `data/archive/reports/`
- `data/archive/screenshots/`

相关 API：

- `/investment/api/lead-lag/source-quality-lineage`
- `/investment/api/lead-lag/report-center`

## Live / Sample 隔离

V3 数据来源等级：

- `live_official`
- `live_public`
- `live_media`
- `user_curated`
- `generated_inference`
- `sample_demo`
- `fallback_placeholder`

执行规则：

- `sample_demo` / `fallback_placeholder` 默认隐藏，且不能进入“可执行”。
- `generated_inference` 不能单独构成高优先级机会。
- 可执行机会至少需要 2 个独立来源，且至少 1 个 live/official/public 来源。
- 调试样例层必须显式传 `include_sample=true`。

## 机会宇宙扩展

机会宇宙注册表由 `scripts/migrate_v3_opportunity_universe.py` 初始化，至少包含 25 个行业/主题模板，覆盖 AI、创新药、半导体、光伏、风电、电网储能、锂电、机器人、军工、金融、资源、能源、消费电子、地产链、农业、化工等方向。

核心表：

- `sector_registry`
- `theme_registry`
- `entity_registry`
- `instrument_registry`
- `mapping_registry`
- `model_registry`
- `thesis_registry`
- `event_template_registry`

相关 API：

- `/investment/api/lead-lag/opportunity-universe`
- `/investment/api/lead-lag/dossier/sector/{sector_id}`
- `/investment/api/lead-lag/dossier/entity/{entity_id}`
- `/investment/api/lead-lag/dossier/instrument/{instrument_id}`

## 报告中心

Report Center 使用 `reports` / `report_sections` 入库，支持全文检索、版本字段、引用关系和本地路径。

目标报告类型：

- 每日晨报、盘前 playbook、午间复核、收盘复盘
- 周报、月报、赛道专题
- Entity / Instrument dossier
- 事件前瞻、事件后复盘、failure postmortem

## 文档入口

- `docs/v2_gap_report.md`
- `docs/v3_architecture.md`
- `docs/evidence_vault_schema.md`
- `docs/source_archive_policy.md`
- `docs/source_reliability_rules.md`
- `docs/opportunity_universe_registry.md`
- `docs/entity_instrument_model.md`
- `docs/live_sample_separation.md`
- `docs/report_center_spec.md`
- `docs/replay_backfill_plan.md`
- `docs/ui_information_density_rules.md`
- `docs/deprecations.md`
- `worklog/codex_worklog.md`

旧的外部编码/翻译增强资产不再作为 V3 开发路径，状态见 `docs/deprecations.md`。不要在文档、脚本或日志中写入 API key、token、cookie 或数据库密码。

## 免责声明

本项目仅用于研究与辅助决策，不构成投资建议。请自行评估并承担风险。
