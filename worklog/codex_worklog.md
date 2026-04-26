# Codex Worklog

## 2026-04-26 V3 Kickoff

### 阶段 1：仓库审计

- 读取长期知识库入口、项目 AGENTS、PROJECT_STATUS。
- 确认当前代码基准：`93fd946`，本地和 `origin/main` 一致。
- 审计 `LeadLagService`、V2 辅助模块、路由、配置、样例数据、SQLite schema。
- 发现 P0：`sample_fallback` 机会卡仍可被标为 `actionable`。
- 发现 P0：已有 `raw_documents/research_reports`，但缺 V3 Evidence Vault schema、FTS、引用和归档路径。
- 发现 P1：机会仍以 instrument/card 平铺，缺母 thesis + 子载体聚合。
- 发现 P1：entity/instrument/mapping registry 缺失。

### 阶段 2：文档

- 新增 V3 审计与架构文档集。
- 新增 deprecated 旧资产说明。

### 下一步

- 实现 Evidence Vault schema 和迁移脚本。
- 实现 Opportunity Universe registry seed。
- 为 `opportunity_queue` 增加 sample/fallback gating 和 parent thesis projection。

### 阶段 3：V3 基础层落地

- 新增 `app/services/evidence_vault.py`：
  - 创建 `source_catalog/source_documents/source_chunks/citations/extracted_facts/archived_links/reports/report_sections`。
  - 支持本地文本归档、chunk、中文 LIKE 搜索 fallback、报告搜索、URL 证据面板。
  - 支持从现有 `raw_documents` 和 `research_reports` 回填。
- 新增 `scripts/migrate_v3_evidence_vault.py`，可创建 schema 并输出 `ETL_METRICS_JSON`。
- 新增 `app/services/opportunity_universe.py`：
  - 创建 sector/theme/entity/instrument/mapping/model/thesis/event-template registry。
  - 种子行业/主题模板 25 个。
- 新增 `scripts/migrate_v3_opportunity_universe.py`。
- 新增 `app/services/lead_lag_v3.py`：
  - 给机会卡补 `data_source_class`、`live_source_count`、`sample_source_count`、`execution_blockers`、`evidence_panel`、`evidence_checklist`、`next_review_time`。
  - 将 sample/fallback actionable 强制降级。
  - 输出 `parent_thesis_cards` 和 `child_variants`。
  - 事件默认按 V3 规则降噪，sample 事件降为 research-facing/archive-only。
- 新增 V3 API：
  - `/investment/api/lead-lag/source-quality-lineage`
  - `/investment/api/lead-lag/report-center`
  - `/investment/api/lead-lag/opportunity-universe`
  - `/investment/api/lead-lag/dossier/sector/{sector_id}`
  - `/investment/api/lead-lag/dossier/entity/{entity_id}`
  - `/investment/api/lead-lag/dossier/instrument/{instrument_id}`

### 阶段 4：本地验证

- `python3 -m py_compile app/services/evidence_vault.py app/services/opportunity_universe.py app/services/lead_lag_v3.py app/services/lead_lag_service.py app/routers/investment.py scripts/migrate_v3_evidence_vault.py scripts/migrate_v3_opportunity_universe.py`：通过。
- `python3 -m pytest -q tests/test_evidence_vault.py tests/test_opportunity_universe.py tests/test_lead_lag_v3.py tests/test_lead_lag_v2_schema.py tests/test_lead_lag_event_relevance.py tests/test_lead_lag_api.py`：15 passed。
- 首次全量 `python3 -m pytest -q` 暴露历史手工探针 `scripts/test_v2.py` 与根目录 `test_db.py` 被 pytest 误收集，且依赖本机不存在表。
- 新增 `pytest.ini`，限定正式测试入口为 `tests/`。
- 再次执行 `python3 -m pytest -q`：60 passed，5 warnings。
- 本地执行 `python3 scripts/migrate_v3_evidence_vault.py --json`：
  - 回填 `raw_documents=214`、`research_reports=116`、`sources=40`。
  - Evidence Vault `document_count=330`、`archived_document_count=330`、`fts_available=true`。
  - 当前 `parse_failure_rate=0.3515`，主要来自旧 `research_reports.original_asset_status`，后续需单独治理。
- 本地执行 `python3 scripts/migrate_v3_opportunity_universe.py --json`：
  - `sector_registry=25`、`theme_registry=25`、`thesis_registry=25`、`event_template_registry=25`。
- 本地执行 `python3 scripts/investment_job_runner.py validate`：`manifest ok`。

### 剩余

- 在 Windows 生产库备份后执行 V3 migrations。
- Windows 重启服务并 smoke。
- GitHub 提交与生产目录对齐。
- 回写长期知识库。

### 阶段 5：生产迁移排障记录

- Windows 执行迁移时首次遇到 `sqlite3.OperationalError: database is locked`。
- 定位到后台 `scripts\\translate_intelligence.py` 持有 Python 进程，终止后迁移成功。
- 生产 smoke 发现 `/investment/api/lead-lag/opportunity-universe` 在 GET 路径里尝试 seed 写库，服务运行时可能再次遇到 SQLite 锁。
- 修复：registry/dossier API 优先只读已迁移表，缺表时才初始化。
- 修复后本地 `python3 -m pytest -q`：60 passed，5 warnings。
