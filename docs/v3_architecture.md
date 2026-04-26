# Lead-Lag V3 Architecture

目标：在当前 V2 基础上，把 Lead-Lag 从“会给分的研究面板”升级为“证据驱动、可归档、可扩展、可复核”的研究操作系统。

## 架构原则

- 增量升级，不重写 V2。
- Windows 生产目录是唯一标准代码源；GitHub 是镜像和协作远端。
- 生产运行不依赖任何交互式 coding 工具或外部 coding plan。
- 结论必须能反查证据、来源、归档文件和引用片段。
- sample/fallback 永远不能单独形成可执行机会。

## 分层

1. Evidence Vault
   - SQLite + FTS5 保存 source、document、chunk、citation、fact、archived link。
   - 原始 HTML/PDF/text/report 文件落地到 `data/archive/`。

2. Market Data Store
   - DuckDB + Parquet 保存行情、因子、模型分数、回放样本、统计查询。
   - 当前阶段先建立接口和目录约定，后续逐步迁移历史数据。

3. Opportunity Universe Registry
   - SQLite registry 保存 sector/theme/entity/instrument/mapping/model/thesis/event template。
   - 不再只靠硬编码五个赛道。

4. Entity-First Thesis Layer
   - Parent Thesis Card 聚合结论。
   - Child Instrument Variant 展示 A/H/US/ETF/期货/指数等载体差异。

5. Source Reliability & Lineage
   - 每条证据有 source tier、data_source_class、parser version、checksum、extraction quality。
   - 每张机会卡有 evidence panel 和 evidence checklist。

6. Report Center
   - reports/report_sections 入库，支持 FTS、引用反查、版本化、导出 Obsidian。

## 关键模块

- `app/services/evidence_vault.py`: schema、归档、文档入库、chunk、citation、fact、FTS。
- `app/services/opportunity_universe.py`: registry schema、12+ 主题模板 seed、entity/instrument/mapping 查询。
- `app/services/lead_lag_v3.py`: V3 opportunity projection、sample gating、parent thesis 聚合、evidence panel。
- `scripts/migrate_v3_evidence_vault.py`: 初始化 V3 schema 并从现有库回填。
- `scripts/migrate_v3_universe_registry.py`: 初始化机会宇宙 registry。

## API 演进

保留 V2 endpoint，新增 V3 payload 字段：

- `cards`: 默认 live-only / executable-safe 的 Parent Thesis Card。
- `sample_cards`: 样例和 fallback 卡，默认不进主队列。
- `blocked_cards`: 证据不足、sample-only、无映射、过期检查点等被拦截项。
- `parent_thesis_cards`: 母 thesis 卡列表。
- `source_quality_summary`: live/sample/fallback 占比、缺来源结论数、归档覆盖。

后续新增页面/API：

- `/investment/api/lead-lag/source-quality`
- `/investment/api/lead-lag/dossier/entity/{entity_id}`
- `/investment/api/lead-lag/dossier/instrument/{instrument_id}`
- `/investment/api/lead-lag/dossier/sector/{sector_id}`
- `/investment/api/reports/center`

## 验收路径

1. 迁移脚本初始化 schema。
2. 现有 raw/research 文档回填 Evidence Vault。
3. `opportunity_queue` 默认不返回 sample-only actionable。
4. V3 API 返回 parent thesis + variants。
5. Tests 覆盖 schema、gating、聚合、event class、report FTS。
6. Windows 生产目录运行验证。

