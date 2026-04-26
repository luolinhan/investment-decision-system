# Lead-Lag V2 Gap Report

审计日期：2026-04-26

本报告基于当前仓库 `93fd946` 的真实代码、配置、样例数据和本地 `data/investment.db` schema。范围包括 `LeadLagService`、V2 辅助服务、路由、Research/Intelligence 持久化表、样例数据、测试和运维脚本。

## 当前已有功能

- 页面与 API：`/investment/lead-lag` 已接入 Operator/Builder 混合页面，核心 API 包括 `overview`、`decision-center`、`opportunity-queue`、`event-frontline`、`avoid-board`、`macro-bridge`、`transmission-workspace`、`replay-diagnostics`、`sector-evidence`、`briefs/{slot}`。
- 数据来源：`LeadLagService` 会加载 `sample_data/lead_lag/lead_lag_v1.json`，并在可用时融合 Radar snapshot、`intelligence_events`、`research_reports`。
- V2 机会卡：已有 `generation_status`、`decision_chain`、`stock_pool`、`model_discoveries`、`model_groups`、`source_count`、`cache_status`、`mapped_events`、`expected_review_times`。
- 事件相关性：`app/services/lead_lag_events.py` 已能输出 `event_class`、`china_mapping_score`、`tradability_score`、`evidence_quality`、`time_decay`、`relevance_score`，默认前台过滤 market-facing。
- 深证据与回放：已有 `lead_lag_sector_evidence.py`、`lead_lag_diagnostics.py`，覆盖 AI、创新药、半导体、光伏、猪周期五个样例赛道。
- 情报与研报库：`IntelligenceService.ensure_tables()` 已维护 `source_registry`、`raw_documents`、`intelligence_events`、`event_facts`、`event_entities`、`event_updates`、`research_reports`、`research_evidence`。
- 运行验证：当前 Windows 生产目录已治理到 `main == origin/main == 93fd946`，服务可运行。

## 明显缺口

1. 证据留痕不足
   - 当前 `raw_documents` 只有 URL、标题、摘要、raw_text 和 metadata，没有统一 document/chunk/citation/fact/link schema。
   - 机会卡只有 `source_count`、`source_quality`、`mapped_events` 摘要，没有可点击的本地归档路径、引用片段、解析状态、文件 hash。
   - `research_reports.original_asset_path` 存在但不是统一归档策略，缺 archive checksum、parser version、extraction quality。

2. Live / sample / fallback 混用
   - 本地服务调用 `opportunity_queue(limit=5)` 时，首张卡 `cache_status=sample_fallback` 但 `generation_status=actionable`。
   - `event_frontline` 返回的 market-facing 事件可以来自 `sample_data/lead_lag/lead_lag_v1.json`，source tier 为 `sample_fallback`。
   - 现有 UI/API 没有默认过滤 `sample_demo` / `fallback_placeholder` 的硬规则。

3. 机会重复展示
   - 当前 `opportunity_queue` 仍以 instrument/card 为主输出，`count=22`，同一 sector/thesis 下 ETF、A 股、港股、本地代理容易重复平铺。
   - `model_groups` 是模型分组，不是母 thesis 聚合；缺 Parent Thesis Card + Child Instrument Variant。

4. Entity 与 instrument 不分层
   - `DEFAULT_ASSET_NAMES` 和 `_asset_for()` 能补标的名称，但没有 `entity_registry` / `instrument_registry`。
   - 同一实体的 A/H/ADR/ETF/指数/期货无法稳定聚合，也无法在 Dossier 页统一反查。

5. 事件噪音仍会污染可交易层
   - V2 已有 `market-facing/research-facing`，但字段不完整；缺 `tradability_class`、`source_type`、`entity_mapping_score`、`catalyst_window`、`linked_documents`、`linked_theses`。
   - 弱中国映射、无本地流动性确认的事件仍可能通过 sector 逻辑进入前台。

6. 跨市场映射合理性不足
   - 当前映射主要由样例资产、sector assets 和启发式规则给出。
   - 缺 mapping registry、source requirement、bridge validation、污染报警。

7. 回放不完整
   - `replay_stats` 样例只有 5 条，V2 diagnostics 有 horizon/failure_mode 输出，但缺 1/3/5/10/20 日统一补数和 live-only vs mixed-source 对比。
   - unknown failure 的分类依据不足，不能解释无法分类原因。

8. 检查时间陈旧
   - `_expected_review_times()` 基于 last_update 生成字符串，没有 timezone-aware 校验。
   - 缺 stale checkpoint detection、过期提醒、自动滚动到下一个合法时点。

9. 报告中心还不是产品
   - `lead_lag_briefs.py` 可以导出固定时点 brief，但没有 DB reports/report_sections、FTS、引用反查、版本化和 diff。

10. 文档与约定冲突
   - `AGENTS.md` 仍写 GitHub 是 source of truth；用户已明确 Windows 生产目录是唯一标准代码源。
   - 仓库仍有旧外部编码/翻译相关脚本和说明，未集中标记 deprecated。

## 优先修复顺序

1. Evidence Vault schema + 本地文件归档 + 从现有 `raw_documents/research_reports` 回填。
2. live/sample/fallback 数据级标签和可执行 gating。
3. Parent Thesis Card + Child Instrument Variant 输出，先在后端 API 可用，再改 UI。
4. Entity / Instrument / Thesis / Mapping registry，先 seed 12 个可用主题模板。
5. Event Relevance Engine 2.0 字段补齐和 market-facing 硬规则。
6. Report Center schema + FTS + brief 入库。
7. Replay horizon / failure taxonomy 升级。
8. Dossier 页面和 UI 信息密度升级。

## 不建议现在做的事情

- 不建议重写 `LeadLagService`。当前 facade 已连接大量路由和测试，V3 应增量挂载服务模块。
- 不建议直接大改 `static/js/lead_lag.js`。先让 API 输出稳定的 V3 payload，再做前端切换。
- 不建议把历史样例数据删除。应保留为 demo，但必须标为 `sample_demo` 且默认不可执行。
- 不建议把行情时间序列塞进 SQLite。回放、因子、分数应转入 DuckDB/Parquet。
- 不建议继续扩展硬编码 `DEFAULT_ASSET_NAMES`。应迁移到 registry seed。

