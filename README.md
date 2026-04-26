# 中枢研判 / Research Report System

面向投资研判的一体化平台，集成研报与事件催化、机会池评分、市场状态看板、分钟级刷新与策略辅助输出。后端基于 FastAPI，数据存储在本地 SQLite，面向 Windows 环境部署优化。

## Codex 长期维护入口

这个仓库已经作为 Codex 的长期项目工作区使用，推荐入口如下：

- 项目级规则：`AGENTS.md`
- Claude 说明：`CLAUDE.md`
- 长期状态面板：`PROJECT_STATUS.md`
- 主控启动模板：`/Users/lhluo/agent-workspaces/templates/investment-controller-kickoff.md`
- Worker 分发模板：`/Users/lhluo/agent-workspaces/templates/investment-worker-dispatch.md`
- Worker 任务卡：`/Users/lhluo/agent-workspaces/templates/investment-worker-task.md`
- Codex 主控入口：`investment-codex`
- 默认执行层：Codex 内置 worker agent，编码模型固定 `gpt-5.3-codex`
- 外部百炼 / Claude worker 入口已删除，不再作为本项目 agent 架构

推荐流程：

1. 在本仓库目录启动 `investment-codex`
2. 先看 `PROJECT_STATUS.md`
3. 需要并行开发时，使用 Codex 内置 worker agent，并把编码类子 agent 的模型设为 `gpt-5.3-codex`
4. 集成、测试、部署和验收始终在 control 仓库完成

## 功能概览
- 研报与事件催化库：研报抓取、检索、关联股票
- 机会池与评分：质量、增长、估值、技术、确认、催化、风险、覆盖度综合评分
- 市场状态看板：宏观、情绪、指数、流动性等参考指标聚合
- 重大事项情报雷达：官方文档、模型仓库、可信媒体和深度材料持续探听入库，页面只读本地事件档案，并提供中英双语展示
- 分钟级刷新：盘中/盘前数据更新与策略快照
- Lead-Lag Alpha Engine V1：领先-传导-验证引擎，提供 Model Library、Opportunity Board、Cross-Market Map、Industry Transmission、Replay & Validation、Obsidian Research Memory
- Lead-Lag Alpha Engine V2：从研究展示台升级为研究操作系统，默认首页为 Decision Center / Opportunity Queue / Event Frontline / Avoid Board，Builder Mode 提供图谱、回放、深证据和行动记忆诊断

## Lead-Lag Alpha Engine V1

当前仓库内的 Lead-Lag V1 已具备一个可运行闭环：

- 页面入口：`/investment/lead-lag`
- API 前缀：`/investment/api/lead-lag/*`
- 服务实现：`app/services/lead_lag_service.py`
- 样例数据：`sample_data/lead_lag/lead_lag_v1.json`
- 导出脚本：`scripts/export_lead_lag_report.py`
- Aliyun 快照脚本：`scripts/lead_lag_aliyun_collector.py`
- 前置采集/翻译任务：`scripts/lead_lag_pretranslate_task.ps1`
- Windows demo 启动：`scripts/start_lead_lag_demo.ps1`

V1 采用 live-fusion + sample fallback 方式交付：优先读取本地 Radar snapshot、Intelligence 事件库、Research 研报库，并在 live 数据不可用时回退到 `sample_data/lead_lag/lead_lag_v1.json`。Opportunity / Event 输出必须带 `asset_code`、`asset_name`、`market`、`source_url`、`updated_at` 和 `evidence_sources`；live Intelligence / Research 只纳入免费公开可信来源，并在 `live_source_health` 暴露 `quality_filter=free_public_reliable`。

Windows 已配置：
- `InvestmentHub8080`: 开机自启 Investment Hub 8080 服务。
- `LeadLagPretranslate`: 每 5 小时执行一次前置采集和百炼翻译，默认跳过 Shortline 翻译以避免已知 SQLite 锁库问题。

Lead-Lag live evidence should prioritize free, replayable sources in this order: official exchanges and issuer disclosures, regulators, company announcements, official macro data, public research indexes, and only then search enrichment. See `config/source_matrix.example.yaml` and `docs/source_matrix.md`.

## Lead-Lag Alpha Engine V2

V2 的目标是让系统优先回答日常研究操作问题：今天先看什么、为什么现在看、走到第几棒、还缺什么验证、什么条件立刻失效。

当前 V2 已完成审计和契约设计，后续增量实现必须以这些文件为准：

- `docs/v1_gap_report.md`: 当前 V1 的真实缺口。
- `docs/v2_blueprint.md`: Operator Mode / Builder Mode、API、阶段计划和验收。
- `docs/action_schema.md`: OpportunityCard、DecisionCenter、EventRelevance 和评分契约。
- `docs/event_relevance_rules.md`: market-facing / research-facing 事件分类和默认降噪规则。
- `docs/data_source_matrix_v2.md`: 官方/公开/低门槛 provider 合约。
- `docs/research_ops_workflow.md`: 06:00、08:20、11:40、15:15、21:30 固定简报工作流。

V2 开发顺序：先 OpportunityCard 和 Event Relevance Engine，再接 Operator Mode 首页，之后补 Macro / External / HK 桥接、五个重点行业深证据、Graph/Replay/Memory 诊断和固定简报。

当前 V2 闭环状态：

- 已完成：Decision Center、Opportunity Queue、Event Frontline、Avoid Board、What Changed、Macro / External / HK Bridge、固定时点简报生成。
- 已完成：OpportunityCard schema、EventRelevance 降噪、配置化 V2 scoring、Operator Mode 首页。
- 已完成：五个重点行业深证据、Transmission Graph Workspace、Replay Diagnostics、Obsidian action memory。
- 已完成：Windows 五个 Lead-Lag brief 计划任务脚本，生产落地通过 `scripts/setup_lead_lag_brief_tasks.ps1` 注册任务，并由 `scripts/run_lead_lag_brief_task.ps1` 执行实际导出。

## Codex 内置 Agent 协作

- Codex 负责：架构、接口契约、任务拆分、代码审查、验收、回归测试、部署与 worklog。
- Codex 内置 worker agent 负责：大块代码生成、样板脚手架、多文件改造、测试补充、UI 接线与批量机械工作。
- 编码、修 bug、补测试、重构类 worker 必须使用 `gpt-5.3-codex`，不要让 worker 默认继承主控的 `gpt-5.5`。
- 外部百炼 / Claude worker 调度脚本已删除；生产翻译/摘要等百炼业务代码不属于 agent 架构。
- V2 中新增 100 行以上代码、多文件重构、UI 页面搭建、测试补充、provider 样板和 lint/type 修复，优先拆给 Codex 内置 worker；Codex 不盲信产出，必须审查后合入。

## 目录结构
- `app/` FastAPI 应用入口与路由、服务层
- `data/` SQLite 数据库与导出文件，含 `investment.db` 与 `reports.db`
- `quant_workbench/` 量化工作台前端与资源
- `templates/` 后端渲染模板
- `static/` 静态资源

## 环境要求
- Python 3.11+
- Windows 部署默认路径 `C:\Users\Administrator\research_report_system`
- 依赖安装：`pip install -r requirements.txt`

## 快速开始（本地）
1. 安装依赖：`pip install -r requirements.txt`
2. 配置环境：复制 `.env.example` 为 `.env` 并填入必要配置
3. 初始化数据库：`python init_investment_db.py`
4. 启动服务：`python app/main.py`
5. 打开服务：`http://127.0.0.1:8080`

Lead-Lag demo:

```bash
python3 scripts/export_lead_lag_report.py --type daily
python3 scripts/lead_lag_aliyun_collector.py --pretty
python3 -m pytest -q tests/test_lead_lag_service.py tests/test_lead_lag_api.py
```

## Windows 部署
- 启动服务：`start_server.bat` 或 `start_server_detached.ps1`
- 生产启动：`start_investment_hub.ps1`
- 后台服务包装：`start_uvicorn_service.bat`
- 量化工作台：`start_quant_workbench.bat`
- 定时任务：`setup_scheduler_v2.ps1`

## 数据刷新与脚本
- 分钟级刷新：`minute_runtime_refresh.py`
- 行情同步：`sync_market_akshare.py`
- 工作台同步：`sync_quant_workbench.py`
- 核心池财报回填：`refresh_core_pool_fundamentals.py`
- 核心池技术指标：`refresh_core_pool_technical.py`
- 研报补齐：`collect_core_pool_reports.py`
- 空壳财报清理：`cleanup_empty_financials.py`
- CSV 导入：`import_csvs_to_db.py`
- 研报库合并：`merge_reports_db.py`
- 重大事项探听：`scripts/sync_intelligence.py`
- 重大事项翻译：`scripts/translate_intelligence.py`
- Windows 持续探听任务入口：`intelligence_probe_task.bat`
- 阿里云海外采集回灌入口：`intelligence_aliyun_probe_task.bat`
- Lead-Lag 报告导出：`scripts/export_lead_lag_report.py`
- Lead-Lag Aliyun 快照：`scripts/lead_lag_aliyun_collector.py`
- Lead-Lag demo 启动：`scripts/start_lead_lag_demo.ps1`
- Lead-Lag snapshot 同步：`scripts/sync_lead_lag_aliyun_snapshot.ps1`
- Lead-Lag 前置采集/翻译：`scripts/lead_lag_pretranslate_task.ps1`
- 统一任务编排器：`scripts/investment_job_runner.py`（清单见 `config/job_manifest.example.json`）

## 百炼接入
- 配置 `BAILIAN_API_KEY` 后自动启用翻译、摘要、结构化抽取
- 未配置密钥时自动降级为规则分析，保证流程可用
- 不要把真实 token 写入文档、示例 YAML、脚本或日志；仅使用 `.env.local`、`.env` 或 Windows Task Scheduler 环境变量注入
- 配置项（在 `.env` 或 `.env.local` 中设置）：
  - `BAILIAN_API_KEY`: 百炼 API 密钥
  - `BAILIAN_BASE_URL`: API 基础 URL（默认 `https://coding.dashscope.aliyuncs.com/v1`）
  - `BAILIAN_MODEL`: 翻译/结构化模型（默认 `qwen3-coder-plus`）

## 核心数据表
- `stock_pool_constituents` 核心池成分
- `stock_financial` 财报与关键财务指标
- `valuation_bands` 估值带与估值水平
- `technical_indicators` 技术指标与趋势信号
- `stock_factor_snapshot` 因子快照
- `reports` 研报库（位于 `reports.db`）
- `source_registry` 情报源注册和健康状态
- `raw_documents` 原始网页、模型仓库、API 响应和深度材料
- `intelligence_events` 重大事项主表
- `event_facts` 结构化事实基础
- `event_entities` 公司、产品、行业和影响链实体
- `research_reports` 研报、system card、模型卡和官方深度材料

## API 入口
- 健康检查：`/health`
- 首页与仪表盘：`/investment/`
- 情报雷达页面：`/investment/intelligence`
- Lead-Lag 页面：`/investment/lead-lag`
- 机会池概览：`/investment/api/opportunity-pools/overview`
- 情报雷达 API：`/investment/api/intelligence/hub`
- Lead-Lag API：`/investment/api/lead-lag/overview`

## 常见问题
- AkShare 接口被代理阻断时，优先清理系统代理或使用 `sync_no_proxy.bat`。
- “先补数据”提示通常由财报、估值、技术、因子、研报覆盖不足导致，需补齐对应表。

## 配置与安全
`.env` 中可配置端口、日志级别、API 密钥等敏感信息。建议不要提交密钥到仓库。

## 免责声明
本项目用于研究与辅助决策，不构成任何投资建议。请自行评估并承担风险。

## 文档索引

### 投资研判模块
- **运维维护入口**：[`docs/ops_maintenance.md`](docs/ops_maintenance.md)
- **架构重设计**：[`docs/investment_architecture_redesign.md`](docs/investment_architecture_redesign.md)
- **胜率优化执行计划**：[`docs/winrate_optimization_execution_plan.md`](docs/winrate_optimization_execution_plan.md)
- **投资胜率执行计划**：[`docs/investment_winrate_execution_plan.md`](docs/investment_winrate_execution_plan.md)
- **v2026-03-25 发布说明**：[`docs/release_v2026-03-25.md`](docs/release_v2026-03-25.md)
