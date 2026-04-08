# 中枢研判 / Research Report System

面向投资研判的一体化平台，集成研报与事件催化、机会池评分、市场状态看板、分钟级刷新与策略辅助输出。后端基于 FastAPI，数据存储在本地 SQLite，面向 Windows 环境部署优化。

## 功能概览
- 研报与事件催化库：研报抓取、检索、关联股票
- 机会池与评分：质量、增长、估值、技术、确认、催化、风险、覆盖度综合评分
- 市场状态看板：宏观、情绪、指数、流动性等参考指标聚合
- 分钟级刷新：盘中/盘前数据更新与策略快照
- 海外研报：六个月滚动窗口采集、原文归档、百炼翻译与结构化分析

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
2. 配置环境：复制 `.env.local.example` 为 `.env` 并填入必要配置
3. 初始化数据库：`python init_investment_db.py`
4. 启动服务：`python app/main.py`
5. 打开服务：`http://127.0.0.1:8080`

## Windows 部署
- 启动服务：`start_server.bat` 或 `start_server_detached.ps1`
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
- 海外研报同步：`foreign_research_sync.py`

## 海外研报模块
- 页面入口：`/foreign-research/`
- API 入口：
  - `GET /foreign-research/api/summary`
  - `GET /foreign-research/api/sources`
  - `GET /foreign-research/api/documents`
  - `POST /foreign-research/api/ingest/url`
  - `POST /foreign-research/api/ingest/file`
  - `POST /foreign-research/api/crawl/{source_id}`
  - `POST /foreign-research/api/analyze/{document_id}`
- 数据保留：
  - 仅保留最近 180 天原文
  - Windows 负责最终存储与分析
  - 阿里云只做采集中转

## 百炼接入
- 配置 `BAILIAN_API_KEY` 后自动启用翻译、摘要、结构化抽取
- 未配置密钥时自动降级为规则分析，保证流程可用
- 海外研报模块配置项（在 `.env` 或 `.env.local` 中设置）：
  - `BAILIAN_API_KEY`: 百炼 API 密钥
  - `BAILIAN_BASE_URL`: API 基础 URL（默认 `https://coding.dashscope.aliyuncs.com/v1`）
  - `FOREIGN_RESEARCH_MODEL`: 使用的模型（默认 `glm-5`）
  - `FOREIGN_RESEARCH_TIMEOUT_SECONDS`: API 超时秒数（默认 45）
  - `FOREIGN_RESEARCH_RETENTION_DAYS`: 文档保留天数（默认 180）

## 核心数据表
- `stock_pool_constituents` 核心池成分
- `stock_financial` 财报与关键财务指标
- `valuation_bands` 估值带与估值水平
- `technical_indicators` 技术指标与趋势信号
- `stock_factor_snapshot` 因子快照
- `reports` 研报库（位于 `reports.db`）

## API 入口
- 健康检查：`/health`
- 首页与仪表盘：`/investment/`
- 机会池概览：`/investment/api/opportunity-pools/overview`

## 常见问题
- AkShare 接口被代理阻断时，优先清理系统代理或使用 `sync_no_proxy.bat`。
- “先补数据”提示通常由财报、估值、技术、因子、研报覆盖不足导致，需补齐对应表。

## 配置与安全
`.env` 中可配置端口、日志级别、API 密钥等敏感信息。建议不要提交密钥到仓库。

## 免责声明
本项目用于研究与辅助决策，不构成任何投资建议。请自行评估并承担风险。

## 文档索引

### 海外研报模块
- **收口任务清单（距离可用状态）**：[`docs/foreign_research_gap_closure_tasks_2026-03-29.md`](docs/foreign_research_gap_closure_tasks_2026-03-29.md)
  - 包含：当前阻塞问题、优先级任务拆解、验收标准、建议执行顺序
  - 入口路径清晰写在文档顶部，优先让模型读取这份文档再改代码
- **完整交接文档（低成本模型执行版）**：[`docs/foreign_research_handoff_complete_2026-03-29.md`](docs/foreign_research_handoff_complete_2026-03-29.md)
  - 包含：核心逻辑框架、实现方式详解、任务拆解、验收标准、风险说明
  - 适用场景：低成本模型接力开发
- **运行手册**：[`docs/foreign_research_runbook.md`](docs/foreign_research_runbook.md)
  - 定时任务配置、批量导入脚本、常见问题排查
- **首版交接文档**：[`docs/foreign_research_handoff_2026-03-29.md`](docs/foreign_research_handoff_2026-03-29.md)
- **首版发布说明**：[`docs/release_v2026-03-28_foreign_research.md`](docs/release_v2026-03-28_foreign_research.md)

### 批量导入脚本
- 来源批量导入：[`scripts/seed_foreign_sources.py`](scripts/seed_foreign_sources.py)
- 文档批量导入：[`scripts/seed_foreign_documents.py`](scripts/seed_foreign_documents.py)
- 每日增量任务：[`scripts/foreign_research_daily.bat`](scripts/foreign_research_daily.bat)
- 每周清理任务：[`scripts/foreign_research_weekly.bat`](scripts/foreign_research_weekly.bat)

### 投资研判模块
- **架构重设计**：[`docs/investment_architecture_redesign.md`](docs/investment_architecture_redesign.md)
- **胜率优化执行计划**：[`docs/winrate_optimization_execution_plan.md`](docs/winrate_optimization_execution_plan.md)
- **投资胜率执行计划**：[`docs/investment_winrate_execution_plan.md`](docs/investment_winrate_execution_plan.md)
- **v2026-03-25 发布说明**：[`docs/release_v2026-03-25.md`](docs/release_v2026-03-25.md)
