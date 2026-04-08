# 投资中枢胜率优化执行方案（可直接交给低成本代码模型）

## 1. 目标与边界

- 目标不是“盲目追求全市场 80% 胜率”，而是：
  - 在**明确定义的 setup 子策略**内，做到高胜率（局部可达 70%-80%+）。
  - 同时保持盈亏比和回撤可控，避免“高胜率低收益”。
- 核心 KPI：
  - `setup_out_of_sample_win_rate`
  - `profit_factor`
  - `max_drawdown`
  - `signal_to_execution_latency`
  - `data_freshness_score`

## 2. 第一性原理：影响胜率的关键因子

1. 市场环境是否匹配策略（Regime 匹配）。
2. 信号质量是否稳定（因子共振与失效条件）。
3. 执行是否纪律化（仓位、止损、减仓节奏）。
4. 数据是否新鲜且可信（本地快照 + 异常校验）。

对应打穿策略：
- 把首页从“看走势”改成“看驱动 + 看执行 + 看机会质量”。
- 市场状态页从单点指标改为四维状态：波动/流动性/资金/广度。
- 机会池从“列表”改为“可执行质量面板 + 入池/剔除闸门”。

## 3. 信息架构（已按本轮改造落地）

### 左侧菜单（决策优先）

- 决策中枢（`/investment/`）
- 研究证据（`/reports`）
- 自选标的（`/stocks`）
- 内容仪表盘（`/`）
- 系统设置（`/settings`）

### 决策中枢页面

- 中枢首页：胜率驱动因子 + 执行矩阵 + 机会池质量快照 + 指数与自选池。
- 市场状态：四维面板 + 行动提示 + 资金/利率图。
- 机会池：入池质量卡 + 风控闸门 + 基本面/行业/量化表。

## 4. 数据层技术方案

## 4.1 本地优先存储

- 核心 DB：`data/investment.db`（Windows 本地）。
- 实时结果落地：`market_snapshots`（overview/watch_stocks）。
- 页面默认读本地快照，过期再触发刷新。

## 4.2 决策聚合接口

- 新增/使用：`/investment/api/decision-center`
- 返回：
  - `drivers` / `drivers_map`
  - `action_matrix`
  - `opportunity_summary`
  - `data_health` / `storage`
  - `local_storage`（包含 DB 路径与存在性）

## 4.3 数据准确性校验

- 价格源交叉校验（Tencent/Sina/Yahoo）。
- 快照新鲜度评分 + 表健康评分。
- 数据异常触发降级（snapshot fallback）。

## 5. 执行清单（给 gpt-5.3-codex 的最小拆分）

## Phase A（已完成）

- [x] 首页替换三趋势图主位为决策面板。
- [x] 市场状态新增四维面板与行动提示区。
- [x] 机会池新增入池质量卡与风控闸门说明。
- [x] 左侧菜单语义重构为决策优先。
- [x] 前端接入 `/investment/api/decision-center` 渲染。

## Phase B（优先级 P0）

- [x] 新增 `signal_journal` 表：记录每次信号、触发因子、失效条件、执行动作。
- [x] 新增 `strategy_perf_daily` 表：按 setup 输出胜率、盈亏比、回撤、样本数。
- [x] 在策略执行页增加“近 20/60/120 样本”的稳定性分层展示。

## Phase C（优先级 P1）

- [ ] 机会池增加“事件催化”字段（财报、政策、产品周期）。
- [ ] 增加“策略-市场状态适配矩阵”自动校准（每周重估阈值）。
- [ ] 增加“实盘偏差”监控（信号价 vs 执行价）。

## 6. 给低成本模型的执行模板（控额度）

建议一次只做一个 Phase，避免上下文过长。

### Prompt 模板 1（前端增量）

> 在不改后端 schema 的前提下，只修改 `templates/investment.html`，为 `decision-center` 返回增加一个新展示区，包含 3 个卡片与 1 个表格。保持现有函数不删，只增量新增函数。最后给出变更点和新增 DOM id。

### Prompt 模板 2（后端单接口）

> 只修改 `app/routers/investment.py`，新增 `GET /investment/api/strategy/perf`，从 SQLite 读取 `strategy_perf_daily` 最近 120 天数据，返回 JSON。禁止改其它文件。完成后给出 curl 验证命令。

### Prompt 模板 3（部署与验证）

> 不改业务代码，仅输出 Windows 部署与回滚脚本检查清单。目标环境 `C:\\Users\\Administrator\\research_report_system`，端口 `8080`。必须包含 health、overview、decision-center 三个接口验收。

## 7. Windows 部署/验收

1. 同步代码到 Windows 目录：`C:\Users\Administrator\research_report_system`
2. 启动：`start_server_prod.bat`
3. 验收接口：
   - `/health`
   - `/investment/api/overview`
   - `/investment/api/decision-center`
   - `/investment/api/data-health/overview`
4. 页面验收：
   - `/investment/` 首页三块决策面板正常渲染。
   - 市场状态页四维面板与行动提示有值。
   - 机会池页入池质量卡有值。

## 8. 风险提示

- 80% 胜率只能在“窄定义 setup + 严格风控”下追求，不能当成全市场常态。
- 若数据新鲜度不足或快照过期，优先修数据再谈信号优化。
