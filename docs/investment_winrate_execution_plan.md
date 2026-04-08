# 投资中枢胜率优化执行清单（面向 GPT-5.3-Codex）

## 1. 目标与边界

- 目标：将系统从“信息展示”改为“可执行决策中枢”，提升样本外稳定收益能力。
- 边界：不承诺全周期总胜率固定 >80%，改为“分 Setup 局部胜率提升 + 盈亏比约束 + 回撤约束”。
- 约束：优先本地 Windows 数据读取，实时请求仅作为过期刷新和回退。

## 2. 第一性原理：影响胜率的关键因子

1. 市场环境正确率（Regime）
- 波动（VIX）、流动性（SHIBOR 利差）、资金（北向流向）、广度（涨跌家数比）。

2. 信号质量
- 机会池中 A/B 档候选占比、策略分数稳定性、风险标记密度。

3. 执行纪律
- 仓位分层、失效条件触发、数据不新鲜时自动降级。

4. 数据可信度
- 快照新鲜度、关键表完整性、实时与本地快照一致性。

## 3. 信息架构（菜单与页面）

- 左侧菜单优先级：
  - 决策中枢 `/investment/`
  - 研究证据 `/reports`
  - 自选标的 `/stocks`
  - 内容仪表盘 `/`
  - 系统设置 `/settings`

- 中枢首页核心区域：
  - 胜率驱动因子看板（6 因子）
  - 执行矩阵（场景 -> 仓位 -> 动作）
  - 机会池质量快照（A/B/C、买入占比、风险标记）

- 市场状态页：
  - 四维状态（波动/流动性/资金/广度）
  - 行动提示区（当前应做/不应做）
  - 历史趋势图降级为辅助证据。

- 机会池页：
  - 入池质量卡片
  - 入池规则/剔除规则
  - 基本面、行业、量化筛选与策略工作台联动。

## 4. 数据与存储策略

- 本地数据库：`data/investment.db`
- 快照键：
  - `investment.market_overview.v2`
  - `investment.watch_stocks.v2`
- 读取策略：
  - 默认命中本地快照（TTL 300s）
  - 过期后实时刷新并写回本地
  - 实时失败时回退本地快照

## 5. 已落地 API（当前版本）

1. `/investment/api/decision-center`
- 聚合返回：
  - `drivers`（胜率关键因子）
  - `action_matrix`（执行矩阵）
  - `opportunity_snapshot`（机会池质量）
  - `data_health`（健康与新鲜度）
  - `local_storage`（本地库校验）

2. `/investment/api/macro/overview`（增强）
- 新增：
  - `dimensions`（四维面板）
  - `regime`（状态/分数）
  - `breadth_ratio`、`limit_ratio`、`shibor_spread_1w`、`vix_band`

## 6. GPT-5.3-Codex 任务拆解（按优先级）

### P0（必须，先上线）

1. 前端中枢重构（templates）
- 文件：
  - `templates/investment.html`
  - `templates/base.html`
- 验收：
  - 首页三趋势图核心位被决策面板替换
  - 市场状态有四维面板 + 行动提示
  - 机会池顶部有质量卡 + 规则区
  - 左侧菜单完成“决策优先”重排

2. 联调聚合接口
- 读取 `/investment/api/decision-center`
- 渲染函数最少包含：
  - `loadDecisionCenter()`
  - `renderDriverBoard(data)`
  - `renderActionMatrix(data)`
  - `renderOpportunitySnapshot(data)`

### P1（高收益）

1. 数据准确性守护
- 增加一致性检查脚本：快照与关键表最新日期、字段缺失、异常值阈值。
- 输出告警到 `etl_logs` 或健康页问题列表。

2. 机会池评分标准固化
- 对 A/B/C 档增加硬门槛配置（JSON/TOML），避免手工漂移。

### P2（增强）

1. 策略归因闭环
- 新增信号命中率与盈亏比统计表（按 setup/行业/周期）。

2. 自动化任务
- 每日固定时段刷新快照和健康检查，异常自动告警。

## 7. 给 GPT-5.3-Codex 的标准提示词模板

```
请只修改以下文件：<文件列表>
目标：<明确业务目标>
必须实现：<功能点列表>
不要修改：<禁止修改范围>
验收标准：<可测试条件>
输出：1) 变更摘要 2) 变更文件 3) 关键函数和DOM id 4) 本地验证命令
```

## 8. 成本控制（防超额度）

- 每次只派一个明确子任务，避免“全量重写”。
- 上下文只提供相关文件片段，不传全仓库。
- 单次任务目标控制在 1~2 个文件。
- 先做 P0，再按结果决定是否进入 P1/P2。

## 9. 部署与验证

1. 启动（Windows）
- `start_server_prod.bat`

2. 核验顺序
- `/health`
- `/investment/api/overview`
- `/investment/api/decision-center`
- `/investment/api/macro/overview`
- `/investment/`

3. 重点检查
- 页面是否显示本地快照模式与更新时间
- 决策因子是否有值且颜色状态合理
- 机会池质量卡是否与策略执行候选一致

## 10. 风险提示

- 胜率优化必须与盈亏比、回撤联立评估，避免“高胜率低赔率”假象。
- 当 `data_health.summary.error_count > 0` 或 `storage_fresh_pct < 60` 时，系统应自动进入防守执行模式。
