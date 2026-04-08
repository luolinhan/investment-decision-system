# 投资中枢重设计

## 1. 当前问题

- 页面层已经聚合了宏观、基本面、量化、研究和工作台，但数据链路仍然混合了实时请求、日更脚本和手工导入。
- `watch_list` 已经落在本地 SQLite，但实时行情层之前没有真正读取这张表，而是使用代码写死的自选池。
- Windows 本地库存在，但对“实时结果”的持久化只有内存缓存，没有进程重启后的本地快照层。
- 指标体系还停留在展示层，尚未形成“信号定义 -> 回测验证 -> 实盘跟踪 -> 归因复盘”的闭环。

## 2. 目标架构

采用四层结构：

1. 数据采集层
   - 外部行情、宏观利率、新闻、研究入口、财务数据。
   - 采集任务全部异步化或定时化，不直接绑在页面请求上。

2. 本地数据层
   - `investment.db` 作为 Windows 本地核心读库。
   - 日频和分钟级快照统一落本地。
   - 所有页面默认先读本地，只有本地快照过期时才触发刷新。

3. 决策引擎层
   - 宏观 regime、行业轮动、个股六因子、事件催化、风险闸门分开计算。
   - 输出统一的信号卡：`setup`、`action`、`score`、`risk_flags`、`invalid_conditions`。

4. 应用展示层
   - 仪表盘负责“状态感知”。
   - 工作台负责“候选排序与跟踪”。
   - 回测页负责“策略验证”。
   - 数据管理页负责“可观察性和可修复性”。

## 3. 数据分层建议

### 3.1 静态主数据

- 股票池
- 行业映射
- 策略标签
- 信号标签

### 3.2 日频事实表

- `stock_financial`
- `valuation_bands`
- `technical_indicators`
- `north_money`
- `market_sentiment`
- `interest_rates`
- `vix_history`

### 3.3 近实时快照表

- `market_snapshots`
- 后续建议补：
  - `quote_snapshots`
  - `sector_snapshots`
  - `signal_snapshots`

### 3.4 策略验证表

- `strategy_backtest_stats`
- `trade_log`
- `signal_hit_ratio`
- `strategy_drawdown_series`

## 4. 仪表盘设计原则

- 第一屏不再堆指标，而是回答四个问题：
  - 现在是风险扩张还是风险收缩？
  - 哪个市场/行业最强？
  - 哪些自选标的进入可执行区？
  - 当前数据是否新鲜可信？

- 自选池必须展示：
  - 市场
  - 分类
  - 权重
  - 最新报价
  - 估值/盈利质量
  - 数据来源与更新时间

- 页面所有关键区块必须能说明“这条数据来自本地库还是实时刷新”。

## 5. 决策引擎设计

### 5.1 宏观 Regime

- 输入：
  - 利率曲线
  - 北向资金
  - VIX
  - 市场广度
  - 海外指数方向

- 输出：
  - `risk_on`
  - `neutral`
  - `risk_off`

### 5.2 个股评分

建议统一成六因子：

- `quality`
- `growth`
- `valuation`
- `flow`
- `technical`
- `risk`

总分不直接拿来下单，需要再通过：

- 行业过滤
- 催化过滤
- 估值过滤
- 风险闸门

## 6. 胜率目标的正确拆法

“胜率超过 80%”不能直接当成系统总目标，否则很容易为了提高命中率而牺牲赔率和容量。

建议拆成四组指标：

1. 信号质量
   - 分 setup 的样本内胜率
   - 分 setup 的样本外胜率
   - 分行业的稳定性

2. 收益质量
   - 平均盈亏比
   - 收益回撤比
   - 持仓周期收益密度

3. 风险质量
   - 最大回撤
   - 连续亏损次数
   - 失效条件触发率

4. 执行质量
   - 数据延迟
   - 信号刷新延迟
   - 实盘与回测偏差

更合理的主目标是：

- 样本外胜率持续高于 55%-65%
- 平均盈亏比大于 1.5
- 最大回撤在可承受区间内
- 关键 setup 在充足样本下局部接近或超过 80%

## 7. 下一阶段落地顺序

1. 完成本地快照层全覆盖
2. 将自选池、量化候选池、事件池统一成可配置池
3. 新增信号回测与命中率归因表
4. 在工作台中展示“为什么入选、为什么失效、什么时候复核”
5. 加上策略健康监控和自动告警

## 8. 本轮已落地

- 关注股票改为读取本地 `watch_list`
- 支持 `A/HK/US` 三类观察池
- 实时结果写入 Windows 本地 `investment.db` 的 `market_snapshots`
- 仪表盘展示本地快照状态、覆盖率和分市场观察池

## 9. 部署后验证清单

部署脚本：`start_server_prod.bat`

执行后按下面顺序验证：

1. 服务健康检查
   - 访问：`http://127.0.0.1:8080/health`
   - 预期：返回 JSON，且 `status` 为 `healthy`

2. 中枢总览接口
   - 访问：`http://127.0.0.1:8080/investment/api/overview`
   - 预期：返回 `indices`、`rates`、`sentiment`、`watch_stocks`
   - 重点看：`watch_stocks.storage` 存在，`mode/source/updated_at` 合理

3. 关注股票接口
   - 访问：`http://127.0.0.1:8080/investment/api/watch-stocks`
   - 预期：返回 `a_stocks`、`hk_stocks`、`us_stocks`、`summary`
   - 重点看：`summary.coverage_pct` 非异常（明显低于预期时需排查行情源）

4. 数据健康接口
   - 访问：`http://127.0.0.1:8080/investment/api/data-health/overview`
   - 预期：返回 `summary`、`storage`、`tables`、`issues`
   - 重点看：`summary.error_count=0` 或错误可解释，`issues` 无高危缺数

5. 页面访问
   - 访问：`http://127.0.0.1:8080/investment/`
   - 预期：页面可打开，顶部菜单可切换，关键卡片非空白，控制台无持续报错

6. 日志检查
   - 查看：`logs/server.log`
   - 预期：无连续 traceback；若接口失败，能定位到具体模块与报错原因

## 10. 最小化回滚

当新版本出现故障且需快速恢复：

1. 停止当前 Python/Uvicorn 进程。
2. 恢复上一版 `start_server_prod.bat` 与应用代码。
3. 保留 `data/investment.db` 与 `logs/`，不要先删库。
4. 重新启动并先跑第 9 节的 `health` 与 `overview`。

建议保留一个可直接回滚的压缩包（代码 + 启动脚本），确保 5 分钟内可恢复服务。
