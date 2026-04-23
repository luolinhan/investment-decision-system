# Quant Workbench 高胜率升级方案

## 1. 目标定义

### 1.1 业务目标
- 将 `quant_workbench` 从“机会排序看板”升级为“可验证的策略决策系统”。
- 围绕少数高确定性 setup 追求高胜率，而不是对全市场做统一打分。
- 提升系统输出的可执行性，最终输出不仅包括候选标的，还包括：
  - 入场动作
  - 建议仓位
  - 核心逻辑
  - 失效条件
  - 复核时间

### 1.2 成功标准
- 不以“全市场 80% 胜率”作为目标，这在统计上不稳健。
- 采用“特定市场 + 特定信号 + 特定持有期”的策略定义，例如：
  - 财报超预期 + 预期上修 + 资金确认，持有 5-10 个交易日
  - 低估值 + 景气回升 + 结构突破，持有 10-20 个交易日
  - 风险偏好改善 + 行业龙头回踩确认，持有 5-15 个交易日
- 每个 setup 分别统计：
  - 胜率
  - 盈亏比
  - 最大回撤
  - 平均持有期
  - 样本数

### 1.3 约束条件
- 当前系统以 Python + FastAPI + SQLite + Parquet/DuckDB 为主。
- 已有数据层较丰富，但工作台消费层明显偏弱。
- 用户明确希望先完成分析、方案、架构和可行性设计，再交由中等能力模型继续编码部署。

## 2. 当前系统现状

### 2.1 已有能力
- 量化工作台已有技术面、简单基本面、研报标题情绪和宏观 regime。
- 现有机会列表主要由以下信息构成：
  - 趋势/结构：[quant_workbench/service.py](/Users/lhluo/research_report_system/quant_workbench/service.py)
  - PE/PB/ROE：`stock_financial`
  - 标题情绪：`reports`
  - 简单宏观：HSI/YINN/YANG/VIX
- 数据库已经预留多个高级模块：
  - `stock_financial`
  - `valuation_percentile` / `valuation_bands`
  - `technical_indicators`
  - `interest_rates`
  - `market_sentiment`
  - `north_money`
  - `sector_tmt`
  - `sector_biotech`
  - `sector_consumer`
  - `etl_logs`

### 2.2 当前关键问题
- 当前工作台只消费了预留能力中的少数几个字段。
- 缺少“预期差、事件、资金、行业先行指标、交易标签”等决定胜率的核心信息。
- 当前评分是“排序逻辑”，还不是“策略逻辑”。
- 缺少完整回测和标签定义，无法证明所谓高胜率。
- 缺少 setup 级别的策略约束，容易把不同类型标的强行混排。

## 3. 需求分析

### 3.1 核心业务需求
- 系统需要支持“高胜率 setup”而不是“统一总分”。
- 每个候选标的必须能回答五个问题：
  - 为什么做
  - 什么时候做
  - 什么时候不能做
  - 做多大仓位
  - 多久复核一次
- 系统必须可做事后验证，即每次信号产生后能够追踪结果。

### 3.2 数据需求

#### A. 预期差数据
- 一致预期 EPS FY1/FY2
- 近 30/90 天 EPS 预期修正幅度
- 目标价中位数与变动
- 财报 surprise
- 管理层指引变动

#### B. 现金流与盈利质量
- 经营现金流
- 自由现金流
- 经营现金流 / 净利润
- 应收账款周转天数
- 存货周转天数
- CapEx / Revenue
- 应计项质量指标

#### C. 资金行为
- 北向/南向净流入
- 主力净流入
- 融资融券变化
- 换手率历史分位
- 成交额/量能趋势
- 大宗交易和流动性风险

#### D. 行业先行指标
- TMT：MAU、DAU、ARPU、付费率、留存、研发投入
- Biotech：管线阶段、审批预期、合作伙伴、适应症空间
- Consumer：同店增速、门店变化、库存周转、会员增长、线上占比
- 光伏/CXO/互联网等行业还需补充专属景气指标

#### E. 事件与风险
- 财报日
- 解禁日
- 分红/回购/配售
- 监管/政策/临床/产品发布事件
- 财报前后交易限制
- 流动性约束

#### F. 标签与回测
- 信号日期
- 入场价格
- 持有期标签（5/10/20 日）
- 最大浮亏
- 最大浮盈
- 是否达成目标收益
- 是否触发失效条件

### 3.3 非功能需求
- 数据更新任务要可追踪、可恢复、可审计。
- 工作台接口输出要稳定，支持前端直接展示策略建议。
- 需要分阶段上线，不一次性重构全部模块。

## 4. 方案设计

### 4.1 总体方案
- 采用“三层架构”：
  - 数据层：采集、清洗、入库、指标计算
  - 策略层：setup 定义、因子评分、风险过滤、标签生成
  - 展示层：工作台/API/详情页/复核看板

### 4.2 策略方法
- 取消“全市场统一高胜率”假设。
- 改为维护一组 setup：
  - `earnings_revision_breakout`
  - `quality_value_recovery`
  - `risk_on_pullback_leader`
  - `sector_catalyst_confirmation`
- 每个 setup 拥有：
  - 自己的入选条件
  - 自己的风险过滤
  - 自己的目标持有期
  - 自己的胜率统计

### 4.3 因子体系
- 在现有 `quality/growth/valuation/flow/technical/risk` 六因子基础上升级：
  - `quality`：ROE、ROA、毛利率、净利率、现金流质量
  - `growth`：营收/利润增速、预期修正、财报 surprise
  - `valuation`：估值分位、行业相对估值、股息率分位
  - `flow`：北向/南向、主力流入、换手分位、融资变化
  - `technical`：趋势、结构、波动调整后的突破质量
  - `risk`：事件风险、回撤、波动、流动性、相关性

### 4.4 风险过滤
- 以下条件优先作为过滤器而不是打分项：
  - 财报前 3 个交易日
  - 高风险事件窗口
  - 流动性不足
  - 波动率过高
  - 风险偏好环境不支持
- 原因：高胜率系统靠的是“先不做错”，不是“把所有票排得更精细”。

### 4.5 输出设计
- 列表页新增：
  - setup 名称
  - 推荐动作
  - 建议仓位区间
  - 风险等级
  - 事件提醒
- 详情页新增：
  - 预期差
  - 行业先行指标摘要
  - 资金确认信息
  - 标签历史和胜率统计
  - 本次信号的历史可比样本

## 5. 架构设计

### 5.1 数据架构
- 复用现有库表：
  - `stock_financial`
  - `valuation_bands`
  - `technical_indicators`
  - `north_money`
  - `sector_tmt`
  - `sector_biotech`
  - `sector_consumer`
- 新增建议表：
  - `stock_estimate`
  - `stock_flow_daily`
  - `stock_event_calendar`
  - `signal_labels`
  - `strategy_backtest_stats`
  - `setup_candidates`

### 5.2 模块划分
- `quant_workbench/data_ingestion/`
  - 负责补全预期差、事件、资金、行业先行指标
- `quant_workbench/feature_engineering/`
  - 负责六因子和 setup 特征计算
- `quant_workbench/setups/`
  - 每个 setup 一份规则定义
- `quant_workbench/backtest/`
  - 负责标签生成与效果统计
- `quant_workbench/service.py`
  - 只负责聚合结果，不再承载过多策略细节

### 5.3 API 设计
- 新增 API：
  - `/api/setups/overview`
  - `/api/setups/{setup_name}/candidates`
  - `/api/stocks/{code}/edge`
  - `/api/stocks/{code}/events`
  - `/api/stocks/{code}/flow`
  - `/api/backtest/stats`
- 保持现有 `/api/overview` 兼容，但改为聚合 setup 输出。

### 5.4 部署架构
- Windows 机器继续承担 UI/API 服务与定时同步任务。
- 数据同步任务拆分为：
  - 基础行情
  - 财务/估值
  - 资金
  - 事件
  - 行业模型
  - 标签/回测
- 每个任务落 ETL 日志，避免黑盒运行。

## 6. 可行性分析

### 6.1 技术可行性
- 高：现有仓库已经有大量表设计和行业模块接口，基础设施不是空白。
- 高：SQLite + Parquet 足够支撑当前股票池规模。
- 中：如果后续扩到全市场，会需要更强的任务调度和更规范的批处理架构。

### 6.2 数据可行性
- 高：价格、估值、技术指标、部分财务、北向资金已有基础。
- 中：预期差和目标价需要外部一致预期来源，获取难度比基础行情高。
- 中：行业先行指标可通过 CSV 模板+半自动导入先落地，再逐步自动化。
- 中：事件数据可先从公告/手工录入开始，逐步自动化。

### 6.3 业务可行性
- 高：针对少数关注股票池做高胜率 setup 是现实的。
- 低：把 80% 胜率作为全市场统一承诺不可行。
- 高：把目标定义为“少量高胜率 setup + 严格过滤 + 可验证标签”是可执行路线。

### 6.4 风险分析
- 最大风险不是代码，而是标签定义不清，导致结果不可验证。
- 第二风险是预期差数据源质量不足，导致策略失真。
- 第三风险是行业指标维护成本高，若无治理会逐步失效。

## 7. 实施路线

### Phase 0：策略定义与标签
- 明确 3-4 个 setup
- 定义胜率口径、止损口径、持有期口径
- 建立 `signal_labels` 与 `strategy_backtest_stats`

### Phase 1：接入已有但未消费的数据
- `valuation_bands`
- `technical_indicators`
- `north_money`
- `sector_tmt`
- `sector_biotech`
- `sector_consumer`
- 目标：优先利用仓库里已经设计好的能力

### Phase 2：补齐真正缺失的数据
- `stock_estimate`
- `stock_flow_daily`
- `stock_event_calendar`
- 目标：形成预期差、事件、资金三条主线

### Phase 3：策略化重构
- 将当前机会打分改成 setup 驱动
- 输出动作、仓位、失效条件、复核时间
- 接入回测统计和历史样本验证

### Phase 4：运营与部署
- Windows 上完成定时任务拆分
- 加入 ETL 监控
- 完成运行手册和回滚手册

## 8. 对后续执行模型的要求
- 不要再做“大而全”的统一评分增强。
- 优先接入现有数据表，再新建缺失表。
- 任何新功能都要带可验证指标或标签，不接受只增加展示字段。
- 不要覆盖用户当前未提交改动。

## 9. 交付物清单
- 分析设计文档：本文件
- 执行交接单：`docs/quant_workbench_executor_handoff.md`
- 后续编码目标：
  - setup 体系
  - 标签体系
  - 数据接入
  - API 改造
  - 前端增强
  - Windows 部署任务拆分
