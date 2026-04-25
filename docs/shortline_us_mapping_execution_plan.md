# 短线执行层建设方案：美股领先映射 + 跨市场套利

更新时间：2026-04-25  
适用仓库：`investment-control`  
定位：不是做第二个行情终端，而是做一个短线执行层，把“海外先动 -> 中资映射 -> A/H 落地 -> 套利模板”固化成可验证的数据系统。

---

## 1. 目标

在现有 `radar + intelligence + quant_workbench + trade_plan` 之上，新建一个独立的短线子系统，服务两个场景：

1. **海外先动映射**
   - 美股公司、ETF、行业先走强/走弱后，映射到 A 股、港股的直接标的和链条标的。
   - 输出“谁在海外先动、国内该看谁、映射强度多少、是否适合追”的结构化结果。

2. **跨市场套利 / 事件跟随**
   - ADR/H 股价差、A/H 溢价、行业 ETF 替代、海外财报/指引溢出、FDA / 核准 / 政策事件跟随。
   - 输出“可执行 playbook”，不是只给一个新闻标题。

核心输出必须是：

- `us_lead_signal_score`
- `mapping_strength_score`
- `china_follow_through_score`
- `execution_priority`
- `trade_playbook`
- `invalid_condition`
- `latency_status`

---

## 2. 现有模块复用

本仓库已有 4 块可以直接复用，不要推倒重来：

1. `quant_workbench/`
   - 已有 A/H/ETF watchlist
   - 已有日频/5m 数据同步
   - 已有技术、结构、打分、候选池

2. `app/services/intelligence_service.py`
   - 已有事件库、研报库、事实库
   - 可承接“海外事件 -> 中资映射 -> 证据链”

3. `app/services/radar_service.py`
   - 已有宏观、外部风险、港股流动性
   - 可作为短线信号的上层 gating，不直接负责短线执行

4. `app/services/trade_plan_service.py`
   - 已有结构化交易计划框架
   - 可承接短线 playbook 输出，不必另造计划引擎

结论：  
**短线执行层应该是新模块，但数据、候选池、交易计划都复用老模块。**

---

## 3. V1 范围

V1 只做最有价值、最容易形成闭环的四类短线：

1. **ADR / H 股 / A 股直接映射**
   - `BABA -> 9988.HK`
   - `TCEHY -> 0700.HK`
   - `JD -> 9618.HK`
   - `NTES -> 9999.HK`
   - `NIO / XPEV / LI -> 9866.HK / 9868.HK / 2015.HK`

2. **美股龙头 -> 中国供应链 / 同赛道映射**
   - `NVDA / AMD / AVGO / SMCI / MSFT / META / GOOGL`
     -> A/H 算力、光模块、PCB、服务器、IDC、电力链
   - `TSLA / ABB / FANUC / ISRG`
     -> 机器人、汽车零部件、工业自动化、医疗机器人
   - `TSM / ASML / AMAT / LRCX`
     -> 半导体设备、晶圆厂、国产替代链
   - `LLY / NVO / MRNA / BNTX`
     -> 创新药 / GLP-1 / 生物平台映射

3. **美国行业 ETF / 主题 ETF -> 中国 ETF / 龙头映射**
   - `SMH / SOXX -> 半导体 ETF / 芯片链`
   - `ARKQ / BOTZ -> 机器人 ETF / 自动化`
   - `IBB / XBI -> 创新药 / CXO / 港股 biotech`
   - `TAN -> 光伏`
   - `URA / NLR -> 核电`

4. **可直接落地的套利模板**
   - ADR / H 股收盘偏离
   - 美股财报 / 指引 -> 次日 A/H 开盘映射
   - A/H 溢价 + 南向确认
   - 港股强势龙头 -> A 股次日同链条跟随

V1 不做：

- 纯盘口席位博弈
- 高频 tick 级套利
- 需要付费终端才能稳定拿到的 Level 2 数据
- 自动下单

---

## 4. 数据源设计

### 4.1 海外领先层

阿里云负责常驻采集，优先级如下：

1. **价格 / 涨跌 / 盘前盘后**
   - Yahoo Finance / yfinance
   - 必须落 1d、5m、可得的 pre/post 市场快照

2. **正式事件**
   - SEC EDGAR 8-K / 10-Q / 10-K
   - 公司 IR 新闻稿 / RSS
   - FDA / NIH / clinicaltrials
   - 官方博客与产品发布页

3. **辅助新闻层**
   - 阿里云 search-proxy
   - 只作为补充，不作为单独交易信号

### 4.2 中国落地层

Windows 为主存储与主 API：

1. **A 股 / 港股行情**
   - Akshare / Eastmoney / HKEX 补充
   - 日频 + 5m 级别

2. **A/H / 南向 / 港股结构**
   - A/H premium
   - 南向资金
   - 港股卖空
   - 港股指数与 ETF

3. **研报 / 事件证据**
   - 复用 `intelligence` 与 `research_workbench`
   - 给每条映射提供证据链

### 4.3 映射主数据

这是新建的核心资产，不能靠 prompt 临时猜：

#### 表 1：`cross_market_mapping_master`

- `mapping_id`
- `us_symbol`
- `us_name`
- `cn_symbol`
- `cn_name`
- `market` (`A` / `HK` / `ETF`)
- `relation_type`
  - `same_entity`
  - `adr_to_hk`
  - `direct_peer`
  - `supplier`
  - `customer`
  - `thematic_proxy`
  - `etf_proxy`
- `theme`
- `strength_score` (0-100)
- `evidence_source`
- `manual_verified`
- `updated_at`

#### 表 2：`cross_market_signal_events`

- `event_id`
- `source_market` (`US`)
- `source_symbol`
- `event_type`
  - `price_breakout`
  - `earnings_beat`
  - `guidance_raise`
  - `product_release`
  - `regulatory_approval`
  - `capex_signal`
  - `etf_breakout`
- `event_time`
- `headline`
- `facts_json`
- `impact_direction`
- `urgency`
- `source_url`

#### 表 3：`cross_market_signal_candidates`

- `candidate_id`
- `event_id`
- `cn_symbol`
- `mapping_id`
- `mapping_strength_score`
- `us_event_score`
- `china_follow_through_score`
- `execution_priority`
- `playbook_key`
- `invalid_condition`
- `status`

#### 表 4：`cross_market_playbooks`

- `playbook_key`
- `name`
- `market_scope`
- `entry_rule`
- `timing_rule`
- `risk_rule`
- `exit_rule`
- `not_applicable_rule`

---

## 5. 映射规则

映射不能只看“同概念”，必须分层：

### Tier 1：直接映射

最强，可直接交易或直接跟踪：

- `BABA -> 9988.HK`
- `TCEHY -> 0700.HK`
- `JD -> 9618.HK`
- `NTES -> 9999.HK`
- `LI / XPEV / NIO -> 2015.HK / 9868.HK / 9866.HK`

### Tier 2：供应链映射

次强，需要事件类型匹配：

- `NVDA / AVGO / SMCI`
  -> `中际旭创 / 新易盛 / 天孚通信 / 沪电股份 / 工业富联 / 胜宏科技`
- `TSLA`
  -> `拓普集团 / 三花智控 / 旭升集团 / 德赛西威 / 伯特利`
- `AAPL`
  -> `立讯精密 / 歌尔股份 / 蓝思科技 / 工业富联`
- `TSM / ASML / AMAT`
  -> `中芯国际 / 北方华创 / 中微公司 / 拓荆科技 / 芯源微`

### Tier 3：同赛道替代映射

最弱，只能做主题预埋，不适合强追：

- `LLY / NVO`
  -> `信达生物 / 恒瑞医药 / 华东医药`
- `ISRG`
  -> `微创机器人-B / 迈瑞医疗`
- `OKLO / NLR / URA`
  -> `中国核电 / 中广核电力 / 应流股份 / 江苏神通`
- `BOTZ / ARKQ`
  -> `机器人 ETF / 埃斯顿 / 汇川技术 / 绿的谐波`

---

## 6. 信号引擎

### 6.1 美股领先信号打分

`us_event_score = price + volume + event_quality + sector_context`

建议拆成：

- `price_impulse_score`
  - 隔夜涨跌幅
  - 相对行业 ETF 强弱
  - 是否放量
- `event_quality_score`
  - 财报 beat / raise guidance
  - 官方产品发布
  - 监管批准
  - 资本开支上修
- `cleanliness_score`
  - 官方口径优先
  - 单一媒体传闻降权

### 6.2 中国映射打分

`mapping_strength_score = relation_weight + evidence_weight + liquidity_weight`

- 同主体 / ADR-HK 权重最高
- 供应链其次
- 主题代理最低
- A 股 / 港股流动性差的标的降权

### 6.3 国内跟随确认

`china_follow_through_score`

至少看：

- 竞价强弱
- 开盘 15/30 分钟成交额放大
- 同链条 ETF / 龙头是否共振
- 南向 / 北向是否配合
- 宏观 / 外部 risk gating 是否允许追高

### 6.4 总优先级

`execution_priority = us_event_score * 0.35 + mapping_strength_score * 0.35 + china_follow_through_score * 0.30`

输出 3 档：

- `P0`：直接映射 + 强事件 + 开盘确认
- `P1`：链条映射 + 事件清晰 + 半确认
- `P2`：主题预埋 / 候选观察

---

## 7. 可落地的短线套利模板

### Playbook A：ADR / H 股价差修复

适用：

- 港股同时上市，且 ADR 夜里大幅异动

规则：

- ADR 收盘涨跌显著偏离 H 股前收
- 次日港股竞价跟随不足时，进入候选
- 必须叠加流动性和卖空确认

### Playbook B：美股财报 / 指引溢出

适用：

- `NVDA / AVGO / TSM / META / MSFT / TSLA / LLY` 等高影响力标的

规则：

- 财报 beat / raise guidance / capex 上修
- 先映射到中国供应链
- 仅做有直接链条证据的标的，不做泛化概念

### Playbook C：美股主题 ETF -> 中国 ETF / 龙头跟随

适用：

- `SMH / BOTZ / IBB / TAN / URA`

规则：

- 美股 ETF 强于纳指 / 标普
- 中国对应 ETF 与核心龙头竞价同步
- 用 ETF 作为第一执行层，个股作为第二层

### Playbook D：A/H 溢价 + 南向抱团

适用：

- 港股龙头相对 A 股折价明显
- 南向持续流入

规则：

- A/H 溢价走阔后回收
- 南向 3 日 / 5 日净流入增强
- 卖空占比不恶化

### Playbook E：港股先动 -> A 股次日跟随

适用：

- 港股科技、创新药、高股息龙头先启动

规则：

- 港股收盘强度明显
- A 股同链条 ETF / 龙头次日竞价确认
- 不做单票孤立映射，要求板块共振

### Playbook F：监管 / FDA / 核准事件跟随

适用：

- FDA 批准
- 临床 readout
- 核电新机组核准
- 行业政策落地

规则：

- 必须是官方事件
- 先做“直接受益主体”，再看“主题陪跑”
- 盘中只做证据明确的一层映射

---

## 8. 页面设计

建议新增独立页面：

`/investment/shortline`

不要塞进 radar 首页。

页面结构：

1. **Overnight Lead**
   - 昨夜美股强事件
   - pre/post market 重大异动
   - 事件分类与紧急度

2. **China Mapping**
   - 每个海外事件下面挂中国映射标的
   - 展示 `relation_type / strength / evidence`

3. **Execution Board**
   - P0 / P1 / P2
   - 开盘前、开盘后、午盘、尾盘四个状态

4. **Arb Playbooks**
   - ADR/HK
   - A/H 溢价
   - 港股带 A 股
   - ETF 替代

5. **Evidence Panel**
   - 相关新闻
   - 官方来源
   - 研报支持
   - 失效条件

---

## 9. 时效 SLA

短线系统必须按市场时钟跑，不是日更页面：

### 阿里云

- 美股交易时段：每 5 分钟更新价格与重大事件
- 美股收盘后：立即生成 overnight lead snapshot
- 美股盘后财报窗口：高优先级轮询

### Windows

- 06:30 生成中国映射候选
- 08:30 生成盘前执行板
- 09:20 竞价校验
- 09:45 / 10:30 / 13:30 / 14:30 刷新
- 15:10 写入复盘结果

---

## 10. 给阿里 Coding Plan 的实施顺序

### Phase 1：底层主数据

1. 新建 `cross_market_mapping_master`
2. 新建 `cross_market_signal_events`
3. 新建 `cross_market_signal_candidates`
4. 新建 `cross_market_playbooks`
5. 建立初始 mapping seed

### Phase 2：采集

1. 美股价格 / ETF / pre-post 抓取
2. SEC / IR / FDA / 官方博客事件抓取
3. 中国 A/H / ETF / 南向 / AH 溢价接入

### Phase 3：信号引擎

1. `us_event_score`
2. `mapping_strength_score`
3. `china_follow_through_score`
4. `execution_priority`

### Phase 4：页面

1. `/investment/shortline`
2. overnight lead board
3. mapping board
4. playbook board
5. evidence drawer

### Phase 5：闭环

1. 生成 trade plan
2. 开盘后确认
3. 收盘写回复盘标签

---

## 11. 验收标准

### 数据层

- 至少 50 条高质量 `US -> CN` 映射关系
- 至少覆盖：
  - AI 基础设施
  - 半导体
  - 机器人
  - 创新药
  - 光伏
  - 核电
  - 汽车 / 自动驾驶
  - 红利 / 银行 / 券商

### 页面层

- 一个海外事件点开后，必须看到：
  - 对应中国标的
  - 关系类型
  - 映射强度
  - 证据来源
  - 失效条件

### 策略层

- 每条候选必须归属一个 playbook
- 不能只有“看多/看空”，必须有：
  - entry window
  - invalid condition
  - risk note

### 时效层

- overnight lead 到中国映射的生成延迟 < 10 分钟
- 开盘前页面不能出现“大面积空白”

---

## 12. 不允许的实现方式

1. 不允许只做一个静态映射 JSON 然后人工维护页面
2. 不允许只抓新闻标题，不落结构化事件
3. 不允许把所有美股上涨都硬映射成 A 股利好
4. 不允许没有 `relation_type` 和 `strength_score`
5. 不允许没有失效条件

---

## 13. 给阿里 Coding Plan 的直接执行指令

你可以把下面这段直接喂给阿里 Coding Plan：

> 在当前仓库中实现一个新的短线执行子系统 `/investment/shortline`。  
> 目标是把“美股/海外先动 -> A股/港股映射 -> 套利模板 -> 盘前/盘中执行板”做成结构化系统，而不是简单新闻流。  
> 必须优先复用现有 `quant_workbench`、`intelligence_service`、`trade_plan_service`、`radar_service`。  
> 先实现数据模型、映射主表、海外事件采集、US->CN 信号引擎，再实现页面。  
> 每个候选必须输出 `relation_type`、`mapping_strength_score`、`execution_priority`、`trade_playbook`、`invalid_condition`。  
> 先完成 V1：ADR/HK 直接映射、AI/半导体/机器人/创新药/核电/红利几条主线。  
> 代码必须带最小测试与 smoke 验证，不要改坏已有 intelligence/radar/research 页面。

---

## 14. 现有系统的最佳插入点

这次不应该另起一个新数据库、一个新调度器、一个新事件系统，而是插到现有 4 个骨架上：

1. `quant_workbench`
   - 继续承担 A/H/ETF 候选池、技术结构、日频与 5m 级别快照。
   - 短线模块只新增“海外触发因子”和“映射候选层”，不重写行情底盘。

2. `intelligence_service`
   - 继续承担事件库、事实库、证据链。
   - 海外财报、FDA、ClinicalTrials、公司 IR、ETF 异动进入同一套证据层，不再并行造另一套新闻表。

3. `trade_plan_service`
   - 继续承担结构化执行计划、仓位、止损、止盈、失效条件。
   - 短线模块只多加 `us_trigger_symbol`、`mapping_confidence`、`playbook_key` 这类字段。

4. `investment` 路由
   - 新增 `/investment/shortline` 页面和 `/investment/api/shortline/*` API。
   - 不把短线执行板硬塞进 `radar` 首页。

结论：

- 主存储继续用 `data/investment.db`
- 主 UI 继续走现有 FastAPI + Jinja 模板
- 阿里云只做采集和 snapshot 推送
- Windows 继续做主读取、主存储、主页面

---

## 15. 数据源优先级与时效要求

截至 2026-04-25，短线里最值得依赖的官方/准官方事件源如下：

1. SEC EDGAR
   - `data.sec.gov` 的 submissions / companyfacts API 可以直接读 JSON。
   - 官方文档说明：filings disseminated 后近实时更新，bulk zip 夜间重发。
   - 用途：财报、8-K、指引、资本开支、风险提示。

2. FDA / openFDA
   - `Drugs@FDA` / openFDA 的 drug 数据适合做创新药事件层。
   - 官方页面说明：`Drugs@FDA` API 工作日更新。
   - 用途：批准、标签、审评、监管变化。

3. ClinicalTrials.gov API v2
   - 官方说明当前数据一般在周一到周五、美国东部时间上午完成刷新。
   - 用途：临床试验推进、readout、终点、状态变化。

4. 公司 IR / 官方博客 / 官方 newsroom
   - 财报新闻稿、产品发布、资本开支、订单、合作。
   - 用途：AI、半导体、机器人、工业自动化主题。

5. 行情层
   - V1 用 `yfinance/Yahoo + Akshare + Eastmoney + HKEX/公开补充源`
   - 这能先跑起来，但要接受一个现实：如果你要求分钟级长期稳定，后续要升级到付费美股行情源。免费源只能作为 V1。

建议的 source tier：

- `T0` 官方：SEC / FDA / ClinicalTrials / 公司 IR
- `T1` 准官方/稳定公共：Yahoo / Akshare / Eastmoney / HKEX 公共页面
- `T2` 搜索补充：阿里云 search-proxy，只补事实，不单独触发交易信号

---

## 16. V1 应覆盖的短线 playbook 池

除了 ADR/HK 直接映射，V1 至少再做 8 条高价值模板：

1. 纳指/费半隔夜强势 -> A/H 科技与芯片次日跟随
2. 美股龙头财报/指引上修 -> 中国供应链映射
3. 中概指数/中概龙头暴涨 -> 港股科技与 A 股相关链条跟随
4. FDA 批准 / 临床 readout -> A/H 创新药与 CRO 跟随
5. 美股机器人/自动化异动 -> A/H 机器人、伺服、减速器、工控映射
6. 美股光伏政策/龙头异动 -> A 股光伏链跟随
7. 美股核电/能源政策事件 -> A/H 核电链跟随
8. 美股农业/饲料/猪价事件 -> A 股养猪链跟随

每条 playbook 都必须有：

- `trigger_condition`
- `mapped_assets`
- `execution_window`
- `invalid_condition`
- `required_confirmation`
- `source_tier`

并明确分成两类：

- `event_required`: 依赖官方事件源，如财报、FDA、ClinicalTrials、政策公告
- `price_only_ok`: 行情本身可先触发，如纳指/费半/中概指数隔夜异动

---

## 17. 给阿里 Coding Plan 的文件边界

为了避免它改散，建议直接限定文件范围：

### 必做新增

- `app/services/shortline_service.py`
- `templates/shortline.html`
- `scripts/sync_shortline_us_events.py`
- `scripts/build_shortline_candidates.py`
- `tests/test_shortline_service.py`

### 允许修改

- `app/routers/investment.py`
- `app/services/intelligence_service.py`
- `app/services/trade_plan_service.py`
- `app/services/investment_db_service.py`

### 原则上不要碰

- `templates/radar.html`
- `app/services/radar_service.py`
- `templates/research_workbench.html`
- `scripts/sync_intelligence.py`

除非为了复用公共函数，且改动必须非常小。

---

## 18. 分阶段实施要求

### Phase 1：最小可跑版本

- 建 `cross_market_mapping_master`
- 建 `cross_market_signal_events`
- 建 `cross_market_signal_candidates`
- 接入 20-30 个高价值美股/ETF/中概触发源
- 页面先做列表+详情，不先做花哨图表

### Phase 2：证据链补强

- 把事件同步写入 `intelligence_events`
- 给每个映射候选挂 `evidence_links`
- 接入百炼：做中文摘要、重点提炼、失效条件归纳

### Phase 3：交易计划闭环

- 候选一键转 trade plan
- 开盘前/开盘后状态变化
- 收盘后写回“是否兑现/是否失效”

---

## 19. 我这边的验收标准

阿里 Coding Plan 完成后，我会按下面 5 类验收，不满足就继续修：

1. 结构验收
   - 没有新造第二套数据库
   - 没有把短线逻辑散落到 5 个旧页面
   - API、服务、模板边界清楚

2. 数据验收
   - 至少 50 条高质量 `US -> CN` 映射
   - 至少 8 条 playbook 可返回非空结果
   - 事件与候选均有时间戳、source、relation_type

3. 页面验收
   - `/investment/shortline` 首屏不能空
   - 每条候选能看到映射对象、证据、执行窗口、失效条件
   - 筛选器可按市场、行业、playbook、优先级过滤

4. 时效验收
   - 隔夜事件到候选生成延迟可控
   - 盘前生成板在 08:30 前可读
   - 盘中刷新不把主站点拖死

5. 回归验收
   - `radar / intelligence / research` 页面不回退
   - 关键 API smoke test 通过
   - 数据库迁移兼容旧库
