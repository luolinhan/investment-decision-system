# 短线执行层 - 阿里 Coding Plan 任务卡

你要在当前仓库中实现一个新的短线执行子系统 `/investment/shortline`。

## 目标

不要做第二个行情终端。  
要做的是“美股/海外先动 -> A股/港股映射 -> 套利模板 -> 盘前/盘中执行板”。

系统必须输出结构化结果，而不是只有新闻标题。

每条候选至少要有：

- `relation_type`
- `mapping_strength_score`
- `execution_priority`
- `playbook_key`
- `invalid_condition`
- `evidence_links`
- `latency_status`

## 现有模块必须优先复用

- `quant_workbench`
- `app/services/intelligence_service.py`
- `app/services/trade_plan_service.py`
- `app/routers/investment.py`

不要另造一套数据库，不要另造一套事件系统。

## 存储与架构约束

- 主存储继续使用 `data/investment.db`
- 阿里云只负责采集和 snapshot 推送
- Windows 负责主 API、主页面、主存储、主读取
- 页面通过 FastAPI + Jinja 渲染

## 允许新增的文件

- `app/services/shortline_service.py`
- `templates/shortline.html`
- `scripts/sync_shortline_us_events.py`
- `scripts/build_shortline_candidates.py`
- `tests/test_shortline_service.py`

## 允许修改的文件

- `app/routers/investment.py`
- `app/services/intelligence_service.py`
- `app/services/trade_plan_service.py`
- `app/services/investment_db_service.py`

## 原则上不要修改

- `app/services/radar_service.py`
- `templates/radar.html`
- `templates/research_workbench.html`
- `scripts/sync_intelligence.py`

除非是非常小的复用性改动。

## 数据模型

至少实现 3 张表：

### `cross_market_mapping_master`

- `mapping_id`
- `us_symbol`
- `us_name`
- `cn_symbol`
- `cn_name`
- `market`
- `relation_type`
- `theme`
- `strength_score`
- `evidence_source`
- `manual_verified`
- `updated_at`

### `cross_market_signal_events`

- `event_id`
- `source_market`
- `source_symbol`
- `event_type`
- `event_time`
- `headline`
- `facts_json`
- `impact_direction`
- `urgency`
- `source_url`

### `cross_market_signal_candidates`

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

## V1 必做行业与映射

至少覆盖：

- AI
- 半导体
- 机器人
- 创新药
- 光伏
- 核电
- 养猪
- 红利/高股息

至少内置：

- ADR/HK 直接映射
- 美股龙头 -> 中国供应链映射
- 美股主题 ETF -> 中国 ETF/龙头映射

## V1 必做 playbook

至少 8 条：

1. 纳指/费半隔夜强势 -> A/H 科技芯片跟随
2. 美股龙头财报/指引 -> 中国供应链映射
3. 中概指数/中概龙头 -> 港股科技/A股链条
4. FDA/临床 readout -> 创新药/CRO
5. 机器人/自动化异动 -> 机器人链
6. 光伏政策/龙头异动 -> 光伏链
7. 核电/能源政策 -> 核电链
8. 农业/饲料/猪价 -> 养猪链

每条 playbook 都必须给：

- 触发条件
- 映射对象
- 执行窗口
- 必要确认
- 失效条件

## 数据源要求

优先级：

- `T0` 官方：SEC EDGAR、FDA/openFDA、ClinicalTrials.gov、公司 IR
- `T1` 公共行情：Yahoo/yfinance、Akshare、Eastmoney、HKEX 公共页面
- `T2` 搜索补充：阿里云 search-proxy

不要让搜索新闻单独触发交易信号；搜索只能补充事实链。

## 页面要求

新增：

- `GET /investment/shortline`
- `GET /investment/api/shortline/overview`
- `GET /investment/api/shortline/candidates`
- `GET /investment/api/shortline/events`
- `GET /investment/api/shortline/playbooks`

页面至少包含：

1. Overnight Lead
2. China Mapping
3. Execution Board
4. Playbook Board
5. Evidence Drawer

## 百炼要求

对英文官方事件和研报补：

- 中文标题
- 中文摘要
- 3-5 条重点
- 失效条件归纳

不要在页面请求时临时翻译，要先落库。

## 质量要求

- `/investment/shortline` 首屏不能空
- 至少 50 条高质量 `US -> CN` 映射关系
- 每条候选必须有 source / time / relation_type / invalid_condition
- 不破坏已有 `radar / intelligence / research` 页面
- 提供最小测试与 smoke 脚本

## 交付顺序

1. 先做数据表与 seed mapping
2. 再做采集脚本
3. 再做信号计算
4. 再做 API
5. 最后做页面

不要反过来先堆页面。
