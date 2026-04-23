# Quant Workbench 执行交接单

## 1. 目标
- 根据 `docs/quant_workbench_high_winrate_design.md` 完成高胜率策略系统的第一阶段编码和部署。
- 第一阶段不追求全量重构，只完成最关键的可执行闭环。

## 2. 执行范围

### 2.1 必做
- 为工作台建立 setup 驱动的输出结构。
- 接入数据库中已存在但未在 `quant_workbench` 使用的数据：
  - `valuation_bands`
  - `technical_indicators`
  - `north_money`
  - `sector_tmt`
  - `sector_biotech`
  - `sector_consumer`
- 新增标签表和回测统计表：
  - `signal_labels`
  - `strategy_backtest_stats`
- 为现有工作台详情页增加：
  - setup 名称
  - 建议动作
  - 建议仓位
  - 失效条件
  - 事件提醒占位
  - 回测统计占位

### 2.2 可选
- 新增 `stock_event_calendar`
- 新增 `stock_flow_daily`
- 新增简单的 `stock_estimate` 占位表和导入脚本

## 3. 建议改动目录
- `quant_workbench/service.py`
- `quant_workbench/strategy.py`
- `quant_workbench/app.py`
- `quant_workbench/templates/quant_workbench.html`
- 新增：
  - `quant_workbench/setups.py`
  - `quant_workbench/backtest.py`
  - `quant_workbench/db_views.py`
  - `scripts/` 下的数据迁移或同步脚本

## 4. 数据库任务
- 编写 migration/init 逻辑，确保以下表存在：

```sql
CREATE TABLE IF NOT EXISTS signal_labels (
  signal_date TEXT,
  code TEXT,
  setup_name TEXT,
  hold_days INTEGER,
  entry_price REAL,
  exit_price REAL,
  max_gain REAL,
  max_drawdown REAL,
  win_flag INTEGER,
  invalidated INTEGER DEFAULT 0,
  PRIMARY KEY(signal_date, code, setup_name, hold_days)
);

CREATE TABLE IF NOT EXISTS strategy_backtest_stats (
  stat_date TEXT,
  setup_name TEXT,
  hold_days INTEGER,
  sample_size INTEGER,
  win_rate REAL,
  avg_return REAL,
  avg_max_drawdown REAL,
  profit_loss_ratio REAL,
  PRIMARY KEY(stat_date, setup_name, hold_days)
);
```

## 5. 策略输出格式
- 每个候选项至少输出：
  - `setup_name`
  - `action`
  - `position_range`
  - `factors`
  - `invalid_conditions`
  - `review_at`
  - `event_summary`
  - `backtest_stats`

## 6. 实施顺序
1. 先封装读取已有数据库表的 adapter，不要把 SQL 散在服务层。
2. 再把现有评分结果改造成 setup 输出。
3. 再补标签生成与简单回测统计。
4. 最后改前端页面和部署脚本。

## 7. 部署要求
- Windows 启动方式保持兼容：
  - `start_quant_workbench.bat`
  - `sync_quant_workbench_task.bat`
- 如果新增同步脚本，需要提供独立 bat 文件。
- 不要修改用户现有 SSH/调度逻辑，只在项目目录内新增脚本。

## 8. 验证清单
- `/api/overview` 正常返回
- `/api/stocks/{code}` 正常返回新增字段
- 前端详情页正常展示新增字段
- 新建表成功
- 标签计算脚本可跑
- Windows 启动脚本仍可启动服务

## 9. 注意事项
- 当前工作区是脏树，禁止回滚无关改动。
- 现有 `quant_workbench/strategy.py` 和 `quant_workbench/service.py` 已有未提交修改，必须在此基础上增量修改。
- 优先完成可验证闭环，不要一次性追求所有高级数据源自动化。
