# 香港流动性 & 五角大楼披萨数据源说明

## 概述

本文档说明 Radar 子系统中两个补充数据链路的数据源、采集逻辑和用途：

1. **香港流动性雷达** (`sync_radar_hk.py`)
   - 南向/北向资金
   - 恒指/国指/恒生科技指数
   - A/H 溢价指数
   - 访客流量 (catalog placeholder)

2. **五角大楼披萨历史** (`sync_pentagon_pizza_history.py`)
   - Pentagon Pizza Index 历史时间序列
   - 温度带分类 (过冷/偏冷/中性/偏热/过热)

---

## 1. 香港流动性雷达

### 数据库

- **目标库**: `data/radar/radar.duckdb`
- **表清单**:
  - `hk_south_flow` - 南向资金
  - `hk_north_money` - 北向资金
  - `hk_indices` - 恒生指数系列
  - `ah_premium` - A/H 溢价指数
  - `hk_visitor_arrivals` - 访客流量 (placeholder)

### 数据源

#### 1.1 南向/北向资金

- **Primary**: `investment.db` SQLite 回填
  - 表: `south_flow`, `north_money`
  - 说明: 优先从现有 investment.db 读取历史，避免重复采集
  
- **Fallback**: `akshare.stock_hsgt_hist_em()`
  - 说明: 如需增量更新，可调用 akshare 接口
  - 频率: 每日一次
  - 代理: 需要 HTTP 代理 (代码中已配置环境变量)

#### 1.2 恒生指数系列

- **数据源**: `akshare.index_hk_hist()`
  - 恒生指数 (HSI): 2000-01-01 ~ 至今
  - 国企指数 (HSCC): 2000-01-01 ~ 至今
  - 恒生科技指数 (HSTECH): 2020-01-01 ~ 至今
- **字段**: date, open, high, low, close, volume
- **频率**: 每日一次
- **代理**: 需要 HTTP 代理

#### 1.3 A/H 溢价指数

- **数据源**: `akshare.index_hk_hist(symbol="AH股溢价指数")`
  - 覆盖: 2015-01-01 ~ 至今
  - 字段: date, value (close)
- **Fallback**: Catalog placeholder
  - 如 akshare 失败，写入带 notes 的占位记录，便于 catalog 展示

#### 1.4 访客流量 (可选)

- **状态**: Placeholder / 手动采集
- **建议来源**:
  - 香港政府统计处: `https://www.censtatd.gov.hk`
  - 旅客出入境数据 (月度)
- **字段设计**:
  - arrivals_total, arrivals_air, arrivals_land, arrivals_sea
  - departures_total, departures_air, departures_land, departures_sea

### 依赖

```bash
pip install akshare duckdb pandas
```

### 执行

```bash
python3 scripts/sync_radar_hk.py
```

### 增量策略

- 南向/北向: 基于 `trade_date` PRIMARY KEY 去重，回填后一般无需每日运行
- 指数: 每次全量拉取，INSERT OR REPLACE
- A/H 溢价: 同指数

---

## 2. 五角大楼披萨历史

### 背景

Pentagon Pizza Index 是一个非官方但有趣的地缘政治指标：

- **原理**: 五角大楼附近披萨店订单量变化反映国防承包商/军方人员活动强度
- **解释**:
  - **Level 1-2 (过冷/偏冷)**: 订单稀少，国防活动低迷
  - **Level 3 (中性)**: 正常基线
  - **Level 4-5 (偏热/过热)**: 订单激增，可能预示重大国防合同、演习或地缘事件

### 数据库

- **目标库**: `data/radar/radar.duckdb`
- **表**: `pentagon_pizza_history`
- **字段**:
  - `date` (DATE, PK)
  - `level` (INTEGER 1-5)
  - `headline` (VARCHAR)
  - `status` (VARCHAR) - observed/sample
  - `description` (VARCHAR)
  - `temperature_band` (VARCHAR) - 过冷/偏冷/中性/偏热/过热
  - `created_at` (TIMESTAMP)

### 数据源

- **主站**: `https://pizzint.watch`
- **采集方式**: HTML 抓取
  - 解析日期、level、headline、description
  - 正则匹配 (适应网站结构变化)
- **降级策略**:
  - 抓取失败 → 生成 30 天示例数据 (catalog-ready)
  - 示例数据可用于测试和展示

### 温度带映射

| Level | Temperature Band | 含义 |
|-------|------------------|------|
| 1 | 过冷 | 订单非常稀少 |
| 2 | 偏冷 | 订单较少 |
| 3 | 中性 | 正常水平 |
| 4 | 偏热 | 订单增加 |
| 5 | 过热 | 订单激增 |

### 依赖

```bash
pip install requests duckdb pandas
```

### 执行

```bash
python3 scripts/sync_pentagon_pizza_history.py
```

### 增量策略

- 基于 `date` PRIMARY KEY 检查是否已存在
- 仅插入新日期记录
- 可每日运行补充最新数据

### 局限性

- 非官方数据，娱乐性质较强
- 网站结构可能变化，需定期维护抓取逻辑
- 建议结合其他地缘/国防指标交叉验证

---

## 通用说明

### 目标库路径

两个脚本共用同一个 DuckDB:

```
data/radar/radar.duckdb
```

首次运行自动创建目录和表。

### 可重复执行

- 所有 INSERT 使用 `INSERT OR REPLACE` 或基于 PRIMARY KEY 去重
- 失败时打印详细错误信息
- 不破坏现有数据

### 与其他脚本关系

- **不依赖** `scripts/sync_intelligence.py`
- **不修改** 任何 router/template/service 旧文件
- **独立运行**: 可单独执行，也可纳入定时任务

### 下一步建议

1. **香港流动性**:
   - 纳入每日 ETL 流程 (指数、A/H 溢价每日更新)
   - 资金流数据定期回填 (投资.db 有更新时运行)
   - 访客流量后续补充真实数据源

2. **披萨指数**:
   - 纳入每日轻量抓取
   - 监控网站结构变化
   - 考虑添加可视化 (温度带时间序列图)

3. **Catalog 集成**:
   - 两个数据集均可在 catalog 中注册为独立 dataset
   - A/H 溢价和披萨指数的 placeholder 确保 catalog 可展示结构

---

*文档生成日期: 2026-04-24*
