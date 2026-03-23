# 投资决策系统项目总结

---

# 项目概述

## 项目名称
**投资决策系统** - 6模块投资分析仪表板

## 代码仓库
- GitHub: https://github.com/luolinhan/investment-decision-system

## 访问地址
- 内网: http://192.168.3.87:8080/investment/
- 外网(Tailscale): http://100.64.93.19:8080/investment/

---

# 一、系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                           用户访问                                   │
│              http://100.64.93.19:8080/investment/                    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Windows 服务器 (192.168.3.87)                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │  FastAPI    │  │   SQLite    │  │   Jinja2    │                 │
│  │  后端服务    │  │   数据库    │  │   前端模板   │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
│                                                                      │
│  项目位置: C:\Users\Administrator\research_report_system            │
└─────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ data.json
                                    │
┌─────────────────────────────────────────────────────────────────────┐
│                    阿里云服务器 (47.88.90.29)                         │
│                                                                      │
│  数据采集脚本: /root/fetch_aliyun_v2.py                              │
│  数据来源: 腾讯API (qt.gtimg.cn)                                      │
└─────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ API请求
                                    │
┌─────────────────────────────────────────────────────────────────────┐
│                         腾讯财经 API                                  │
│                    https://qt.gtimg.cn                               │
│                                                                      │
│  提供数据: 股票价格、PE、PB、涨跌幅、指数数据                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

# 二、技术栈

| 层级 | 技术 | 版本/说明 |
|------|------|----------|
| **后端** | Python FastAPI | 异步Web框架 |
| **数据库** | SQLite | 本地数据库 |
| **前端** | Jinja2 + Chart.js | 模板渲染 + 图表 |
| **数据采集** | Python urllib | 阿里云执行 |
| **组网** | Tailscale | 跨网络访问 |

---

# 三、6个功能模块

| 模块 | 功能 | 数据状态 |
|------|------|----------|
| **全景中枢** | 指数卡片、VIX、利率、关注股票 | ✅ 已完成 |
| **宏观流动性** | 利率趋势、流动性指标 | ⚠️ 部分完成 |
| **微观基本面** | 股票财务指标、PE/PB/ROE | ✅ 已完成 |
| **行业模型** | TMT/医药/消费行业数据 | ❌ 待开发 |
| **量化技术** | 估值分位数、技术指标 | ❌ 待开发 |
| **数据管理** | 数据导入、日志查看 | ⚠️ 部分完成 |

---

# 四、数据库表结构

```sql
-- 指数历史数据
index_history (code, name, trade_date, close, change_pct)

-- 股票财务数据
stock_financial (code, name, report_date, pe_ttm, pb, roe,
                 gross_margin, net_margin, revenue_yoy, net_profit_yoy, dividend_yield)

-- 关注股票列表
watch_list (code, name, market, category, enabled)

-- VIX历史
vix_history (trade_date, vix_close)

-- 利率数据
interest_rates (trade_date, shibor_overnight, shibor_1w, shibor_1m, ...)

-- 其他表（待使用）
valuation_bands, technical_indicators, sector_tmt, sector_biotech, sector_consumer, etl_logs
```

---

# 五、关键文件位置

## Windows 服务器

```
C:\Users\Administrator\research_report_system\
├── app\
│   ├── main.py                    # FastAPI入口
│   ├── routers\
│   │   └── investment.py          # 投资模块API路由
│   └── services\
│       └── investment_db_service.py  # 数据库服务
├── templates\
│   └── investment.html            # 前端页面 (6个Tab)
├── data\
│   ├── investment.db              # SQLite数据库
│   ├── data.json                  # 数据中转文件
│   └── templates\
│       └── fundamentals_latest.csv # 股票数据CSV
├── fetch_stock_windows.py         # 股票采集(本地)
├── fetch_market_data.py           # 指数采集(本地)
├── update_from_json.py            # 从JSON更新数据库
├── daily_update.bat               # 定时任务脚本
├── daily_update_all.py            # 综合更新脚本
└── init_investment_db.py          # 数据库初始化
```

## 阿里云服务器

```
/root/
├── fetch_aliyun_v2.py             # 数据采集脚本
└── openclaw.pem                   # SSH私钥
```

---

# 六、数据采集流程

## 正常流程

```bash
# 步骤1: 在阿里云采集数据
ssh -i ~/.ssh/openclaw.pem root@47.88.90.29
cd /root && python3 fetch_aliyun_v2.py

# 步骤2: 复制数据到Windows
# (数据会输出JSON，保存到Windows的data.json)

# 步骤3: 在Windows更新数据库
cd C:\Users\Administrator\research_report_system
python update_from_json.py

# 步骤4: 重启服务（如需要）
taskkill /f /im python.exe
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## 定时任务

Windows已配置每天15:30自动执行 `daily_update.bat`

---

# 七、踩过的坑

## 问题1: 东方财富API无法访问
- **现象**: `push2.eastmoney.com` 返回连接关闭
- **原因**: 代理/网络问题
- **解决**: 改用腾讯API `qt.gtimg.cn`

## 问题2: Windows代理导致API请求失败
- **现象**: Python请求超时或连接失败
- **原因**: Clash代理配置问题
- **解决**: 在阿里云服务器采集数据，绕过Windows代理

## 问题3: 数据库重复记录
- **现象**: 每只股票有多条记录
- **原因**: INSERT时未清理旧数据
- **解决**: 先DELETE再INSERT，或在查询时用子查询取最新

## 问题4: 港股PB数据缺失
- **现象**: 港股PB字段返回非数值
- **原因**: 腾讯API港股字段格式不同
- **解决**: 接受缺失，A股PB正常显示

## 问题5: VIX和SHIBOR API不可用
- **现象**: 新浪/东方财富的VIX/SHIBOR接口返回空
- **解决**: 使用固定默认值（VIX=18.5, SHIBOR手动估算）

---

# 八、后续待完善

## 高优先级

| 任务 | 说明 |
|------|------|
| 行业模型数据 | TMT、医药、消费行业指标 |
| 量化技术模块 | 估值分位数、技术指标计算 |
| 市场情绪数据 | 涨跌停统计、板块轮动 |
| 数据自动同步 | 阿里云→Windows自动化 |

## 中优先级

| 任务 | 说明 |
|------|------|
| 美股实时数据 | 道琼斯/纳斯达克/标普500实时价格 |
| 港股PB补充 | 寻找其他数据源 |
| 历史数据积累 | 保存每日数据用于趋势分析 |
| 报警功能 | 异常波动提醒 |

---

# 九、快速操作指南

## 启动服务

```powershell
cd C:\Users\Administrator\research_report_system
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## 更新数据

```powershell
# 方法1: 从阿里云获取最新数据
python update_from_json.py

# 方法2: 本地采集（可能因代理失败）
python fetch_stock_windows.py
python fetch_market_data.py
```

## 查看数据库

```powershell
python -c "
import sqlite3
c = sqlite3.connect('data/investment.db').cursor()
c.execute('SELECT code, name, pe_ttm FROM stock_financial')
for r in c.fetchall(): print(r)
"
```

## 检查服务状态

```powershell
curl http://localhost:8080/investment/api/overview
```

---

# 十、关注股票列表

| 代码 | 名称 | 行业 | 市场 |
|------|------|------|------|
| sh603259 | 药明康德 | CXO | A股 |
| sh600438 | 通威股份 | 光伏 | A股 |
| sh601012 | 隆基绿能 | 光伏 | A股 |
| sz002459 | 晶澳科技 | 光伏 | A股 |
| sz300763 | 锦浪科技 | 光伏 | A股 |
| sh688235 | 百济神州 | 医药 | A股 |
| sh600196 | 复星医药 | 医药 | A股 |
| sh601888 | 中国中免 | 消费 | A股 |
| hk02269 | 药明生物 | CXO | 港股 |
| hk06160 | 百济神州 | 医药 | 港股 |
| hk01177 | 中国生物制药 | 医药 | 港股 |
| hk01880 | 中国中免 | 消费 | 港股 |
| hk00700 | 腾讯控股 | 科技 | 港股 |
| hk03690 | 美团-W | 科技 | 港股 |
| hk01810 | 小米集团-W | 科技 | 港股 |
| hk01024 | 快手-W | 科技 | 港股 |
| hk09988 | 阿里巴巴-W | 科技 | 港股 |
| hk00883 | 中国海洋石油 | 能源 | 港股 |

---

# 十一、连接信息速查

| 节点 | IP | 用户 | 密码/密钥 |
|------|-----|------|----------|
| Windows | 192.168.3.87 / 100.64.93.19 | Administrator | zxsoft00# |
| 阿里云 | 47.88.90.29 | root | ~/.ssh/openclaw.pem |

---

# 十二、API接口文档

## 全景中枢

| 接口 | 说明 |
|------|------|
| GET /investment/api/overview | 市场概览（指数、VIX、利率、关注股票） |
| GET /investment/api/watch-stocks | 关注股票实时行情 |
| GET /investment/api/index-history/{symbol} | 指数历史数据 |

## 微观基本面

| 接口 | 说明 |
|------|------|
| GET /investment/api/fundamentals/watch-list | 关注股票财务数据 |
| GET /investment/api/fundamentals/financial/{code} | 单只股票财务详情 |

## 宏观流动性

| 接口 | 说明 |
|------|------|
| GET /investment/api/macro/overview | 宏观概览 |
| GET /investment/api/macro/rates-history | 利率历史 |

## 数据管理

| 接口 | 说明 |
|------|------|
| GET /investment/api/etl/logs | ETL日志 |
| POST /investment/api/etl/import-csv | 导入CSV数据 |

---

*文档更新时间: 2026-03-23*