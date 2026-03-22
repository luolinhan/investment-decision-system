# 港股研报下载系统 - 技术方案

## 项目概述

在 Windows 电脑上部署研报下载系统，自动抓取港股大公司（阿里巴巴、腾讯、美团、快手、小米等）和创新药公司的券商研报，使用 Claude 生成摘要，通过 Web 界面查看，Mac 可通过 Tailscale 访问。

## 系统架构

```
┌────────────────────────────────────────────────────────────────────┐
│                      Windows 服务器 (Tailscale: 100.64.93.19)       │
├────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
│  │ 数据采集层   │───▶│ 处理存储层   │───▶│ Web展示层 (FastAPI)     │ │
│  │             │    │             │    │                         │ │
│  │ • AKShare   │    │ • PDF存储   │    │ • 研报列表浏览          │ │
│  │ • 东方财富  │    │ • SQLite    │    │ • PDF在线预览          │ │
│  │ • 慧博投研  │    │ • 元数据     │    │ • 摘要展示              │ │
│  │ • 雪球      │    │             │    │ • 搜索筛选              │ │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘ │
│         │                  │                      │                 │
│         ▼                  ▼                      ▼                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
│  │ Claude API  │    │ 定时任务     │    │ 端口: 8080              │ │
│  │ (摘要生成)  │    │ (每日更新)   │    │ Mac通过Tailscale访问     │ │
│  └─────────────┘    └─────────────┘    └─────────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

## 数据源方案

### 主数据源：东方财富 + AKShare（免费、稳定）

**优点**：
- 完全免费，无需登录
- AKShare 有现成接口，维护活跃
- 研报元数据完整（标题、机构、日期、评级）

**接口**：
```python
import akshare as ak
# 券商研报列表
df = ak.stock_report_em(symbol="研究报告")
# 个股研报
df = ak.stock_individual_info_em(symbol="阿里巴巴-W")
```

### 补充数据源：慧博投研（需登录，研报最全）

**优点**：
- 研报最全，覆盖所有券商
- 有摩根士丹利、高盛、瑞银等大行研报
- PDF下载完整

**缺点**：
- 需要登录（免费注册即可）
- 有每日下载限制

### 辅助数据源：雪球（观点+摘要）

**优点**：
- 有机构观点摘要
- 用户讨论活跃

**用途**：
- 补充市场情绪和观点
- 获取研报摘要信息

## 目标公司清单

### 港股恒生科技成分股
| 代码 | 名称 | 重点方向 |
|------|------|---------|
| 09988.HK | 阿里巴巴-W | 电商、云计算、AI |
| 00700.HK | 腾讯控股 | 游戏、社交、金融科技 |
| 03690.HK | 美团-W | 本地生活、外卖 |
| 01024.HK | 快手-W | 短视频、电商 |
| 01810.HK | 小米集团-W | 手机、IoT、汽车 |

### 创新药（待确认）
| 代码 | 名称 | 重点方向 |
|------|------|---------|
| 01811.HK | 信达生物 | 肿瘤药 |
| 06160.HK | 百济神州 | 创新药 |
| 600276.SH | 恒瑞医药 | 创新药 |

## 技术栈

### 后端
- **Python 3.11+**
- **FastAPI** - Web框架
- **SQLite** - 轻量数据库
- **AKShare** - 数据获取
- **APScheduler** - 定时任务
- **httpx** - HTTP客户端

### 前端
- **Jinja2模板** - 服务端渲染
- **Bootstrap 5** - UI框架
- **PDF.js** - PDF在线预览
- **Alpine.js** - 轻量交互

### AI摘要
- **Claude API** - 使用 Claude 生成研报摘要
- 需要配置 ANTHROPIC_API_KEY

### 部署
- **Windows Python 环境** - 直接运行（更简单）
- 可选 Docker（需要额外配置）

## 项目结构

```
research_report_system/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI 入口
│   ├── config.py               # 配置管理
│   ├── models.py               # 数据模型
│   ├── database.py             # 数据库操作
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── reports.py          # 研报接口
│   │   └── settings.py         # 设置接口
│   ├── services/
│   │   ├── __init__.py
│   │   ├── collector.py        # 数据采集服务
│   │   ├── akshare_source.py   # AKShare数据源
│   │   ├── huibo_source.py     # 慧博数据源
│   │   └── summarizer.py       # AI摘要服务
│   └── utils/
│       ├── __init__.py
│       └── helpers.py
├── templates/
│   ├── base.html
│   ├── index.html              # 首页
│   ├── report_detail.html      # 研报详情
│   └── settings.html           # 设置页面
├── static/
│   ├── css/
│   ├── js/
│   └── pdf.js/                 # PDF预览库
├── data/
│   ├── reports.db              # SQLite数据库
│   └── pdfs/                   # PDF文件存储
├── scheduler.py                # 定时任务
├── requirements.txt
├── .env                        # 环境变量
└── README.md
```

## 数据库设计

### reports 表
```sql
CREATE TABLE reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    institution TEXT,           -- 研报机构
    author TEXT,                -- 分析师
    rating TEXT,                -- 评级
    publish_date DATE,
    pdf_url TEXT,
    local_pdf_path TEXT,        -- 本地PDF路径
    summary TEXT,               -- AI摘要
    raw_content TEXT,           -- 原文内容
    source TEXT,                -- 来源
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### stocks 表（关注的股票）
```sql
CREATE TABLE stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    market TEXT,                -- HK/A/US
    category TEXT,              -- 科技/医药
    enabled BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 核心功能

### 1. 自动采集
- 每日凌晨自动抓取新研报
- 支持手动触发采集
- 去重处理

### 2. PDF下载
- 自动下载PDF到本地
- 断点续传支持
- 文件命名规则：`{日期}_{机构}_{股票代码}_{标题}.pdf`

### 3. AI摘要
- 使用 Claude API 生成摘要
- 摘要包括：核心观点、评级变化、风险提示
- 批量处理队列

### 4. Web界面
- 研报列表（分页、筛选、搜索）
- PDF在线预览
- 按股票/机构/日期筛选
- 收藏功能

### 5. 多端访问
- Windows本地访问：http://localhost:8080
- Mac远程访问：http://100.64.93.19:8080 (Tailscale)

## 实现步骤

### Phase 1: 基础框架（1-2天）
1. 项目初始化
2. FastAPI 基础框架
3. 数据库设计和初始化
4. 基础前端页面

### Phase 2: 数据采集（2-3天）
1. AKShare 数据源接入
2. 研报列表获取
3. PDF下载功能
4. 定时任务配置

### Phase 3: AI摘要（1天）
1. Claude API 集成
2. 摘要生成逻辑
3. 批量处理队列

### Phase 4: Web界面优化（1-2天）
1. 研报列表页面
2. PDF预览功能
3. 筛选搜索功能
4. 响应式设计

### Phase 5: 测试部署（1天）
1. Windows环境测试
2. Mac远程访问测试
3. 文档编写

## 环境要求

### Windows服务器
- Python 3.11+
- 10GB+ 存储空间（PDF文件）
- 网络连接

### API密钥
- Anthropic API Key（Claude摘要）

### 可选
- 慧博投研账号（免费注册）