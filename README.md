# 研报下载系统

自动采集港股、A股券商研报，提供智能摘要和便捷查阅。

## 功能特点

- 🔄 **自动采集**：每日自动采集最新研报
- 📊 **多数据源**：支持东方财富、慧博投研等数据源
- 📱 **Web界面**：响应式设计，支持移动端访问
- 🔍 **智能搜索**：按股票、机构、日期等筛选
- 📄 **PDF管理**：自动下载和在线预览
- 🤖 **AI摘要**：支持Claude API生成研报摘要

## 快速开始

### 1. 启动服务

双击 `start.bat` 即可启动服务。

首次启动会自动：
- 创建Python虚拟环境
- 安装所需依赖
- 初始化数据库

### 2. 访问系统

- 本地访问：http://localhost:8080
- 远程访问：http://100.64.93.19:8080 (通过Tailscale)

## 项目结构

```
research_report_system/
├── app/                    # 应用代码
│   ├── main.py            # FastAPI入口
│   ├── config.py          # 配置管理
│   ├── models.py          # 数据模型
│   ├── database.py        # 数据库操作
│   ├── routers/           # API路由
│   └── services/          # 业务服务
├── templates/              # 前端模板
├── static/                 # 静态文件
├── data/                   # 数据存储
│   ├── reports.db         # SQLite数据库
│   └── pdfs/              # PDF文件
├── logs/                   # 日志文件
├── start.bat              # 启动脚本
└── requirements.txt       # Python依赖
```

## 配置

编辑 `.env` 文件进行配置：

```env
# 慧博投研账号（可选，获取更多研报）
HUIBOR_USERNAME=your_username
HUIBOR_PASSWORD=your_password

# Claude API（可选，AI摘要功能）
ANTHROPIC_API_KEY=your_api_key

# 定时任务
SCHEDULER_HOUR=6
SCHEDULER_MINUTE=0
```

## API接口

### 研报接口

- `GET /api/reports` - 获取研报列表
- `GET /api/reports/{id}` - 获取研报详情
- `GET /api/reports/{id}/pdf` - 获取研报PDF
- `POST /api/reports/collect` - 触发采集

### 股票接口

- `GET /api/reports/stocks/list` - 获取股票列表
- `POST /api/reports/stocks/add` - 添加股票
- `DELETE /api/reports/stocks/{id}` - 删除股票

### 统计接口

- `GET /api/reports/stats/overview` - 统计概览

## 关注股票

默认关注以下股票：

### 港股恒生科技
- 阿里巴巴-W (09988.HK)
- 腾讯控股 (00700.HK)
- 美团-W (03690.HK)
- 快手-W (01024.HK)
- 小米集团-W (01810.HK)

### 港股医药
- 百济神州 (06160.HK)
- 荣昌生物 (09995.HK)
- 信达生物 (01811.HK)
- 中国生物制药 (01177.HK)
- 复星医药 (02196.HK)
- 药明康德 (02359.HK)
- 药明生物 (02269.HK)
- 昭衍新药 (06127.HK)
- 华润医药 (03320.HK)

### 港股其他
- 中国中免 (01880.HK)
- 中国东方航空 (00670.HK)
- 中国海洋石油 (00883.HK)

### A股光伏
- 晶澳科技 (002459.SZ)
- 通威股份 (600438.SH)
- 隆基绿能 (601012.SH)
- 锦浪科技 (300763.SZ)

## 开发计划

- [ ] 慧博投研数据源完善
- [ ] AI摘要功能
- [ ] 经济数据集成
- [ ] 邮件/微信推送
- [ ] 研报对比分析

## 许可证

MIT License