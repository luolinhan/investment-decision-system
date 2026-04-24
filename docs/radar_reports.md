# Radar Reports 导出指南

## 概述

`export_radar_reports.py` 用于将 Investment Radar 的统一概览导出为 Obsidian 笔记或本地 Markdown 报告。

数据源：
- `app.services.radar_service.RadarService`
- DuckDB `data/radar/radar.duckdb`
- SQLite Intelligence 库（通过 RadarService 间接读取）
- Obsidian Memory 索引（通过 RadarService 间接读取）

支持的报告类型：
- **日报** (`--report daily`)
- **周报** (`--report weekly`)
- **月报** (`--report monthly`)
- **主题追踪** (`--report thesis`)
- **到期报告集** (`--report due`)：始终导出日报和 thesis，周一额外导出周报，每月 1 日额外导出月报
- **全量报告集** (`--report all`)

## 使用方法

### 导出到 Obsidian Vault

```bash
# 使用环境变量配置的 Obsidian 路径（默认自动探测知识库路径）
python3 scripts/export_radar_reports.py --report due

# 指定自定义 Vault 路径
python3 scripts/export_radar_reports.py --report daily --vault-path /path/to/your/vault
```

报告会保存到 `Investment-Radar-Reports/` 子目录，不会影响原有笔记。

### 导出到本地临时目录（测试用）

```bash
python3 scripts/export_radar_reports.py --report daily --output-dir ./reports
python3 scripts/export_radar_reports.py --report weekly --output-dir ./reports
python3 scripts/export_radar_reports.py --report monthly --output-dir ./reports
python3 scripts/export_radar_reports.py --report thesis --output-dir ./reports/thesis
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `INVESTMENT_OBSIDIAN_VAULT` | Obsidian 仓库路径（优先） | 自动探测 |
| `OBSIDIAN_VAULT_PATH` | Obsidian 仓库路径（兼容） | 自动探测 |
| `INVESTMENT_DB_PATH` | DuckDB 路径覆盖（传给 RadarService） | `./data/radar/radar.duckdb` |

## 报告内容

每份报告包含：
- **Summary**：`macro_regime`、外部风险分、港股流动性分、赛道预埋分、置信度、覆盖率
- **Macro / External / HK**：驱动项、覆盖率、更新时间、趋势窗口
- **Thesis Board**：赛道卡、已验证/未验证变量、失效条件、观察池
- **Pentagon Pizza**：当前温度带、分位、短期变化
- **Data Gaps**：核心缺口与来源
- **Pipeline Health**：最近 source runs、失败数、最近同步时间
- **Research Memory**：Obsidian 关联笔记

## 主题追踪报告

`--report thesis` 会为每个 thesis card 生成独立追踪卡片，包含：
- 领先变量 / 已验证变量 / 未验证变量
- 风险变量 / 失效条件 / 观察池
- 相关政策事件
- 相关 Obsidian 研究记忆

这些卡片保存在 `Investment-Radar-Reports/Thesis-Tracking/` 子目录。

## 自动化运行（Windows）

当前仓库已提供：

```batch
radar_report_task.bat
```

默认执行：

```batch
python scripts\export_radar_reports.py --report due --force-refresh
```

## 验证

运行以下命令验证脚本：

```bash
python3 -m py_compile scripts/export_radar_reports.py
python3 scripts/export_radar_reports.py --report daily --output-dir /tmp/test
python3 scripts/export_radar_reports.py --report thesis --output-dir /tmp/test
```
