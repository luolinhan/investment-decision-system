# Radar Reports 导出指南

## 概述

`export_radar_reports.py` 用于将 Investment Radar 的智能概览导出为 Obsidian 笔记或本地 Markdown 报告。

支持的报告类型：
- **日报** (`--report daily`)：最近 1 天的事件和研究
- **周报** (`--report weekly`)：最近 7 天的事件和研究
- **月报** (`--report monthly`)：最近 30 天的事件和研究
- **主题追踪** (`--report thesis`)：按研究主题拆分的追踪卡片

## 使用方法

### 导出到 Obsidian Vault

```bash
# 使用环境变量配置的 Obsidian 路径（默认：~/Documents/Obsidian/投资决策系统）
python3 scripts/export_radar_reports.py --report daily

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
| `OBSIDIAN_VAULT_PATH` | Obsidian 仓库路径 | `~/Documents/Obsidian/投资决策系统` |
| `INVESTMENT_DB_PATH` | SQLite 数据库路径 | `./data/investment.db` |

## 报告内容

每份报告包含：
- **Summary**：活跃事件数、P0/P1 事件数、研究报数、启用源数
- **Gaps / Issues**：采集缺口和问题检测
- **分类事件**：AI 模型、生物医药、港股市场、外部事件
- **近期研究**：研究摘要和论点
- **源健康状态**：失败的源及其错误
- **最近采集运行**：采集时间、状态、记录数

## 主题追踪报告

`--report thesis` 会为每个研究主题生成独立的追踪卡片，包含：
- 研究来源和摘要
- 相关事件
- 待办事项（验证、检查、更新）

这些卡片保存在 `Investment-Radar-Reports/Thesis-Tracking/` 子目录。

## 自动化运行（Windows）

可创建批处理脚本 `radar_report_task.bat`：

```batch
@echo off
cd /d "%~dp0"
python scripts\export_radar_reports.py --report daily
python scripts\export_radar_reports.py --report thesis
echo Radar reports exported at %date% %time%
```

然后通过 Windows 任务计划程序设置定时任务。

## 验证

运行以下命令验证脚本：

```bash
python3 -m py_compile scripts/export_radar_reports.py
python3 scripts/export_radar_reports.py --report daily --output-dir /tmp/test
```
