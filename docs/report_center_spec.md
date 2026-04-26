# Report Center Spec

## 报告类型

- daily
- weekly
- monthly
- sector
- company
- event_preview
- post_event
- postmortem

## 必须能力

- 所有报告入 `reports`。
- 章节入 `report_sections`。
- 全文检索走 `reports_fts`。
- 报告能反查 `citations`。
- 报告能导出到 Obsidian 独立目录。
- 报告保存 version 和 content_hash，用于后续 diff。

## 与现有 brief 的关系

`lead_lag_briefs.py` 继续生成固定时点 brief。V3 增加入库步骤，把 brief 同步到 Report Center，而不是只写 Markdown/JSON 文件。

