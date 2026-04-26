# Source Archive Policy

## 目标

任何被用于机会卡、事件、报告或回放结论的来源，都必须可复核。

## 归档规则

- HTML 保存到 `data/archive/html/`。
- PDF 保存到 `data/archive/pdf/`。
- 提取文本保存到 `data/archive/text/`。
- 生成报告保存到 `data/archive/reports/`。
- 截图保存到 `data/archive/screenshots/`。

## 数据库记录

每个归档文件必须在 `source_documents` 中保存：

- 原始 URL
- canonical URL
- 本地路径
- 文件 hash
- 内容 hash
- 抓取时间
- 发布时间
- parser version
- parse status
- extraction quality

## 不可接受

- 只有摘要、没有原始 URL。
- 只有 URL、没有 fetched_at。
- 只有模型推断、没有 source document。
- sample/fallback 来源被标为 live。

