# Live / Sample / Fallback Separation

## 规则

- `sample_demo` 和 `fallback_placeholder` 不进入默认主机会队列。
- `generated_inference` 必须有 live source 支撑才能提升。
- UI 默认隐藏 sample/fallback。
- API 必须返回 `data_source_class`、`live_source_count`、`sample_source_count`。
- sample/fallback 卡如展示，只能进入 demo/diagnostic 区。

## 当前 V2 问题

当前 V2 可生成 `cache_status=sample_fallback` 且 `generation_status=actionable` 的卡。这是 V3 的 P0 修复项。

## V3 输出

- `cards`: 默认 live-only / executable-safe。
- `sample_cards`: 样例卡。
- `blocked_cards`: 被 gating 拦截的卡。
- `source_quality_summary`: live/sample/fallback 占比和缺来源结论数。

