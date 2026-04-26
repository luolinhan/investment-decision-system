# Source Reliability Rules

## data_source_class

- `live_official`: 交易所、监管、公司公告、官方宏观。
- `live_public`: 可复现的公开数据、公开研究索引、公共行情数据。
- `live_media`: 媒体报道或新闻聚合，只能辅助确认。
- `user_curated`: 用户研究笔记和人工维护清单，只能作上下文。
- `generated_inference`: 系统生成的推断，不能单独形成高优先级机会。
- `sample_demo`: 样例数据。
- `fallback_placeholder`: 回退占位。

## 可执行机会门槛

- 至少 2 个独立来源。
- 至少 1 个 `live_official` 或 `live_public`。
- `generated_inference` 不能单独构成高优先级机会。
- `sample_demo` 和 `fallback_placeholder` 不允许进入默认主机会队列。

## 降级规则

- 无本地可交易 instrument：降级。
- source URL 缺失：降级。
- 文档未归档：降级或标为 partial。
- 检查点过期：标记 stale。
- 跨市场映射无合理路径：降级并产生 mapping pollution alert。

