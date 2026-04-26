# Entity / Instrument Model

## Entity

Entity 是经济或产业对象：

- 公司
- 主题
- 产业节点
- 商品
- 指数/风格

字段：

- `entity_id`
- `entity_type`
- `name_zh`
- `name_en`
- `sector_id`
- `theme_ids`
- `description`

## Instrument

Instrument 是可观察或可交易载体：

- A 股
- 港股
- 美股
- ETF
- 期货
- 指数

字段：

- `instrument_id`
- `entity_id`
- `market`
- `ticker`
- `name`
- `instrument_type`
- `currency`
- `exchange`
- `liquidity_tier`
- `active`

## 聚合规则

- 同一 entity 的不同 instrument 在母 thesis 下聚合。
- 不同 instrument 展示流动性、可交易性、风险和本地因子差异。
- ETF / proxy 不能替代公司实体，只能作为 proxy variant。

