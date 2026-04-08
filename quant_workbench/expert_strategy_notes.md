# Quant Workbench Expert Expansion

## 1. Fields & Metrics
- `eps_fy1`, `eps_fy2`, `eps_rev_30d`, `eps_rev_90d`, `target_price_median`, `target_upside_pct`
- `ocf_to_np`, `fcf_margin`, `capex_to_revenue`, `receivable_days_yoy`, `inventory_days_yoy`
- `pe_ttm_pctile_5y`, `pb_pctile_5y`, `ev_ebitda_pctile_5y`, `sector_pe_relative`
- `northbound_net_5d`, `southbound_net_5d`, `main_inflow_5d`, `margin_balance_chg_20d`, `turnover_pctile_1y`
- `beta_250d`, `vol_20d`, `downside_vol_60d`, `max_drawdown_1y`, `corr_hsi_120d`
- `earnings_days_to_next`, `unlock_days_to_next`, `dividend_days_to_next`, `event_risk_flag`
- `sentiment_dispersion`, `coverage_breadth_30d`, `sentiment_accel_20d`

## 2. Data Sources & Mapping
1. **Price / volatility / correlations** – reuse existing `parquet` files; compute rolling stats and quantiles with DuckDB/pandas.
2. **Cash flow & quality** – extend `investment.db` (table `stock_financial`) with `operating_cash_flow`, `fcf`, `capex`, `receivable_days`, `inventory_days`; maintain historical snapshots.
3. **Consensus & target prices** – add table `stock_estimate(consensus_date, code, eps_fy1, eps_fy2, target_price, coverage)` populated from Tushare/同花顺 or internal manual inputs.
4. **Capital flow** – AkShare APIs for `north_money_flow`, `south_money_flow`, `fund_main_net`, `margin_balance`; store daily aggregates in `stock_flow_daily`.
5. **Risk & event calendar** – ingest AkShare calendar + manual events into `stock_event_calendar` (event_type, importance, date); compute days-to-next and flags.
6. **Sentiment breadth** – expand `reports.db` with coverage count, standard deviation of `_score_title`, acceleration over 20-day windows.

## 3. Scoring Framework
1. **Six factor totals**: `quality`, `growth`, `valuation`, `flow`, `technical`, `risk`. Map each to 0..100 via `score = clip(mean_zscore * 10 + 50, 0, 100)`.
2. **Model weights**:
   - *Conservative*: `total = 0.25*quality + 0.20*growth + 0.15*valuation + 0.15*flow + 0.20*technical - 0.15*risk`
   - *Aggressive*: `total = 0.15*quality + 0.25*growth + 0.10*valuation + 0.20*flow + 0.25*technical - 0.15*risk`
3. **Risk deduction**: `risk_score = 0.35*vol_rank + 0.25*mdd_rank + 0.25*danger_event + 0.15*corr_rank`.
4. **Grade thresholds**: `A` if `total>=75` and `technical>=65` and `growth>=60` and `risk<=40`; `B` if `total>=62`; else `C`.

## 4. Output Template
- `action`: `buy/watch/avoid`
- `position_range`: e.g., `2%-5%`
- `thesis`: three bullets combining growth/valuation/flow
- `invalid_conditions`: sharpe-containing failure triggers (e.g., `跌破60日线且主力流出`)
- `review_at`: next event or periodic date (财报前/每周)

## 5. Persistence Layers (SQL)
```sql
CREATE TABLE IF NOT EXISTS stock_factor_snapshot (
  trade_date TEXT,
  code TEXT,
  model TEXT,
  quality REAL,
  growth REAL,
  valuation REAL,
  flow REAL,
  technical REAL,
  risk REAL,
  total REAL,
  primary_key (trade_date, code, model)
);

CREATE TABLE IF NOT EXISTS stock_event_calendar (
  code TEXT,
  event_type TEXT,
  event_date TEXT,
  importance INTEGER,
  note TEXT
);

CREATE TABLE IF NOT EXISTS stock_estimate (
  consensus_date TEXT,
  code TEXT,
  eps_fy1 REAL,
  eps_fy2 REAL,
  target_price REAL,
  target_upside REAL,
  coverage INTEGER,
  PRIMARY KEY(consensus_date, code)
);

CREATE TABLE IF NOT EXISTS stock_flow_daily (
  trade_date TEXT,
  code TEXT,
  northbound_net REAL,
  southbound_net REAL,
  main_inflow REAL,
  margin_balance REAL,
  turnover_rank REAL,
  PRIMARY KEY(trade_date, code)
);
```

## 6. Scoring Implementation Snippet
```python
def score_factor(value, zscore_table):
    raw = zscore_table.get(value.code, 0)
    return max(min(50 + raw * 10, 100), 0)

quality = score_factor(stock, quality_table)
growth = score_factor(stock, growth_table)
valuation = score_factor(stock, valuation_table)
flow = score_factor(stock, flow_table)
technical = score_factor(stock, technical_table)
risk = compute_risk_rank(stock, event_table)
total = weights[model]['quality'] * quality + ... - weights[model]['risk'] * risk

record_snapshot(trade_date, code, model, quality, growth, valuation, flow, technical, risk, total)
```

## 7. Implementation Plan
1. Day 1-2: Import capital flow, risk, event, estimate tables; compute z-scores/quantiles.
2. Day 3: Expand scoring engine to write snapshots and output thesis/position template.
3. Day 4: Add `stock_factor_snapshot` read path for `/api/overview` so UI can show more context.
4. Day 5: Front-end surfaces new fields (`position_range`, `invalid_conditions`, `review_at`).
