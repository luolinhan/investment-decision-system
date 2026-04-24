# Radar Data Contract

> Defines the schema, semantics, and operational expectations for the macro
> radar data layer that powers Investment Hub's signal generation and dashboard.

## 1. Storage

| Item              | Value                                              |
|-------------------|----------------------------------------------------|
| Engine            | DuckDB                                             |
| Default DB path   | `data/radar/radar.duckdb`                          |
| Parquet export dir| `data/radar/parquet/`                              |
| Timezone          | UTC (all timestamps stored as ISO 8601 strings)    |

## 2. Tables

### 2.1 `indicator_catalog`

Master directory of every indicator the radar system knows about.

| Column             | Type   | Nullable | Description                                    |
|--------------------|--------|----------|------------------------------------------------|
| `indicator_code`   | TEXT   | NO (PK)  | Unique code, e.g. `CN_M1_YOY`, `US_10Y_YIELD`  |
| `category`         | TEXT   | YES      | `macro_cn` / `global_macro` / `sentiment` / `flow` / `sector` |
| `indicator_type`   | TEXT   | YES      | `money_supply` / `inflation` / `rate` / `commodity` / `derived` / … |
| `frequency`        | TEXT   | YES      | `daily` / `weekly` / `monthly` / `quarterly`   |
| `direction`        | TEXT   | YES      | `leading` / `coincident` / `lagging` / `contrarian` |
| `half_life_days`   | REAL   | YES      | Estimated information decay half-life          |
| `affected_assets`  | TEXT   | YES      | JSON array: `["A_shares", "CN_bonds"]`         |
| `affected_sectors` | TEXT   | YES      | JSON array: `["banking", "real_estate"]`       |
| `source`           | TEXT   | YES      | Data source identifier                         |
| `confidence`       | REAL   | YES      | 0.0 – 1.0                                      |
| `last_update`      | TEXT   | YES      | ISO 8601 UTC                                   |
| `status`           | TEXT   | YES      | `active` / `planned` / `disabled`              |
| `notes`            | TEXT   | YES      | Free-text notes                                |

### 2.2 `indicator_observations`

Time-series values for individual indicators.

| Column             | Type    | Nullable | Description                              |
|--------------------|---------|----------|------------------------------------------|
| `id`               | INTEGER | NO (PK)  | Auto-increment                           |
| `indicator_code`   | TEXT    | NO (FK)  | → `indicator_catalog.indicator_code`     |
| `obs_date`         | TEXT    | NO       | ISO date (`YYYY-MM-DD` or `YYYY-MM-01`)  |
| `value`            | REAL    | YES      | Numeric observation value                |
| `unit`             | TEXT    | YES      | `pct` / `usd_per_oz` / `index` / `rate`  |
| `source`           | TEXT    | YES      | Origin of this observation               |
| `fetch_ts`         | TEXT    | YES      | ISO 8601 UTC when fetched                |
| `quality_flag`     | TEXT    | YES      | `good` / `estimated` / `stale` / `missing` |
| `notes`            | TEXT    | YES      | Free-text notes                          |

**Uniqueness**: `(indicator_code, obs_date, source)` — the same date+source pair
is upserted on repeat runs.

### 2.3 `source_runs`

Audit log of every sync execution.

| Column            | Type    | Nullable | Description                     |
|-------------------|---------|----------|---------------------------------|
| `id`              | INTEGER | NO (PK)  | Auto-increment                  |
| `source_name`     | TEXT    | NO       | e.g. `sync_radar_macro:VIX`     |
| `target_table`    | TEXT    | NO       | Which table was written         |
| `started_at`      | TEXT    | NO       | ISO 8601 UTC                    |
| `finished_at`     | TEXT    | YES      | ISO 8601 UTC                    |
| `status`          | TEXT    | YES      | `success` / `partial` / `failed`|
| `rows_read`       | INTEGER | YES      | Source rows consumed            |
| `rows_upserted`   | INTEGER | YES      | Rows written                    |
| `error_message`   | TEXT    | YES      | Error string if any             |
| `notes`           | TEXT    | YES      | Additional context              |

## 3. Indicator Status Semantics

| Status     | Meaning                                                      |
|------------|--------------------------------------------------------------|
| `active`   | Collectable now; data pipeline is operational                |
| `planned`  | Valuable indicator but source is not yet wired up            |
| `disabled` | Known to be unreliable, duplicated, or requires paid access  |

**Rule**: No indicator should be marked `active` if its data pipeline cannot
produce observations. `planned` and `disabled` indicators exist in the catalog
so the team knows what to build next and what to skip.

## 4. Python API

```python
from app.services.radar_store import RadarStore

store = RadarStore()

# Ensure tables exist
store.ensure_schema()

# Upsert catalog entries
store.upsert_indicator_catalog([
    {"indicator_code": "CN_M1_YOY", "category": "macro_cn", ...},
])

# Upsert observations
store.upsert_indicator_observations([
    {"indicator_code": "CN_M1_YOY", "obs_date": "2026-03-01", "value": 1.2, ...},
])

# Record a sync run
store.record_source_run(
    source_name="my_sync_script",
    target_table="indicator_observations",
    started_at="2026-04-24T10:00:00+00:00",
    status="success",
    rows_read=100,
    rows_upserted=95,
)

# Export to Parquet
path = store.export_table_to_parquet("indicator_catalog")
```

## 5. Sync Scripts

| Script                             | Purpose                                |
|------------------------------------|----------------------------------------|
| `scripts/sync_radar_catalog.py`    | Populate/refresh the indicator catalog |
| `scripts/sync_radar_macro_external.py` | Fetch macro & global data, write observations |

Both scripts are **idempotent**: running them multiple times produces the same
result without duplicating rows. Failed collections are logged to `source_runs`
with `status=partial` or `status=failed`.

## 6. Data Sources

| Indicator Group     | Primary Source         | Fallback           |
|---------------------|------------------------|---------------------|
| China macro         | akshare                | —                   |
| US rates            | yfinance               | stooq               |
| FX (DXY, USD/CNH)   | yfinance               | akshare             |
| Commodities         | yfinance               | —                   |
| VIX                 | yfinance:^VIX          | —                   |
| Derived indicators  | computed from above    | —                   |

## 7. Parquet Export

Each table can be exported with:

```python
store.export_table_to_parquet("indicator_observations")
# → data/radar/parquet/indicator_observations.parquet
```

Parquet files are overwritten on each export. Use them for:
- Backup / archival
- Loading into analytics tools (Polars, PySpark, DuckDB CLI)
- Sharing with non-Python consumers

## 8. Version History

| Version | Date       | Change                                 |
|---------|------------|----------------------------------------|
| V1      | 2026-04-24 | Initial contract, 52 indicators        |
