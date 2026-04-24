# Radar Sector Thesis Data Sources

> Tracks data sources, coverage, and gaps for sector thesis scoring (semiconductor, solar/PV, pig cycle).
> Last updated: 2026-04-25

## Overview

This document describes the data sources wired into `scripts/sync_radar_sector.py` for
collecting historical sector indicators into the radar DuckDB store (`data/radar/radar.duckdb`).

All indicators are written to `indicator_observations` with a `source_runs` entry for each run.
The script is idempotent (INSERT OR REPLACE on composite PK: indicator_code, obs_date, source).

## Indicators by Sector

### 半导体 (Semiconductor)

| Indicator Code | Description | Source | Frequency | Coverage | Status |
|---|---|---|---|---|---|
| `CN_SW_SEMI_INDEX` | 申万半导体二级行业指数收盘价 | akshare: index_hist_sw(801081) | daily | ~1999-12 ~ 2026-04, ~6300 obs | active |
| `CN_SW_ELEC_INDEX` | 申万电子一级行业指数收盘价（半导体上位代理） | akshare: index_hist_sw(801080) | daily | ~1999-12 ~ 2026-04, ~6300 obs | active |

**What works:**
- Long history (~26 years) of daily closing prices for both the semiconductor sub-sector
  and the broader electronics sector.
- Good coverage for trend analysis, momentum, and drawdown calculations.

**Gaps:**
- **集成电路产量** (integrated circuit production volume): NBS publishes monthly data,
  but akshare does not expose a reliable function for this. Need to find an alternative source
  (e.g., direct NBS API, Wind, or CEIC).
- **高技术产业投资** (high-tech investment): FAI by sector is published by NBS quarterly
  but not accessible via akshare.
- **半导体设备出货额** (semiconductor equipment billings): SEMI publishes global data
  monthly — not available in akshare.
- **库存周期代理** (inventory cycle proxy): Could derive from receivables/inventory
  data in financial statements, but requires company-level aggregation.

### 光伏 / 太阳能 (Solar/PV)

| Indicator Code | Description | Source | Frequency | Coverage | Status |
|---|---|---|---|---|---|
| `CN_SW_SOLAR_INDEX` | 申万光伏设备二级行业指数收盘价 | akshare: index_hist_sw(801735) | daily | ~2021-12 ~ 2026-04, ~1050 obs | active |
| `CN_SW_BATTERY_INDEX` | 申万电池二级行业指数收盘价（储能/光伏配套代理） | akshare: index_hist_sw(801737) | daily | ~2021-12 ~ 2026-04, ~1050 obs | active |

**What works:**
- Daily index prices for solar equipment and battery sub-sectors.
- Both series start from Dec 2021 (申万 reclassification), covering the major PV cycle.

**Gaps:**
- **太阳能电池产量** (solar cell production): NBS monthly data not available via akshare.
- **光伏组件价格** (PV module prices): CPIA (中国光伏行业协会) publishes weekly spot
  prices for silicon wafers, cells, and modules. No akshare function; could be scraped from
  pv-info.cpiia.org or infolib.cn.
- **多晶硅/硅料价格** (polysilicon pricing): Key leading indicator for the PV supply chain.
  Available from SMM (上海有色网) or PV Insight — requires web scraping or paid API.
- **光伏新增装机量** (new PV installations): NEA (国家能源局) publishes monthly data.
  Not available in akshare.
- **出口数据** (PV export data): customs data for solar panel exports — not in akshare.

### 猪周期 (Pig Cycle)

| Indicator Code | Description | Source | Frequency | Coverage | Status |
|---|---|---|---|---|---|
| `CN_HOG_PRICE_INDEX` | 生猪价格指数（预售/成交加权） | akshare: index_hog_spot_price | daily | ~560 obs, covers ~2024-2026 | active |
| `CN_HOG_SPOT_PRICE` | 生猪现货价格（元/公斤） | akshare: spot_hog_year_trend_soozhu | daily | ~200 obs, from ~2025-09 | active |
| `CN_HOG_FUTURES_PRICE` | 生猪期货主力合约（元/公斤） | akshare: futures_hog_core | daily | ~367 obs, covers ~2025-01 | active |
| `CN_SW_AGRI_INDEX` | 申万农林牧渔一级行业指数（猪周期上位） | akshare: index_hist_sw(801010) | daily | ~1999-12 ~ 2026-04, ~6300 obs | active |

**What works:**
- Futures price has the longest continuous series among pig-specific indicators.
- Agriculture sector index provides a ~26-year contextual backdrop.
- Spot price index gives a composite view of hog market sentiment.

**Gaps:**
- **能繁母猪存栏** (breeding sow inventory): Published monthly by MOA (农业农村部).
  This is the most important leading indicator for the pig cycle (10-12 month lead).
  Not available in akshare. Could try scraping from http://www.moa.gov.cn or
  sourcing from Wind/CEIC.
- **生猪存栏/出栏** (hog inventory/slaughter): NBS quarterly data — not in akshare.
- **猪粮比价** (hog-to-grain price ratio): MOA publishes weekly. Key threshold is 6:1
  for breeding profitability. Not in akshare.
- **自繁自养利润** (breeding profit margin): MySteel (我的钢铁网) publishes weekly
  breeding margin data — not in akshare.
- **仔猪价格** (piglet price): Leading indicator (6 month lead for market hogs).
  Some akshare functions exist (`spot_hog_crossbred_soozhu`) but only covers recent
  ~15 data points — too short for cycle analysis.
- **冻猪肉库存** (frozen pork inventory): Industry data, typically paid-source only.

## Architecture Notes

- All Shenwan sector indices use `index_hist_sw` with the sector code as `symbol`.
- Shenwan codes: first-level = `801XXX.SI` (6 digits), second-level = `801XXX.SI` (7 digits).
- Hog data uses Soozhu (搜猪网) and DCE (大连商品交易所) futures via akshare.
- All times stored as UTC ISO 8601 in `fetch_ts`; observation dates are local (Beijing).

## Next Steps (Priority Order)

1. **能繁母猪存栏**: Highest impact gap. Check if Wind/CEIC provides this, or
   scrape from MOA monthly bulletins. Target: monthly series back to 2010.
2. **集成电路产量**: NBS data — check `macro_china_*` akshare functions for new additions,
   or scrape from stats.gov.cn. Target: monthly series.
3. **光伏组件价格链**: Scrape CPIA weekly price data (硅料→硅片→电池→组件).
   Would give a full supply-chain view.
4. **猪粮比价**: Derive if we can get hog price + corn price. Corn price may be
   available via akshare futures commodity data.
5. **全球半导体指数**: Add SOX (费城半导体指数) via yfinance (`^SOX`) as a
   global leading indicator for China semiconductor thesis.

## Run Commands

```bash
# Dry run (syntax check)
python3 -m py_compile scripts/sync_radar_sector.py

# Execute
python scripts/sync_radar_sector.py

# Verify data in DuckDB
python3 -c "
import duckdb
db = duckdb.connect('data/radar/radar.duckdb')
for code in ['CN_SW_SEMI_INDEX', 'CN_SW_SOLAR_INDEX', 'CN_HOG_PRICE_INDEX']:
    df = db.execute(
        \"SELECT indicator_code, MIN(obs_date) as first_date, MAX(obs_date) as last_date, COUNT(*) as obs_count \"
        \"FROM indicator_observations WHERE indicator_code = ? GROUP BY indicator_code\",
        (code,)
    ).fetchdf()
    print(df.to_string(index=False))
db.close()
"
```
