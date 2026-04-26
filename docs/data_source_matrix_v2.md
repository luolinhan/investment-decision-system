# Lead-Lag V2 Data Source Matrix

Audit date: 2026-04-26

V2 data sources must be free, public, replayable where possible, and mapped to a provider contract. This document extends the existing `config/source_matrix.example.yaml` and `docs/source_matrix.md` from a source list into an operating contract.

## Provider Metadata Contract

Every ingested record must retain:

- `source`
- `source_url`
- `ingest_time`
- `effective_time`
- `frequency`
- `unit`
- `confidence`
- `transformation`
- `freshness`
- `staleness_reason`
- `cache_status`
- `content_hash`
- `parser_version`
- `license_note`

## Source Tiers

T0:

- Official statistical, central bank, exchange, regulator, issuer, filing, and approval sources.

T1:

- Public institutional research indexes and official-ish data portals with stable methodology.

T2:

- Reputable media and public research aggregators used only for enrichment and cross-check.

T3:

- Search, forum, social, and unstructured enrichment. Never sufficient alone for Operator Mode actionability.

## Required Provider Matrix

| Domain | Provider Key | Priority | Current State | V2 Requirement |
|---|---:|---:|---|---|
| 国家统计局 CPI/PPI/FAI/地产/工业/零售 | `nbs_macro` | T0 | Macro scripts have partial coverage through Radar | Add Lead-Lag provider output with regime mapping and freshness metadata |
| 人民银行金融统计/社融/M1/M2 | `pboc_credit` | T0 | Macro scripts have partial coverage through Radar | Feed credit impulse into Macro Regime and Opportunity ranking |
| HKEX Southbound/Northbound | `hkex_connect_flow` | T0 | HK Radar has southbound/northbound foundations | Add bridge completeness and flow confirmation fields |
| HKEX short selling | `hkex_short_selling` | T0 | Missing as Lead-Lag provider | Add short interest/activity pressure for HK crowding and squeeze checks |
| Stock Connect holdings | `hkex_connect_holdings` | T0 | Missing as Lead-Lag provider | Add holdings change and foreign/local activity confirmation |
| A/H premium | `ah_premium` | T0/T1 | HK Radar has A/H foundation | Feed HK bridge and cross-market valuation state |
| 香港入境/访客活动 | `hk_visitor_activity` | T0 | HK Radar has visitor/activity table foundation | Map to HK activity and consumption/service sector risk appetite |
| NMPA / CDE | `nmpa_cde` | T0 | Missing dedicated adapter | Add IND/NDA/approval/readout event provider for innovative drugs |
| A-share announcements | `cninfo_sse_szse_announcements` | T0 | Source matrix lists announcements | Add replayable company action adapter and asset mapping |
| HK announcements | `hkexnews_announcements` | T0 | Source matrix lists announcements | Add HK issuer bridge and China mapping |
| US filings/earnings | `sec_issuer_filings` | T0 | Source matrix lists company sources | Add US leader asset read-through to China chain |
| Public research indexes | `public_research_indexes` | T1 | Intelligence/Research loaders include free public domains | Use only as evidence enrichment, not sole action trigger |
| User Obsidian | `obsidian_vault` | local | Indexed read-only | Convert notes into action memory and keep writes in separate output directory |

## Macro Regime Inputs

Minimum V2 fields:

- CPI
- Core CPI
- PPI
- Social financing
- M1
- M2
- Fixed asset investment
- Property investment or proxy
- Macro quadrant
- Style mapping to growth, cyclicals, dividend, HK beta

Usage:

- Feeds Decision Center risk budget.
- Feeds OpportunityCard tradability and actionability.
- Feeds Do Not Chase when macro backdrop contradicts a high-beta thesis.

## External Risk Inputs

Minimum V2 fields:

- DXY
- US Treasury 2Y
- US Treasury 10Y
- Yield curve change
- Gold
- VIX or fear proxy
- CNH / RMB risk proxy
- FTSE China or China ADR risk proxy

Usage:

- Determines high-beta permission.
- Adjusts HK bridge asset score.
- Raises invalidation when external risk tightens against the opportunity.

## HK Liquidity & Activity Inputs

Minimum V2 fields:

- Southbound flow
- Northbound linkage
- HK short selling
- A/H premium
- Stock Connect holdings change
- HK visitor activity
- HK risk appetite label

Usage:

- Feeds bridge completeness.
- Feeds crowding/squeeze warnings.
- Feeds China beta release or avoid state.

## Deep Sector Evidence Inputs

AI:

- Overseas leader assets.
- Capex/order/demand proxies.
- Model capability and cost change.
- China mapping: GPU, CPU, switch, optical module, PCB, power, liquid cooling, IDC, applications.

Innovative drugs:

- BD deal amount and structure.
- Clinical progress.
- Key readouts.
- IND / NDA / approvals.
- Overseas licensing heat.
- China mapping: innovative drugs, CRO/CXO, upstream research services, commercialization.

Semiconductors:

- Overseas leader earnings/guidance.
- Inventory, orders, capex, advanced packaging.
- China mapping: equipment, materials, foundry, packaging/testing, design.

Solar:

- Inventory.
- Capacity cuts.
- Price chain.
- Leader production cuts, shutdowns, M&A, capex contraction.
- Distinguish trading repair from fundamental repair.

Hog cycle:

- Breeding sow inventory.
- Live hog inventory.
- Spot and futures prices.
- Capacity reduction.
- Farming profit.
- Feed cost.
- Pig-grain ratio.

## Provider Health

Each provider should report:

- `provider_key`
- `last_success_at`
- `last_attempt_at`
- `records_loaded`
- `freshness_status`
- `error`
- `fallback_used`
- `cache_status`
- `parser_version`

Operator Mode should expose only source count, source quality, freshness, and cache status. Builder Mode can expose full health diagnostics.

## Immediate Backfill Priorities

1. Wire existing Radar macro/external/HK outputs into Lead-Lag scoring.
2. Add HKEX short selling and Stock Connect holdings provider placeholders.
3. Add NMPA/CDE provider placeholder and schema.
4. Convert company announcements into EventRelevance candidates.
5. Convert Obsidian note matches into action memory summaries.
