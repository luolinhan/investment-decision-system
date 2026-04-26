# Lead-Lag V1 Gap Report

Audit date: 2026-04-26

Scope: current runnable Lead-Lag Alpha Engine V1 in this repository. This report is based on the existing service, routes, templates, JavaScript, sample data, Obsidian service, tests, and ops scripts. It is not a generic wishlist.

## Current V1 Baseline

V1 is a working research display surface, not an empty scaffold.

- Backend entrypoint: `app/services/lead_lag_service.py`.
- Page route: `/investment/lead-lag` in `app/routers/investment.py`.
- API prefix: `/investment/api/lead-lag/*`.
- UI assets: `templates/lead_lag.html` and `static/js/lead_lag.js`.
- Sample bundle: `sample_data/lead_lag/lead_lag_v1.json`.
- Existing live fusion: Radar snapshot, Intelligence events, Research reports.
- Existing fallback: sample data remains available when live evidence is absent.
- Existing production posture: Windows is the runtime; Mac is control plane; Aliyun is collector/search/snapshot only.

Positive foundation already present:

- `LeadLagService` loads sample data and merges live evidence.
- Asset code/name/market normalization exists across default assets, graph nodes, watchlists, opportunities, and SQLite stocks.
- Live Intelligence and Research loaders apply a first reliability gate for free/public source URLs.
- Basic opportunity stage buckets and first/second/next baton grouping already exist.
- Transmission graph edge normalization already supports relation, sign, strength, lag, evidence, confidence, and last verification fields.
- Obsidian is indexed read-only through `ObsidianMemoryService`.
- Smoke tests cover route/API availability and some asset normalization.

## Mandatory V2 Gaps

### 1. Homepage Is Not Operator Mode

Current state:

- `/investment/lead-lag` returns `lead_lag.html`.
- The page title is still a research graph workspace.
- The first visual row is Model Library, Opportunity Board, and Cross-Market Map.
- Event, Replay, and Memory appear as lower display panels.

Why this fails V2:

- The page does not answer "今天先看什么" first.
- The user sees model inventory before decision priority.
- There is no Decision Center, What Changed, Event Frontline, or Do Not Chase board.

Required fix:

- Default homepage becomes Operator Mode.
- Builder Mode keeps the existing model, graph, replay, and memory workspace behind a secondary section or route.

### 2. Placeholder Fields Are Exposed To Users

Current state:

- `static/js/lead_lag.js` renders fallbacks such as `暂无机会说明`, `暂无模型说明`, `暂无节点说明`, `暂无事件说明`, `暂无验证说明`, and `暂无记忆摘要`.
- `renderMetaPair()` renders empty values as `-`.
- Opportunity cards can show `Driver -`, `Confirm -`, `Risk -`.
- Event cards can show `Impact -`, `Watch -`, `Owner -`.
- Replay cards can show `Failure Mode -`.

Why this fails V2:

- Empty placeholders make the system look more complete than it is.
- For decision use, missing evidence must be explicit: "缺少哪些验证导致无法生成".

Required fix:

- Critical fields must be populated or the card must show `missing_confirmations`.
- Optional display fields should be hidden when absent.
- UI must not render the known empty placeholders in Operator Mode.

### 3. Opportunity Cards Are Not Executable

Current backend output:

- `list_opportunities()` returns title/name, asset code/name/market, score, rationale, driver, confirmation, risk, baton, stage, source URL, update time, and evidence sources.
- `_live_opportunities()` builds live cards mainly from Radar sector cards plus one event/report.
- `_candidate_score()` uses hard-coded weights.

Missing V2 fields:

- `why_now`
- `leader_asset`
- `bridge_asset`
- `local_asset`
- `local_proxy_assets`
- `baton_stage`
- `expected_lag_days`
- `expected_review_times`
- `confirmations`
- `missing_confirmations`
- `invalidation_rules`
- `crowding_state`
- `liquidity_state`
- `actionability_score`
- `tradability_score`
- `evidence_completeness`
- `freshness_score`
- `noise_penalty`
- `historical_replay_summary`
- `mapped_events`
- `mapped_notes`
- `source_count`

Why this fails V2:

- The card explains a theme but does not define an action window.
- The card does not show what to check next, what invalidates it, or whether it is tradable now.

Required fix:

- Introduce formal `OpportunityCard` schema.
- Add score config under `config/lead_lag_v2.example.yaml` or equivalent.
- Add `/investment/api/lead-lag/opportunity-queue` while preserving old `/opportunities` as compatibility.

### 4. Event Flow Is A News Stream, Not A Catalyst Engine

Current state:

- `_load_intelligence_events()` reads active events, filters by source quality, joins entities/assets, and emits event objects.
- `events_calendar()` merges sample events with live intelligence events and sorts them.
- The UI renders the whole event calendar.

Missing V2 fields:

- `event_type`
- `event_class`: `market-facing` or `research-facing`
- `sector_mapping`
- `asset_mapping`
- `china_mapping_score`
- `tradability_score`
- `evidence_quality`
- `time_decay`
- `owner`
- `watch_items`
- `base_case`
- `bull_case`
- `bear_case`
- `expected_path`
- `invalidation`

Why this fails V2:

- Generic developer ecosystem events can crowd out market-facing catalysts.
- Events are not ranked by China tradability or mapping strength.
- There is no expected path from event to opportunity to validation.

Required fix:

- Add Event Relevance Engine.
- Default Event Frontline returns only `market-facing`.
- Research-facing events remain available through a toggle or Builder Mode.

### 5. Model Library Is Still Template-Oriented

Current state:

- `list_models()` compresses model family data into display fields.
- `_enrich_model_family()` fills defaults for missing model attributes.
- UI shows model description, lead, universe, confidence.

Missing V2 fields:

- Whether the model is currently tradable.
- Number of active opportunity cards linked to the model.
- Current actionable stage.
- Most recent trigger.
- Most recent invalidation reason.
- Last 20 replay outcomes.

Why this fails V2:

- Models are displayed as inventory rather than decision engines.

Required fix:

- Link model -> opportunity -> validation -> invalidation -> replay.
- Builder Mode model library should become diagnostics, not the default landing surface.

### 6. Transmission Graph Is Flattened

Current state:

- Backend normalizes graph nodes and edges.
- `get_industry_transmission()` outputs simplified paths with name, driver, summary, signal, and steps.
- UI shows 1/2/3/4 step transmission cards.

Missing V2 outputs:

- Real graph workspace with node/edge payloads.
- First baton, second baton, third baton.
- Validation baton.
- Hedge or avoid assets.
- Current bottleneck.
- Verified vs unverified edges.

Why this fails V2:

- The graph exists structurally but is not used to explain current blockage or next action.

Required fix:

- Add graph query endpoint that exposes nodes, edges, baton tiers, validation state, and current bottleneck.
- Keep simplified cards only as Operator summaries.

### 7. Macro / External / HK Bridge Is Too Shallow

Current state:

- `liquidity()` appends broad External Risk and HK Liquidity scores from Radar summary.
- Existing scripts already collect or plan macro/external/HK indicators in Radar, but Lead-Lag does not consume them as decision inputs.

Missing V2 layers:

- Macro Regime: CPI, Core CPI, PPI, social financing, M1/M2, fixed asset investment, property proxies, four-quadrant regime, style mapping.
- External Risk: DXY, US 2Y/10Y, curve, gold, VIX, CNH/RMB risk proxy, FTSE China risk proxy, China high-beta impact.
- HK Liquidity & Activity: southbound, northbound linkage, HK short selling, A/H premium, Stock Connect holdings, visitor activity, HK risk appetite.

Why this fails V2:

- High-beta timing cannot be justified without external and HK bridge context.
- Liquidity is shown as an isolated panel rather than affecting ranking and risk budget.

Required fix:

- Add `macro_bridge()` or equivalent.
- Feed macro/external/HK factors into OpportunityCard scoring and Decision Center risk budget.

### 8. Replay Is Not Yet Practical

Current state:

- `replay_validation()` returns aggregate hit rate, win rate, validation score, and scenarios.
- UI renders hit rate, lead window, and failure mode.

Missing V2 diagnostics:

- 1/3/5/10/20 day performance distribution.
- Regime split.
- Crowded before/after performance difference.
- Failure mode ranking.
- Stage transition performance.
- Factors that cause false positives.

Why this fails V2:

- A single hit rate cannot tell whether a current card is still worth acting on.

Required fix:

- Add `replay_diagnostics()` and wire summary into OpportunityCard.
- Builder Mode keeps full diagnostics; Operator Mode shows concise replay summary.

### 9. Obsidian Memory Is A Search Result, Not Action Memory

Current state:

- Obsidian service scans markdown notes, tags, paths, and modified times.
- Lead-Lag memory returns note lists, links, and tags.
- UI can show `暂无记忆摘要`.

Missing V2 fields:

- `thesis_summary`
- `prior_wins`
- `prior_failures`
- `typical_trap`
- `similar_cases`
- `review_notes`
- Mapping to model, thesis, asset, sector, and event.

Why this fails V2:

- Notes are not converted into reusable decision experience.

Required fix:

- Add research memory action extraction.
- Feed memory alignment and traps into OpportunityCard and Do Not Chase board.

### 10. Tests Are V1 Smoke Tests

Current state:

- Tests check API 200, model count, stage buckets, asset code/name, and source health.
- No tests fail when placeholders appear.
- No tests enforce OpportunityCard or EventRelevance contracts.

Required fix:

- Add schema contract tests.
- Add event classification tests.
- Add empty-field rendering or API guard tests.
- Add score config tests to prevent hard-coded hidden weights.

## V2 Minimum Closure Definition

V2 should not start with a large UI rewrite. The minimum useful closure is:

1. Define schemas and score config.
2. Build OpportunityCard from current V1 data plus live evidence.
3. Classify events into market-facing and research-facing.
4. Add Decision Center, Opportunity Queue, Event Frontline, What Changed, and Avoid Board APIs.
5. Update homepage to consume the new APIs.
6. Move current model/graph/replay/memory panels under Builder Mode.
7. Add tests that enforce no critical empty fields in Operator Mode payloads.

## Acceptance Gates

- Homepage defaults to Decision Center, not Model Library.
- Operator cards answer why now, baton stage, missing validation, invalidation, crowding, liquidity, next check time, and replay summary.
- Default event stream only shows market-facing catalysts.
- Macro/external/HK bridge affects opportunity ranking.
- Obsidian output is an action memory summary, not just note titles.
- Known placeholders are not rendered in Operator Mode.
- Production runtime does not depend on Bailian or Claude Code.
