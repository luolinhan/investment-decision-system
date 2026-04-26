# Lead-Lag V2 Blueprint

Audit date: 2026-04-26

Goal: upgrade the existing runnable V1 from a research display surface into a research operating system that answers:

1. 今天先看什么？
2. 为什么现在看它？
3. 它已经走到第几棒？
4. 还缺什么验证？
5. 什么条件下立刻失效？

## Design Principles

- Incremental upgrade, no rewrite.
- Windows remains the only production runtime for UI, API, database, reports, and model replay.
- Aliyun remains collector/search/snapshot export only.
- Mac remains control plane only.
- Production runtime must not call Bailian Coding Plan, Claude Code, Codex workers, or any interactive coding tool.
- Repository code production should use Codex internal worker agents on `gpt-5.3-codex` for bounded high-volume tasks, with Codex controller reviewing and integrating.
- UI defaults to Chinese and must hide optional empty fields.
- Critical missing evidence must be explicit, not rendered as placeholders.

## Product Modes

### Operator Mode

Default homepage. This is the daily decision surface.

Primary sections:

- Decision Center
- Opportunity Queue
- What Changed Since Yesterday
- Event Frontline
- Do Not Chase / Avoid Board
- Macro / External / HK bridge summary

Operator Mode output must be concise and actionable. It should not expose raw model inventory before the daily decision view.

### Builder Mode

Deep research and debugging workspace.

Primary sections:

- Model Library
- Transmission Graph Workspace
- Replay & Validation
- Research Memory
- Raw Event / Research stream
- Source health and provider diagnostics

Builder Mode keeps the current V1 panels but strengthens them with V2 diagnostics.

## Target API Surface

Existing V1 endpoints remain temporarily for compatibility.

New V2 endpoints:

- `GET /investment/api/lead-lag/decision-center`
- `GET /investment/api/lead-lag/opportunity-queue`
- `GET /investment/api/lead-lag/what-changed`
- `GET /investment/api/lead-lag/event-frontline`
- `GET /investment/api/lead-lag/avoid-board`
- `GET /investment/api/lead-lag/macro-bridge`
- `GET /investment/api/lead-lag/transmission-workspace`
- `GET /investment/api/lead-lag/replay-diagnostics`
- `GET /investment/api/lead-lag/research-memory/actions`
- `GET /investment/api/lead-lag/briefs/{slot}`

Recommended query parameters:

- `as_of`
- `region`
- `sector`
- `mode`
- `event_class`
- `window`
- `min_tradability`
- `include_research_facing`

V2 implementation must make these parameters real filters. V1 currently accepts some route parameters but drops them when service methods do not support them.

## Service Architecture

Keep `LeadLagService` as the facade. Add bounded helpers if the file becomes too large.

Recommended modules:

- `app/services/lead_lag_service.py`: facade and compatibility methods.
- `app/services/lead_lag_schema.py`: dataclass or typed dict contracts.
- `app/services/lead_lag_scoring.py`: configured score composition.
- `app/services/lead_lag_events.py`: Event Relevance Engine.
- `app/services/lead_lag_memory.py`: Obsidian action memory extraction.
- `app/services/lead_lag_briefs.py`: scheduled brief generation.

If speed matters, start by adding methods in `LeadLagService`, then split after tests pass.

## Data Flow

1. Raw evidence
   - Radar snapshot.
   - Intelligence events.
   - Research reports.
   - Obsidian notes.
   - Sample bundle fallback.

2. Evidence normalization
   - Asset mapping.
   - Sector mapping.
   - Source quality.
   - Freshness.
   - Event class.

3. Relevance and score layer
   - Event Relevance Engine.
   - Macro / External / HK bridge.
   - OpportunityCard scorer.
   - Replay diagnostics.
   - Memory action summaries.

4. Operator outputs
   - Decision Center.
   - Opportunity Queue.
   - Event Frontline.
   - Avoid Board.
   - Fixed-time briefs.

5. Builder outputs
   - Model diagnostics.
   - Graph workspace.
   - Replay detail.
   - Research memory detail.

## V2 Minimum Implementation Plan

### Phase 0 - Audit and Contracts

Deliverables:

- `docs/v1_gap_report.md`
- `docs/v2_blueprint.md`
- `docs/action_schema.md`
- `docs/event_relevance_rules.md`
- `docs/data_source_matrix_v2.md`
- `docs/research_ops_workflow.md`
- `worklog/bailian_tasks.md`

Acceptance:

- Gaps cite current repo behavior.
- Schemas define non-empty policy.
- Worklog splits worker tasks by file ownership.

### Phase 1 - OpportunityCard and Scoring

Deliverables:

- Formal `OpportunityCard` builder.
- Configurable score weights.
- `opportunity-queue` API.
- Compatibility from old opportunities to new schema.
- Tests for required fields and missing evidence handling.

Acceptance:

- Key fields are never blank.
- If evidence is missing, `missing_confirmations` explains why.
- `tradability_score`, `actionability_score`, `evidence_completeness`, `freshness_score`, and `noise_penalty` are present and config-driven.

### Phase 2 - Event Relevance Engine

Deliverables:

- Event classification into `market-facing` and `research-facing`.
- `event-frontline` API.
- Default filter for market-facing events.
- China tradability and asset mapping filters.
- Event relevance tests.

Acceptance:

- Developer ecosystem noise is not shown in the default main event stream unless it maps clearly to tradable China assets.
- Every event has expected path and invalidation.

### Phase 3 - Operator Mode Homepage

Deliverables:

- Decision Center.
- Opportunity Queue UI.
- What Changed UI.
- Event Frontline UI.
- Do Not Chase UI.
- Builder Mode entry for the old workspace.

Acceptance:

- `/investment/lead-lag` lands on Operator Mode.
- Known placeholders are absent from Operator Mode.
- The first screen answers today's priorities.

### Phase 4 - Macro / External / HK Bridge

Deliverables:

- Macro Regime payload.
- External Risk payload.
- HK Liquidity & Activity payload.
- Opportunity sorting integration.

Acceptance:

- Macro/HK data affects ranking and risk budget.
- The bridge is not an isolated dashboard panel.

### Phase 5 - Deep Sector Evidence

Deliverables:

- AI evidence chain.
- Innovative drug evidence chain.
- Semiconductor evidence chain.
- Solar evidence chain.
- Hog cycle evidence chain.

Acceptance:

- Each sector distinguishes first baton, second baton, validation baton, missing validation, and invalidation.

### Phase 6 - Builder Diagnostics

Deliverables:

- Transmission Graph Workspace.
- Replay Diagnostics.
- Research Memory Actions.

Acceptance:

- Graph exposes nodes, edges, strength, lag, verification, bottleneck, and avoid/hedge candidates.
- Replay shows horizon distribution, regime split, crowded before/after, failure modes, and stage transitions.
- Memory produces action summaries and traps.

### Phase 7 - Fixed-Time Research Ops

Deliverables:

- 06:00 overnight digest.
- 08:20 pre-open playbook.
- 11:40 morning review.
- 15:15 close review.
- 21:30 US watch / mapping check.
- Markdown export to Obsidian output directory.
- Windows task scripts.

Acceptance:

- Each slot can be generated independently.
- Briefs include top opportunities, new catalysts, invalidations, next checks, and do-not-chase list.

## Score Configuration

Create or extend a YAML config with weights for:

- `tradability_score`
- `actionability_score`
- `evidence_completeness`
- `freshness_score`
- `noise_penalty`
- `decision_priority_score`

Hard-coded hidden weights are not acceptable in V2. Defaults may live in code only as fallback when config is missing, and that fallback must be visible in source health or diagnostics.

## Codegen Work Split

Codex keeps design, code review, integration, tests, docs, deploy, and acceptance.

Codex internal `gpt-5.3-codex` workers should handle:

- Bulk schema/test scaffolding.
- Large UI markup and JavaScript rewiring.
- Provider adapter boilerplate.
- Replay and graph payload transformations.
- Windows task script templates.
- Mechanical lint/type/test fixes.

File ownership must be assigned before each worker task.

## V2 Acceptance Checklist

- Homepage is Decision Center.
- Opportunity cards have no critical blank fields.
- Opportunity cards explain why now, baton stage, missing validation, invalidation, crowding, liquidity, next check time, and replay summary.
- Default events are market-facing.
- Macro / External / HK bridge affects opportunity ranking.
- Five deep sectors are evidence-backed.
- Replay includes distributions, regimes, crowding effect, failure modes, and stage transitions.
- Obsidian memory becomes action experience.
- Five fixed-time briefs can be generated.
- README, AGENTS, CLAUDE, CHANGELOG, ROADMAP, and worklog are updated.
