# Changelog

All notable changes to the Lead-Lag Alpha Engine docs and delivery templates are recorded here.

## [0.4.0] - 2026-04-26

### Added
- Added V2 audit report documenting concrete V1 gaps across homepage mode, opportunity cards, event relevance, model library, graph, macro/HK bridge, replay, Obsidian memory, and tests.
- Added V2 blueprint for Operator Mode, Builder Mode, new API surface, service boundaries, phased implementation, and acceptance gates.
- Added formal V2 action schemas for `OpportunityCard`, `DecisionCenter`, `WhatChanged`, `AvoidItem`, and `EventRelevance`.
- Added event relevance rules that split default market-facing events from research-facing noise.
- Added V2 data source matrix for official/public provider contracts and metadata requirements.
- Added research ops workflow for 06:00, 08:20, 11:40, 15:15, and 21:30 Lead-Lag briefs.
- Added `worklog/bailian_tasks.md` with Codex controller / `gpt-5.3-codex` worker ownership, queued worker prompts, and review gates.
- Added V2 service-layer Operator payloads: `decision_center`, `opportunity_queue`, `event_frontline`, `avoid_board`, and `what_changed`.
- Added config-driven V2 scoring and `config/lead_lag_v2.example.yaml`.
- Added Event Relevance Engine with default `market-facing` filtering and developer-ecosystem noise demotion.
- Added Operator Mode homepage sections and moved the old model/graph/replay/memory workspace under Builder Mode.
- Added V2 schema/event/API tests.
- Added Macro / External / HK Bridge builder with provider metadata, freshness, missing-field reporting, and sample fallback.
- Added bridge impact into OpportunityCard scoring and Decision Center risk budget.
- Added fixed-time Lead-Lag brief generator for 06:00, 08:20, 11:40, 15:15, and 21:30 workflows.
- Added `scripts/export_lead_lag_brief.py` for Markdown/JSON brief export with dry-run support.
- Added Transmission Graph Workspace and Replay Diagnostics builders with graph edge status, baton paths, bottlenecks, horizon distributions, failure modes, crowded-before/after, and stage transition diagnostics.
- Added five-sector Deep Evidence Engine for AI、创新药、半导体、光伏、猪周期 with evidence layers, missing validation, provider gaps, and action readiness.
- Added Obsidian Action Memory builder that converts notes into thesis summaries, prior wins/failures, traps, similar cases, review notes, and mapped opportunities.
- Added `scripts/setup_lead_lag_brief_tasks.ps1` and `scripts/run_lead_lag_brief_task.ps1` for five Windows scheduled Lead-Lag brief exports.

### Changed
- Updated README, AGENTS, and CLAUDE guidance so future Lead-Lag V2 work starts from the audit and schema contract rather than blind UI changes.
- Updated Lead-Lag route integration so V2 wrapped payloads are preserved rather than unwrapped to bare `items`.
- Updated `/investment/api/lead-lag/briefs/{slot}` to generate real V2 brief payloads.
- Updated Builder Mode to consume V2 graph workspace, replay diagnostics, sector deep evidence, and action memory APIs instead of only V1 compatibility lists.
- Updated codegen operating rule to stop using Aliyun Bailian Coding Plan for repository code production; bounded codegen now uses Codex internal `gpt-5.3-codex` workers.

### Fixed
- Fixed frontend V2 collection extraction for `cards`, `events`, and `items`.
- Fixed EventRelevance `expected_path` and `invalidation` to use structured arrays.
- Removed known Operator Mode placeholder strings from Lead-Lag UI.
- Fixed sample fallback provider metadata for macro bridge missing fields.
- Fixed Windows brief scheduler to generate short per-slot `.cmd` wrappers after `schtasks /TR` rejected inline quoting and long PowerShell commands.

## [0.3.0] - 2026-04-25

### Added
- Added Lead-Lag live evidence fusion from Radar snapshot, Intelligence events, and Research reports.
- Added normalized asset fields on opportunity/event outputs: `asset_code`, `asset_name`, `market`, `source_url`, `updated_at`, and `evidence_sources`.
- Added Windows scheduled tasks for `InvestmentHub8080` startup and `LeadLagPretranslate` 5-hour pre-collection / Bailian translation cadence.
- Added `scripts/lead_lag_pretranslate_task.ps1` for front-loaded Intelligence collection, Bailian translation, and Lead-Lag snapshot export.
- Added `free_public_reliable` live-source quality filtering for Intelligence events and Research reports, including public research domains such as NBER, OECD, IMF, BIS, Stanford HAI, Brookings, CSET, and RAND.

### Changed
- Lead-Lag overview now reports `source=live_fusion` when live local evidence is available, while keeping sample data as fallback.
- Lead-Lag overview now exposes `live_event_count`, `live_research_count`, and live source-health quality metadata.
- Improved `scripts/lead_lag_pretranslate_task.ps1` logging so each Python step records stdout/stderr and exit code.
- Expanded source matrix docs/config toward free official and public sources.

## [0.2.1] - 2026-04-25

### Added
- Added tracked Windows background service entrypoint `run_uvicorn_service.py`.
- Added tracked Windows wrapper `start_uvicorn_service.bat` for detached API startup.

### Changed
- Hardened `start_investment_hub.ps1` with explicit environment setup, worker count, and immediate-exit detection.
- Hardened `app/main.py` logging bootstrap so background `pythonw` launches skip console handlers and keep file logging only.
- Unified `start_investment_hub.ps1` onto the detached `start_uvicorn_service.bat -> run_uvicorn_service.py` path used by Windows production.
- Updated Windows deployment documentation and README to point to the production start path.

## [0.2.0] - 2026-04-25

### Added
- Added runnable Lead-Lag V1 backend service at `app/services/lead_lag_service.py`.
- Added sample-data bundle at `sample_data/lead_lag/lead_lag_v1.json` with 10 model families, 5 thesis cards, transmission graph, events, replay stats, watchlists, and source health.
- Added `/investment/lead-lag` page route and `/investment/api/lead-lag/*` endpoints.
- Added Lead-Lag API and service tests.
- Added `.env.example`, `config/lead_lag.example.yaml`, `config/source_matrix.example.yaml`.
- Added report export, Aliyun collector snapshot, Windows demo start, and snapshot sync scripts.

### Changed
- Updated CI to lint, type-check, test, and smoke-import the Lead-Lag service and API tests.
- Updated Bailian workflow scripts to prefer `BAILIAN_HIGH_QUOTA_WORKER` / `BAILIAN_PREFERRED_WORKER` aliases when configured.
- Updated README, AGENTS, PROJECT_STATUS, and roadmap to treat Lead-Lag as a first-class surface.

## [0.1.0] - 2026-04-25

### Added
- Added Lead-Lag Alpha Engine V1 architecture, stage machine, transmission graph, source matrix, model family, Windows deployment, Aliyun collector, and Obsidian integration docs.
- Added `docs/lead_lag_worklog.md` as the execution ledger for Bailian deliveries, Codex review conclusions, acceptance decisions, and follow-up fixes.
- Added GitHub CI starter workflow covering `lint`, `type`, `tests`, and `smoke` with runtime-safe fallbacks for the current repository shape.
- Added issue templates and pull request template aligned with the Codex/Bailian collaboration contract.

### Defined
- Defined role split: Codex owns design, review, change acceptance, and CI gate criteria; Bailian owns large-batch code generation under the approved design package.
- Defined Lead-Lag Alpha Engine V1 as a staged pipeline from source intake to alpha candidate output, human review, and production sync.

### Notes
- This release only introduces documentation and CI scaffolding. No business code, README, AGENTS, or PROJECT_STATUS changes are included.
