# Roadmap

## Lead-Lag Alpha Engine V3

### Scope
- Upgrade V2 into an evidence-driven research operating system.
- Make every high-priority conclusion traceable to source documents, local archives, data points, citations, and report records.
- Keep Windows production as the only runtime acceptance target.

### Milestone V3-0 - Audit and Architecture
- [x] Audit V2 service, routing, sample bundle, live fusion, SQLite schema, and tests.
- [x] Add `docs/v2_gap_report.md`.
- [x] Add `docs/v3_architecture.md`.
- [x] Add Evidence Vault, archive, reliability, registry, dossier, report, replay, and UI density specs.
- [x] Add `docs/deprecations.md` for old external enhancement assets.

### Milestone V3-1 - Evidence Vault and Source Archive
- [x] Add Evidence Vault SQLite schema.
- [x] Add local archive directory policy.
- [x] Add migration/backfill script for existing `raw_documents` and `research_reports`.
- [x] Add document/report search helpers and source quality summary.
- [ ] Add PDF/HTML binary archiver and parser status dashboard.
- [ ] Add citation extraction into `citations` and `extracted_facts` from new collectors.

### Milestone V3-2 - Opportunity Universe and Entity Layer
- [x] Add sector/theme/entity/instrument/mapping/model/thesis/event-template registry tables.
- [x] Seed at least 12 industry/theme templates; current seed contains 25 templates.
- [x] Add sector/entity/instrument dossier API skeletons.
- [ ] Expand entity and instrument seeds for all 25 sectors.
- [ ] Add mapping pollution checks into scoring, not only diagnostics.

### Milestone V3-3 - Live/Sample Isolation and Thesis Grouping
- [x] Add V3 data source classes.
- [x] Hide sample/fallback opportunities and events by default.
- [x] Force sample/fallback opportunities out of executable status.
- [x] Add parent thesis cards and child instrument variants.
- [x] Add evidence panel, evidence checklist, execution blockers, and stale review-time rolling.
- [x] Wire parent thesis cards into the Lead-Lag page as the default visible layout.
- [x] Add front-end filters for market, stage, family, live-only, archived-only, and sample/fallback inspection.
- [ ] Add source-tier and sector-specific quick filters to every dense list.

### Milestone V3-4 - Report Center and Replay
- [x] Add report tables and report search API.
- [ ] Generate daily/weekly/monthly/report-center outputs into `reports` and Obsidian export directory.
- [ ] Add report version diff.
- [ ] Add replay backfill for 1/3/5/10/20 horizons into DuckDB/Parquet.
- [ ] Reduce `unknown_failure` through failure taxonomy classifiers.

### Milestone V3-5 - Production Acceptance
- [x] Run full local test suite for current UI integration phase.
- [ ] Apply V3 migrations on Windows production after database backup.
- [ ] Restart `InvestmentHub8080`.
- [ ] Smoke `/health`, `/investment/lead-lag`, Opportunity Queue, Source Quality Lineage, Report Center, Opportunity Universe, and Dossier APIs.
- [ ] Push GitHub commit and confirm Windows production matches GitHub.
- [ ] Update long-term knowledge base with migration and validation results.

## Lead-Lag Alpha Engine V2

### Scope
- Upgrade V1 from a research display surface into a research operating system.
- Default homepage becomes Operator Mode: Decision Center, Opportunity Queue, What Changed, Event Frontline, Do Not Chase.
- Builder Mode keeps model library, transmission graph, replay diagnostics, research memory, and raw event/debug surfaces.
- Continue to use Codex for architecture/review/acceptance and Codex built-in worker agents on `gpt-5.3-codex` for bounded high-volume code generation.

### Milestone V2-0 - Audit and Contracts
- [x] Audit current V1 backend, frontend, data/ops, and tests.
- [x] Add `docs/v1_gap_report.md`.
- [x] Add `docs/v2_blueprint.md`.
- [x] Add `docs/action_schema.md`.
- [x] Add `docs/event_relevance_rules.md`.
- [x] Add `docs/data_source_matrix_v2.md`.
- [x] Add `docs/research_ops_workflow.md`.

### Milestone V2-1 - OpportunityCard and Event Relevance
- [x] Implement V2 OpportunityCard builder and `opportunity-queue` API.
- [x] Move hard-coded scoring into configurable weights.
- [x] Implement Event Relevance Engine and `event-frontline` API.
- [x] Add schema and event classification tests.

### Milestone V2-2 - Operator Mode
- [x] Add Decision Center API.
- [x] Add What Changed API.
- [x] Add Avoid Board API.
- [x] Make `/investment/lead-lag` default to Operator Mode.
- [x] Move V1 model/graph/replay/memory panels into Builder Mode.
- [x] Remove critical placeholder rendering from Operator Mode.

### Milestone V2-3 - Macro / External / HK Bridge
- [x] Add Macro Regime payload.
- [x] Add External Risk payload.
- [x] Add HK Liquidity & Activity payload.
- [x] Feed bridge state into OpportunityCard ranking and Decision Center risk budget.

### Milestone V2-4 - Deep Sector Evidence
- [x] AI evidence chain.
- [x] Innovative drug evidence chain.
- [x] Semiconductor evidence chain.
- [x] Solar evidence chain.
- [x] Hog cycle evidence chain.

### Milestone V2-5 - Builder Diagnostics and Research Ops
- [x] Upgrade Transmission Graph Workspace.
- [x] Upgrade Replay & Validation diagnostics.
- [x] Convert Obsidian memory into action experience.
- [x] Implement five fixed-time Lead-Lag briefs and Obsidian export.
- [x] Add Windows scheduled task scripts and production smoke checklist.

## Lead-Lag Alpha Engine V1

### Scope
- Build a document-first V1 for a cross-market lead-lag alpha pipeline.
- Keep Codex in charge of architecture decisions, review comments, CI rules, and acceptance gates.
- Keep Codex built-in worker agents focused on high-volume code generation, schema expansion, adapter implementation, and low-risk mechanical wiring.

## Milestone 0 - Spec Freeze
- [x] Define architecture boundaries.
- [x] Define source tiers and transmission graph.
- [x] Define stage machine and worklog format.
- [x] Define CI starter template.

## Milestone 1 - Data Contract and Ingestion Skeleton
- [x] Freeze source registry for `T0/T1/T2/T3`.
- [x] Freeze canonical event schema for lead signals, lag targets, evidence, confidence, and decay.
- [x] Add sample-data bundle under `sample_data/lead_lag/`.
- [x] Add service and fixture-based tests.

## Milestone 2 - Transmission Graph and Candidate Engine
- [x] Implement market-to-theme-to-symbol propagation graph.
- [x] Implement score composition for lead strength, lag sensitivity, timing, and invalidation.
- [x] Add replay fixtures for at least three historical scenarios.
- [x] Require Codex review before merge of graph scoring logic.

## Milestone 3 - Review and Acceptance Workflow
- [ ] Persist candidate review state, rejection reasons, and fix requests.
- [x] Build structured worklog updates for each accepted code batch.
- [x] Define acceptance checklist for data freshness, determinism, and rollback.
- [x] Add CI threshold docs for tests and smoke coverage.

## Milestone 4 - Windows Production Path
- [x] Package Windows demo/start and export execution path.
- [x] Add Aliyun collector snapshot exporter and Windows sync script.
- [x] Add high-quality free source matrix and front-loaded translation task config.
- [x] Add deployment rehearsal checklist and rollback drills.
- [x] Add Obsidian-backed read path with sample-data fallback.
- [x] Track detached Windows uvicorn startup files in-repo.
- [x] Re-run Windows production smoke after host reachability recovers.

## Milestone 5 - Live Evidence Expansion
- [x] Define free source priority: official exchanges, regulators, company announcements, official macro, public research indexes, search enrichment.
- [x] Define Windows pre-collection and pre-translation wrapper without committed secrets.
- [x] Add first live fusion adapters for Radar snapshot, Intelligence events, and Research reports.
- [x] Validate production scheduler cadence on Windows before enabling live Lead-Lag scoring.
- [x] Add first reliability gate for live evidence: free/public source domains, source URL presence, source health metadata, and future-date guard.
- [ ] Add adapter-level tests for canonical URL, content hash, parser version, and fallback behavior.

## Exit Criteria for V1
- Pipeline stages are documented and reproducible.
- Codex can review and accept worker-generated code against explicit gates.
- CI can execute `lint`, `type`, `tests`, and `smoke` in GitHub Actions without relying on undocumented local state.
- Worklog can trace delivery batch, review result, defects, and final acceptance.
