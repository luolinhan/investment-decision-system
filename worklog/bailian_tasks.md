# Lead-Lag V2 Codegen Worklog

## Operating Rule

- 2026-04-26 update: stop using Aliyun Bailian Coding Plan for new code production in this repository.
- Large code tasks now use Codex internal coding workers, preferably `gpt-5.3-codex`, with Codex as controller/reviewer/integrator.
- Production runtime must not depend on any coding agent, Bailian tool, or non-interactive LLM API.
- All generated code must be reviewed, tested, and deployed by the controller before acceptance.

## V2 Baseline Completed

| ID | Task | Worker / Source | Files | Review Status |
| --- | --- | --- | --- | --- |
| V2-001 | V1 audit and V2 docs | Codex controller | `docs/v1_gap_report.md`, `docs/v2_blueprint.md`, `docs/action_schema.md`, `docs/event_relevance_rules.md`, `docs/data_source_matrix_v2.md`, `docs/research_ops_workflow.md` | Accepted |
| V2-002 | OpportunityCard schema and scoring | Codex controller | `app/services/lead_lag_schema.py`, `app/services/lead_lag_scoring.py`, `config/lead_lag_v2.example.yaml`, `tests/test_lead_lag_v2_schema.py` | Accepted |
| V2-003 | Event Relevance Engine | Codex controller | `app/services/lead_lag_events.py`, `tests/test_lead_lag_event_relevance.py` | Accepted |
| V2-004 | Operator Mode UI and V2 API routes | Codex controller | `templates/lead_lag.html`, `static/js/lead_lag.js`, `static/css/lead_lag.css`, `app/routers/investment.py`, `tests/test_lead_lag_api.py` | Accepted |
| V2-005 | Macro / External / HK bridge | Codex controller | `app/services/lead_lag_macro.py`, `tests/test_lead_lag_macro_bridge.py` | Accepted |
| V2-006 | Fixed research brief generator | Codex controller | `app/services/lead_lag_briefs.py`, `scripts/export_lead_lag_brief.py`, `tests/test_lead_lag_briefs.py` | Accepted |

## 2026-04-26 Baseline Verification

- Local: `python3 -m pytest -q tests/test_lead_lag_macro_bridge.py tests/test_lead_lag_briefs.py tests/test_lead_lag_v2_schema.py tests/test_lead_lag_event_relevance.py tests/test_lead_lag_service.py tests/test_lead_lag_api.py` passed with 17 tests.
- Local: `node --check static/js/lead_lag.js` passed.
- Local: placeholder scan found no V1 empty placeholders in the Lead-Lag UI files.
- Windows: `/investment/lead-lag` returned HTTP 200.
- Windows: `/investment/api/lead-lag/opportunity-queue`, `/event-frontline`, and `/briefs/pre_open_playbook` returned V2 payloads.

## Current Deepening Queue

| ID | Task | Owner | Write Scope | Status |
| --- | --- | --- | --- | --- |
| V2-D1 | Transmission Graph Workspace and Replay Diagnostics | `gpt-5.3-codex` worker | `app/services/lead_lag_diagnostics.py`, `tests/test_lead_lag_diagnostics.py` | Accepted locally |
| V2-D2 | Five-sector Deep Evidence Engine | `gpt-5.3-codex` worker | `app/services/lead_lag_sector_evidence.py`, `tests/test_lead_lag_sector_evidence.py` | Accepted locally |
| V2-D3 | Obsidian Action Memory and Windows brief scheduler | `gpt-5.3-codex` worker | `app/services/lead_lag_memory_actions.py`, `scripts/setup_lead_lag_brief_tasks.ps1`, `scripts/run_lead_lag_brief_task.ps1`, `tests/test_lead_lag_memory_actions.py` | Accepted locally after controller scheduler fix |
| V2-D4 | API/UI integration, docs, Windows deploy | Codex controller | router, LeadLagService, UI, docs, deployment | Accepted on Windows |

## 2026-04-26 Deepening Review Notes

- V2-D1 review: accepted. Module is pure, no runtime LLM/API dependency, graph edges follow required schema, replay uses sample/card-derived diagnostics and marks `sample_derived`.
- V2-D2 review: accepted. Five sector payloads expose evidence layers, missing validation, provider gaps, and do not fabricate official live values.
- V2-D3 review: accepted after controller fixes. `schtasks /TR` first rejected inline `cmd /c ... && ...` quoting, then rejected long absolute PowerShell commands over 261 characters. The setup script now writes short per-slot `.cmd` wrappers under `scripts\LeadLagBrief*.cmd`; scheduled tasks point to those wrappers, and wrappers call `scripts/run_lead_lag_brief_task.ps1`.
- Controller integration added V2 service facade methods and API/UI wiring for `transmission-workspace`, `replay-diagnostics`, `research-memory/actions`, and `sector-evidence`.

## 2026-04-26 Windows Acceptance

- Deployed Lead-Lag service/router/UI/scripts to `C:\Users\Administrator\research_report_system`.
- Restarted Windows 8080 service via `start_uvicorn_service.ps1`.
- Smoke passed: `/investment/lead-lag` returned 200.
- Smoke passed: `/investment/api/lead-lag/transmission-workspace`, `/replay-diagnostics`, `/sector-evidence`, `/research-memory/actions`, `/briefs/pre_open_playbook`, and `/opportunity-queue` returned V2 payloads.
- Registered `LeadLagBrief0600`, `LeadLagBrief0820`, `LeadLagBrief1140`, `LeadLagBrief1515`, and `LeadLagBrief2130` as Windows SYSTEM scheduled tasks.
- Verified `schtasks /Run /TN LeadLagBrief0820` completed with last result `0` and wrote `0820-pre-open-playbook.md/json` under the Administrator Obsidian `40-任务/Lead-Lag Ops/2026-04-26/` directory.
- Wrote long-term verification notes to `/Users/lhluo/Documents/Obsidian/知识库/90-日志/2026-04-26-Lead-Lag-V2深化.md`.

## Open Risks

- HKEX short selling, Stock Connect holdings, NBS/PBOC macro, CDE/NMPA providers are currently provider-ready / partial-live, not full official ingestion.
- Replay diagnostics must clearly separate sample-derived statistics from live evidence.
- Obsidian memory must remain read-only for source notes; generated brief exports must write to a separate output directory.
- Windows scheduled tasks are verified for `LeadLagBrief0820`; monitor the first natural 06:00/11:40/15:15/21:30 runs for schedule timing and log rotation.
