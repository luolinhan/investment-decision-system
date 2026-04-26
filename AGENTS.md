# Investment Project Guide

## Scope

This repository is the long-term control workspace for the China-asset research system.

Primary goals:
- keep Windows production stable
- improve data coverage and freshness
- maintain the Radar, Intelligence, Research, and Shortline surfaces
- build and maintain the Lead-Lag Alpha Engine surface
- use Codex as controller and Codex built-in worker agents for bounded execution

## Canonical Topology

- Control repo: `/Users/lhluo/agent-workspaces/investment-control`
- External Bailian / Claude worker repos and launchers have been removed from this machine.
- Windows production repo: `C:\Users\Administrator\research_report_system`
- Windows production URL: `http://100.64.93.19:8080/investment/`
- Aliyun role: collection, search adaptation, snapshot export only
- GitHub is the source of truth

## Project Rules

- Windows is the only production runtime for UI, API, and primary storage.
- Mac is control plane only. No long-running production services belong here.
- Do not mix unrelated repo dirt into commits.
- Do not revert user changes unless explicitly asked.
- Prefer focused commits by surface: `radar`, `intelligence`, `shortline`, `research`, `infra`.
- For production fixes, validate on Windows before claiming done.

## Multi-Agent Rules

- `investment-codex` owns architecture, task split, integration, tests, deploy, and final acceptance.
- Do not use external Bailian / Claude worker launchers for multi-agent work; the local worker launchers have been removed.
- When the user explicitly asks for multi-agent or parallel agent execution, use Codex built-in worker agents.
- Coding worker agents must use model `gpt-5.3-codex`; do not let routine coding workers inherit `gpt-5.5`.
- Keep the main Codex session on architecture, task split, hard debugging, review, integration, tests, deployment, and final acceptance.
- Workers must not edit the same file set in the same task.
- Workers do not push, deploy, or widen scope on their own.
- For Lead-Lag work, prefer:
  - Backend worker (`gpt-5.3-codex`): `app/services/lead_lag_service.py`, `sample_data/lead_lag/*`
  - Frontend worker (`gpt-5.3-codex`): `app/routers/investment.py`, `templates/lead_lag.html`, `static/js/lead_lag.js`, `templates/base.html`
  - Test/docs worker (`gpt-5.3-codex`, or `gpt-5.4-mini` for docs-only): tests, CI, docs, export/ops scripts
- For Lead-Lag V2 work, read `docs/v1_gap_report.md`, `docs/v2_blueprint.md`, `docs/action_schema.md`, and `docs/event_relevance_rules.md` before coding.
- Lead-Lag V2 must default to Operator Mode: Decision Center, Opportunity Queue, What Changed, Event Frontline, and Do Not Chase. Builder Mode keeps model library, graph, replay, raw event, and memory diagnostics.
- Lead-Lag V2 Operator payloads must not expose critical blank fields or placeholders. If evidence is missing, return `missing_confirmations` and `missing_evidence_reason`.
- Lead-Lag V2 scoring weights must be configurable; do not add hidden hard-coded weights except transparent fallback defaults.
- Production runtime must not depend on Bailian Coding Plan, Claude Code, or any interactive coding tool.

Preferred execution policy:

- Keep Codex on architecture, prioritization, debugging, review, and production acceptance.
- Push bounded code volume to Codex built-in `worker` agents on `gpt-5.3-codex` when the user has asked for multi-agent execution.
- Default parallelism target is `1 + 2`; burst to `1 + 3` for large tasks with clearly disjoint file ownership.

Task card template:
- `/Users/lhluo/agent-workspaces/templates/investment-controller-kickoff.md`
- `/Users/lhluo/agent-workspaces/templates/investment-worker-dispatch.md`
- `/Users/lhluo/agent-workspaces/templates/investment-worker-task.md`

## First Files To Read

- `README.md`
- `PROJECT_STATUS.md`
- `app/routers/investment.py`
- `app/services/shortline_service.py`
- `app/services/intelligence_service.py`
- `app/services/radar_service.py`
- `app/services/lead_lag_service.py`
- `sample_data/lead_lag/lead_lag_v1.json`

## Standard Commands

Local checks:

```bash
python3 -m pytest -q tests/test_shortline_service.py
python3 -m py_compile app/services/shortline_service.py
```

Shortline local data tasks:

```bash
python3 scripts/sync_shortline_us_events.py --include-official --build-candidates
python3 scripts/translate_shortline_events.py --limit 20
```

Windows production access:

```bash
ssh win-exec-tail
```

Production API smoke:

```bash
curl -s http://100.64.93.19:8080/investment/api/shortline/overview
curl -s http://100.64.93.19:8080/investment/api/intelligence/hub
curl -s http://100.64.93.19:8080/investment/api/radar/overview
```

## Current Long-Term Workstreams

1. Radar data freshness and missing indicators
2. Research workbench quality and bilingual research enrichment
3. Shortline official event chain and T1 bilingual completion
4. Hong Kong liquidity and A/H microstructure completeness
5. Windows runtime robustness and task scheduling hygiene
6. Lead-Lag Alpha Engine V1 from sample-data demo to live evidence-backed surface
7. Lead-Lag Alpha Engine V2 from research display surface to Operator Mode research OS

## Done Means

A task is not done until:
- code is merged in this repo
- Windows production behavior is verified when relevant
- the API or page is checked
- the change is written back to the knowledge base when it affects long-term operation
