# Investment Project Guide

## Scope

This repository is the long-term control workspace for the China-asset research system.

Primary goals:
- keep Windows production stable
- improve data coverage and freshness
- maintain the Radar, Intelligence, Research, and Shortline surfaces
- use Codex as controller and Claude workers as bounded execution agents

## Canonical Topology

- Control repo: `/Users/lhluo/agent-workspaces/investment-control`
- Worker A repo: `/Users/lhluo/agent-workspaces/investment-worker-a`
- Worker B repo: `/Users/lhluo/agent-workspaces/investment-worker-b`
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
- `investment-worker-a` handles routine code production, scripts, and test fill-in.
- `investment-worker-b` handles heavier refactors and harder debugging.
- Workers must not edit the same file set in the same task.
- Workers do not push, deploy, or widen scope on their own.

Task card template:
- `/Users/lhluo/agent-workspaces/templates/investment-worker-task.md`

## First Files To Read

- `README.md`
- `PROJECT_STATUS.md`
- `app/routers/investment.py`
- `app/services/shortline_service.py`
- `app/services/intelligence_service.py`
- `app/services/radar_service.py`

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

## Done Means

A task is not done until:
- code is merged in this repo
- Windows production behavior is verified when relevant
- the API or page is checked
- the change is written back to the knowledge base when it affects long-term operation
