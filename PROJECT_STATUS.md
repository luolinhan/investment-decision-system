# Project Status

## Identity

- Project: Investment Decision System
- Control workspace: `/Users/lhluo/agent-workspaces/investment-control`
- Production host: Windows
- Production URL: `http://100.64.93.19:8080/investment/`
- Source of truth: GitHub `luolinhan/investment-decision-system`

## Surfaces

- Daily execution workbench: `/investment/`
- Legacy multi-indicator dashboard: `/investment/legacy`
- Intelligence source desk: `/investment/intelligence`
- Lead-Lag theme research: `/investment/lead-lag`
- Stocks: `/stocks`

## Long-Term Maintenance Mode

This repo should be handled as a persistent Codex project, not a one-off task folder.

Expected workflow:
- open with `investment-codex`
- use Codex built-in worker agents only for bounded implementation tasks when the user asks for multi-agent execution
- keep acceptance, deployment, and production verification in the control repo

Current recommended agent mix:
- Codex controller: current high-capability model for architecture, debugging, review, and acceptance
- Coding workers: Codex built-in `worker` agents on `gpt-5.3-codex`
- Lightweight docs/research workers: `gpt-5.4-mini` only when the task is low-risk and non-production
- External Bailian / Claude worker launchers have been deleted from this machine

Prompt templates:
- `/Users/lhluo/agent-workspaces/templates/investment-controller-kickoff.md`
- `/Users/lhluo/agent-workspaces/templates/investment-worker-dispatch.md`
- `/Users/lhluo/agent-workspaces/templates/investment-worker-task.md`

## Current Stable Facts

- Windows is the only production runtime for UI, API, and SQLite/DuckDB state
- Windows is the default data and storage node; Mac and Aliyun must not be required for runtime acceptance
- Aliyun only handles optional upstream collection and snapshot/export work
- `/investment/` is the simplified daily execution workbench; the old dashboard is retained at `/investment/legacy`
- Shortline official event chain now includes `SEC + FDA + ClinicalTrials + Company IR`
- Shortline T0 official events now have bilingual completion in production
- Windows detached `uvicorn` is run through `pythonw` startup script to avoid no-stdout crashes
- Lead-Lag V2 Operator/Builder surfaces are deployed on Windows with Transmission Graph Workspace, Replay Diagnostics, Sector Deep Evidence, Obsidian Action Memory, and fixed-time brief tasks

## Active Priorities

1. Keep Windows service, local storage, and GitHub `main` aligned before accepting user-facing phases
2. Restore data freshness so `/investment/api/practical-brief` can open the execution gate
3. Keep the daily workbench constrained to risk, top actions, watchlist, and data status
4. Move Radar, Shortline, Universe, and diagnostics behind research/admin flows until their data is fresh
5. Populate execution journal and strategy samples before expanding strategy metrics
6. Expand Lead-Lag only after market-facing events and review checkpoints are fresh

## Standard Acceptance Checklist

- repo diff stays within task scope
- local tests pass
- Windows production path verified if user-facing
- API returns current expected fields
- knowledge base updated for operationally relevant changes

## Recommended Entry Command

Controller:

```bash
investment-codex
```
