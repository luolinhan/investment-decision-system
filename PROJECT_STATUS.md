# Project Status

## Identity

- Project: Investment Decision System
- Control workspace: `/Users/lhluo/agent-workspaces/investment-control`
- Production host: Windows
- Production URL: `http://100.64.93.19:8080/investment/`
- Source of truth: GitHub `luolinhan/investment-decision-system`

## Surfaces

- Radar: `/investment/radar`
- Intelligence: `/investment/intelligence`
- Shortline: `/investment/shortline`
- Research workbench: `/investment/research-workbench`
- Lead-Lag: `/investment/lead-lag`

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
- Aliyun only handles collection and snapshot/export work
- Shortline official event chain now includes `SEC + FDA + ClinicalTrials + Company IR`
- Shortline T0 official events now have bilingual completion in production
- Windows detached `uvicorn` is run through `pythonw` startup script to avoid no-stdout crashes
- Lead-Lag V2 Operator/Builder surfaces are deployed on Windows with Transmission Graph Workspace, Replay Diagnostics, Sector Deep Evidence, Obsidian Action Memory, and fixed-time brief tasks

## Active Priorities

1. Expand bilingual completion from `T0` official shortline events to selected `T1` market events
2. Continue filling missing Radar indicators with stable official or primary sources
3. Improve Research workbench structure, filtering, and bilingual extraction quality
4. Tighten Windows task scheduling and service restart reliability
5. Reduce page-level empty-state failures by preferring persisted snapshots over live fetches
6. Expand Lead-Lag official provider coverage for HKEX short selling / Stock Connect holdings / NBS / PBOC / CDE-NMPA feeds

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
