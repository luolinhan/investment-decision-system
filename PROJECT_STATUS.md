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

## Long-Term Maintenance Mode

This repo should be handled as a persistent Codex project, not a one-off task folder.

Expected workflow:
- open with `investment-codex`
- use worker agents only for bounded implementation tasks
- keep acceptance, deployment, and production verification in the control repo

## Current Stable Facts

- Windows is the only production runtime for UI, API, and SQLite/DuckDB state
- Aliyun only handles collection and snapshot/export work
- Shortline official event chain now includes `SEC + FDA + ClinicalTrials + Company IR`
- Shortline T0 official events now have bilingual completion in production
- Windows detached `uvicorn` is run through `pythonw` startup script to avoid no-stdout crashes

## Active Priorities

1. Expand bilingual completion from `T0` official shortline events to selected `T1` market events
2. Continue filling missing Radar indicators with stable official or primary sources
3. Improve Research workbench structure, filtering, and bilingual extraction quality
4. Tighten Windows task scheduling and service restart reliability
5. Reduce page-level empty-state failures by preferring persisted snapshots over live fetches

## Standard Acceptance Checklist

- repo diff stays within task scope
- local tests pass
- Windows production path verified if user-facing
- API returns current expected fields
- knowledge base updated for operationally relevant changes

## Recommended Entry Commands

Controller:

```bash
investment-codex
```

Workers:

```bash
investment-worker-a
investment-worker-b
```
