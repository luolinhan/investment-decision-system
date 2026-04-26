# Investment Project Guide

## Scope

This repository is the control workspace for Investment Hub and the Lead-Lag Alpha Engine.

## Canonical Runtime

- Control repo: `/Users/lhluo/agent-workspaces/investment-control`
- Windows production repo: `C:\Users\Administrator\research_report_system`
- Windows production URL: `http://100.64.93.19:8080/investment/`
- Windows production is the only runtime acceptance target.
- GitHub `main` and Windows production must be kept aligned before claiming a phase is done.

## Working Rules

- Default response language is Chinese.
- Read `README.md`, `PROJECT_STATUS.md`, and this file before project-wide changes.
- For Lead-Lag V3 work, start from `docs/v2_gap_report.md`, `docs/v3_architecture.md`, and `worklog/codex_worklog.md`.
- Do not revert user changes unless explicitly requested.
- Do not commit secrets, cookies, API keys, database passwords, or tokens.
- Keep AGENTS.md short; detailed design belongs in `docs/`.

## Lead-Lag V3 Rules

- Evidence must be traceable to original URL, local archive, source metadata, and citation/fact records.
- `sample_demo` and `fallback_placeholder` must be hidden by default and must not be executable.
- Opportunities should be grouped as parent thesis cards with child instrument variants.
- Entity and instrument are separate layers; cross-market mappings require validation.
- Main event flow defaults to `market-facing`; weak or sample events stay in research/background/archive layers.

## Standard Commands

Local checks:

```bash
python3 -m py_compile app/services/lead_lag_service.py app/routers/investment.py
python3 -m pytest -q tests/test_lead_lag_api.py tests/test_lead_lag_v3.py
```

V3 migrations:

```bash
python3 scripts/migrate_v3_evidence_vault.py --json
python3 scripts/migrate_v3_opportunity_universe.py --json
```

Windows production access:

```bash
ssh win-exec-tail
```

Production smoke:

```bash
curl -s http://100.64.93.19:8080/health
curl -s http://100.64.93.19:8080/investment/api/lead-lag/opportunity-queue
curl -s http://100.64.93.19:8080/investment/api/lead-lag/source-quality-lineage
```

## Done Means

- Code is committed in this repo.
- Tests and relevant smoke checks pass.
- Windows production code matches GitHub and runs.
- Long-term knowledge base is updated for operational changes.
