# Investment Hub Windows Deployment

## Role of Windows Node

Windows is the only production runtime for Investment Hub:
- FastAPI service and operator UI
- local SQLite investment database
- local quant workbench market files
- local logs, job locks, and generated briefs
- scheduled collection and refresh hooks

Mac is only a control/development machine. Aliyun is optional for upstream collection, but the production app must not require Mac or Aliyun storage to answer daily decisions.

## Runtime Defaults

The detached service entrypoint sets these defaults unless the Windows task environment overrides them:

```text
INVESTMENT_NODE_ROLE=windows_all_in_one
INVESTMENT_DATA_SOURCE_MODE=windows_local
INVESTMENT_CONTROLLER_HOST=windows-local
INVESTMENT_COLLECTOR_HOST=windows-local
INVESTMENT_DB_PATH=C:\Users\Administrator\research_report_system\data\investment.db
INVESTMENT_STORAGE_ROOT=C:\Users\Administrator\research_report_system\data
RADAR_SNAPSHOT_ONLY=1
```

The UI should be served from:

```text
http://100.64.93.19:8080/investment/
```

The default page is now the simplified daily execution workbench. The legacy multi-indicator dashboard remains at:

```text
http://100.64.93.19:8080/investment/legacy
```

## Deployment Flow

1. Pull or sync accepted code into the Windows workspace.
2. Confirm the scheduled service task points to `run_uvicorn_service.py` in the Windows repo.
3. Restart the Windows service task.
4. Smoke-test health, runtime profile, daily brief, decision center, and data freshness.
5. Do not claim the phase is done unless Windows production and GitHub `main` are aligned.

## Core Scripts

- detached service entry: `run_uvicorn_service.py`
- alternate Windows entry: `scripts/windows_run_uvicorn_service.py`
- unified job runner: `python scripts/investment_job_runner.py`
- example manifest: `config/job_manifest.example.json`
- Lead-Lag brief export: `python scripts/export_lead_lag_brief.py --slot pre_open_playbook`

## Minimum Checklist

- Python environment matches the repo baseline.
- required env file exists and is validated locally.
- `data/` path is writable.
- `data/investment.db` is readable by the service account.
- `data/quant_workbench/` is writable by the service account.
- service start script is callable from Windows Task Scheduler.
- the running OpenAPI schema includes the expected Investment Hub routes.

## Smoke Expectations

- `/health` returns healthy.
- `/investment/api/runtime/profile` reports `windows_all_in_one` and `windows_local`.
- `/investment/api/practical-brief` returns `execution_gate`, `top_actions`, `watchlist`, and `data_status`.
- `/investment/api/decision-center` does not fail.
- `/investment/api/data-health/overview` has no unexpected missing critical tables.
- `/investment/` returns the daily execution workbench.
- `/investment/legacy` returns the old dashboard for diagnostics.

## Rollback Rule

If the simplified workbench causes route failure, schema mismatch, or repeated runtime exceptions, revert the web/API batch to the last accepted commit and record the incident in the worklog before the next promotion.
