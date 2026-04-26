# Lead-Lag Alpha Engine V1 Windows Deployment

## Role of Windows Node

Windows remains the serving node for Investment Hub:
- primary API
- operator UI
- local SQLite or local cache-backed reads
- scheduled execution hooks

Aliyun does not replace Windows. It feeds Windows.

## Deployment Flow

1. Codex approves a Bailian batch after CI and review.
2. Accepted artifacts are synced to the Windows workspace.
3. Windows service starts or reloads the API process.
4. Post-deploy smoke validates import health, endpoint reachability, and data freshness.

## V1 Scripts

- demo start: `scripts/start_lead_lag_demo.ps1`
- batch wrapper: `scripts/start_lead_lag_demo.bat`
- snapshot sync: `scripts/sync_lead_lag_aliyun_snapshot.ps1`
- report export: `python scripts/export_lead_lag_report.py --type daily`
- production start: `start_investment_hub.ps1`
- background service wrapper: `start_uvicorn_service.bat`
- background service process launcher: `start_uvicorn_service.ps1`
- detached service entry: `run_uvicorn_service.py`
- pre-collection and Bailian translation task: `scripts/lead_lag_pretranslate_task.ps1`

## Minimum V1 Checklist

- Python environment matches the repo baseline.
- required env file exists and is validated locally.
- `data/` path is writable.
- service start script is callable.
- rollback path is documented before promotion.

## Smoke Expectations

- import `app.main`
- service startup path resolves templates and static assets
- local DB path is readable if present
- Lead-Lag routes do not break the existing Investment Hub routes
- `/investment/lead-lag` returns `200`
- `/investment/api/lead-lag/overview` returns a non-empty JSON object
- overview reports `source=live_fusion`, non-null `live_event_count` / `live_research_count`, and `quality_filter=free_public_reliable` when live evidence is available
- `/investment/api/lead-lag/opportunities` rows include `asset_code`, `asset_name`, and `market`

## Rollback Rule

If a Lead-Lag batch causes route failure, schema mismatch, or repeated runtime exceptions, revert to the last accepted batch and record the incident in the worklog before the next Bailian round.
