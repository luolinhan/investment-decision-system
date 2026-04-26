# Lead-Lag Alpha Engine Worklog

This file is the execution ledger for Lead-Lag Alpha Engine V1.

## Usage

- One batch per section.
- Bailian records delivery scope and self-check evidence.
- Codex records review conclusion, acceptance decision, and required fixes.
- Keep entries append-only.

---

## Batch Template

### Batch ID
- `batch_id`:
- `date`:
- `branch_or_ref`:
- `owner`:
- `stage`:

### Bailian Delivery
- `planned_scope`:
- `delivered_files`:
- `generated_modules`:
- `self_check_commands`:
- `known_gaps`:

### Codex Review
- `reviewer`: Codex
- `review_summary`:
- `findings`:
- `risk_level`:
- `required_fixes`:

### Acceptance
- `decision`: pending / accepted / rejected / accepted_with_followups
- `decision_date`:
- `evidence`:
- `rollback_note`:

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-001 |  |  | todo |  |

---

## Batch 2026-04-25-bootstrap

### Batch ID
- `batch_id`: 2026-04-25-bootstrap
- `date`: 2026-04-25
- `branch_or_ref`: docs/bootstrap
- `owner`: Codex
- `stage`: S0 Spec Locked

### Bailian Delivery
- `planned_scope`: pending first implementation batch
- `delivered_files`: none
- `generated_modules`: none
- `self_check_commands`: none
- `known_gaps`: implementation not started

### Codex Review
- `reviewer`: Codex
- `review_summary`: V1 documentation, CI starter, and collaboration contract initialized.
- `findings`: none
- `risk_level`: low
- `required_fixes`: populate first Bailian delivery section before implementation begins

### Acceptance
- `decision`: accepted_with_followups
- `decision_date`: 2026-04-25
- `evidence`: docs and CI scaffolding added
- `rollback_note`: no runtime rollback required because this batch is documentation-only

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-001 | medium | Bailian | todo | first code batch must attach self-check evidence |

---

## Batch 2026-04-25-v1-runtime

### Batch ID
- `batch_id`: 2026-04-25-v1-runtime
- `date`: 2026-04-25
- `branch_or_ref`: main workspace
- `owner`: Codex + Bailian workers
- `stage`: S4 Integrated

### Bailian Delivery
- `planned_scope`: runnable Lead-Lag V1 backend service, sample data, routes, page wiring, and tests
- `delivered_files`: `app/services/lead_lag_service.py`, `sample_data/lead_lag/lead_lag_v1.json`, `tests/test_lead_lag_service.py`, `app/routers/investment.py`, `templates/base.html`
- `generated_modules`: lead-lag sample-data engine, page/API route adapters
- `self_check_commands`: `python3 -m py_compile app/services/lead_lag_service.py app/routers/investment.py`, `python3 -m pytest -q tests/test_lead_lag_service.py`, `python3 -m pytest -q tests/test_lead_lag_api.py`
- `known_gaps`: still sample-data-first; live Radar / Intelligence / Research evidence fusion remains the next tranche

### Codex Review
- `reviewer`: Codex
- `review_summary`: Accepted after integrating service-to-frontend adapter methods, reviewing route fallbacks, and adding API-level contract coverage.
- `findings`: one integration mismatch was found during review: service returned rich payloads while frontend expected flattened lists; fixed in `LeadLagService` via `get_/list_` adapters before acceptance.
- `risk_level`: medium
- `required_fixes`: next tranche should replace sample-data-only overview/liquidity/opportunity inputs with mixed live evidence where stable.

### Acceptance
- `decision`: accepted_with_followups
- `decision_date`: 2026-04-25
- `evidence`: local compile passed; service tests passed; API route/page tests passed
- `rollback_note`: page routes and service are additive; rollback is deleting Lead-Lag route/service imports and scripts without touching existing Radar/Intelligence/Shortline flows

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-002 | medium | Codex | done | aligned service outputs with existing frontend JS contract |
| FIX-003 | medium | Bailian | todo | connect sample-data engine to live evidence layers incrementally |

---

## Batch 2026-04-25-windows-hardening

### Batch ID
- `batch_id`: 2026-04-25-windows-hardening
- `date`: 2026-04-25
- `branch_or_ref`: main workspace
- `owner`: Codex
- `stage`: S4 Windows Runtime Hardening

### Bailian Delivery
- `planned_scope`: none; this batch was handled directly by Codex because it was operational glue and startup hardening
- `delivered_files`: `run_uvicorn_service.py`, `start_uvicorn_service.bat`, `start_investment_hub.ps1`, `docs/windows_deployment.md`, `README.md`
- `generated_modules`: detached Windows uvicorn service entrypoint
- `self_check_commands`: `python3 -m py_compile run_uvicorn_service.py`
- `known_gaps`: Windows host was intermittently unreachable during final production verification; sync and smoke must be re-run once the host is reachable

### Codex Review
- `reviewer`: Codex
- `review_summary`: Added tracked startup files to eliminate machine-local drift, fixed the no-console uvicorn logging failure mode by using `log_config=None`, and removed the unconditional console log handler in `app/main.py` so background `pythonw` startup can persist.
- `findings`: Windows had an untracked `run_uvicorn_service.py` that could fail under `pythonw` because uvicorn's default formatter expected `sys.stdout.isatty()`. After that was fixed, `app/main.py` still assumed a console-backed `StreamHandler`, which made background startup brittle until the handler list was made console-aware.
- `risk_level`: medium
- `required_fixes`: sync the tracked startup files to Windows and verify `/investment/lead-lag` plus `/investment/api/lead-lag/overview` after the host recovers

### Acceptance
- `decision`: accepted_with_followups
- `decision_date`: 2026-04-25
- `evidence`: local compile passed; Windows production returned `200` for `/investment/lead-lag` and `/investment/api/lead-lag/overview` after the detached startup path and logging fix were synced
- `rollback_note`: revert the new Windows startup files if the detached path causes regressions; keep `start_server_prod.bat` available as a fallback

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-004 | medium | Codex | done | tracked detached Windows uvicorn entrypoint |
| FIX-005 | medium | Codex | todo | re-sync hardened startup files to Windows and re-run smoke once host is reachable |

---

## Batch 2026-04-25-source-matrix-pretranslate

### Batch ID
- `batch_id`: 2026-04-25-source-matrix-pretranslate
- `date`: 2026-04-25
- `branch_or_ref`: main workspace
- `owner`: Codex
- `stage`: S3 Config / Docs / Scripts

### Bailian Delivery
- `planned_scope`: Extend Lead-Lag high-quality free source matrix and document front-loaded Windows collection / translation task.
- `delivered_files`: `config/source_matrix.example.yaml`, `docs/source_matrix.md`, `docs/aliyun_collector.md`, `docs/bailian_workflow.md`, `README.md`, `ROADMAP.md`, `docs/lead_lag_worklog.md`, `scripts/lead_lag_pretranslate_task.ps1`
- `generated_modules`: Windows Lead-Lag pre-collection and pre-translation wrapper script.
- `self_check_commands`: `python3 - <<'PY' ... yaml.safe_load(...) ... PY`, `python3 -m py_compile scripts/lead_lag_aliyun_collector.py scripts/translate_intelligence.py scripts/translate_shortline_events.py`, `git diff --name-only -- app/services/lead_lag_service.py app/routers/investment.py scripts/sync_intelligence.py`
- `known_gaps`: live T0/T1 adapters are still future work; this batch is docs/config/scripts only.

### Codex Review
- `reviewer`: Codex
- `review_summary`: Source tiers now separate official exchanges, regulators, company announcements, macro official data, public research indexes, search enrichment, and operator memory. Windows pre-translation is documented and scripted without embedding secrets.
- `findings`: none at patch time
- `risk_level`: low
- `required_fixes`: future adapter batches must attach canonical URLs, content hashes, parser versions, license notes, and replay tests.

### Acceptance
- `decision`: accepted_with_followups
- `decision_date`: 2026-04-25
- `evidence`: YAML parse passed; related Python scripts compile; secret-pattern scan returned no matches for edited docs/config/script set.
- `rollback_note`: docs/config/scripts only; rollback by removing the new wrapper and reverting source-matrix documentation changes.

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-006 | medium | Bailian | todo | implement first replayable T0/T1 live adapters after source inventory review |

---

## Batch 2026-04-25-live-fusion

### Batch ID
- `batch_id`: 2026-04-25-live-fusion
- `date`: 2026-04-25
- `branch_or_ref`: main workspace
- `owner`: Codex + Bailian worker
- `stage`: S5 Live Evidence Integrated

### Bailian Delivery
- `planned_scope`: use Bailian worker for source matrix, docs, and pre-translation wrapper; Codex owns service integration and production validation.
- `delivered_files`: `app/services/lead_lag_service.py`, `tests/test_lead_lag_service.py`, `tests/test_lead_lag_api.py`, `scripts/lead_lag_pretranslate_task.ps1`, `config/source_matrix.example.yaml`, `docs/source_matrix.md`, `docs/aliyun_collector.md`, `docs/bailian_workflow.md`, `README.md`, `ROADMAP.md`
- `generated_modules`: Lead-Lag live evidence adapter and Windows pre-translation task wrapper.
- `self_check_commands`: `python3 -m py_compile app/services/lead_lag_service.py app/main.py scripts/lead_lag_aliyun_collector.py scripts/export_lead_lag_report.py`, `python3 -m pytest -q tests/test_lead_lag_service.py tests/test_lead_lag_api.py`, Windows `/investment/api/lead-lag/overview` smoke.
- `known_gaps`: canonical URL / content hash / parser version tests are not yet implemented for each source adapter.

### Codex Review
- `reviewer`: Codex
- `review_summary`: Lead-Lag now fuses local live evidence from Radar snapshot, Intelligence events, and Research reports while preserving sample fallback. Opportunity and event outputs expose stock code, stock name, market, source URL, update time, and evidence source list. Live Intelligence / Research rows now pass a `free_public_reliable` source-quality gate before scoring.
- `findings`: Windows PowerShell output can display UTF-8 JSON as mojibake; verified payload correctness with Python `json.dumps(..., ensure_ascii=True)`.
- `risk_level`: medium
- `required_fixes`: add deeper adapter tests for source reproducibility and avoid Shortline translation in the 5-hour task until the known SQLite lock issue is fixed.

### Acceptance
- `decision`: accepted_with_followups
- `decision_date`: 2026-04-25
- `evidence`: local tests passed; Windows production returned `source=live_fusion`, `live_event_count=30`, `live_research_count=80`, and `quality_filter=free_public_reliable`; `LeadLagPretranslate` task created with 5-hour cadence, formal run completed with `records_processed=27`, `research_translated=27`, `translator=bailian`, and `records_failed=0`; scheduled task auto-ran again at `2026-04-26 04:27`, last result `0`, next run `2026-04-26 09:27`.
- `rollback_note`: set `LEAD_LAG_LIVE_ENABLED=0` to fall back to sample-only behavior; remove `LeadLagPretranslate` scheduled task if translation load needs to pause.

### Fix Tracking

| Fix ID | Severity | Owner | Status | Notes |
| --- | --- | --- | --- | --- |
| FIX-007 | medium | Codex | done | Lead-Lag live fusion with stock code/name fields |
| FIX-008 | medium | Codex | done | Windows `LeadLagPretranslate` task created every 5 hours |
| FIX-009 | medium | Codex | todo | source-level canonical URL / hash / parser-version tests |
| FIX-010 | medium | Codex | done | first live-source reliability filter added for free public sources, including public research domains like NBER/OECD/IMF/BIS/Stanford/Brookings/CSET/RAND |
