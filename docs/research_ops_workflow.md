# Lead-Lag Research Ops Workflow

Audit date: 2026-04-26

V2 turns Lead-Lag into a fixed-cadence research operating system. The system should generate briefings that connect opportunities, catalysts, invalidations, and next checks.

## Daily Cadence

| Time | Slot | Purpose |
|---:|---|---|
| 06:00 | `overnight_digest` | Overnight global leaders, US/HK bridge, external risk, new events |
| 08:20 | `pre_open_playbook` | Pre-open priority list, first/second baton, avoid list, risk budget |
| 11:40 | `morning_review` | Midday validation, failed opens, crowding changes, next checks |
| 15:15 | `close_review` | Close validation, upgraded/downgraded opportunities, replay notes |
| 21:30 | `us_watch_mapping` | US watchlist, cross-market mapping, next-day pre-triggers |

## Brief Schema

```yaml
LeadLagBrief:
  slot: overnight_digest | pre_open_playbook | morning_review | close_review | us_watch_mapping
  as_of: datetime
  headline: string
  today_focus:
    - string
  new_catalysts:
    - event_id: string
      title: string
      expected_path: string
  invalidation_alerts:
    - opportunity_id: string
      rule: string
      status: triggered | near_trigger | watch
  next_checkpoints:
    - time: datetime
      item: string
  top_opportunities:
    - opportunity_id: string
      thesis: string
      actionability_score: number
      tradability_score: number
  do_not_chase:
    - thesis: string
      reason: string
  macro_external_hk_context:
    macro_regime: string
    external_risk: string
    hk_liquidity: string
  source_summary:
    source_count: integer
    freshness: string
    cache_status: live | cached | sample_fallback
```

## Slot Rules

### 06:00 Overnight Digest

Inputs:

- US leaders and ADRs.
- External risk.
- HK pre-market bridge if available.
- Overnight Intelligence events.
- Research reports ingested overnight.

Output emphasis:

- What changed while China market was closed.
- Which signals can become first baton.
- Which events are research-facing only.

### 08:20 Pre-Open Playbook

Inputs:

- Decision Center.
- Opportunity Queue.
- Event Frontline.
- Macro / External / HK bridge.
- Previous close and overnight changes.

Output emphasis:

- Today's Top 3.
- First baton / second baton / pre-trigger candidates.
- Today's do-not-chase list.
- Risk budget and invalidation conditions.

### 11:40 Morning Review

Inputs:

- Morning price/flow validation when available.
- Event updates.
- Opportunity score deltas.

Output emphasis:

- Which opportunities validated.
- Which opportunities failed to confirm.
- Which themes became crowded too early.
- What must be checked after lunch.

### 15:15 Close Review

Inputs:

- Full-day validation.
- Crowding/liquidity changes.
- Replay analogs.
- Obsidian notes and review items.

Output emphasis:

- Stage transitions.
- Upgrades/downgrades.
- Failure mode notes.
- Next day's watchlist.

### 21:30 US Watch / Mapping Check

Inputs:

- US watchlist and leader assets.
- Global macro/external risk.
- Company earnings/guidance/capex events.
- Cross-market mapping.

Output emphasis:

- Which US leaders matter for China tomorrow.
- Which bridge assets to watch.
- Which local mappings need pre-open validation.

## Obsidian Export

Default output directory:

```text
<vault>/40-任务/Lead-Lag Ops/YYYY-MM-DD/
```

Write policy:

- Source notes remain read-only.
- Generated briefs write only to the dedicated Lead-Lag Ops directory.
- Do not write secrets, tokens, cookies, or private key contents.

File naming:

- `0600-overnight-digest.md`
- `0820-pre-open-playbook.md`
- `1140-morning-review.md`
- `1515-close-review.md`
- `2130-us-watch-mapping.md`

## Script Contract

Recommended script:

```bash
python3 scripts/export_lead_lag_brief.py --slot pre_open_playbook
```

Recommended options:

- `--as-of`
- `--output-dir`
- `--obsidian`
- `--no-obsidian`
- `--json`
- `--markdown`
- `--dry-run`

Implementation notes:

- `app/services/lead_lag_briefs.py` is the independent fixed-time brief composer. It reads existing V2 outputs from `LeadLagService` and does not require Bailian, Codex workers, or live API keys.
- `scripts/export_lead_lag_brief.py --slot <slot>` exports Markdown by default. Add `--json` to emit the structured payload, or pass both `--markdown --json` to write both formats.
- `--output-dir` writes directly to the supplied directory. Without `--output-dir`, `--obsidian` writes to the dedicated `40-任务/Lead-Lag Ops/YYYY-MM-DD/` directory; `--no-obsidian` falls back to `reports/lead_lag/briefs/`.
- `--dry-run` prints planned output paths and must not create directories or files.

Windows scheduled task names:

- `LeadLagBrief0600`
- `LeadLagBrief0820`
- `LeadLagBrief1140`
- `LeadLagBrief1515`
- `LeadLagBrief2130`

Windows task setup:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\setup_lead_lag_brief_tasks.ps1 -RepoRoot C:\Users\Administrator\research_report_system -Force
```

The setup script writes short per-slot wrappers such as `scripts\LeadLagBrief0820.cmd` because `schtasks /TR` has a short command-length limit. The wrappers call `scripts\run_lead_lag_brief_task.ps1`, which sets the repository working directory, resolves Python to an absolute executable, runs `scripts\export_lead_lag_brief.py`, and appends logs under `logs\lead_lag_briefs`. The Python brief exporter prefers `INVESTMENT_OBSIDIAN_VAULT` / `OBSIDIAN_VAULT_PATH`, and on Windows falls back to the Administrator Obsidian vault when it exists.

## Optional LLM Usage

LLM providers may be used outside the runtime for:

- Translating ingested foreign-language source material.
- Drafting summaries for research reports.
- Non-production research enrichment.

LLM providers and coding agents must not be required for:

- Production scoring.
- Runtime API response.
- Scheduled brief generation if no API key is available.
- Repository code production inside this project; bounded codegen should use Codex internal `gpt-5.3-codex` workers and controller review.

Fallback:

- If an LLM provider is unavailable, use rule-based summaries and mark `cache_status` or `generation_mode` accordingly.

## Acceptance

- Each slot can be generated independently.
- Briefs include today focus, catalysts, invalidations, next checks, Top opportunities, and Do Not Chase list.
- Briefs can export to Markdown.
- Obsidian export writes only to the dedicated output directory.
- Windows tasks can run without Mac staying awake.
