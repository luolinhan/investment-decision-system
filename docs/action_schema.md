# Lead-Lag V2 Action Schema

Audit date: 2026-04-26

This document defines the V2 action contracts. The goal is to prevent the UI from showing blank placeholders and to make every card useful for daily research decisions.

## Non-Empty Policy

Critical Operator Mode fields must not be empty.

If the system cannot produce a critical field, it must:

- Set `generation_status` to `insufficient_evidence`.
- Populate `missing_confirmations`.
- Populate `missing_evidence_reason`.
- Avoid rendering fake confidence or placeholder text.

Optional display fields may be hidden by the UI when absent.

Forbidden Operator Mode placeholders:

- `暂无机会说明`
- `Impact -`
- `Watch -`
- `Risk -`
- `Failure Mode -`
- `暂无记忆摘要`
- generic `-` in critical fields

## OpportunityCard

Canonical object for Operator Mode.

```yaml
OpportunityCard:
  id: string
  generation_status: actionable | watch_only | insufficient_evidence | invalidated
  thesis: string
  region: CN | HK | US | Global | CrossMarket
  sector: string
  model_family: string
  leader_asset:
    code: string
    name: string
    market: string
    role: leader
  bridge_asset:
    code: string
    name: string
    market: string
    role: bridge
  local_asset:
    code: string
    name: string
    market: string
    role: local_mapping
  local_proxy_assets:
    - code: string
      name: string
      market: string
      reason: string
  baton_stage: first_baton | second_baton | third_baton | validation_baton | pre_trigger | crowded | invalidated
  why_now: string
  driver: string
  confirmations:
    - string
  missing_confirmations:
    - string
  missing_evidence_reason: string
  risk: string
  invalidation_rules:
    - string
  expected_lag_days:
    min: integer
    max: integer
  expected_review_times:
    - datetime
  crowding_state:
    label: low | medium | high | crowded
    score: number
    explanation: string
  liquidity_state:
    label: poor | acceptable | good | excellent
    score: number
    explanation: string
  actionability_score: number
  tradability_score: number
  evidence_completeness: number
  freshness_score: number
  noise_penalty: number
  decision_priority_score: number
  historical_replay_summary:
    hit_rate: number
    best_horizon: string
    worst_failure_mode: string
    stage_note: string
  mapped_events:
    - event_id: string
      title: string
      event_class: market-facing | research-facing
      relevance_score: number
  mapped_notes:
    - note_id: string
      title: string
      memory_type: thesis_summary | prior_win | prior_failure | trap | similar_case | review_note
  source_count: integer
  source_quality:
    label: T0 | T1 | T2 | T3 | mixed
    explanation: string
  confidence: number
  cache_status: live | cached | sample_fallback
  last_update: datetime
```

Required for display:

- `thesis`
- `why_now`
- `model_family`
- `leader_asset` or explicit missing reason
- `bridge_asset` or explicit missing reason
- `local_asset` or explicit missing reason
- `baton_stage`
- `expected_lag_days`
- `driver`
- `confirmations` or `missing_confirmations`
- `risk`
- `invalidation_rules`
- `crowding_state`
- `liquidity_state`
- `actionability_score`
- `tradability_score`
- `evidence_completeness`
- `freshness_score`
- `historical_replay_summary`
- `source_count`
- `last_update`

## DecisionCenter

```yaml
DecisionCenter:
  as_of: datetime
  headline: string
  main_conclusion: string
  do_not_do_today:
    - string
  top_directions:
    - rank: integer
      sector: string
      thesis: string
      reason: string
      opportunity_id: string
  baton_summary:
    first_baton:
      - opportunity_id: string
    second_baton:
      - opportunity_id: string
    pre_trigger:
      - opportunity_id: string
  risk_budget:
    label: conservative | balanced | aggressive | no_new_risk
    reason: string
  key_invalidations:
    - string
  next_check_time: datetime
  source_count: integer
  cache_status: live | cached | sample_fallback
```

## WhatChanged

```yaml
WhatChanged:
  as_of: datetime
  since: datetime
  new_signals:
    - string
  upgraded_opportunities:
    - opportunity_id: string
      reason: string
  downgraded_or_invalidated:
    - opportunity_id: string
      reason: string
  crowding_up:
    - thesis: string
      reason: string
  macro_external_policy_changes:
    - string
```

## AvoidItem

```yaml
AvoidItem:
  id: string
  thesis: string
  reason_type: crowded | incomplete_evidence | baton_finished | invalidation_triggered | liquidity_mismatch
  reason: string
  evidence:
    - string
  related_assets:
    - code: string
      name: string
      market: string
  next_review_time: datetime
  source_count: integer
  last_update: datetime
```

## EventRelevance

```yaml
EventRelevance:
  event_id: string
  title: string
  event_type: policy | macro | earnings | guidance | capex | approval | clinical | price_spread | inventory | liquidity | company_action | developer_ecosystem | research_update | other
  event_class: market-facing | research-facing
  sector_mapping:
    - sector: string
      score: number
  asset_mapping:
    - code: string
      name: string
      market: string
      role: leader | bridge | local | validation | avoid
      score: number
  china_mapping_score: number
  tradability_score: number
  evidence_quality: number
  time_decay: number
  relevance_score: number
  owner: string
  watch_items:
    - string
  base_case: string
  bull_case: string
  bear_case: string
  expected_path:
    - from: string
      to: string
      relation: string
      expected_lag_days:
        min: integer
        max: integer
  invalidation:
    - string
  source:
    name: string
    url: string
    tier: T0 | T1 | T2 | T3
  ingest_time: datetime
  effective_time: datetime
  freshness: number
  staleness_reason: string
```

## Score Definitions

All weights must be configurable.

### tradability_score

Measures whether it can be acted on now.

Inputs:

- liquidity
- event proximity
- signal freshness
- crowding penalty
- clarity of invalidation
- bridge completeness

### actionability_score

Measures whether it is inside the action window.

Inputs:

- catalyst timing
- confirmation count
- lag window
- entry clarity
- next review time

### evidence_completeness

Measures whether the evidence chain is complete.

Inputs:

- lead signal
- bridge signal
- local validation
- flow confirmation
- sector evidence
- memory alignment

### freshness_score

Measures whether the signal is stale.

Inputs:

- time since trigger
- time since last validation
- event decay
- price already moved vs not yet moved

### noise_penalty

Measures whether the conclusion is inflated by weak events.

Inputs:

- source quality
- source duplication
- weak mapping
- non-tradable event contamination

## Configuration Contract

Recommended config path: `config/lead_lag_v2.example.yaml`.

Minimum keys:

```yaml
scoring:
  tradability_score:
    liquidity: 0.25
    event_proximity: 0.15
    signal_freshness: 0.20
    crowding_penalty: -0.20
    invalidation_clarity: 0.20
    bridge_completeness: 0.20
  actionability_score:
    catalyst_timing: 0.25
    confirmation_count: 0.25
    lag_window: 0.20
    entry_clarity: 0.15
    next_review_time: 0.15
  evidence_completeness:
    lead_signal: 0.20
    bridge_signal: 0.20
    local_validation: 0.20
    flow_confirmation: 0.15
    sector_evidence: 0.15
    memory_alignment: 0.10
  freshness_score:
    trigger_age: 0.30
    validation_age: 0.25
    event_decay: 0.25
    price_move_remaining: 0.20
  noise_penalty:
    weak_source: 0.30
    duplicate_source: 0.20
    weak_mapping: 0.30
    non_tradable_contamination: 0.20
thresholds:
  min_operator_tradability: 55
  min_operator_actionability: 55
  min_event_china_mapping: 45
  crowded_threshold: 75
```

The service may ship with safe fallback defaults, but diagnostics must disclose when fallback defaults are used.
