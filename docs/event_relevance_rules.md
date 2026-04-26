# Event Relevance Rules

Audit date: 2026-04-26

V2 treats events as catalysts, not as a news pile. Every event must be classified, mapped, scored, and linked to a possible transmission path or explicitly downgraded.

## Event Classes

### Market-Facing

Displayed by default in Operator Mode.

An event is market-facing when it has at least one of:

- Direct mapping to A-share, HK, or China ADR tradable assets.
- Clear China sector mapping with a plausible lead-lag path.
- Official policy, macro, regulatory, exchange, issuer, earnings, approval, price, inventory, liquidity, or capital flow impact.
- Near-term catalyst timing that can change actionability or invalidation.

### Research-Facing

Hidden from the default Operator event stream, available in Builder Mode or optional toggle.

An event is research-facing when:

- It is useful background but has weak China tradability.
- It relates to developer ecosystem, model benchmarks, academic research, or tooling without direct asset mapping.
- It may become market-facing later after bridge evidence appears.

## Default Filtering

Operator Mode default event stream:

- Include only `event_class = market-facing`.
- Require `china_mapping_score >= configured threshold`.
- Require at least one mapped sector or asset.
- Require source URL and source tier.
- Sort by `relevance_score`, then `effective_time`, then `evidence_quality`.

Events below threshold go to research-facing or raw event view.

## Developer Ecosystem Noise Rule

Developer events are not automatically market-facing.

Examples that default to research-facing:

- SDK release.
- GitHub repository trend.
- Framework version update.
- Benchmark leaderboard update without asset linkage.
- Open-source model release without hardware, cloud, capex, or listed-company read-through.

They may be promoted to market-facing only if:

- They materially affect a mapped China AI chain node.
- There is a leader/bridge/local asset path.
- The event changes capex, demand, supply, pricing, or approval probability.
- Evidence quality is sufficient and not duplicated.

## Scoring Fields

### china_mapping_score

Inputs:

- Direct asset mapping.
- Sector mapping confidence.
- A/H/ADR bridge completeness.
- Relevance to China demand, supply, policy, liquidity, or valuation.

### tradability_score

Inputs:

- Mapped assets have name/code/market.
- Liquidity is acceptable.
- Time window is actionable.
- Catalyst is not stale.
- Invalidation is clear.

### evidence_quality

Inputs:

- Source tier.
- Official or issuer source bonus.
- Multiple independent sources.
- Recency.
- Parser confidence.
- Content hash and canonical URL availability.

### time_decay

Inputs:

- Event age.
- Expected catalyst half-life.
- Whether validation already occurred.
- Whether price has already moved beyond normal lag window.

### relevance_score

Suggested formula:

```text
relevance_score =
  0.30 * china_mapping_score +
  0.25 * tradability_score +
  0.25 * evidence_quality +
  0.20 * time_decay -
  noise_penalty
```

Weights must be configurable.

## Required Event Output

Each event sent to Operator Mode must include:

- `event_id`
- `title`
- `event_type`
- `event_class`
- `sector_mapping`
- `asset_mapping`
- `china_mapping_score`
- `tradability_score`
- `evidence_quality`
- `time_decay`
- `relevance_score`
- `owner`
- `watch_items`
- `base_case`
- `bull_case`
- `bear_case`
- `expected_path`
- `invalidation`
- `source`
- `ingest_time`
- `effective_time`
- `freshness`
- `staleness_reason`

## Event Type Rules

Policy:

- Market-facing when official policy, fiscal, credit, regulation, approval, or enforcement changes a sector's expected earnings or risk appetite.

Macro:

- Market-facing when it affects China style mapping, liquidity regime, or high-beta risk budget.

Earnings / Guidance:

- Market-facing when a leader asset or bridge asset has mapped China supply chain or demand read-through.

Capex:

- Market-facing when customer spending affects listed suppliers, equipment, components, or cloud/IDC chain nodes.

Clinical / Approval:

- Market-facing when mapped to China innovative drug, CXO, CRO, upstream research service, or commercialization assets.

Price / Spread:

- Market-facing when commodity, product, freight, or component spreads affect margin of listed sectors.

Inventory / Utilization:

- Market-facing when inventory or utilization state changes cycle stage or validation.

Liquidity:

- Market-facing when DXY, US yields, VIX, CNH, HK short selling, southbound, A/H premium, or Stock Connect changes risk budget.

Research Update:

- Research-facing by default unless it changes a mapped opportunity's evidence completeness or invalidation.

Developer Ecosystem:

- Research-facing by default unless promoted by the noise rule above.

## Promotion and Demotion

Promote to market-facing when:

- `china_mapping_score` crosses threshold.
- At least one local asset or proxy asset is mapped.
- Expected path has a plausible lag window.
- There is a concrete watch item and invalidation.

Demote to research-facing when:

- Mapping is weak or generic.
- Event is stale.
- Source is duplicated.
- It has no tradable China asset path.
- It cannot alter actionability, tradability, invalidation, or evidence completeness.

## UI Rules

Operator Mode:

- Show market-facing only by default.
- Show sector, mapped assets, expected path, watch items, invalidation, and confidence.
- Do not show empty Impact/Watch fields.

Builder Mode:

- Can show research-facing and raw events.
- Must mark why an event was not promoted.
