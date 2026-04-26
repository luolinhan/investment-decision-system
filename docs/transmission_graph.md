# Lead-Lag Alpha Engine V1 Transmission Graph

## Graph Purpose

The transmission graph explains how a lead signal propagates into lagging targets. V1 uses a directed multigraph so the same event can travel through multiple edge types.

## Node Types

- `lead_event`
- `macro_regime`
- `theme`
- `industry`
- `symbol`
- `basket`
- `review_gate`

## Edge Types

1. `direct_listing`
   - ADR, dual listing, A/H shared economics.
2. `supply_chain`
   - upstream/downstream relationships.
3. `peer_readthrough`
   - same product family or pricing anchor.
4. `theme_proxy`
   - ETF, index, or basket as theme transmitters.
5. `policy_transmission`
   - regulation or policy to beneficiaries / losers.
6. `macro_beta`
   - rate, liquidity, FX, and risk regime transmission.

## Edge Attributes

- `confidence`
- `expected_lag_hours`
- `direction_rule`
- `decay_rule`
- `evidence_required`
- `invalidation_rule`

## V1 Scoring Pass

Candidate score is a combination of:

- lead signal strength
- source tier weight
- edge confidence
- timing freshness
- regime alignment
- target liquidity fit
- invalidation penalty

## Review Requirement

Any new edge family proposed by Bailian must be reviewed by Codex before merge. The review must state:
- why the edge exists
- what evidence supports it
- what failure mode should invalidate it
