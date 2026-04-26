# Lead-Lag Alpha Engine V1 Architecture

## 1. Objective

Lead-Lag Alpha Engine V1 targets a narrow problem: detect leading cross-market signals, map them onto lagging tradable assets, and emit reviewable alpha candidates instead of free-form commentary.

## 2. Ownership Model

### Codex responsibilities
- Own system design, interface contracts, review comments, acceptance criteria, and CI gate definitions.
- Review every Bailian delivery against docs, tests, and rollback expectations.
- Sign off or reject batches in `docs/lead_lag_worklog.md`.

### Bailian responsibilities
- Generate large blocks of implementation code under the approved contracts.
- Fill adapters, schemas, ETL glue, and repetitive wiring tasks.
- Return delivery notes, known gaps, and self-check evidence for Codex review.

## 3. V1 System Boundaries

The engine is split into six layers:

1. Source Intake
   - Official, public market, search, and internal sources enter through typed collectors.
2. Normalization
   - Raw source payloads become canonical lead events, evidence items, and lag targets.
3. Transmission Graph
   - Events propagate through theme, supply-chain, ADR/H-share, ETF basket, and macro regime edges.
4. Candidate Scoring
   - The engine computes direction, strength, decay, timing window, and invalidation hints.
5. Review and Acceptance
   - Codex reviews Bailian-delivered behavior and approves release readiness.
6. Production Sync
   - Accepted outputs sync to Windows-serving APIs, dashboards, and downstream notes.

## 4. Core Runtime Objects

### Lead Event
- The earliest trusted signal.
- Examples: earnings surprise, FDA approval, macro print, sector ETF breakout, official filing.

### Transmission Edge
- A directed relationship connecting a lead event to a lagging market, sector, theme, or symbol.
- Carries edge type, confidence, decay rule, and invalidation rule.

### Alpha Candidate
- The scored output for downstream review.
- Must include signal source, mapped targets, rationale, timing window, and failure conditions.

## 5. Deployment Shape

- Aliyun node: scheduled collection, source probing, raw payload capture, and upload-ready snapshots.
- Windows node: primary API, operator-facing UI, local storage, and execution-oriented read path.
- Obsidian: long-lived memory for design decisions, acceptance notes, and postmortem summaries.

## 6. Engineering Rules for V1

- Search is evidence enrichment, not a primary trigger.
- Each stage must support deterministic replay from stored payloads.
- Every major Bailian batch must map back to a Codex-approved spec section.
- CI must protect import integrity, contract drift, and baseline smoke behavior.
