# Lead-Lag Alpha Engine V1 Stage Machine

## Purpose

The stage machine standardizes execution, review, and acceptance. It is the control plane Codex uses to judge a Bailian delivery.

## Stage Definitions

| Stage | Name | Owner | Exit Condition |
| --- | --- | --- | --- |
| S0 | Spec Locked | Codex | docs and contracts frozen for the batch |
| S1 | Bailian Build | Bailian | code batch delivered with self-check notes |
| S2 | Static Gate | CI | lint/type/tests baseline passes |
| S3 | Smoke Gate | CI + Codex | import/runtime smoke acceptable |
| S4 | Review | Codex | findings written, severity assigned |
| S5 | Fix Round | Bailian | requested fixes returned |
| S6 | Acceptance | Codex | approved for merge or deployment |
| S7 | Post-Deploy Observe | Codex + operator | no blocking regression after rollout |

## Allowed Transitions

- `S0 -> S1`
- `S1 -> S2`
- `S2 -> S3`
- `S3 -> S4`
- `S4 -> S5`
- `S5 -> S2`
- `S4 -> S6`
- `S6 -> S7`

## Rejection Conditions

- Contracts differ from the approved docs.
- Generated code lacks runnable evidence.
- CI fails on the declared baseline.
- Smoke exposes import, schema, or path regressions.
- Review finds blocking correctness or rollback risk.

## Logging Rule

Each stage transition must be logged in `docs/lead_lag_worklog.md` with:
- batch id
- owner
- date
- evidence
- result
- next action
