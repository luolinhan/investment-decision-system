# Lead-Lag Alpha Engine V1 Obsidian Integration

## Purpose

Obsidian stores durable engineering memory for this engine. It is where decisions, review outcomes, and postmortems remain queryable after a code drop.

## What Goes to Obsidian

- architecture decisions
- accepted or rejected Bailian batch summaries
- known failure modes
- deployment verification results
- follow-up tasks and research notes

## What Does Not Go to Obsidian

- secrets
- raw credentials
- tokens
- unredacted private endpoints

## Suggested Note Flow

1. Spec accepted by Codex
2. Bailian delivery linked with batch id
3. Codex review findings summarized
4. acceptance / rejection recorded
5. deployment verification and regressions appended

## Current V1 Integration

- runtime service: `app/services/obsidian_memory_service.py`
- lead-lag adapter: `app/services/lead_lag_service.py`
- fallback rule: if no vault path exists, Lead-Lag returns sample-data memory rows instead of failing the page

## V1 Naming Recommendation

- project context: `20-项目/Investment-Decision-System`
- execution log summary: `40-任务`
- verification and incidents: `90-日志`

The Markdown worklog in this repo is the working ledger. Obsidian is the durable memory mirror.
