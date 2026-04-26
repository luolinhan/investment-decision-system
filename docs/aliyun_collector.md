# Lead-Lag Alpha Engine V1 Aliyun Collector

## Purpose

Aliyun is the remote collection and probing node. It should gather raw lead indicators efficiently without becoming the serving node.

## Responsibilities

- run scheduled collectors
- fetch T0/T1 source payloads
- call search-proxy for T2 enrichment when needed
- persist raw snapshots or upload-ready payloads
- emit health and freshness metadata
- preserve canonical official URLs and content hashes for replay

## Non-Responsibilities

- no operator-facing primary UI
- no final acceptance authority
- no direct production truth override over Windows local validated data
- no storage of API keys, cookies, tokens, or webhook secrets in repo files or collector snapshots

## V1 Collector Outputs

- raw payload snapshot
- normalized event preview
- source metadata
- fetch errors
- freshness timestamps

Current V1 script:

- `python scripts/lead_lag_aliyun_collector.py --output data/lead_lag/collector_snapshot.json --pretty`

The current collector is sample-data-first and snapshot-oriented. It is intentionally safe to run on Aliyun before live adapters are attached.

## High-Quality Free Source Priority

Collector adapters should be added in this order:

1. T0 official exchange and issuer disclosures: HKEX/HKEXnews, SSE, SZSE, official company IR pages.
2. T0 regulator and official macro feeds: SEC, FDA, CSRC, SFC, FRED, BEA, BLS, EIA, PBOC, China NBS.
3. T1 public research indexes: ClinicalTrials.gov, PubMed, arXiv, Semantic Scholar, public industry reports.
4. T2 search enrichment: Aliyun search-proxy for discovery only, followed by canonical-page fetch when possible.

T0/T1 payloads must include `source_tier`, `source_class`, canonical `source_url`, `fetched_at`, `content_hash`, `trace_id`, `parser_version`, and `license_note`.

## Windows Pre-Collection / Translation Handoff

Windows remains the serving and primary validation runtime. Aliyun may export snapshots, but Windows should front-load bilingual completion before operators review Lead-Lag candidates:

```powershell
.\scripts\lead_lag_pretranslate_task.ps1 -TranslationLimit 100 -ShortlineLimit 20
```

This wrapper calls the existing intelligence collector and translators without changing `scripts/sync_intelligence.py`. It uses a lock directory under `tmp\`, writes logs under `logs\lead-lag-pretranslate.log`, and treats missing `BAILIAN_API_KEY` as fallback mode. Real secrets must be provided through `.env.local`, `.env`, or Windows Task Scheduler environment variables, never through committed scripts or YAML examples.

## Collaboration Rule

- Codex defines collector contract and review checklist.
- Bailian can generate adapter and scheduler code in bulk.
- Any collector batch must include a source inventory and retry behavior note for Codex review.
