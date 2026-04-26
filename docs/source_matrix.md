# Lead-Lag Alpha Engine V1 Source Matrix

## Source Tiers

| Tier | Meaning | Can Trigger Signal | Typical Free Examples |
| --- | --- | --- | --- |
| T0 | Official / primary disclosure | Yes | HKEX, SSE, SZSE, SEC EDGAR, FDA, CSRC, SFC, company IR, FRED, BEA, BLS, PBOC, China NBS |
| T1 | Public reliable confirmation / open indexes | Yes, with rules | ClinicalTrials.gov, arXiv, PubMed, Semantic Scholar, Yahoo Finance, AkShare, EIA, IEA public reports |
| T2 | Search enrichment / discovery | No, evidence only | Aliyun search-proxy, official-site search, regulator and exchange news pages |
| T3 | Internal memory / operator notes | No, context only | Obsidian notes, review worklog, postmortems |

## Free Reliable Source Layers

### T0 Official Exchanges

Use official exchange sources for issuer announcements, trading calendars, market notices, north/southbound flow references, derivatives statistics, and listing-rule events.

Free source examples:

- HKEX / HKEXnews: Hong Kong issuer announcements and exchange notices.
- SSE: Shanghai listed-company announcements and exchange notices.
- SZSE: Shenzhen listed-company announcements and exchange notices.
- Cboe public market statistics: options and volatility market structure references.

### T0 Regulators

Use regulator sources for filings, approvals, rule changes, enforcement, clinical/regulatory milestones, and policy changes.

Free source examples:

- SEC EDGAR and SEC newsroom.
- FDA press announcements and product/regulatory pages.
- CSRC public disclosures.
- Hong Kong SFC news and announcements.

### T0 Company Announcements

Use company sources for IR releases, earnings decks, product milestones, production guidance, tender wins, supply-chain disclosures, and exchange-filed notices.

Free source examples:

- Company investor-relations pages.
- Exchange-hosted issuer announcements.
- Official press rooms and product update pages.

### T0 Macro Official

Use official macro sources for growth, inflation, labor, fiscal, credit, central-bank, customs, inventory, and energy inputs that can drive sector lead-lag chains.

Free source examples:

- FRED, BEA, BLS, EIA.
- PBOC, China NBS, official customs/statistics pages.

### T1 Public Research Indexes

Use open indexes for technology diffusion, healthcare catalysts, scientific validation, and public prior-art checks. These sources can confirm a thesis but should not override T0 official evidence.

Free source examples:

- arXiv and Semantic Scholar for public research discovery.
- PubMed and ClinicalTrials.gov for biomedical research and trial status.
- Public industry association reports where source URL, publication date, and license note are retained.

## V1 Source Rules

- T0 is preferred whenever available.
- T1 can trigger when numeric facts are directly verifiable.
- T2 cannot independently open a trade candidate.
- T3 supports review and memory, not market truth.
- Any adapter that uses a non-official mirror must keep the original official URL in evidence.
- Search snippets are discovery aids only; final evidence must be fetched from the canonical page when possible.
- No source matrix file may contain real API keys, cookies, private tokens, or webhook URLs.

## Required Fields Per Source Record

- `source_tier`
- `source_class`
- `source_name`
- `source_url`
- `fetched_at`
- `content_hash`
- `trace_id`
- `parser_version`
- `license_note`

## Front-Loaded Collection and Translation

Windows production should run collection and translation before Lead-Lag review windows so the UI consumes already-normalized, bilingual evidence.

Recommended command:

```powershell
.\scripts\lead_lag_pretranslate_task.ps1 -TranslationLimit 100 -ShortlineLimit 20
```

Current production cadence uses the Windows `LeadLagPretranslate` scheduled task every 5 hours with `-TranslationLimit 300 -SkipShortline`. Shortline translation stays out of this cadence until its SQLite lock behavior is fixed.

Lead-Lag live adapters apply a first-pass quality gate before scoring: the row must have a public source URL, match the free trusted-domain matrix or an official/regulatory verification status, pass confidence thresholds, and avoid obvious future-dated evidence. Source health exposes this as `quality_filter=free_public_reliable`.

The task runs:

- `scripts\sync_intelligence.py` for existing Intelligence source collection.
- `scripts\translate_intelligence.py` for Bailian-backed bilingual completion, falling back safely when `BAILIAN_API_KEY` is absent.
- `scripts\translate_shortline_events.py` for T0 shortline event translation.
- `scripts\lead_lag_aliyun_collector.py` for a Lead-Lag snapshot handoff.

Secrets must live in `.env.local`, `.env`, or the Windows Task Scheduler environment. The example config uses placeholders only.

## Review Contract

When Bailian adds a new source adapter, Codex checks:
- trigger eligibility
- refresh expectation
- replayability
- failure mode
- data licensing / operational risk
- secret handling and log redaction
- translation behavior when `BAILIAN_API_KEY` is missing

## Current V1 Bundle

The runnable V1 demo currently ships as `sample_data/lead_lag/lead_lag_v1.json` and mirrors this source matrix in deterministic sample form before live adapters are turned on.
