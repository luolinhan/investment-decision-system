# Lead-Lag Alpha Engine V1 Model Families

## Purpose

V1 does not depend on a single monolithic model. It uses model families by task, with Codex deciding suitability and Bailian implementing the surrounding code paths.

## Family A - Extraction Models

### Job
- Extract structured facts from filings, disclosures, official notices, and research text.

### Output
- `event_type`
- `entities`
- `direction`
- `magnitude_hint`
- `effective_time`
- `evidence_quotes`

### Ownership
- Codex defines schema and review rubric.
- Bailian generates parser glue and batch extraction pipelines.

## Family B - Mapping Models

### Job
- Map lead symbols/themes to lagging China, HK, or US tradables.

### Output
- `edge_type`
- `target_symbol`
- `mapping_confidence`
- `theme_tags`
- `invalidation_hint`

### Constraints
- Must preserve source provenance.
- Cannot invent unsupported mappings without evidence.

## Family C - Summary Models

### Job
- Summarize a candidate for operator review and Obsidian memory.

### Output
- Chinese title
- concise rationale
- review checklist
- risk / invalidation summary

### Constraints
- Summary is downstream presentation only.
- It cannot override source facts or score outputs.

## Family D - Review Models

### Job
- Assist Codex in triaging Bailian deliveries, regression notes, and defect clustering.

### Output
- structured review findings
- acceptance risk tags
- suggested fix groups

## Selection Principles

- Use deterministic code first, models second.
- Use official-source extraction before public-news summarization.
- Keep prompts versioned and traceable in the worklog when they affect acceptance.
