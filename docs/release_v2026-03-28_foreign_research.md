# Investment Hub Release `v2026.03.28-foreign-research`

Release date: `2026-03-28`

## Included Changes

1. Overseas research module
- Added `/foreign-research/` page and APIs.
- Added source registry, document archive, analysis summary, and cleanup workflow.

2. Windows-first storage
- Raw documents are stored under `data/foreign_research/`.
- Alibaba Cloud is used as a relay only.
- Retention window is limited to 180 days.

3. Bailian integration
- Added OpenAI-compatible Bailian analysis flow.
- Supports translation, summarization, stance classification, and structured extraction.
- Falls back to rule-based analysis when API keys are not configured.

4. CLI sync tool
- Added `foreign_research_sync.py` for crawling, ingesting, analyzing, and cleanup.
- Suitable for scheduled jobs on Aliyun or Windows.

## Deployment Notes

- The new module is registered in the main FastAPI app.
- The UI is linked from the main sidebar.
- Recommended next step is to configure real source URLs and legal access credentials for authorized portals.

