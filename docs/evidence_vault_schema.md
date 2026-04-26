# Evidence Vault Schema

V3 使用 SQLite + FTS5 管理证据库，原始文件同时落地本地归档目录。

## 表

### source_catalog

- `source_id`
- `source_name`
- `source_type`: `official | exchange | filing | company_ir | news | report | user_note | generated`
- `domain`
- `reliability_tier`
- `default_weight`
- `enabled`

### source_documents

- `document_id`
- `source_id`
- `title`
- `canonical_url`
- `original_url`
- `local_archive_path`
- `content_type`
- `language`
- `published_at`
- `fetched_at`
- `author_org`
- `checksum`
- `content_hash`
- `parser_version`
- `parse_status`
- `extraction_quality`
- `summary`
- `raw_text`
- `markdown_text`
- `data_source_class`

### source_chunks

- `chunk_id`
- `document_id`
- `chunk_index`
- `text`
- `token_estimate`
- `entity_tags`
- `sector_tags`

### citations

- `citation_id`
- `document_id`
- `quote_text`
- `normalized_fact`
- `page_or_section`
- `used_by_object_type`
- `used_by_object_id`

### extracted_facts

- `fact_id`
- `document_id`
- `fact_type`
- `entity_id`
- `metric_name`
- `metric_value`
- `metric_unit`
- `metric_date`
- `confidence`
- `extraction_method`

### archived_links

- `link_id`
- `document_id`
- `url`
- `link_type`
- `anchor_text`
- `archived_status`
- `last_checked_at`

### reports

- `report_id`
- `report_type`
- `title`
- `local_path`
- `generated_at`
- `as_of_date`
- `related_entities`
- `related_sectors`
- `version`
- `content_hash`

### report_sections

- `section_id`
- `report_id`
- `section_title`
- `body_markdown`
- `linked_citations`
- `linked_opportunities`

## FTS

- `source_documents_fts(title, summary, raw_text, markdown_text)`
- `reports_fts(title, body)`

## 目录

- `data/archive/html/`
- `data/archive/pdf/`
- `data/archive/text/`
- `data/archive/reports/`
- `data/archive/screenshots/`

## 迁移策略

1. 创建 schema。
2. 从 `raw_documents` 回填 `source_documents`，raw_text 写入 `data/archive/text/`。
3. 从 `research_reports` 回填 `source_documents` 和 `reports`。
4. 对每个文档生成 chunk。
5. 对已有 `research_evidence/event_facts` 生成 citation 和 extracted_fact。

