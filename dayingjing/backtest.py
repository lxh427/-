from __future__ import annotations

from typing import Any


BACKTEST_EXPORT_COLUMNS = [
    {"key": "stock_code", "label": "Stock Code", "type": "string"},
    {"key": "stock_name", "label": "Stock Name", "type": "string"},
    {"key": "published_at", "label": "Published At", "type": "datetime"},
    {"key": "evidence_text", "label": "Evidence Text", "type": "string"},
    {"key": "evidence_type", "label": "Evidence Type", "type": "string"},
    {"key": "query_mode", "label": "Query Mode", "type": "string"},
    {"key": "query_context", "label": "Query Context", "type": "string"},
    {"key": "relation_tier", "label": "Relation Tier", "type": "string"},
    {"key": "relation_reason", "label": "Relation Reason", "type": "string"},
    {"key": "signal_family", "label": "Signal Family", "type": "string"},
    {"key": "score", "label": "Evidence Score", "type": "number"},
    {"key": "occurrence_count", "label": "Occurrence Count", "type": "number"},
    {"key": "source_label", "label": "Primary Source", "type": "string"},
    {"key": "source_site", "label": "Source Site", "type": "string"},
    {"key": "path_1", "label": "Path 1", "type": "string"},
    {"key": "path_2", "label": "Path 2", "type": "string"},
    {"key": "path_3", "label": "Path 3", "type": "string"},
    {"key": "reason_1", "label": "Reason 1", "type": "string"},
    {"key": "reason_2", "label": "Reason 2", "type": "string"},
    {"key": "reason_3", "label": "Reason 3", "type": "string"},
    {"key": "entity_1", "label": "Entity 1", "type": "string"},
    {"key": "entity_2", "label": "Entity 2", "type": "string"},
    {"key": "entity_3", "label": "Entity 3", "type": "string"},
    {"key": "event_title_1", "label": "Event 1", "type": "string"},
    {"key": "event_title_2", "label": "Event 2", "type": "string"},
    {"key": "article_title_1", "label": "Article 1", "type": "string"},
    {"key": "article_title_2", "label": "Article 2", "type": "string"},
    {"key": "url", "label": "URL", "type": "string"},
]


def backtest_module_status() -> dict[str, Any]:
    return {
        "enabled": False,
        "status": "reserved",
        "owner": "teammate",
        "message": "Backtest module is reserved for a teammate and currently exposes only a schema shell.",
    }


def backtest_schema_payload() -> dict[str, Any]:
    return {
        "ok": True,
        "module": backtest_module_status(),
        "schemaVersion": "2026-04-05-backtest-reserved-v2",
        "inputFormat": "evidence_csv",
        "columns": BACKTEST_EXPORT_COLUMNS,
        "notes": [
            "This project currently focuses on evidence collection and structuring, not prediction output.",
            "The future backtest module should consume evidence-level data instead of full articles.",
            "Only fields that still exist in the evidence pipeline are kept in this schema.",
        ],
    }
