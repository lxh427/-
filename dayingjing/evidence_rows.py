from __future__ import annotations

import re
from typing import Any

from .text_utils import normalize_text


def _evidence_merge_key(text: str) -> str:
    value = normalize_text(text)
    if not value:
        return ""
    value = re.sub(r'["\'\[\]<>【】《》（）()]', "", value)
    value = re.sub(r"\d{6}(?:\.[A-Z]{2})?", "", value)
    value = re.sub(r"\d{4}[-/\u5e74]\d{1,2}[-/\u6708]\d{1,2}(?:\u65e5)?", "", value)
    value = re.sub(r"\d{1,2}:\d{2}", "", value)
    value = re.sub(r"\d+\s*/\s*\d+", "", value)
    value = re.sub(
        r"(?:\u516c\u544a\u663e\u793a|\u516c\u544a\u79f0|\u516c\u53f8\u516c\u544a|\u516c\u53f8\u8868\u793a|\u8bb0\u8005\u83b7\u6089|\u8bb0\u8005\u4e86\u89e3\u5230|\u8bb0\u8005\u6ce8\u610f\u5230|\u636e\u6089|\u5176\u4e2d|\u76ee\u524d|\u6b64\u524d|\u540c\u65f6|\u6b64\u5916)",
        "",
        value,
    )
    value = re.sub(r"[，。！？；：、,.!?;:\-\s]", "", value)
    return value.strip()


def _evidence_merge_keys_similar(left: str, right: str) -> bool:
    left_value = str(left).strip()
    right_value = str(right).strip()
    if not left_value or not right_value:
        return False
    if left_value == right_value:
        return True

    shorter, longer = (left_value, right_value) if len(left_value) <= len(right_value) else (right_value, left_value)
    if len(shorter) >= 12 and shorter in longer:
        return True

    prefix = shorter[: min(16, len(shorter))]
    suffix = shorter[max(0, len(shorter) - 12) :]
    return len(prefix) >= 10 and len(suffix) >= 8 and prefix in longer and suffix in longer


def _evidence_text_penalty(text: str) -> float:
    value = normalize_text(text)
    if not value:
        return float("inf")

    penalty = float(len(value))
    if re.search(r"\u8bc1\u5238\u4ee3\u7801|\u8bc1\u5238\u7b80\u79f0|\u516c\u544a\u7f16\u53f7|\u672c\u516c\u53f8\u8463\u4e8b\u4f1a|\u91cd\u5927\u9057\u6f0f|\u6295\u8d44\u98ce\u9669|\u7279\u6b64\u516c\u544a", value):
        penalty += 120
    if re.search(r"[；;:：]", value):
        penalty += 8
    if not re.search(r"[。！？!?]$", value):
        penalty += 4
    return penalty


def _prefer_evidence_text(current_text: str, candidate_text: str) -> bool:
    current_penalty = _evidence_text_penalty(current_text)
    candidate_penalty = _evidence_text_penalty(candidate_text)
    if candidate_penalty + 6 < current_penalty:
        return True
    if candidate_penalty == current_penalty:
        return len(normalize_text(candidate_text)) < len(normalize_text(current_text))
    return False


def _supporting_source_entry(event: dict[str, Any], item: dict[str, Any]) -> dict[str, str]:
    # 证据的来源快照，保持字段扁平，便于前端和导出直接使用。
    return {
        "published_at": str(item.get("publishedAt", "")).strip(),
        "source_type": str(item.get("sourceType", "")).strip(),
        "source_label": str(item.get("sourceLabel", "")).strip(),
        "source_site": str(item.get("sourceSite", "")).strip(),
        "event_title": str(event.get("eventTitle", "") or item.get("title", "")).strip(),
        "article_title": str(item.get("title", "")).strip(),
        "url": str(item.get("url", "")).strip(),
        "origin_url": str(item.get("originUrl", "")).strip(),
    }


def _supporting_source_key(payload: dict[str, str]) -> str:
    return "\u241f".join(
        [
            payload.get("source_label", ""),
            payload.get("source_site", ""),
            payload.get("article_title", ""),
            payload.get("url", ""),
            payload.get("origin_url", ""),
        ]
    ).lower()


def _merge_supporting_source(target: dict[str, dict[str, str]], event: dict[str, Any], item: dict[str, Any]) -> None:
    payload = _supporting_source_entry(event, item)
    merge_key = _supporting_source_key(payload)
    if not merge_key.strip("\u241f"):
        return

    existing = target.get(merge_key)
    if not existing:
        target[merge_key] = payload
        return

    if payload.get("published_at", "") > existing.get("published_at", ""):
        existing["published_at"] = payload["published_at"]

    for field in ("source_type", "source_label", "source_site", "event_title", "article_title", "url", "origin_url"):
        if not existing.get(field) and payload.get(field):
            existing[field] = payload[field]


def _ordered_unique_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _sorted_supporting_sources(source_map: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    # 优先按时间倒序，时间相同再按来源和标题稳定排序。
    return sorted(
        source_map.values(),
        key=lambda item: (
            str(item.get("published_at", "")),
            str(item.get("source_label", "")),
            str(item.get("article_title", "")),
            str(item.get("url", "")),
        ),
        reverse=True,
    )


def build_evidence_rows(profile: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # 按“证据文本”聚合，最终一行只代表一条证据，不代表一篇文章。
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        for item in event.get("items", []):
            if item.get("evidenceConfirmed") is False:
                continue
            for detail in item.get("evidenceDetails", []):
                text = str(detail.get("text", "")).strip()
                if not text:
                    continue
                merge_key = _evidence_merge_key(text)
                if not merge_key:
                    continue

                target_key = merge_key
                for existing_key in list(grouped.keys()):
                    if _evidence_merge_keys_similar(existing_key, merge_key):
                        target_key = existing_key
                        break

                # 同一条证据出现多次时，保留权重更高的版本做主记录。
                candidate_weight = (
                    int(item.get("relationTierRank", 0)) * 1000
                    + int(item.get("score", 0)) * 10
                    + int(detail.get("score", 0))
                )
                existing = grouped.get(target_key)
                if not existing:
                    grouped[target_key] = {
                        "mergeKey": merge_key,
                        "evidence_text": text,
                        "evidence_type": str(detail.get("type", "")).strip() or "fact",
                        "evidence_label": str(detail.get("label", "")).strip() or "clue",
                        "detail_score": int(detail.get("score", 0)),
                        "published_at": str(item.get("publishedAt", "")),
                        "score": int(item.get("score", 0)),
                        "signal_family": str(item.get("signalFamily", "")),
                        "relation_tier": str(item.get("relationTier", "")),
                        "relation_tier_label": str(item.get("relationTierLabel", "")),
                        "relation_tier_rank": int(item.get("relationTierRank", 0)),
                        "relation_reason": str(item.get("relationReason", "")),
                        "hit_mode": str(item.get("hitMode", "")),
                        "query_context": str(item.get("queryContext", "")),
                        "query_mode": str(item.get("queryMode", "")),
                        "source_type": str(item.get("sourceType", "")),
                        "source_label": str(item.get("sourceLabel", "")),
                        "source_site": str(item.get("sourceSite", "")),
                        "event_title": str(event.get("eventTitle", "") or item.get("title", "")),
                        "article_title": str(item.get("title", "")),
                        "url": str(item.get("url", "")),
                        "origin_url": str(item.get("originUrl", "")),
                        "evidence_confirmed": item.get("evidenceConfirmed", False),
                        "occurrence_count": 1,
                        "_weight": candidate_weight,
                        "_matched_entities": {str(value).strip() for value in item.get("matchedEntities", []) if str(value).strip()},
                        "_paths": {str(value).strip() for value in item.get("paths", []) if str(value).strip()},
                        "_reasons": {str(value).strip() for value in item.get("reasons", []) if str(value).strip()},
                        "_supporting_sources": {},
                    }
                    _merge_supporting_source(grouped[target_key]["_supporting_sources"], event, item)
                    continue

                existing["occurrence_count"] += 1
                existing["_matched_entities"].update(str(value).strip() for value in item.get("matchedEntities", []) if str(value).strip())
                existing["_paths"].update(str(value).strip() for value in item.get("paths", []) if str(value).strip())
                existing["_reasons"].update(str(value).strip() for value in item.get("reasons", []) if str(value).strip())
                _merge_supporting_source(existing["_supporting_sources"], event, item)

                if _prefer_evidence_text(str(existing.get("evidence_text", "")), text):
                    existing["evidence_text"] = text
                    existing["mergeKey"] = merge_key

                if candidate_weight > int(existing.get("_weight", 0)):
                    existing.update(
                        {
                            "evidence_type": str(detail.get("type", "")).strip() or existing["evidence_type"],
                            "evidence_label": str(detail.get("label", "")).strip() or existing["evidence_label"],
                            "detail_score": int(detail.get("score", 0)),
                            "published_at": str(item.get("publishedAt", "")) or existing["published_at"],
                            "score": int(item.get("score", 0)),
                            "signal_family": str(item.get("signalFamily", "")) or existing["signal_family"],
                            "relation_tier": str(item.get("relationTier", "")) or existing["relation_tier"],
                            "relation_tier_label": str(item.get("relationTierLabel", "")) or existing["relation_tier_label"],
                            "relation_tier_rank": int(item.get("relationTierRank", 0)),
                            "relation_reason": str(item.get("relationReason", "")) or existing["relation_reason"],
                            "hit_mode": str(item.get("hitMode", "")) or existing["hit_mode"],
                            "query_context": str(item.get("queryContext", "")) or existing["query_context"],
                            "query_mode": str(item.get("queryMode", "")) or existing["query_mode"],
                            "source_type": str(item.get("sourceType", "")) or existing["source_type"],
                            "source_label": str(item.get("sourceLabel", "")) or existing["source_label"],
                            "source_site": str(item.get("sourceSite", "")) or existing["source_site"],
                            "event_title": str(event.get("eventTitle", "") or item.get("title", "")) or existing["event_title"],
                            "article_title": str(item.get("title", "")) or existing["article_title"],
                            "url": str(item.get("url", "")) or existing["url"],
                            "origin_url": str(item.get("originUrl", "")) or existing["origin_url"],
                            "evidence_confirmed": item.get("evidenceConfirmed", False),
                            "_weight": candidate_weight,
                        }
                    )

    rows: list[dict[str, Any]] = []
    for index, entry in enumerate(
        sorted(grouped.values(), key=lambda item: (str(item.get("published_at", "")), int(item.get("_weight", 0))), reverse=True),
        start=1,
    ):
        supporting_sources = _sorted_supporting_sources(entry["_supporting_sources"])
        rows.append(
            {
                "row_id": index,
                "stock_code": profile["code"],
                "stock_name": profile["name"],
                "published_at": entry.get("published_at", ""),
                "evidence_text": entry.get("evidence_text", ""),
                "evidence_type": entry.get("evidence_type", ""),
                "evidence_label": entry.get("evidence_label", ""),
                "detail_score": entry.get("detail_score", 0),
                "signal_family": entry.get("signal_family", ""),
                "relation_tier": entry.get("relation_tier", ""),
                "relation_tier_label": entry.get("relation_tier_label", ""),
                "relation_tier_rank": entry.get("relation_tier_rank", 0),
                "relation_reason": entry.get("relation_reason", ""),
                "hit_mode": entry.get("hit_mode", ""),
                "query_context": entry.get("query_context", ""),
                "query_mode": entry.get("query_mode", ""),
                "score": entry.get("score", 0),
                "source_type": entry.get("source_type", ""),
                "source_label": entry.get("source_label", ""),
                "source_site": entry.get("source_site", ""),
                "event_title": entry.get("event_title", ""),
                "article_title": entry.get("article_title", ""),
                "occurrence_count": entry.get("occurrence_count", 1),
                "matched_entities": sorted(entry["_matched_entities"])[:10],
                "paths": sorted(entry["_paths"])[:6],
                "reasons": sorted(entry["_reasons"])[:8],
                "source_sites": _ordered_unique_strings([item["source_site"] for item in supporting_sources if item.get("source_site")]),
                "source_labels": _ordered_unique_strings([item["source_label"] for item in supporting_sources if item.get("source_label")]),
                "article_titles": _ordered_unique_strings([item["article_title"] for item in supporting_sources if item.get("article_title")]),
                "event_titles": _ordered_unique_strings([item["event_title"] for item in supporting_sources if item.get("event_title")]),
                "source_urls": _ordered_unique_strings([item["url"] for item in supporting_sources if item.get("url")]),
                "origin_urls": _ordered_unique_strings([item["origin_url"] for item in supporting_sources if item.get("origin_url")]),
                "supporting_sources": supporting_sources,
                "evidence_confirmed": entry.get("evidence_confirmed", False),
                "url": entry.get("url", ""),
                "origin_url": entry.get("origin_url", ""),
            }
        )
    return rows
