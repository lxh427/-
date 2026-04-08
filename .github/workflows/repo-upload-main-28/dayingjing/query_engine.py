from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

from .articles import _summary_article, get_article_content
from .config import (
    CACHE_SCHEMA_VERSION,
    FAST_CONFIRMED_FETCH_LIMIT,
    MACRO_TERMS,
    PRIORITIZED_ITEM_LIMIT,
    QUERY_CACHE_MINUTES,
    enabled_source_types,
    source_setting,
)
from .evidence_rows import build_evidence_rows
from .event_builder import build_events
from .scoring import event_key, overview_payload, preliminary_score, score_item, source_route_fit_score
from .sources import collect_candidate_items
from .text_utils import evidence_details, evidence_profile, title_dedup_key

def query_cache_key(profile: dict[str, Any], from_date: str, to_date: str, sensitivity: str, detail_level: str) -> str:
    return f"{CACHE_SCHEMA_VERSION}|{profile['code']}|{from_date}|{to_date}|{sensitivity}|{detail_level}"


def candidate_cache_key(profile: dict[str, Any], from_date: str, to_date: str) -> str:
    return f"{CACHE_SCHEMA_VERSION}|{profile['code']}|{from_date}|{to_date}|candidates"


def _route_fit_adjustment(base_value: int, fit_score: int, floor: int = 0) -> int:
    # 路由越匹配，来源优先级越高；明显不匹配则下调。
    if fit_score >= 8:
        return base_value + 2
    if fit_score >= 4:
        return base_value + 1
    if fit_score <= -6:
        return max(floor, base_value - 2)
    if fit_score <= -3:
        return max(floor, base_value - 1)
    return base_value


def source_rank(source_type: str, profile: dict[str, Any] | None = None) -> int:
    base_rank = int(source_setting(source_type, "rank", 0) or 0)
    if not profile:
        return base_rank
    return _route_fit_adjustment(base_rank, source_route_fit_score(profile, source_type))


def source_priority_cap(source_type: str, profile: dict[str, Any] | None = None) -> int:
    base_cap = int(source_setting(source_type, "priorityCap", 0) or 0)
    if not profile:
        return base_cap
    return _route_fit_adjustment(base_cap, source_route_fit_score(profile, source_type), floor=1)


def is_official_source(item: dict[str, Any]) -> bool:
    source_type = str(item.get("sourceType", ""))
    source_site = str(item.get("sourceSite", "")).lower()
    url = str(item.get("url", "")).lower()
    return (
        bool(source_setting(source_type, "isOfficial", False))
        or any(host in f"{source_site} {url}" for host in ("gov.cn", "cninfo.com.cn", "sse.com.cn", "szse.cn", "sseinfo.com"))
    )


def dedupe_candidates(raw_items: list[dict[str, Any]], profile: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in raw_items:
        key = title_dedup_key(item.get("title", "")) or f"{item.get('sourceType', '')}|{item.get('url', '')}"
        existing = grouped.get(key)
        if not existing:
            grouped[key] = item
            continue
        current_tuple = (
            source_rank(str(item.get("sourceType", "")), profile),
            len(str(item.get("summary", ""))),
            str(item.get("publishedAt", "")),
        )
        existing_tuple = (
            source_rank(str(existing.get("sourceType", "")), profile),
            len(str(existing.get("summary", ""))),
            str(existing.get("publishedAt", "")),
        )
        if current_tuple > existing_tuple:
            grouped[key] = item
    return list(grouped.values())


def prioritize_candidates(profile: dict[str, Any], items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for item in items:
        enriched = dict(item)
        enriched["preScore"] = preliminary_score(profile, enriched)
        ranked.append(enriched)

    ranked.sort(
        key=lambda item: (
            -int(item["preScore"]),
            -source_rank(str(item.get("sourceType", "")), profile),
            str(item.get("publishedAt", "")),
        )
    )

    selected: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for source_type in enabled_source_types():
        source_items = [item for item in ranked if item.get("sourceType") == source_type][: source_priority_cap(source_type, profile)]
        for item in source_items:
            unique_key = f"{item.get('sourceType', '')}|{item.get('url', '')}|{title_dedup_key(item.get('title', ''))}"
            if unique_key not in seen_keys:
                seen_keys.add(unique_key)
                selected.append(item)

    for item in ranked:
        if len(selected) >= PRIORITIZED_ITEM_LIMIT:
            break
        unique_key = f"{item.get('sourceType', '')}|{item.get('url', '')}|{title_dedup_key(item.get('title', ''))}"
        if unique_key not in seen_keys:
            seen_keys.add(unique_key)
            selected.append(item)
    return selected[:PRIORITIZED_ITEM_LIMIT]


def _fetch_threshold(sensitivity: str) -> int:
    return {"explore": 12, "balanced": 16, "integrated": 14}.get(sensitivity, 14)


def _response_threshold(sensitivity: str) -> int:
    return {"strict": 60, "explore": 22, "balanced": 36, "integrated": 26}.get(sensitivity, 26)


def plan_article_fetches(profile: dict[str, Any], items: list[dict[str, Any]], sensitivity: str, detail_level: str) -> list[dict[str, Any]]:
    # 先决定哪些候选值得抓正文，避免慢源把整体查询拖死。
    prefer_summary_only = detail_level == "fast"
    article_fetch_threshold = _fetch_threshold(sensitivity)
    source_deep_fetch_counts: dict[str, int] = {}
    fast_source_deep_fetch_counts: dict[str, int] = {}
    fast_confirmed_fetches = 0

    processing_items = list(items)
    if prefer_summary_only:
        source_leads = []
        seen_sources: set[str] = set()
        for item in sorted(
            items,
            key=lambda entry: (
                -source_rank(str(entry.get("sourceType", "")), profile),
                -int(entry.get("preScore", 0)),
                str(entry.get("publishedAt", "")),
            ),
        ):
            source_type = str(item.get("sourceType", ""))
            if source_type not in seen_sources:
                seen_sources.add(source_type)
                source_leads.append(item)
        fast_mix: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for candidate in source_leads + list(items):
            key = f"{candidate.get('sourceType', '')}|{candidate.get('url', '')}|{title_dedup_key(candidate.get('title', ''))}"
            if key not in seen_keys:
                seen_keys.add(key)
                fast_mix.append(candidate)
        processing_items = fast_mix[:PRIORITIZED_ITEM_LIMIT]

    planned: list[dict[str, Any]] = []
    for item in processing_items:
        source_type = str(item.get("sourceType", ""))
        deep_fetch_cap = int(source_setting(source_type, "deepFetchCap", 0) or 0)
        fast_deep_fetch_cap = int(source_setting(source_type, "fastDeepFetchCap", 0) or 0)
        current_deep_fetch_count = source_deep_fetch_counts.get(source_type, 0)
        current_fast_deep_fetch_count = fast_source_deep_fetch_counts.get(source_type, 0)
        must_deep_fetch = (
            is_official_source(item)
            or source_type in {"cninfoAnnouncement", "bulletin"}
            or item.get("queryMode") == "scenario"
            or int(item.get("preScore", 0)) >= article_fetch_threshold + 10
        )
        use_fast_confirmed_fetch = (
            prefer_summary_only
            and fast_confirmed_fetches < FAST_CONFIRMED_FETCH_LIMIT
            and (must_deep_fetch or int(item.get("preScore", 0)) >= article_fetch_threshold + 12)
            and (fast_deep_fetch_cap <= 0 or current_fast_deep_fetch_count < fast_deep_fetch_cap)
        )
        should_deep_fetch_now = True if not prefer_summary_only else use_fast_confirmed_fetch

        planned_item = dict(item)
        planned_item["shouldDeepFetchNow"] = should_deep_fetch_now
        planned.append(planned_item)

        if should_deep_fetch_now and prefer_summary_only:
            if deep_fetch_cap > 0:
                source_deep_fetch_counts[source_type] = current_deep_fetch_count + 1
            fast_source_deep_fetch_counts[source_type] = current_fast_deep_fetch_count + 1
            fast_confirmed_fetches += 1
    return planned


def _attach_article_content(
    planned_items: list[dict[str, Any]],
    profile: dict[str, Any],
    cache_store: Any,
    http_client: Any,
    allow_slow_pdf_fallback: bool = True,
) -> list[dict[str, Any]]:
    enriched_items: list[dict[str, Any]] = [dict(item) for item in planned_items]
    fetch_groups: dict[str, list[dict[str, Any]]] = {}
    for item in enriched_items:
        if item.get("shouldDeepFetchNow") and str(item.get("url", "")).strip():
            fetch_groups.setdefault(str(item.get("url", "")).strip(), []).append(item)
    max_workers = min(24, max(6, len(fetch_groups) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[Any, list[dict[str, Any]]] = {}
        for url, grouped_items in fetch_groups.items():
            future = executor.submit(
                get_article_content,
                url,
                profile,
                MACRO_TERMS,
                cache_store,
                http_client,
                "",
                allow_slow_pdf_fallback,
            )
            futures[future] = grouped_items

        for item in enriched_items:
            if not item.get("shouldDeepFetchNow") or not str(item.get("url", "")).strip():
                summary_text = str(item.get("summary") or item.get("title") or "")
                item["article"] = _summary_article(summary_text)

        for future in as_completed(futures):
            try:
                article = future.result() or {}
            except Exception:
                article = {}
            for item in futures[future]:
                if str(article.get("contentMode", "")) == "fulltext":
                    item["article"] = dict(article)
                else:
                    item["article"] = _summary_article(str(item.get("summary") or item.get("title") or ""))
    return enriched_items


def _scored_items(
    items: list[dict[str, Any]],
    profile: dict[str, Any],
    sensitivity: str,
) -> tuple[list[dict[str, Any]], int]:
    threshold = _response_threshold(sensitivity)
    confirmed_items: list[dict[str, Any]] = []
    pending_qualified_items = 0
    for item in items:
        article = item.get("article") or _summary_article(str(item.get("summary") or item.get("title") or ""))
        content_mode = str(article.get("contentMode", "fallback"))
        evidence_confirmed = content_mode == "fulltext"
        evidence_text = str(article.get("effectiveText") or article.get("rawText") or "")
        if article.get("rawText") and article.get("effectiveText") and article.get("rawText") != article.get("effectiveText"):
            evidence_text = f"{article.get('effectiveText')}\n{article.get('rawText')}"
        item["rawText"] = article.get("rawText", "")
        item["effectiveText"] = article.get("effectiveText", "")
        item["contentMode"] = content_mode
        item["evidenceConfirmed"] = evidence_confirmed
        item["evidenceDetails"] = evidence_details(profile, evidence_text, take=6, title_context=str(item.get("title", "")))
        item["evidenceBullets"] = [detail["text"] for detail in item["evidenceDetails"]]
        item["evidenceProfile"] = evidence_profile(item["evidenceDetails"])
        scored = score_item(profile, item)
        item.update(scored)
        item["eventId"] = event_key(profile, item)
        if int(item.get("score", 0)) >= threshold and evidence_confirmed:
            confirmed_items.append(item)
        elif int(item.get("score", 0)) >= threshold:
            pending_qualified_items += 1
    return confirmed_items, pending_qualified_items



def _used_source_labels(scored_items: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    for item in scored_items:
        source_type = str(item.get("sourceType", ""))
        label = str(item.get("sourceLabel", "")).strip() or str(source_setting(source_type, "label", "")).strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def _candidate_cache_payload(prioritized: list[dict[str, Any]], raw_count: int, deduped_count: int) -> dict[str, Any]:
    return {
        "prioritized": prioritized,
        "rawCount": raw_count,
        "dedupedCount": deduped_count,
    }


def _evidence_relation_counts(evidence_rows: list[dict[str, Any]]) -> tuple[int, int]:
    # 统计硬关联/映射关联，供前端摘要卡直接使用。
    direct_related = sum(1 for row in evidence_rows if row.get("relation_tier") in {"hard", "semi"})
    mapped_related = sum(
        1
        for row in evidence_rows
        if row.get("relation_tier") in {"mapped", "soft"} or row.get("signal_family") == "narrative"
    )
    return direct_related, mapped_related


class QueryEngine:
    def __init__(self, cache_store: Any, http_client: Any) -> None:
        self.cache_store = cache_store
        self.http_client = http_client

    def get_cached_response(self, key: str) -> dict[str, Any] | None:
        cached = self.cache_store.get("query", key)
        return cached if isinstance(cached, dict) else None

    def set_cached_response(self, key: str, payload: dict[str, Any]) -> None:
        self.cache_store.set("query", key, payload, datetime.now() + timedelta(minutes=QUERY_CACHE_MINUTES))

    def get_cached_candidates(self, key: str) -> tuple[list[dict[str, Any]], int, int] | None:
        cached = self.cache_store.get("candidate", key)
        if not isinstance(cached, dict) or not cached.get("prioritized"):
            return None
        prioritized = list(cached["prioritized"])
        raw_count = int(cached.get("rawCount", len(prioritized)))
        deduped_count = int(cached.get("dedupedCount", len(prioritized)))
        return prioritized, raw_count, deduped_count

    def set_cached_candidates(self, key: str, prioritized: list[dict[str, Any]], raw_count: int, deduped_count: int) -> None:
        self.cache_store.set(
            "candidate",
            key,
            _candidate_cache_payload(prioritized, raw_count, deduped_count),
            datetime.now() + timedelta(minutes=QUERY_CACHE_MINUTES),
        )

    def build_query_response(self, profile: dict[str, Any], from_date: str, to_date: str, sensitivity: str, detail_level: str = "full") -> dict[str, Any]:
        # 先取候选缓存，再做正文抓取和证据抽取，避免重复抓取慢源。
        candidate_key = candidate_cache_key(profile, from_date, to_date)
        cached_candidates = self.get_cached_candidates(candidate_key)
        if cached_candidates:
            prioritized, raw_candidate_count, deduped_candidate_count = cached_candidates
        else:
            raw_items = collect_candidate_items(profile, from_date, to_date, self.cache_store, self.http_client)
            deduped_items = dedupe_candidates(raw_items, profile)
            prioritized = prioritize_candidates(profile, deduped_items)
            raw_candidate_count = len(raw_items)
            deduped_candidate_count = len(deduped_items)
            self.set_cached_candidates(candidate_key, prioritized, raw_candidate_count, deduped_candidate_count)

        planned = plan_article_fetches(profile, prioritized, sensitivity, detail_level)
        with_articles = _attach_article_content(
            planned,
            profile,
            self.cache_store,
            self.http_client,
            allow_slow_pdf_fallback=detail_level != "fast",
        )
        # 整条链路按“候选 -> 正文 -> 评分 -> 事件 -> 证据行”推进。
        scored_items, pending_items = _scored_items(with_articles, profile, sensitivity)
        events = build_events(scored_items)
        evidence_rows = build_evidence_rows(profile, events)
        overview = overview_payload(profile, scored_items, events)
        direct_related, mapped_related = _evidence_relation_counts(evidence_rows)

        return {
            "ok": True,
            "profile": profile,
            "stats": {
                "candidateNews": deduped_candidate_count,
                "events": len(events),
                "evidenceUnits": len(evidence_rows),
                "directRelated": direct_related,
                "mappedRelated": mapped_related,
                "sources": _used_source_labels(scored_items),
            },
            "overview": overview,
            "meta": {
                "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "threshold": _response_threshold(sensitivity),
                "detailLevel": detail_level,
                "rawCandidateCount": raw_candidate_count,
                "dedupedCandidateCount": deduped_candidate_count,
                "prioritizedCandidateCount": len(prioritized),
                "confirmedItems": len(scored_items),
                "pendingItems": pending_items,
                "evidencePolicy": "confirmed-only",
            },
            "events": events,
            "evidenceRows": evidence_rows,
        }
