from __future__ import annotations

import re
from typing import Any

from .text_utils import evidence_profile, unique_evidence_details

# 这里集中处理事件摘要和事件卡片组装，主查询链路保持精简。

_SUMMARY_BAD_RE = re.compile(
    r"(?:"
    r"\u4e0d\u786e\u5b9a\u6027|\u5c06\u6709\u5229\u4e8e|\u6295\u8d44\u5efa\u8bae|\u98ce\u9669\u63d0\u793a|"
    r"\u5e74\u5ea6\u62a5\u544a\u6458\u8981|\u516c\u53f8\u57fa\u672c\u60c5\u51b5|\u80a1\u7968\u7b80\u51b5|"
    r"\u5e38\u7528\u8bcd\u8bed\u91ca\u4e49|\u516c\u53f8\u6307|\u62a5\u544a\u671f\u6307|\u6267\u884c\u4e8b\u52a1\u5408\u4f19\u4eba|"
    r"\u6ce8\u518c\u5730\u5740|\u4e3b\u8981\u529e\u516c\u5730\u5740|\u4e3b\u8981\u80a1\u4e1c|"
    r"\u65b0\u6d6a\u8d22\u7ecf_\u65b0\u6d6a\u7f51|\u516c\u53f8\u516c\u544a_|"
    r"\u8bc1\u5238\u53d8\u52a8\u6708\u62a5\u8868|H\u80a1\u516c\u544a|\u6cd5\u5f8b\u610f\u89c1\u4e66|"
    r"\u4f1a\u8ba1\u6570\u636e\u5dee\u5f02|\u51c0\u8d44\u4ea7\u5dee\u5f02\u60c5\u51b5|\u65f6\u95f4\u8303\u56f4|"
    r"\u80a1\u6743\u767b\u8bb0\u65e5|\u5b9e\u65bd\u6743\u76ca\u5206\u6d3e|"
    r"\u5177\u4f53\u65e5\u671f\u5c06\u5728.*?\u516c\u544a\u4e2d\u660e\u786e|\u5ba1\u6279\u53ca\u5176\u4ed6\u76f8\u5173\u7a0b\u5e8f|"
    r"\u63d0\u4ea4\u516c\u53f8\u80a1\u4e1c(?:\u5927\u4f1a|\u4f1a)\u5ba1\u8bae|"
    r"\u65e0\u987b\u63d0\u4ea4\u516c\u53f8\u80a1\u4e1c(?:\u5927\u4f1a|\u4f1a)\u5ba1\u8bae|"
    r"\u4e0d\u9700\u8981\u7ecf\u8fc7\u6709\u5173\u90e8\u95e8\u6279\u51c6|"
    r"\u9664\u6743\u9664\u606f\u4e8b\u9879|\u56de\u8d2d\u4ef7\u683c\u4e0a\u9650|\u56de\u8d2d\u4e13\u7528\u8bc1\u5238\u8d26\u6237\u671f\u95f4\u4e0d\u4eab\u53d7\u5229\u6da6\u5206\u914d|"
    r"\u53ca\u65f6\u5c65\u884c\u4fe1\u606f\u62ab\u9732\u4e49\u52a1|\u901a\u77e5\u503a\u6743\u4eba"
    r")",
    re.I,
)

_SUMMARY_ACTION_RE = re.compile(
    r"(?:"
    r"\u6536\u5230.*?(?:\u901a\u77e5\u4e66|\u6279\u51c6|\u6838\u51c6)|"
    r"\u83b7\u5f97.*?(?:\u6279\u51c6|\u8d44\u683c\u8ba4\u5b9a|\u53d7\u7406|\u8bb8\u53ef)|"
    r"\u4e2d\u6807|\u7b7e\u7f72\u5408\u540c|\u56de\u8d2d|\u589e\u6301|\u51cf\u6301|"
    r"\u505c\u4ea7|\u590d\u4ea7|\u590d\u822a|\u96c6\u91c7|\u7eb3\u5165\u533b\u4fdd"
    r")",
    re.I,
)

_SUMMARY_OPERATING_RE = re.compile(
    r"(?:"
    r"\u6536\u5165|\u8425\u6536|\u5229\u6da6|\u51c0\u5229\u6da6|\u6bdb\u5229\u7387|"
    r"\u9500\u91cf|\u4ea7\u91cf|\u51fa\u8d27|\u8fd0\u4ef7|\u8ba2\u5355|\u5408\u540c\u91d1\u989d|"
    r"\u7814\u53d1\u6295\u5165|\u73b0\u91d1\u7ea2\u5229|\u6d3e\u53d1\u73b0\u91d1\u7ea2\u5229"
    r")",
    re.I,
)

_SUMMARY_CHANGE_RE = re.compile(r"(?:\u540c\u6bd4|\u73af\u6bd4|\u589e\u957f|\u4e0b\u964d|\u63d0\u5347|\u51cf\u5c11|\u4e8f\u635f|\u626d\u4e8f)", re.I)

_SUMMARY_PAGE_RE = re.compile(r"(?:\b\d{1,4}/\d{1,4}\b|\u5e74\u5ea6\u62a5\u544a\d{1,4}/\d{1,4})", re.I)

_SUMMARY_GENERIC_RE = re.compile(
    r"(?:"
    r"\u5e7f\u6cdb\u5e03\u5c40|\u6218\u7565\u652f\u67f1|\u79ef\u6781\u63a8\u52a8|\u6301\u7eed\u62d3\u5c55\u5e02\u573a|"
    r"\u5e02\u573a\u53d1\u5c55\u524d\u666f\u5e7f\u9614|\u5de9\u56fa.*?\u7ade\u4e89\u4f18\u52bf"
    r")",
    re.I,
)

_SUMMARY_DIVIDEND_RE = re.compile(r"(?:\u6bcf10\u80a1|\u6bcf\u80a1\u6d3e\u53d1|\u6d3e\u53d1\u73b0\u91d1\u7ea2\u5229|\u8f6c\u589e|\u9001\u7ea2\u80a1)", re.I)

_SUMMARY_REJECT_RE = re.compile(
    r"(?:"
    r"\u8bc1\u5238\u4ee3\u7801[:：]?\d{6}|\u8bc1\u5238\u7b80\u79f0[:：]|\u516c\u544a\u7f16\u53f7[:：]|"
    r"(?:\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a)20\d{2}\u5e74\d{1,2}\u6708\d{1,2}\u65e5|"
    r"\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801|\u5173\u8054\u65b9\u57fa\u672c\u60c5\u51b5|\u516c\u53f8\u57fa\u672c\u4fe1\u606f"
    r")",
    re.I,
)

_SUMMARY_HISTORY_RE = re.compile(
    r"(?:创立于|成立于|发展历程|先后荣获|曾获|获评|入选|示范企业|论坛|展会)",
    re.I,
)

_SUMMARY_EVENT_NARRATIVE_RE = re.compile(
    r"(?:愿景|畅想|分享|演讲|论坛|大会|专家认为|如何|几关)",
    re.I,
)

_SUMMARY_PROCEDURAL_RE = re.compile(
    r"(?:议案|摘要的议案|证券交易账户|非交易过户|锁定期|草案)",
    re.I,
)

_SUMMARY_INTRO_RE = re.compile(
    r"(?:技术地位|全球知名|新兴公司|能够提供|平台化基础系统软件|发展历程|历程的缩影)",
    re.I,
)

_SUMMARY_BOARD_PLAN_RE = re.compile(
    r"(?:董事会决议通过|利润分配预案|公积金转增股本预案)",
    re.I,
)

_SUMMARY_CAPITAL_ACTION_RE = re.compile(
    r"(?:投资金额|同比例增资|增资人民币|货币出资|拟对.*增资)",
    re.I,
)

_SUMMARY_COMPLIANCE_RE = re.compile(
    r"(?:至本次关联交易为止.*未达到.*5%|未构成.*重大资产重组)",
    re.I,
)

_SUMMARY_SECTION_PREFIX_RE = re.compile(
    r"^(?:[（(]?[一二三四五六七八九十\d]+[)）]?|[一二三四五六七八九十]+、)",
    re.I,
)

_EVENT_BACKGROUND_TITLE_RE = re.compile(
    r"(?:观察|解读|怎么看|如何看|深度|专题|专访|透视|图解|前瞻|全景)",
    re.I,
)

_TITLE_ONLY_SUMMARY_RE = re.compile(
    r"(?:员工持股计划.{0,20}(?:实施进展|锁定期届满|非交易过户|完成购买)|延期换届选举|提示性公告)",
    re.I,
)

def _summary_candidate_score(text: str, base_score: int, title: str, official_tokens: tuple[str, ...]) -> int:
    score = base_score
    if _SUMMARY_ACTION_RE.search(text):
        score += 12
    if _SUMMARY_OPERATING_RE.search(text) and _SUMMARY_CHANGE_RE.search(text):
        score += 10
    elif _SUMMARY_OPERATING_RE.search(text):
        score += 4
    if _SUMMARY_DIVIDEND_RE.search(text):
        score += 8
    if _SUMMARY_BAD_RE.search(text):
        score -= 16
    if _SUMMARY_PAGE_RE.search(text):
        score -= 20
    if _SUMMARY_GENERIC_RE.search(text) and not _SUMMARY_CHANGE_RE.search(text):
        score -= 6
    if len(text) > 120:
        score -= 5
    if any(token in title for token in official_tokens) and any(token in text for token in official_tokens):
        score += 8
    return score

def _extract_years(text: str) -> list[int]:
    return [int(year) for year in re.findall(r"(20\d{2})年", str(text or ""))]

def _clean_summary_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return value
    value = re.sub(r"^[（(]?[一二三四五六七八九十\d]+[）)]", "", value).strip()
    value = re.sub(r"^[一二三四五六七八九十]+、", "", value).strip()
    value = re.sub(r"^(?:投资金额|关联方基本情况\d*[、，.]?|药物的基本情况)", "", value).strip("：:，, ")
    value = re.sub(
        r"^(?:董事会决议通过的)?本报告期利润分配预案或公积金转增股本预案(?:根据按企业会计准则编制的经审计的.*?财务报告，)?",
        "",
        value,
    ).strip()
    value = re.sub(r"^根据按企业会计准则编制的经审计的.*?财务报告，", "", value).strip()
    value = re.sub(r"^(?:经营情况讨论与分析|技术地位)", "", value).strip("：:，, ")
    value = re.sub(r"\s{2,}", " ", value).strip()
    return value

def _compact_summary_text(text: str, limit: int = 110) -> str:
    value = _clean_summary_text(text)
    if len(value) <= limit:
        return value
    if _SUMMARY_CAPITAL_ACTION_RE.search(value):
        amount_positions = [match.end() for match in re.finditer(r"万元", value)]
        if len(amount_positions) >= 2 and amount_positions[1] <= limit + 24:
            return value[: amount_positions[1]].strip()
        if amount_positions and amount_positions[0] <= limit + 24:
            return value[: amount_positions[0]].strip()
    if "其中" in value:
        list_cut = value.find("、", value.find("其中") + 2)
        if 0 < list_cut <= limit + 24:
            return value[:list_cut].strip()
    for separator in ("。", "；", ";"):
        index = value.find(separator, 40)
        if 0 < index <= limit + 10:
            return value[: index + 1].strip()
    cut = value.rfind("，", 40, limit + 1)
    if cut < 40:
        cut = value.find("，", limit, min(len(value), limit + 24))
    if cut >= 40:
        return value[:cut].strip()
    return value[:limit].rstrip("，,；;：: ").strip()

def _fallback_event_summary(top: dict[str, Any]) -> str:
    summary = _compact_summary_text(str(top.get("summary", "")).strip())
    title = str(top.get("title", "")).strip()
    if summary and not _SUMMARY_REJECT_RE.search(summary):
        has_hard_signal = bool(
            _SUMMARY_ACTION_RE.search(summary)
            or _SUMMARY_DIVIDEND_RE.search(summary)
            or (_SUMMARY_OPERATING_RE.search(summary) and (bool(_SUMMARY_CHANGE_RE.search(summary)) or bool(re.search(r"\d", summary))))
        )
        if has_hard_signal and len(summary) <= 120 and not _SUMMARY_EVENT_NARRATIVE_RE.search(summary):
            return summary
        if title:
            return title
        return summary[:120]
    if title and not _SUMMARY_REJECT_RE.search(title):
        return title
    return summary or title

def _summary_candidate_score_v2(text: str, base_score: int, title: str, official_tokens: tuple[str, ...]) -> int:
    score = _summary_candidate_score(text, base_score, title, official_tokens)
    text_years = _extract_years(text)
    title_years = _extract_years(title)
    has_action = bool(_SUMMARY_ACTION_RE.search(text))
    has_operating = bool(_SUMMARY_OPERATING_RE.search(text))
    if text_years and title_years and max(text_years) <= max(title_years) - 2 and not (has_action or has_operating):
        score -= 14
    if _SUMMARY_HISTORY_RE.search(text):
        score -= 10
    if _SUMMARY_EVENT_NARRATIVE_RE.search(text) and not (has_action or has_operating or _SUMMARY_DIVIDEND_RE.search(text)):
        score -= 8
    if _SUMMARY_PROCEDURAL_RE.search(text) and not (has_action or has_operating or _SUMMARY_DIVIDEND_RE.search(text)):
        score -= 12
    if _SUMMARY_INTRO_RE.search(text) and not (has_action or has_operating or _SUMMARY_DIVIDEND_RE.search(text)):
        score -= 10
    if _SUMMARY_BOARD_PLAN_RE.search(text) and len(text) > 50:
        score -= 10
    if _SUMMARY_CAPITAL_ACTION_RE.search(text):
        score += 12
    if _SUMMARY_COMPLIANCE_RE.search(text):
        score -= 14
    if _SUMMARY_SECTION_PREFIX_RE.search(text) and not (has_action or has_operating):
        score -= 6
    if len(text) > 90 and not (has_action or has_operating or _SUMMARY_DIVIDEND_RE.search(text)):
        score -= 6
    if text_years and title_years and max(text_years) >= max(title_years) - 1 and (has_action or has_operating):
        score += 4
    return score

def _event_rank_score(top: dict[str, Any], relation_lead: dict[str, Any], event_evidence_profile: dict[str, Any], event_evidence_details: list[dict[str, Any]]) -> int:
    score = int(top.get("score", 0))
    relation_rank = int(relation_lead.get("relationTierRank", 0))
    fact_density = int(event_evidence_profile.get("factDensity", 0))
    title = str(top.get("title", ""))
    summary = str(top.get("summary", ""))
    score += min(4, fact_density // 25)
    if relation_rank >= 3:
        score += 5
    elif relation_rank == 2:
        score += 2
    if relation_rank <= 2 and not event_evidence_details:
        score -= 8
    if relation_rank <= 2 and fact_density < 40:
        score -= 4
    if relation_rank <= 2 and len(event_evidence_details) <= 1:
        score -= 2
    if relation_rank <= 2 and _EVENT_BACKGROUND_TITLE_RE.search(title):
        score -= 6
    if relation_rank <= 2 and len(summary) > 120 and not (_SUMMARY_ACTION_RE.search(summary) or _SUMMARY_OPERATING_RE.search(summary)):
        score -= 4
    if _TITLE_ONLY_SUMMARY_RE.search(title):
        score -= 10
    return score

def build_events(scored_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # 先聚成事件，再从事件里挑摘要和证据概览。
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in scored_items:
        grouped.setdefault(str(item.get("eventId", "")), []).append(item)

    events: list[dict[str, Any]] = []
    official_tokens = ("\u83b7\u6279", "\u6279\u51c6", "\u8ba4\u5b9a", "\u53d7\u7406", "\u4e2d\u6807", "\u7b7e\u7ea6", "\u56de\u8d2d", "\u590d\u822a", "\u96c6\u91c7")
    for current_event_id, items in grouped.items():
        items = sorted(items, key=lambda entry: (-int(entry.get("score", 0)), str(entry.get("publishedAt", ""))))
        top = items[0]
        relation_lead = sorted(items, key=lambda entry: (-int(entry.get("relationTierRank", 0)), -int(entry.get("score", 0))))[0]
        event_evidence_details = unique_evidence_details([entry.get("evidenceDetails", []) for entry in items], take=6)
        event_evidence_profile = evidence_profile(event_evidence_details)
        response_items = [_response_item_snapshot(entry) for entry in items]

        summary_candidates: list[tuple[int, int, str]] = []
        for detail in event_evidence_details:
            text = str(detail.get("text", "")).strip()
            if not text or _SUMMARY_REJECT_RE.search(text):
                continue
            cleaned_text = _clean_summary_text(text)
            if not cleaned_text or _SUMMARY_REJECT_RE.search(cleaned_text):
                continue
            summary_candidates.append(
                (
                    _summary_candidate_score_v2(text, int(detail.get("score", 0)), str(top.get("title", "")), official_tokens),
                    len(cleaned_text),
                    _compact_summary_text(cleaned_text),
                )
            )
        summary_candidates.sort(key=lambda item: (-item[0], item[1]))
        event_summary = (
            summary_candidates[0][2]
            if summary_candidates and summary_candidates[0][0] >= 18
            else _fallback_event_summary(top)
        )

        if int(relation_lead.get("relationTierRank", 0)) <= 2 and (
            not event_evidence_details or int(event_evidence_profile.get("factDensity", 0)) < 40
        ):
            event_summary = str(top.get("title", "")).strip() or event_summary
        if _TITLE_ONLY_SUMMARY_RE.search(str(top.get("title", ""))):
            event_summary = str(top.get("title", "")).strip() or event_summary

        published_values = [str(entry.get("publishedAt", "")) for entry in items if entry.get("publishedAt")]
        event_rank_score = _event_rank_score(top, relation_lead, event_evidence_profile, event_evidence_details)
        events.append(
            {
                "eventId": current_event_id,
                "eventTitle": top.get("title", ""),
                "summary": event_summary,
                "topScore": top.get("score", 0),
                "eventRankScore": event_rank_score,
                "signalFamily": top.get("signalFamily", ""),
                "relationTier": relation_lead.get("relationTier", ""),
                "relationTierLabel": relation_lead.get("relationTierLabel", ""),
                "relationReason": relation_lead.get("relationReason", ""),
                "evidenceCount": len(items),
                "evidenceDetails": event_evidence_details,
                "evidenceBullets": list(dict.fromkeys([bullet for entry in items for bullet in entry.get("evidenceBullets", [])]))[:5],
                "evidenceTypes": list(event_evidence_profile.get("typeLabels", [])),
                "evidenceTypeCounts": list(event_evidence_profile.get("typeCounts", [])),
                "contentModes": list(dict.fromkeys([entry.get("contentMode", "") for entry in items if entry.get("contentMode")])),
                "evidenceConfirmed": True,
                "tags": list(dict.fromkeys([tag for entry in items for tag in entry.get("tags", [])]))[:8],
                "paths": list(dict.fromkeys([path for entry in items for path in entry.get("paths", [])]))[:6],
                "matchedEntities": list(dict.fromkeys([entity for entry in items for entity in entry.get("matchedEntities", [])]))[:10],
                "hitModes": list(dict.fromkeys([entry.get("hitMode", "") for entry in items if entry.get("hitMode")])),
                "reasons": list(dict.fromkeys([reason for entry in items for reason in entry.get("reasons", [])]))[:8],
                "sourceLabels": list(dict.fromkeys([entry.get("sourceLabel", "") for entry in items if entry.get("sourceLabel")])),
                "sourceSites": list(dict.fromkeys([entry.get("sourceSite", "") for entry in items if entry.get("sourceSite")]))[:8],
                "firstSeen": min(published_values) if published_values else "",
                "lastSeen": max(published_values) if published_values else "",
                "scenarioHit": any(entry.get("queryMode") == "scenario" for entry in items),
                "items": response_items,
            }
        )

    return sorted(
        events,
        key=lambda entry: (
            int(entry.get("eventRankScore", 0)),
            int(entry.get("topScore", 0)),
            str(entry.get("lastSeen", "")),
        ),
        reverse=True,
    )

def _response_item_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    evidence_text = "\n".join([str(entry).strip() for entry in item.get("evidenceBullets", []) if str(entry).strip()][:4])
    if not evidence_text:
        evidence_text = str(item.get("effectiveText", "")).strip()[:800]
    return {
        "title": item.get("title", ""),
        "publishedAt": item.get("publishedAt", ""),
        "sourceType": item.get("sourceType", ""),
        "sourceLabel": item.get("sourceLabel", ""),
        "sourceSite": item.get("sourceSite", ""),
        "queryContext": item.get("queryContext", ""),
        "queryTerm": item.get("queryTerm", ""),
        "queryMode": item.get("queryMode", ""),
        "contentMode": item.get("contentMode", ""),
        "evidenceConfirmed": item.get("evidenceConfirmed", False),
        "evidenceDetails": list(item.get("evidenceDetails", [])),
        "evidenceBullets": list(item.get("evidenceBullets", [])),
        "evidenceTypes": list(item.get("evidenceTypes", [])),
        "evidenceTypeCounts": list(item.get("evidenceTypeCounts", [])),
        "effectiveText": evidence_text,
        "score": item.get("score", 0),
        "signalFamily": item.get("signalFamily", ""),
        "hitMode": item.get("hitMode", ""),
        "relationTier": item.get("relationTier", ""),
        "relationTierLabel": item.get("relationTierLabel", ""),
        "relationTierRank": item.get("relationTierRank", 0),
        "relationReason": item.get("relationReason", ""),
        "matchedEntities": list(item.get("matchedEntities", [])),
        "paths": list(item.get("paths", [])),
        "reasons": list(item.get("reasons", [])),
        "tags": list(item.get("tags", [])),
        "url": item.get("url", ""),
        "originUrl": item.get("originUrl", ""),
    }
