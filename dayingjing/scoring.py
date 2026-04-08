from __future__ import annotations

import re
from typing import Any

from .config import MACRO_TERMS, SOURCE_CATALOG, source_setting
from .text_utils import evidence_profile, keyword_hits, top_occurrence_values

_BACKGROUND_FEATURE_TITLE_RE = re.compile(r"(?:\u89e3\u8bfb|\u4e13\u9898|\u6df1\u5ea6|\u89c2\u5bdf|\u524d\u77bb)", re.I)
_GENERIC_POLICY_BACKGROUND_TITLE_RE = re.compile(r"(?:\u901a\u77e5|\u610f\u89c1|\u65b9\u6848|\u529e\u6cd5|\u89c4\u5212)", re.I)
_WEAK_MAPPING_SOURCE_TYPES = {"eastmoneyFastNews", "csMarketNews"}
_LOW_VALUE_ANNOUNCEMENT_TITLE_RE = re.compile(
    r"(?:\u63d0\u793a\u6027\u516c\u544a|\u6362\u5c4a|\u5458\u5de5\u6301\u80a1\u8ba1\u5212.{0,24}(?:\u8fdb\u5c55|\u9501\u5b9a\u671f|\u8fc7\u6237|\u5b8c\u6210\u8d2d\u4e70))",
    re.I,
)
_LOW_VALUE_ANNOUNCEMENT_TEXT_RE = re.compile(r"(?:\u8bae\u6848|\u8bc1\u5238\u4ea4\u6613\u8d26\u6237|\u9501\u5b9a\u671f|\u6362\u5c4a)", re.I)
_GOVERNANCE_ANNOUNCEMENT_RE = re.compile(r"(?:\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a|\u80a1\u4e1c\u5927\u4f1a).{0,16}(?:\u51b3\u8bae|\u6362\u5c4a|\u63d0\u793a)", re.I)
_NARRATIVE_RE = re.compile(r"(?:\u8c10\u97f3|\u70ed\u8bae|\u8054\u60f3|\u6982\u5ff5\u80a1|\u7ef0\u53f7)", re.I)
_MACRO_RE = re.compile(r"(?:\u75ab\u60c5|\u65b0\u51a0|\u4f0a\u6717|\u4fc4\u4e4c|\u4e2d\u4e1c|\u6218\u4e89|\u7ea2\u6d77|\u970d\u5c14\u6728\u5179|\u5173\u7a0e|\u5236\u88c1|\u7279\u6717\u666e)", re.I)
_CHAIN_RE = re.compile(r"(?:\u8fd0\u4ef7|\u4ea7\u4e1a\u94fe|\u4f9b\u5e94\u94fe|\u51fa\u53e3\u9650\u5236|GPU|NVIDIA|\u56fd\u4ea7\u66ff\u4ee3|\u6e2f\u53e3|\u822a\u8fd0|\u6cb9\u4ef7|\u7b97\u529b)", re.I)
_FUNDAMENTAL_RE = re.compile(r"(?:\u516c\u544a|\u83b7\u6279|\u4e2d\u6807|\u5408\u540c|\u8ba2\u5355|\u4e1a\u7ee9|\u56de\u8d2d|\u589e\u6301|\u51cf\u6301|\u96c6\u91c7|\u533b\u4fdd|\u836f\u76d1|\u4e34\u5e8a|\u7b7e\u7ea6)", re.I)
_HIGH_VALUE_ANNOUNCEMENT_RE = re.compile(
    r"(?:\u83b7\u6279|\u4e2d\u6807|\u91cd\u5927\u5408\u540c|\u8ba2\u5355|\u56de\u8d2d|\u589e\u6301|\u51cf\u6301|\u4e1a\u7ee9\u9884\u544a|\u5e74\u5ea6\u62a5\u544a|\u534a\u5e74\u5ea6\u62a5\u544a|\u5b63\u5ea6\u62a5\u544a|\u5229\u6da6\u5206\u914d|\u5206\u7ea2|\u5173\u8054\u4ea4\u6613|\u6536\u8d2d|\u589e\u8d44|\u7b7e\u7f72|\u8bc9\u8bbc|\u7acb\u6848|\u5904\u7f5a|\u95ee\u8be2|\u505c\u724c|\u590d\u724c|\u505c\u4ea7|\u6218\u7565\u5408\u4f5c|\u4ea7\u80fd)",
    re.I,
)
_MEDIUM_VALUE_ANNOUNCEMENT_RE = re.compile(r"(?:\u5e74\u5ea6\u62a5\u544a|\u534a\u5e74\u5ea6\u62a5\u544a|\u5b63\u5ea6\u62a5\u544a|\u5229\u6da6\u5206\u914d|\u5206\u7ea2)", re.I)
_LOW_VALUE_ANNOUNCEMENT_RE = re.compile(
    r"(?:\u8bc1\u5238\u53d8\u52a8\u6708\u62a5\u8868|\u6301\u7eed\u7763\u5bfc|\u6838\u67e5\u610f\u89c1|\u6cd5\u5f8b\u610f\u89c1\u4e66|H\u80a1\u516c\u544a|\u4e13\u9879\u8bf4\u660e|\u901a\u77e5\u503a\u6743\u4eba)",
    re.I,
)
_GOVERNANCE_TITLE_RE = re.compile(r"(?:\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a|\u80a1\u4e1c\u5927\u4f1a).{0,16}(?:\u4f1a\u8bae)?\u51b3\u8bae\u516c\u544a", re.I)
_STAFF_PLAN_TITLE_RE = re.compile(r"\u5458\u5de5\u6301\u80a1\u8ba1\u5212.{0,20}(?:\u5b9e\u65bd\u8fdb\u5c55|\u9501\u5b9a\u671f|\u975e\u4ea4\u6613\u8fc7\u6237|\u5b8c\u6210\u8d2d\u4e70)", re.I)
_HINT_TITLE_RE = re.compile(r"\u63d0\u793a\u6027\u516c\u544a", re.I)
_REVIEW_TITLE_RE = re.compile(r"\u6838\u67e5\u610f\u89c1", re.I)
_INTERMEDIARY_TITLE_RE = re.compile(
    r"(?:\u6cd5\u5f8b\u610f\u89c1(?:\u4e66)?|\u4e13\u9879\u8bf4\u660e|\u4e13\u9879\u6838\u67e5\u610f\u89c1|\u901a\u77e5\u503a\u6743\u4eba(?:\u7684\u516c\u544a)?|\u81ea\u4e3b\u884c\u6743\u6a21\u5f0f|\u6301\u7eed\u7763\u5bfc|\u72ec\u7acb\u8d22\u52a1\u987e\u95ee\u62a5\u544a|\u4fdd\u8350\u673a\u6784\u6838\u67e5\u610f\u89c1)",
    re.I,
)
_INCENTIVE_TITLE_RE = re.compile(
    r"(?:(?:\u80a1\u7968\u671f\u6743\u6fc0\u52b1\u8ba1\u5212|\u9650\u5236\u6027\u80a1\u7968\u6fc0\u52b1\u8ba1\u5212).{0,24}(?:\u6ce8\u9500|\u884c\u6743|\u9884\u7559\u6388\u4e88)|(?:\u56de\u8d2d\u6ce8\u9500).{0,12}(?:\u9650\u5236\u6027\u80a1\u7968|\u80a1\u7968\u671f\u6743)|(?:\u6fc0\u52b1\u5bf9\u8c61).{0,18}(?:\u56de\u8d2d\u6ce8\u9500|\u6ce8\u9500|\u884c\u6743))",
    re.I,
)
_EVIDENCE_TYPE_LABELS = {
    "fact": "\u4e8b\u5b9e",
    "quant": "\u91cf\u5316",
    "policy": "\u653f\u7b56",
    "path": "\u4f20\u5bfc",
    "impact": "\u5f71\u54cd",
    "narrative": "\u53d9\u4e8b",
}


def profile_route_tags(profile: dict[str, Any]) -> set[str]:
    return {str(tag).strip().lower() for tag in profile.get("routeTags", []) if str(tag).strip()}


def source_route_fit_score(profile: dict[str, Any], source_type: str) -> int:
    routes = profile_route_tags(profile)
    if not routes:
        return 0
    if source_type == "cninfoAnnouncement":
        return 8 if routes & {"company_news", "medical", "technology", "semiconductor", "defense", "industrial", "new_energy"} else 6
    if source_type == "bulletin":
        return 6 if "company_news" in routes else 2
    if source_type == "stockNews":
        if routes & {"company_news", "medical", "consumer", "travel", "technology", "semiconductor", "new_energy", "defense"}:
            return 5
        return 1
    if source_type == "industryNews":
        if routes & {"shipping", "travel", "energy", "commodity", "property", "infrastructure", "consumer", "new_energy", "industrial", "macro_sensitive"}:
            return 6
        if routes & {"medical", "technology", "semiconductor", "defense"}:
            return 2
        return -2
    if source_type == "eastmoneyFocus":
        if routes & {"company_news", "macro_sensitive", "policy", "technology", "semiconductor", "medical", "consumer", "travel", "new_energy"}:
            return 5
        return 1
    if source_type == "eastmoneyFastNews":
        if routes & {"shipping", "travel", "energy", "commodity", "finance", "property", "infrastructure", "macro_sensitive", "policy"}:
            return 8
        if routes & {"technology", "semiconductor", "medical", "new_energy", "defense", "industrial", "company_news"}:
            return 5
        return 2
    if source_type == "csMarketNews":
        if routes & {"shipping", "travel", "energy", "commodity", "finance", "property", "infrastructure", "macro_sensitive", "policy"}:
            return 7
        if routes & {"medical", "technology", "semiconductor", "consumer", "new_energy", "industrial"}:
            return 4
        return 0
    if source_type == "nmpaOfficial":
        return 10 if "medical" in routes else -8
    if source_type == "nhsaOfficial":
        return 10 if "medical" in routes else -8
    if source_type == "miitOfficial":
        return 9 if routes & {"technology", "semiconductor", "new_energy", "defense", "industrial"} else -6
    if source_type == "ndrcOfficial":
        if routes & {"shipping", "travel", "energy", "commodity", "finance", "property", "infrastructure", "consumer", "new_energy", "macro_sensitive", "policy", "industrial"}:
            return 8
        return -4
    if source_type in {"search360", "sogouSearch"}:
        if routes & {"shipping", "travel", "energy", "commodity", "finance", "property", "infrastructure", "consumer", "new_energy", "industrial", "macro_sensitive", "policy"}:
            return 5
        if routes & {"company_news", "medical", "technology", "semiconductor", "defense"}:
            return 4
        return 2
    return 0


def mapping_signal_adjustment(
    profile: dict[str, Any],
    item: dict[str, Any],
    mapped_only: bool,
    title_text: str,
    theme_hits: list[str],
    factor_hits: list[str],
    authority_hits: list[str],
    macro_hits: list[str],
    item_evidence_profile: dict[str, Any],
) -> dict[str, Any]:
    routes = profile_route_tags(profile)
    source_type = str(item.get("sourceType", ""))
    evidence_count = len(item.get("evidenceDetails", []))
    fact_density = int(item_evidence_profile.get("factDensity", 0))
    technology_like = bool(routes & {"technology", "semiconductor"})

    generic_theme_only_mapping = mapped_only and bool(theme_hits) and not (factor_hits or authority_hits or macro_hits)
    strong_factor_mapping = mapped_only and bool(factor_hits) and bool(macro_hits or authority_hits)
    broad_feature_mapping = mapped_only and bool(_BACKGROUND_FEATURE_TITLE_RE.search(title_text))
    generic_policy_background = mapped_only and bool(_GENERIC_POLICY_BACKGROUND_TITLE_RE.search(title_text))
    thin_mapping_evidence = mapped_only and evidence_count == 0
    low_fact_background = mapped_only and fact_density < 40
    weak_source_background = mapped_only and source_type in _WEAK_MAPPING_SOURCE_TYPES and evidence_count < 2

    score = 0
    reasons: list[str] = []
    if generic_theme_only_mapping:
        score -= 18
        reasons.append("\u4ec5\u547d\u4e2d\u4e3b\u9898\u8bcd\uff0c\u7f3a\u5c11\u516c\u53f8\u6216\u653f\u7b56\u951a\u70b9\u3002")
    elif strong_factor_mapping:
        score += 4
        reasons.append("\u540c\u65f6\u547d\u4e2d\u654f\u611f\u56e0\u5b50\u4e0e\u5b8f\u89c2/\u653f\u7b56\u952e\u8bcd\u3002")

    if broad_feature_mapping:
        score -= 8
        reasons.append("\u6807\u9898\u66f4\u50cf\u80cc\u666f\u89e3\u8bfb\u6216\u4e13\u9898\u6587\u7a3f\u3002")
    if thin_mapping_evidence:
        score -= 6
        reasons.append("\u6b63\u6587\u6ca1\u6709\u62bd\u51fa\u8db3\u591f\u8bc1\u636e\u53e5\u3002")
    if low_fact_background:
        score -= 4
    if weak_source_background:
        score -= 3
    if technology_like and broad_feature_mapping:
        score -= 4
    if technology_like and generic_policy_background and not (factor_hits and (macro_hits or authority_hits)):
        score -= 4
        reasons.append("\u66f4\u50cf\u884c\u4e1a\u80cc\u666f\uff0c\u7f3a\u5c11\u8d34\u8fd1\u516c\u53f8\u7684\u4f20\u5bfc\u952e\u70b9\u3002")

    return {
        "score": score,
        "reasons": reasons,
        "genericThemeOnly": generic_theme_only_mapping,
        "strongFactor": strong_factor_mapping,
        "broadFeature": broad_feature_mapping,
    }


def announcement_signal_adjustment(item: dict[str, Any], title_text: str, item_evidence_profile: dict[str, Any]) -> dict[str, Any]:
    source_type = str(item.get("sourceType", ""))
    if source_type not in {"cninfoAnnouncement", "bulletin"}:
        return {"score": 0, "reasons": []}

    evidence_texts = [str(detail.get("text", "")).strip() for detail in item.get("evidenceDetails", []) if str(detail.get("text", "")).strip()]
    low_value_title = bool(_LOW_VALUE_ANNOUNCEMENT_TITLE_RE.search(title_text))
    governance_title = bool(_GOVERNANCE_ANNOUNCEMENT_RE.search(title_text))
    evidence_hits = sum(1 for text in evidence_texts if _LOW_VALUE_ANNOUNCEMENT_TEXT_RE.search(text))
    fact_density = int(item_evidence_profile.get("factDensity", 0))

    score = 0
    reasons: list[str] = []
    if low_value_title:
        score -= 12
        reasons.append("\u516c\u544a\u66f4\u504f\u7a0b\u5e8f\u6216\u9636\u6bb5\u6027\u8fdb\u5c55\u3002")
    if governance_title and not re.search(r"\u7acb\u6848|\u5904\u7f5a|\u95ee\u8be2|\u505c\u724c|\u590d\u724c|\u83b7\u6279|\u91cd\u5927\u5408\u540c|\u4e2d\u6807", title_text):
        score -= 10
        reasons.append("\u516c\u544a\u66f4\u504f\u6cbb\u7406\u6216\u7a0b\u5e8f\u5b89\u6392\u3002")
    if evidence_texts and evidence_hits == len(evidence_texts):
        score -= 8
    elif evidence_hits >= 1 and fact_density >= 50:
        score -= 4
    if low_value_title and len(evidence_texts) <= 2:
        score -= 4
    return {"score": score, "reasons": reasons}


def announcement_priority(title: str) -> dict[str, Any]:
    if not title:
        return {"score": 0, "reason": ""}
    if _LOW_VALUE_ANNOUNCEMENT_RE.search(title):
        return {"score": -14, "reason": "\u5b58\u91cf\u62ab\u9732\u6216\u4e2d\u4ecb\u6587\u4ef6\u3002"}
    if _HIGH_VALUE_ANNOUNCEMENT_RE.search(title):
        return {"score": 12, "reason": "\u9ad8\u4ef7\u503c\u516c\u544a\u4e8b\u4ef6\u3002"}
    if _MEDIUM_VALUE_ANNOUNCEMENT_RE.search(title):
        return {"score": 8, "reason": "\u7ecf\u8425\u6216\u5206\u914d\u4fe1\u606f\u3002"}
    if _GOVERNANCE_TITLE_RE.search(title):
        return {"score": -10, "reason": "\u6cbb\u7406\u7c7b\u516c\u544a\u3002"}
    return {"score": 0, "reason": ""}


def title_signal_adjustment(title: str) -> dict[str, Any]:
    if not title:
        return {"score": 0, "reason": ""}
    if _GOVERNANCE_TITLE_RE.search(title):
        return {"score": -18, "reason": "\u6807\u9898\u5c5e\u4e8e\u6cbb\u7406\u51b3\u8bae\u516c\u544a\u3002"}
    if re.search(r"(?:\u5ef6\u671f\u6362\u5c4a|\u6362\u5c4a\u9009\u4e3e)", title):
        return {"score": -16, "reason": "\u6807\u9898\u5c5e\u4e8e\u6362\u5c4a\u6216\u5ef6\u671f\u6362\u5c4a\u4e8b\u9879\u3002"}
    if _STAFF_PLAN_TITLE_RE.search(title):
        return {"score": -14, "reason": "\u6807\u9898\u66f4\u50cf\u5458\u5de5\u6301\u80a1\u8ba1\u5212\u9636\u6bb5\u8fdb\u5c55\u3002"}
    if _REVIEW_TITLE_RE.search(title) and not re.search(r"\u83b7\u6279|\u4e2d\u6807|\u8ba2\u5355|\u91cd\u5927\u5408\u540c|\u56de\u8d2d|\u589e\u6301|\u51cf\u6301|\u7acb\u6848|\u5904\u7f5a", title):
        return {"score": -18, "reason": "\u6807\u9898\u5c5e\u4e8e\u6838\u67e5\u610f\u89c1\u3002"}
    if _INTERMEDIARY_TITLE_RE.search(title):
        return {"score": -16, "reason": "\u6807\u9898\u5c5e\u4e8e\u4e2d\u4ecb\u610f\u89c1\u6216\u7a0b\u5e8f\u6027\u8bf4\u660e\u3002"}
    if _INCENTIVE_TITLE_RE.search(title):
        return {"score": -22, "reason": "\u6807\u9898\u66f4\u50cf\u6fc0\u52b1\u8ba1\u5212\u6ce8\u9500\u6216\u884c\u6743\u5b89\u6392\u3002"}
    if _HINT_TITLE_RE.search(title) and not re.search(r"\u590d\u724c|\u505c\u724c|\u7ec8\u6b62|\u98ce\u9669|\u7acb\u6848|\u5904\u7f5a|\u83b7\u6279|\u53d7\u7406", title):
        return {"score": -6, "reason": "\u6807\u9898\u504f\u63d0\u793a\u6027\u62ab\u9732\u3002"}
    return {"score": 0, "reason": ""}


def preliminary_score(profile: dict[str, Any], item: dict[str, Any]) -> int:
    text = f"{item.get('title', '')} {item.get('summary', '')}"
    direct_hits = keyword_hits([profile.get("name", ""), profile.get("code", "")], text)
    alias_hits = keyword_hits(profile.get("aliases", []), text)
    sub_hits = keyword_hits(profile.get("subsidiaries", []), text)
    product_hits = keyword_hits(profile.get("products", []), text)
    theme_hits = keyword_hits(profile.get("themes", []), text)
    factor_hits = keyword_hits(profile.get("sensitiveFactors", []), text)
    hook_hits = keyword_hits(profile.get("narrativeHooks", []), text)
    authority_hits = keyword_hits(profile.get("policyAuthorities", []), text)
    macro_hits = keyword_hits(MACRO_TERMS, text)
    priority = announcement_priority(item.get("title", ""))
    title_adjustment = title_signal_adjustment(item.get("title", ""))

    score = 0
    score += len(direct_hits) * 12
    score += len(alias_hits) * 10
    score += len(sub_hits) * 9
    score += len(product_hits) * 8
    score += min(12, len(theme_hits) * 4)
    score += min(12, len(factor_hits) * 4)
    score += min(8, len(hook_hits) * 3)
    score += min(12, len(authority_hits) * 5)
    if macro_hits and (factor_hits or theme_hits or authority_hits):
        score += min(10, 4 + (len(macro_hits) * 2))
    score += priority["score"] + title_adjustment["score"]
    if item.get("queryMode") == "scenario":
        score += 4
    if item.get("queryMode") == "policy":
        score += 8
    source_type = str(item.get("sourceType", ""))
    score += int(source_setting(source_type, "sourceScore", 0) or 0)
    score += source_route_fit_score(profile, source_type)
    return score


def signal_family(item: dict[str, Any], profile: dict[str, Any], text: str) -> str:
    if item.get("sourceType") in {"bulletin", "cninfoAnnouncement"}:
        return "fundamental"
    content = f"{item.get('title', '')} {item.get('summary', '')} {text}"
    if _NARRATIVE_RE.search(content):
        return "narrative"
    if _MACRO_RE.search(content):
        return "macro"
    if _CHAIN_RE.search(content):
        return "chain"
    if _FUNDAMENTAL_RE.search(content):
        return "fundamental"
    if item.get("sourceType") == "industryNews":
        return "chain"
    if keyword_hits(profile.get("narrativeHooks", []), content):
        return "narrative"
    return "fundamental"


def relation_tier_label(tier: str) -> str:
    return {
        "hard": "\u786c\u5173\u8054",
        "semi": "\u534a\u663e\u5f0f\u5173\u8054",
        "mapped": "\u6620\u5c04\u5173\u8054",
        "soft": "\u8f6f\u5173\u8054",
        "watch": "\u5019\u9009\u89c2\u5bdf",
    }.get(tier, "\u5019\u9009\u89c2\u5bdf")


def relation_tier_profile(hit_mode: str) -> dict[str, Any]:
    if hit_mode == "\u76f4\u63a5\u5173\u8054":
        return {"key": "hard", "label": relation_tier_label("hard"), "rank": 4, "reason": "\u6b63\u6587\u76f4\u63a5\u547d\u4e2d\u516c\u53f8\u540d\u3001\u4ee3\u7801\u6216\u7a33\u5b9a\u522b\u540d\u3002"}
    if hit_mode == "\u534a\u663e\u5f0f\u5173\u8054":
        return {"key": "semi", "label": relation_tier_label("semi"), "rank": 3, "reason": "\u6b63\u6587\u547d\u4e2d\u5b50\u516c\u53f8\u3001\u4ea7\u54c1\u3001\u9879\u76ee\u7b49\u5b9e\u4f53\u3002"}
    if hit_mode in {"\u4ea7\u4e1a\u94fe\u6620\u5c04", "\u5b8f\u89c2\u6620\u5c04"}:
        return {"key": "mapped", "label": relation_tier_label("mapped"), "rank": 2, "reason": "\u4e8b\u4ef6\u901a\u8fc7\u4ea7\u4e1a\u94fe\u3001\u653f\u7b56\u6216\u5b8f\u89c2\u8def\u5f84\u6620\u5c04\u5230\u4e2a\u80a1\u3002"}
    if hit_mode == "\u9690\u5f0f\u8054\u60f3":
        return {"key": "soft", "label": relation_tier_label("soft"), "rank": 1, "reason": "\u66f4\u591a\u5c5e\u4e8e\u9898\u6750\u4f20\u64ad\u6216\u53d9\u4e8b\u8054\u60f3\u3002"}
    return {"key": "watch", "label": relation_tier_label("watch"), "rank": 0, "reason": "\u5f53\u524d\u4ec5\u6784\u6210\u5019\u9009\u89c2\u5bdf\u3002"}


def impact_paths(profile: dict[str, Any], content: str, family: str, hit_mode: str) -> list[str]:
    paths: list[str] = []
    if re.search(r"\u83b7\u6279|\u4e34\u5e8a|\u836f\u76d1|\u533b\u4fdd|\u5b64\u513f\u836f|\u9002\u5e94\u75c7", content):
        paths.append("\u5ba1\u6279/\u4e34\u5e8a -> \u4ea7\u54c1\u9884\u671f -> \u533b\u836f\u4f30\u503c")
    if re.search(r"\u8ba2\u5355|\u7b7e\u7ea6|\u5408\u540c|\u4e2d\u6807", content):
        paths.append("\u8ba2\u5355/\u5408\u540c -> \u6536\u5165\u9884\u671f -> \u76c8\u5229\u5f39\u6027")
    if re.search(r"\u7ea2\u6d77|\u970d\u5c14\u6728\u5179|\u4f0a\u6717|\u822a\u8fd0|\u8fd0\u4ef7|\u7ed5\u822a", content):
        paths.append("\u5730\u7f18\u6270\u52a8 -> \u822a\u7ebf/\u8fd0\u4ef7 -> \u822a\u8fd0\u76c8\u5229")
    if re.search(r"\u75ab\u60c5|\u65b0\u51a0|\u5ba2\u6d41|\u56fd\u9645\u822a\u73ed|\u51fa\u5165\u5883|\u65c5\u6e38|\u514d\u7b7e", content):
        paths.append("\u51fa\u884c\u653f\u7b56/\u5ba2\u6d41 -> \u7ecf\u8425\u4fee\u590d -> \u4e1a\u7ee9\u9884\u671f")
    if re.search(r"NVIDIA|GPU|\u7b97\u529b|\u56fd\u4ea7\u66ff\u4ee3|\u51fa\u53e3\u9650\u5236", content, re.I):
        paths.append("\u5916\u90e8\u9650\u5236/\u9700\u6c42\u6269\u5bb9 -> \u56fd\u4ea7\u66ff\u4ee3 -> \u82af\u7247\u9884\u671f")
    if re.search(r"\u836f\u76d1\u5c40|\u533b\u4fdd\u5c40|\u536b\u5065\u59d4|\u5de5\u4fe1\u90e8|\u53d1\u6539\u59d4|\u5546\u52a1\u90e8|\u6d77\u5173|\u5f81\u6c42\u610f\u89c1|\u653f\u7b56|\u901a\u77e5|\u529e\u6cd5", content):
        paths.append("\u76d1\u7ba1/\u653f\u7b56\u53d8\u5316 -> \u884c\u4e1a\u89c4\u5219 -> \u9884\u671f\u4fee\u6b63")
    if family == "narrative" or hit_mode == "\u9690\u5f0f\u8054\u60f3":
        paths.append("\u70ed\u70b9\u4f20\u64ad -> \u9898\u6750\u8054\u60f3 -> \u77ed\u7ebf\u8d44\u91d1")
    if not paths:
        paths.append(
            {
                "fundamental": "\u516c\u53f8\u4e8b\u4ef6 -> \u57fa\u672c\u9762\u9884\u671f -> \u4f30\u503c\u4fee\u6b63",
                "chain": "\u4ea7\u4e1a\u94fe\u4e8b\u4ef6 -> \u4f9b\u9700/\u6210\u672c\u53d8\u5316 -> \u76c8\u5229\u9884\u671f",
                "macro": "\u5b8f\u89c2\u4e8b\u4ef6 -> \u884c\u4e1a\u9884\u671f -> \u4e2a\u80a1\u6620\u5c04",
            }.get(family, "\u60c5\u7eea\u4f20\u64ad -> \u5019\u9009\u89c2\u5bdf")
        )
    return list(dict.fromkeys(paths))[:3]


def evidence_type_label(value: str) -> str:
    return _EVIDENCE_TYPE_LABELS.get(str(value), str(value) or "\u7ebf\u7d22")


def _match_breakdown(
    direct_hits: list[str],
    alias_hits: list[str],
    sub_hits: list[str],
    product_hits: list[str],
    theme_hits: list[str],
    factor_hits: list[str],
    hook_hits: list[str],
    authority_hits: list[str],
    macro_hits: list[str],
    announcement_score: int,
    source_score: int,
    scenario_bonus: int,
) -> dict[str, int]:
    return {
        "direct": len(direct_hits),
        "alias": len(alias_hits),
        "subsidiary": len(sub_hits),
        "product": len(product_hits),
        "theme": len(theme_hits),
        "factor": len(factor_hits),
        "narrative": len(hook_hits),
        "authority": len(authority_hits),
        "macro": len(macro_hits),
        "announcement": max(0, announcement_score),
        "scenario": scenario_bonus,
        "source": max(0, source_score),
    }


def _hit_mode(
    direct_hits: list[str],
    alias_hits: list[str],
    sub_hits: list[str],
    product_hits: list[str],
    theme_hits: list[str],
    factor_hits: list[str],
    hook_hits: list[str],
    authority_hits: list[str],
    macro_hits: list[str],
    family: str,
) -> str:
    if direct_hits:
        return "\u76f4\u63a5\u5173\u8054"
    if alias_hits or sub_hits or product_hits:
        return "\u534a\u663e\u5f0f\u5173\u8054"
    if family == "narrative" or hook_hits:
        return "\u9690\u5f0f\u8054\u60f3"
    if family == "chain" and (theme_hits or factor_hits):
        return "\u4ea7\u4e1a\u94fe\u6620\u5c04"
    if family == "macro" and (macro_hits or authority_hits or factor_hits or theme_hits):
        return "\u5b8f\u89c2\u6620\u5c04"
    if authority_hits and family == "fundamental":
        return "\u5b8f\u89c2\u6620\u5c04"
    return "\u5019\u9009\u89c2\u5bdf"


def score_item(profile: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    content = " ".join(
        [
            str(item.get("title", "")),
            str(item.get("summary", "")),
            str(item.get("rawText", "")),
            str(item.get("effectiveText", "")),
        ]
    )
    item_evidence_profile = item.get("evidenceProfile") or evidence_profile(item.get("evidenceDetails", []))
    direct_hits = keyword_hits([profile.get("name", ""), profile.get("code", "")], content)
    alias_hits = keyword_hits(profile.get("aliases", []), content)
    sub_hits = keyword_hits(profile.get("subsidiaries", []), content)
    product_hits = keyword_hits(profile.get("products", []), content)
    theme_hits = keyword_hits(profile.get("themes", []), content)
    factor_hits = keyword_hits(profile.get("sensitiveFactors", []), content)
    hook_hits = keyword_hits(profile.get("narrativeHooks", []), content)
    authority_hits = keyword_hits(profile.get("policyAuthorities", []), content)
    macro_hits = keyword_hits(MACRO_TERMS, content)
    priority = announcement_priority(item.get("title", ""))
    title_adjustment = title_signal_adjustment(item.get("title", ""))
    source_score = int(source_setting(str(item.get("sourceType", "")), "sourceScore", 0) or 0)
    route_fit_score = source_route_fit_score(profile, str(item.get("sourceType", "")))
    scenario_bonus = 4 if item.get("queryMode") == "scenario" else 0
    policy_bonus = 8 if item.get("queryMode") == "policy" else 0
    title_text = str(item.get("title", ""))
    mapped_only = not (direct_hits or alias_hits or sub_hits or product_hits)
    mapping_adjustment = mapping_signal_adjustment(
        profile,
        item,
        mapped_only,
        title_text,
        theme_hits,
        factor_hits,
        authority_hits,
        macro_hits,
        item_evidence_profile,
    )
    announcement_adjustment = announcement_signal_adjustment(item, title_text, item_evidence_profile)

    score = 0
    score += len(direct_hits) * 16
    score += len(alias_hits) * 13
    score += len(sub_hits) * 11
    score += len(product_hits) * 10
    score += min(16, len(theme_hits) * 5)
    score += min(16, len(factor_hits) * 5)
    score += min(10, len(hook_hits) * 3)
    score += min(16, len(authority_hits) * 5)
    if macro_hits and (theme_hits or factor_hits or authority_hits):
        score += min(12, 4 + (len(macro_hits) * 2))
    score += priority["score"] + title_adjustment["score"] + scenario_bonus + policy_bonus + source_score + route_fit_score
    score += int(mapping_adjustment["score"])
    score += int(announcement_adjustment["score"])
    score += min(10, int(item_evidence_profile.get("factDensity", 0)) // 10)
    score += min(8, len(item.get("evidenceDetails", [])) * 2)

    family = signal_family(item, profile, content)
    hit_mode = _hit_mode(direct_hits, alias_hits, sub_hits, product_hits, theme_hits, factor_hits, hook_hits, authority_hits, macro_hits, family)
    tier = relation_tier_profile(hit_mode)
    paths = impact_paths(profile, content, family, hit_mode)

    reasons: list[str] = []
    if priority["reason"]:
        reasons.append(priority["reason"])
    if title_adjustment["reason"]:
        reasons.append(title_adjustment["reason"])
    if source_score >= 18:
        reasons.append(SOURCE_CATALOG.get(str(item.get("sourceType", "")), {}).get("sourceReason", "\u6765\u81ea\u9ad8\u53ef\u4fe1\u6765\u6e90\u3002"))
    if route_fit_score >= 6:
        reasons.append("\u6765\u6e90\u4e0e\u80a1\u7968\u753b\u50cf\u5339\u914d\u5ea6\u8f83\u9ad8\u3002")
    reasons.extend(mapping_adjustment["reasons"])
    reasons.extend(announcement_adjustment["reasons"])
    reasons.append(tier["reason"])
    if theme_hits:
        reasons.append(f"\u547d\u4e2d\u4e3b\u9898\u8bcd\uff1a{'?'.join(theme_hits[:3])}")
    if factor_hits:
        reasons.append(f"\u547d\u4e2d\u654f\u611f\u56e0\u5b50\uff1a{'?'.join(factor_hits[:3])}")

    matched_entities = list(dict.fromkeys(direct_hits + alias_hits + sub_hits + product_hits + theme_hits + factor_hits + hook_hits + authority_hits))[:12]
    tags = list(dict.fromkeys(theme_hits + factor_hits + authority_hits + macro_hits))[:8]
    breakdown = _match_breakdown(
        direct_hits,
        alias_hits,
        sub_hits,
        product_hits,
        theme_hits,
        factor_hits,
        hook_hits,
        authority_hits,
        macro_hits,
        priority["score"],
        source_score,
        scenario_bonus + policy_bonus,
    )

    return {
        "score": score,
        "reasons": list(dict.fromkeys([reason for reason in reasons if reason]))[:8],
        "matchedEntities": matched_entities,
        "signalFamily": family,
        "hitMode": hit_mode,
        "relationTier": tier["key"],
        "relationTierLabel": tier["label"],
        "relationTierRank": tier["rank"],
        "relationReason": tier["reason"],
        "tags": tags,
        "evidenceTypes": list(item_evidence_profile.get("typeLabels", [])),
        "evidenceTypeCounts": list(item_evidence_profile.get("typeCounts", [])),
        "factDensity": int(item_evidence_profile.get("factDensity", 0)),
        "paths": paths,
        "breakdown": breakdown,
    }


def event_key(profile: dict[str, Any], item: dict[str, Any]) -> str:
    for term in profile.get("products", []) + profile.get("subsidiaries", []) + profile.get("aliases", []) + MACRO_TERMS:
        term = str(term).strip()
        if term and term in str(item.get("title", "")):
            return f"{profile['code']}:{term}"
    clean_title = str(item.get("title", ""))
    clean_title = clean_title.replace(profile.get("name", ""), "")
    clean_title = re.sub(r"\(\d{6}(\.[A-Z]{2})?\)", "", clean_title)
    clean_title = re.sub(r"[-|?].*$", "", clean_title)
    clean_title = re.sub(r"[\s\[\](){}<>,.:;!?]+", "", clean_title)
    clean_title = clean_title[:28] or str(item.get("title", ""))
    return f"{profile['code']}:{clean_title}"


def _count_payload(values: list[dict[str, Any]], *, labeler: Any | None = None) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for item in values:
        key = str(item.get("value", "")).strip()
        if not key:
            continue
        payload.append(
            {
                "key": key,
                "value": labeler(key) if labeler else key,
                "count": int(item.get("count", 0)),
            }
        )
    return payload


def overview_payload(profile: dict[str, Any], scored_items: list[dict[str, Any]], sorted_events: list[dict[str, Any]]) -> dict[str, Any]:
    family_counts = _count_payload(top_occurrence_values([event.get("signalFamily", "") for event in sorted_events], take=8))
    relation_counts = _count_payload(
        top_occurrence_values([event.get("relationTier", "") for event in sorted_events], take=8),
        labeler=relation_tier_label,
    )
    top_tags = _count_payload(top_occurrence_values([item.get("tags", []) for item in scored_items], take=8))
    top_sites = _count_payload(
        top_occurrence_values(
            [str(item.get("sourceSite", "")).strip() or str(item.get("sourceLabel", "")).strip() for item in scored_items],
            take=6,
        )
    )
    top_sources = _count_payload(top_occurrence_values([event.get("sourceLabels", []) for event in sorted_events], take=5))
    top_paths = _count_payload(top_occurrence_values([event.get("paths", []) for event in sorted_events], take=6))
    top_evidence_types = _count_payload(
        top_occurrence_values([item.get("evidenceTypes", []) for item in scored_items], take=6),
        labeler=evidence_type_label,
    )

    timeline_map: dict[str, int] = {}
    source_values: set[str] = set()
    for item in scored_items:
        published_date = str(item.get("publishedAt", ""))[:10]
        if published_date:
            timeline_map[published_date] = timeline_map.get(published_date, 0) + 1
        source_value = str(item.get("sourceSite", "")).strip() or str(item.get("sourceLabel", "")).strip()
        if source_value:
            source_values.add(source_value)

    timeline = [{"date": date, "count": count} for date, count in sorted(timeline_map.items())]
    scenario_count = len([item for item in scored_items if item.get("queryMode") == "scenario"])

    return {
        "dominantFamily": family_counts[0]["key"] if family_counts else "",
        "dominantRelation": relation_counts[0]["key"] if relation_counts else "",
        "familyCounts": family_counts,
        "relationCounts": relation_counts,
        "topTags": top_tags,
        "topSites": top_sites,
        "topSources": top_sources,
        "topPaths": top_paths,
        "topEvidenceTypes": top_evidence_types,
        "timeline": timeline,
        "scenarioHits": scenario_count,
        "sourceCount": len(source_values),
    }
