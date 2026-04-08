from __future__ import annotations

import html
import re
from collections import Counter
from datetime import datetime
from typing import Iterable
from urllib.parse import urljoin

from .config import MACRO_TERMS


_NOISE_RE = re.compile(
    r"登录新浪财经app|24小时滚动播报|扫描二维码关注|新浪简介|广告服务|aboutsina|联系我们|招聘信息|"
    r"通行证注册|产品答疑|网站律师|sinaenglish|版权所有|allrightsreserved|copyright|"
    r"返回顶部|点击查看原文|打开app|返回搜狐|查看更多|官方账号|21健讯daily|欢迎与21世纪经济报道新健康团队共同关注|"
    r"^第\d+页$|^第\d+页共\d+页$|^\d+/\d+$|^page\d+$"
)
_TAG_RE = re.compile(r"(?is)<[^>]+>")
_PAGE_MARKER_ONLY_RE = re.compile(
    r"^\s*(?:page\s*\d+|\d+\s*/\s*\d+|\u7b2c\s*\d+\s*\u9875(?:\s*/\s*\u5171?\s*\d+\s*\u9875)?)\s*$",
    re.I,
)
_LEADING_PAGE_MARKER_RE = re.compile(
    r"^\s*(?:page\s*\d+[:：]?\s*|\d+\s*/\s*\d+\s*|\u7b2c\s*\d+\s*\u9875(?:\s*/\s*\u5171?\s*\d+\s*\u9875)?\s*)",
    re.I,
)
_ANNOUNCEMENT_HEADER_TOKEN_RE = re.compile(
    r"(?:"
    r"\u8bc1\u5238\u4ee3\u7801|\u8bc1\u5238\u7b80\u79f0|\u516c\u544a\u7f16\u53f7|"
    r"\u80a1\u7968\u4ee3\u7801|\u80a1\u7968\u7b80\u79f0|\u516c\u53f8\u4ee3\u7801|\u516c\u53f8\u7b80\u79f0|"
    r"\u503a\u5238\u4ee3\u7801|\u503a\u5238\u7b80\u79f0|\u4e0a\u5e02\u5730\u70b9"
    r")[:：]",
    re.I,
)
_DISCLOSURE_DISCLAIMER_RE = re.compile(
    r"^(?:"
    r"\u672c\u516c\u53f8\u8463\u4e8b\u4f1a\u53ca\u5168\u4f53\u8463\u4e8b"
    r"|\u672c\u516c\u53f8\u53ca\u8463\u4e8b\u4f1a\u5168\u4f53\u6210\u5458"
    r").{0,120}(?:"
    r"\u4e0d\u5b58\u5728\u4efb\u4f55\u865a\u5047\u8bb0\u8f7d"
    r"|\u8bef\u5bfc\u6027\u9648\u8ff0"
    r"|\u91cd\u5927\u9057\u6f0f"
    r"|\u627f\u62c5(?:\u4e2a\u522b\u53ca\u8fde\u5e26)?\u6cd5\u5f8b\u8d23\u4efb"
    r").*$",
    re.I,
)
_SHORT_BOILERPLATE_RE = re.compile(
    r"^(?:"
    r"\u7279\u6b64\u516c\u544a"
    r"|\u98ce\u9669\u63d0\u793a"
    r"|\u656c\u8bf7\u5e7f\u5927\u6295\u8d44\u8005(?:\u7406\u6027\u6295\u8d44)?"
    r"|\u6295\u8d44\u8005(?:\u7406\u6027)?\u6295\u8d44(?:\u6ce8\u610f)?\u98ce\u9669"
    r")[\u3002\uff01\uff1b;! ]*$",
    re.I,
)
_WARNING_SENTENCE_RE = re.compile(
    r"(?:"
    r"\u98ce\u9669\u63d0\u793a|\u656c\u8bf7\u5e7f\u5927\u6295\u8d44\u8005|\u6295\u8d44\u98ce\u9669|"
    r"\u5b58\u5728\u4e0d\u786e\u5b9a\u6027|\u9ad8\u98ce\u9669|\u5c1a\u5b58\u5728\u4e0d\u786e\u5b9a\u6027"
    r")",
    re.I,
)
_FACT_SIGNAL_RE = re.compile(
    r"(?:"
    r"\u83b7\u6279|\u6279\u51c6|\u4e2d\u6807|\u8ba2\u5355|\u5408\u540c|\u7b7e\u7f72|\u5408\u4f5c|\u6388\u6743|"
    r"\u6269\u4ea7|\u6295\u4ea7|\u505c\u4ea7|\u56de\u8d2d|\u51cf\u6301|\u8bc9\u8bbc|\u5904\u7f5a|\u5236\u88c1|"
    r"\u5173\u7a0e|\u51b2\u7a81|\u6218\u4e89|\u505c\u706b|\u590d\u822a|\u6062\u590d|\u96c6\u91c7|\u533b\u4fdd|"
    r"\u4e34\u5e8a|\u5ba1\u6279|\u51fa\u53e3|\u8fdb\u53e3|\u8865\u8d34|\u6536\u5165|\u5229\u6da6|\u589e\u957f|"
    r"\u4e0b\u964d|\u4e8f\u635f|\u626d\u4e8f|\u73b0\u91d1\u5206\u7ea2"
    r")",
    re.I,
)
_QUANT_SIGNAL_RE = re.compile(
    r"(?:\d+(?:\.\d+)?%|\d+(?:\.\d+)?(?:\u4ebf\u5143|\u4e07\u4ebf|\u4e07\u7f8e\u5143|\u7f8e\u5143|\u4e07\u80a1|\u4ebf\u80a1|\u4e07\u53f0))",
    re.I,
)
_CHECKBOX_MARKER_RE = re.compile(
    r"(?:[□■☑√]\s*\u9002\u7528\s*[□■☑√]\s*\u4e0d\u9002\u7528|[□■☑√]\s*\u4e0d\u9002\u7528\s*[□■☑√]\s*\u9002\u7528)",
    re.I,
)
_SECTION_HEADING_RE = re.compile(
    r"(?:\u7b2c[\u4e00-\u5341\d]+\u8282|\u7ecf\u8425\u60c5\u51b5\u8ba8\u8bba\u4e0e\u5206\u6790|\u7ba1\u7406\u5c42\u8ba8\u8bba\u4e0e\u5206\u6790)",
    re.I,
)
_FUND_WRAPPER_MARKER_RE = re.compile(
    r"(?:ETF|LOF|REIT|\u57fa\u91d1|\u8054\u63a5\u57fa\u91d1|\u6807\u7684\u6307\u6570|\u7ba1\u7406\u8d39)",
    re.I,
)
_SIGNATURE_BLOCK_RE = re.compile(
    r"(?:\u7279\u6b64\u516c\u544a.*(?:\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a).*(?:20\d{2}\u5e74|\d{4}-\d{2}-\d{2}))",
    re.I,
)
_INTRO_NOISE_RE = re.compile(
    r"^(?:\u8bc1\u5238\u4e4b\u661f\u6d88\u606f|\u8463\u79d8\u56de\u7b54|\u6295\u8d44\u8005(?:\u63d0\u95ee)?[:：])",
    re.I,
)



_PROCEDURAL_LEGAL_OPINION_RE = re.compile(
    r"(?:"
    r"\u672c\u6240\u5f8b\u5e08\u8ba4\u4e3a|\u5f8b\u5e08\u8ba4\u4e3a|\u5f8b\u5e08\u7684\u6cd5\u5f8b\u610f\u89c1|"
    r"\u6cd5\u5f8b\u610f\u89c1(?:\u4e66)?|\u5df2\u5c31\u672c\u6b21.*?\u5c65\u884c\u4e86\u5fc5\u8981\u7684\u5ba1\u6279\u7a0b\u5e8f|"
    r"\u7b26\u5408\u300a.*?(?:\u7ba1\u7406\u529e\u6cd5|\u6fc0\u52b1\u8ba1\u5212|\u516c\u53f8\u7ae0\u7a0b).*?\u76f8\u5173\u89c4\u5b9a"
    r")",
    re.I,
)
_LOW_SIGNAL_EQUITY_SENTENCE_RE = re.compile(
    r"(?:"
    r"(?:\u80a1\u7968\u671f\u6743\u6fc0\u52b1\u8ba1\u5212|\u9650\u5236\u6027\u80a1\u7968\u6fc0\u52b1\u8ba1\u5212|\u6fc0\u52b1\u5bf9\u8c61).{0,24}"
    r"(?:\u6ce8\u9500|\u884c\u6743|\u56de\u8d2d\u6ce8\u9500|\u9884\u7559\u6388\u4e88|\u81ea\u4e3b\u884c\u6743\u6a21\u5f0f)|"
    r"(?:\u56de\u8d2d\u6ce8\u9500).{0,12}(?:\u9650\u5236\u6027\u80a1\u7968|\u80a1\u7968\u671f\u6743)"
    r")",
    re.I,
)
_PROCEDURAL_APPROVAL_RE = re.compile(
    r"(?:"
    r"\u5c1a\u987b\u83b7\u5f97.*?(?:\u6279\u51c6|\u5ba1\u8bae)|"
    r"\u63d0\u4ea4.*?(?:\u80a1\u4e1c\u5927\u4f1a|\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a).{0,8}\u5ba1\u8bae|"
    r"\u5173\u8054\u4eba\u5c06\u653e\u5f03\u884c\u4f7f.*?\u6295\u7968\u6743"
    r")",
    re.I,
)



def split_text_units(text: str | None) -> list[str]:
    clean = html_decode(text).replace("\r", "\n")
    clean = re.sub(r"\n{2,}", "\n", clean)
    if not clean.strip():
        return []
    return [piece for piece in re.split(r"(?:\n+|(?<=[。！？!?；;]))", clean) if piece and piece.strip()]


def html_decode(text: str | None) -> str:
    if not text:
        return ""
    return html.unescape(text)


def normalize_text(text: str | None) -> str:
    value = html_decode(text).lower()
    return re.sub(r"\s+", "", value)


def strip_html(raw_html: str | None) -> str:
    if not raw_html:
        return ""
    clean = re.sub(r"(?is)<script\b.*?</script>", " ", raw_html)
    clean = re.sub(r"(?is)<style\b.*?</style>", " ", clean)
    clean = re.sub(r"(?is)<!--.*?-->", " ", clean)
    clean = _TAG_RE.sub(" ", clean)
    clean = html_decode(clean)
    clean = re.sub(r"[ \t]+", " ", clean)
    clean = re.sub(r"(\r?\n\s*){2,}", "\n", clean)
    return clean.strip()


def resolve_absolute_url(base_url: str, url: str | None) -> str:
    value = html_decode(url).strip()
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    try:
        return urljoin(base_url, value)
    except Exception:
        return value


def date_only_string(timestamp: str | None) -> str:
    if not timestamp:
        return ""
    return timestamp[:10]


def in_date_range(timestamp: str | None, from_date: str, to_date: str) -> bool:
    day = date_only_string(timestamp)
    if not day:
        return False
    if from_date and day < from_date:
        return False
    if to_date and day > to_date:
        return False
    return True


def unix_ms_to_local_time(value: int | str | None) -> str:
    try:
        return datetime.fromtimestamp(int(value) / 1000).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def month_day_time_to_local_timestamp(text: str | None) -> str:
    value = re.sub(r"\s+", " ", text or "").strip()
    if not value:
        return ""

    match = re.match(r"^(?P<month>\d{1,2})月(?P<day>\d{1,2})日\s+(?P<time>\d{1,2}:\d{2})$", value)
    if match:
        now = datetime.now()
        try:
            hour, minute = [int(part) for part in match.group("time").split(":")]
            candidate = datetime(
                year=now.year,
                month=int(match.group("month")),
                day=int(match.group("day")),
                hour=hour,
                minute=minute,
            )
            if candidate > now.replace(microsecond=0) and (candidate - now).days > 31:
                candidate = candidate.replace(year=candidate.year - 1)
            return candidate.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return ""

    match = re.match(r"^(?P<date>20\d{2}-\d{2}-\d{2})\s+(?P<time>\d{1,2}:\d{2})$", value)
    if match:
        return f"{match.group('date')} {match.group('time')}"
    return ""


def url_path_date_to_timestamp(url: str | None) -> str:
    if not url:
        return ""
    match = re.search(r"/(?P<year>20\d{2})/(?P<month>\d{2})/(?P<day>\d{2})/", url)
    if not match:
        return ""
    return f"{match.group('year')}-{match.group('month')}-{match.group('day')} 00:00"








def repair_pdf_plain_text(text: str | None) -> str:
    if not text:
        return ""
    clean = text.replace("\xa0", " ").replace("\r", "")
    clean = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", clean)
    clean = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[A-Za-z0-9])", "", clean)
    clean = re.sub(r"(?<=[A-Za-z0-9])\s+(?=[\u4e00-\u9fff])", "", clean)
    clean = re.sub(r"(?<=\d)\s+(?=\d)", "", clean)
    clean = re.sub(r"(?<=[A-Za-z])\s+(?=[A-Za-z])", "", clean)
    clean = re.sub(r"\s*([:/.\-+()%])\s*", r"\1", clean)
    lines: list[str] = []
    for line in clean.split("\n"):
        value = clean_extracted_text(re.sub(r"\s{2,}", " ", line).strip())
        if value:
            lines.append(value)
    return "\n".join(lines).strip()


def test_noise_paragraph(text: str | None) -> bool:
    value = clean_extracted_text(text)
    if len(value) < 6:
        return True
    if is_boilerplate_sentence(value):
        return True
    return bool(_NOISE_RE.search(normalize_text(value)))


def add_paragraph_text(paragraphs: list[str], seen: set[str], text: str, min_length: int = 18) -> None:
    value = clean_extracted_text(text)
    if len(value) < min_length or test_noise_paragraph(value):
        return
    key = normalize_text(value)
    if not key or key in seen:
        return
    seen.add(key)
    paragraphs.append(value)


def paragraphs_from_html(raw_html: str | None) -> list[str]:
    clean = re.sub(r"(?is)<script\b.*?</script>", " ", raw_html or "")
    clean = re.sub(r"(?is)<style\b.*?</style>", " ", clean)
    clean = re.sub(r"(?is)<!--.*?-->", " ", clean)
    paragraphs: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"(?is)<p[^>]*>(.*?)</p>", clean):
        add_paragraph_text(paragraphs, seen, strip_html(match.group(1)), min_length=14)
    if paragraphs:
        return paragraphs

    fallback = strip_html(clean)
    for piece in split_text_units(fallback):
        add_paragraph_text(paragraphs, seen, piece, min_length=18)
        if len(paragraphs) >= 12:
            break
    return paragraphs


def paragraphs_from_plain_text(text: str | None) -> list[str]:
    clean = html_decode(text).replace("\r", "")
    clean = re.sub(r"[\t ]{2,}", " ", clean)
    clean = re.sub(r"\n{2,}", "\n", clean)
    paragraphs: list[str] = []
    seen: set[str] = set()
    for block in clean.split("\n"):
        add_paragraph_text(paragraphs, seen, block, min_length=18)
    if paragraphs:
        return paragraphs
    for piece in split_text_units(clean):
        add_paragraph_text(paragraphs, seen, piece, min_length=18)
    return paragraphs


def sentences_from_text(text: str | None) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for piece in split_text_units(text):
        sentence = clean_extracted_text(piece)
        if len(sentence) < 12 or len(sentence) > 180 or test_noise_paragraph(sentence):
            continue
        key = normalize_text(sentence)
        if key and key not in seen:
            seen.add(key)
            results.append(sentence)
    return results




def keyword_hits(terms: Iterable[str], text: str | None) -> list[str]:
    normalized_text = normalize_text(text)
    hits: list[str] = []
    for term in terms:
        candidate = normalize_text(term)
        if not candidate:
            continue
        if len(candidate) < 2 and not re.fullmatch(r"\d{6}", candidate):
            continue
        if candidate in normalized_text and term not in hits:
            hits.append(term)
    return hits


def evidence_type_label(kind: str) -> str:
    return {
        "fact": "事实",
        "quant": "量化",
        "policy": "政策",
        "path": "传导",
        "impact": "影响",
        "narrative": "叙事",
    }.get(kind, "线索")


def evidence_type_profile(sentence: str) -> dict[str, object]:
    text = sentence or ""
    is_quant = bool(re.search(r"\d+(\.\d+)?%|亿元|万亿|万美元|美元|万股|亿股|万吨|万台|例|项|个|批|月|日|年|同比|环比", text))
    is_policy = bool(re.search(r"国家药监局|国家医保局|国家卫健委|工信部|国务院|证监会|国家发改委|商务部|海关总署|征求意见|指导意见|通知|办法|政策|监管|审批|备案|批复|试点", text))
    is_path = bool(re.search(r"导致|带动|拖累|推升|压制|传导|进而|从而|影响到|影响其|受益于|受损于|利于|利空|催化|改善|修复|挤压", text))
    is_impact = bool(re.search(r"预计|预期|有望|可能|或将|意味着|反映出|看好|承压|改善|修复|提振|压制|受压|受益|不利", text))
    is_narrative = bool(re.search(r"概念|题材|情绪|联想|热搜|热度|爆火|传播|短线资金|特朗普|川普|英伟达|NVIDIA", text, re.I))
    is_fact = bool(re.search(r"获批|批准|中标|订单|合同|签署|合作|授权|扩产|投产|停产|回购|减持|诉讼|处罚|制裁|关税|冲突|战争|停火|复航|恢复|集采|医保|临床|审批|出口|进口|补贴|收入|利润|增长|下降|亏损|扭亏|现金分红|公告|披露|发布", text))

    kind = "fact"
    lane = "fact"
    bonus = 3
    if is_policy:
        kind, lane, bonus = "policy", "fact", 7
    elif is_quant:
        kind, lane, bonus = "quant", "fact", 6
    elif is_path:
        kind, lane, bonus = "path", "inference", 5
    elif is_impact:
        kind, lane, bonus = "impact", "inference", 2
    elif is_narrative:
        kind, lane, bonus = "narrative", "narrative", -1
    elif is_fact:
        kind, lane, bonus = "fact", "fact", 4

    return {"type": kind, "label": evidence_type_label(kind), "lane": lane, "bonus": bonus}





_LEADING_ACTOR_RE = re.compile(
    r"^\s*[\"'“‘《]?\s*([\u4e00-\u9fa5A-Za-z0-9·\-]{2,20})(?:\(\d{6}(?:\.[A-Z]{2})?\))?\s*"
    r"(?=(?:\d{1,2}月\d{1,2}日|近日|日前|昨日|今日|在|于|称|表示|指出|披露|公告|发布|宣布|收到|提议|通过|拟|将))",
    re.I,
)
_COMPANY_ACTOR_MARKER_RE = re.compile(
    r"(?:公司|集团|科技|股份|药业|医药|航空|证券|银行|电子|信息|材料|能源|重工|装备|汽车|石化|船舶|半导体|光电|智能|"
    r"生物|医疗|网络|软件|物流|地产|传媒|酒店|旅游|航运|海控|机场|免税)",
    re.I,
)
_GENERIC_LEADING_ACTORS = {
    "公司",
    "本公司",
    "业内",
    "记者",
    "机构",
    "行业",
    "市场",
    "中国",
    "美国",
}
_DOCUMENT_ANCHOR_SENTENCE_RE = re.compile(
    r"(?:公司|本公司|报告期|营业收入|营收|归母净利润|扣非净利润|研发投入|订单|合同|回购|分红|派发现金红利|产能|出货|销量)",
    re.I,
)
_ANALYST_VIEW_SENTENCE_RE = re.compile(r"(?:券商|证券|研报|机构).{0,12}(?:表示|认为|指出)", re.I)


def _unique_clean_terms(*groups: object) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for group in groups:
        values = group if isinstance(group, (list, tuple, set)) else [group]
        for value in values:
            text = str(value).strip()
            if not text:
                continue
            key = normalize_text(text)
            if key and key not in seen:
                seen.add(key)
                results.append(text)
    return results


def _token_matches_any(token: str, values: list[str]) -> bool:
    normalized_token = normalize_text(token)
    if not normalized_token:
        return False
    for value in values:
        normalized_value = normalize_text(value)
        if not normalized_value:
            continue
        if normalized_token == normalized_value:
            return True
        if len(normalized_token) >= 3 and len(normalized_value) >= 3 and (
            normalized_token in normalized_value or normalized_value in normalized_token
        ):
            return True
    return False


def _leading_actor_token(sentence: str) -> str:
    match = _LEADING_ACTOR_RE.match(str(sentence or "").strip())
    if not match:
        return ""
    token = clean_extracted_text(match.group(1))
    if not token:
        return ""
    if token in _GENERIC_LEADING_ACTORS:
        return ""
    if re.fullmatch(r"\d{1,2}月\d{1,2}日", token):
        return ""
    return token


def sentence_attribution_profile(profile: dict, sentence: str, title_context: str = "") -> dict[str, object]:
    text = str(sentence or "").strip()
    title_text = str(title_context or "").strip()
    sentence_identity_hits = _unique_clean_terms(
        keyword_hits([profile.get("name", ""), profile.get("code", "")], text),
        keyword_hits(profile.get("aliases", []), text),
        keyword_hits(profile.get("subsidiaries", []), text),
        keyword_hits(profile.get("products", []), text),
    )
    title_identity_hits = _unique_clean_terms(
        keyword_hits([profile.get("name", ""), profile.get("code", "")], title_text),
        keyword_hits(profile.get("aliases", []), title_text),
        keyword_hits(profile.get("subsidiaries", []), title_text),
        keyword_hits(profile.get("products", []), title_text),
    )
    theme_hits = _unique_clean_terms(keyword_hits(profile.get("themes", []), text))
    factor_hits = _unique_clean_terms(keyword_hits(profile.get("sensitiveFactors", []), text))
    hook_hits = _unique_clean_terms(keyword_hits(profile.get("narrativeHooks", []), text))
    authority_hits = _unique_clean_terms(keyword_hits(profile.get("policyAuthorities", []), text))
    macro_hits = _unique_clean_terms(keyword_hits(MACRO_TERMS, text))
    mapping_hits = _unique_clean_terms(theme_hits, factor_hits, hook_hits, authority_hits, macro_hits)

    identity_terms = _unique_clean_terms(
        [profile.get("name", ""), profile.get("code", "")],
        profile.get("aliases", []),
        profile.get("subsidiaries", []),
        profile.get("products", []),
    )
    mapping_terms = _unique_clean_terms(theme_hits, factor_hits, hook_hits, authority_hits, macro_hits)
    leading_actor = _leading_actor_token(text)
    leading_actor_is_identity = _token_matches_any(leading_actor, identity_terms)
    leading_actor_is_mapping = _token_matches_any(leading_actor, mapping_terms)
    leading_actor_conflict = bool(
        leading_actor
        and not leading_actor_is_identity
        and not leading_actor_is_mapping
        and (
            _COMPANY_ACTOR_MARKER_RE.search(leading_actor)
            or re.fullmatch(r"[\u4e00-\u9fa5]{3,8}", leading_actor)
        )
    )

    has_signal = bool(_FACT_SIGNAL_RE.search(text) or _OFFICIAL_ACTION_RE.search(text) or _QUANT_SIGNAL_RE.search(text))
    document_anchor = bool(title_identity_hits)
    hard_anchor = bool(keyword_hits([profile.get("name", ""), profile.get("code", "")], text) or keyword_hits(profile.get("aliases", []), text))
    semi_anchor = bool(keyword_hits(profile.get("subsidiaries", []), text) or keyword_hits(profile.get("products", []), text))
    analyst_view = bool(_ANALYST_VIEW_SENTENCE_RE.search(text))

    allow = False
    if hard_anchor or semi_anchor:
        allow = True
    elif document_anchor and not leading_actor_conflict:
        if has_signal or _DOCUMENT_ANCHOR_SENTENCE_RE.search(text) or len(mapping_hits) >= 1:
            allow = True
    elif not leading_actor_conflict:
        if len(mapping_hits) >= 3:
            allow = True
        elif len(mapping_hits) >= 2 and (has_signal or authority_hits):
            allow = True
        elif factor_hits and macro_hits and has_signal:
            allow = True

    if analyst_view and not (hard_anchor or semi_anchor or document_anchor):
        allow = False
    if analyst_view and leading_actor and not leading_actor_is_identity:
        allow = False

    bonus = 0
    if hard_anchor:
        bonus += 12
    elif semi_anchor:
        bonus += 8
    elif document_anchor:
        bonus += 5
    elif len(mapping_hits) >= 3:
        bonus += 2
    if leading_actor_conflict:
        bonus -= 24

    return {
        "allow": allow,
        "bonus": bonus,
        "leadingActor": leading_actor,
        "leadingActorConflict": leading_actor_conflict,
        "hardAnchor": hard_anchor,
        "semiAnchor": semi_anchor,
        "documentAnchor": document_anchor,
        "identityHits": sentence_identity_hits,
        "titleIdentityHits": title_identity_hits,
        "mappingHits": mapping_hits,
        "hasSignal": has_signal,
    }



_ANALYST_OPINION_RE = re.compile(
    r"(?:"
    r"\u6295\u8d44\u5efa\u8bae|\u76c8\u5229\u9884\u6d4b|\u5bf9\u5e94PE|\u5bf9\u5e94PB|"
    r"\u7ef4\u6301.*?\u8bc4\u7ea7|\u9996\u6b21\u8986\u76d6|\u4e70\u5165\u8bc4\u7ea7|\u589e\u6301\u8bc4\u7ea7|"
    r"\u63a8\u8350\u8bc4\u7ea7|\u98ce\u9669\u63d0\u793a"
    r")",
    re.I,
)
_IR_PLATFORM_NOISE_RE = re.compile(
    r"(?:"
    r"\u8bc1\u5238\u4e4b\u661f\u6d88\u606f|\u6295\u8d44\u8005\u5173\u7cfb\u5e73\u53f0|"
    r"\u5728\u6295\u8d44\u8005\u5173\u7cfb\u5e73\u53f0\u4e0a\u7b54\u590d|\u7b54\u590d\u6295\u8d44\u8005"
    r")",
    re.I,
)
_OFFICIAL_ACTION_RE = re.compile(
    r"(?:"
    r"\u6536\u5230.*?(?:\u901a\u77e5\u4e66|\u6279\u51c6|\u6838\u51c6|\u6838\u51c6\u7b7e\u53d1)|"
    r"\u83b7\u5f97.*?(?:\u6279\u51c6|\u8d44\u683c\u8ba4\u5b9a|\u53d7\u7406|\u8bb8\u53ef)|"
    r"\u6838\u51c6\u7b7e\u53d1|\u6279\u51c6\u5f00\u5c55\u4e34\u5e8a\u8bd5\u9a8c|\u4e2d\u6807|\u7b7e\u7f72\u5408\u540c"
    r")",
    re.I,
)
_DIRECT_RISK_RE = re.compile(
    r"(?:"
    r"\u5b58\u5728\u4e0d\u786e\u5b9a\u6027|\u5c1a\u5b58\u5728\u4e0d\u786e\u5b9a\u6027|\u656c\u8bf7\u6295\u8d44\u8005|"
    r"\u6ce8\u610f\u6295\u8d44\u98ce\u9669|\u98ce\u9669\u8f83\u9ad8"
    r")",
    re.I,
)





_SITE_DISCLAIMER_RE = re.compile(
    r"(?:"
    r"\u65b0\u6d6a\u8d22\u7ecf\u514d\u8d39\u63d0\u4f9b|\u8d44\u6599\u5747\u6765\u81ea\u76f8\u5173\u5408\u4f5c\u65b9|"
    r"\u4ec5\u4f5c\u4e3a\u7528\u6237\u83b7\u53d6\u4fe1\u606f\u4e4b\u76ee\u7684|\u5e76\u4e0d\u6784\u6210"
    r")",
    re.I,
)
_REPORT_META_RE = re.compile(
    r"(?:"
    r"\u516c\u53f8\u5e74\u5ea6\u62a5\u544a\u5907\u7f6e\u5730\u70b9|\u516c\u53f8\u80a1\u7968\u7b80\u51b5|"
    r"\u80a1\u7968\u79cd\u7c7b\u80a1\u7968\u4e0a\u5e02\u4ea4\u6613\u6240\u80a1\u7968\u7b80\u79f0\u80a1\u7968\u4ee3\u7801|"
    r"\u7b2c\u4e8c\u8282\u516c\u53f8\u57fa\u672c\u60c5\u51b5|\u516c\u53f8\u7b80\u4ecb"
    r")",
    re.I,
)
_GLOSSARY_TABLE_RE = re.compile(
    r"(?:"
    r"\u5e38\u7528\u8bcd\u8bed\u91ca\u4e49|\u516c\u53f8\u6307|\u62a5\u544a\u671f\u6307|"
    r"\u6267\u884c\u4e8b\u52a1\u5408\u4f19\u4eba|\u6210\u7acb\u65e5\u671f|\u6ce8\u518c\u8d44\u672c|\u5b9e\u7f34\u8d44\u672c|"
    r"\u6ce8\u518c\u5730\u5740|\u4e3b\u8981\u529e\u516c\u5730\u5740|\u4e3b\u8981\u80a1\u4e1c|\u6301\u6709\u4efd\u989d"
    r")",
    re.I,
)
_CONDITIONAL_INFERENCE_RE = re.compile(
    r"(?:"
    r"\u82e5.*?\u5c06\u6709\u5229\u4e8e|\u6709\u671b|\u53ef\u80fd|\u9884\u8ba1|\u6216\u5c06"
    r")",
    re.I,
)
_PRIVATE_USE_CHAR_RE = re.compile(r"[\ue000-\uf8ff]")
_PAGE_RESIDUE_RE = re.compile(r"(?:\b\d{1,4}/\d{1,4}\b|\u5e74\u5ea6\u62a5\u544a\d{1,4}/\d{1,4})", re.I)
_MARKET_WIDGET_RE = re.compile(
    r"(?:"
    r"\u65b0\u6d6a\u8d22\u7ecf_\u65b0\u6d6a\u7f51|\u516c\u53f8\u516c\u544a_|"
    r"\u6628\u6536\u76d8|\u4eca\u5f00\u76d8|\u6700\u9ad8\u4ef7|\u6700\u4f4e\u4ef7|"
    r"\u4e0a\u8bc1\u6307\u6570|\u6df1\u8bc1\u6210\u6307|\u6caa\u6df1300"
    r")",
    re.I,
)
_PROCEDURAL_SENTENCE_RE = re.compile(
    r"(?:"
    r"\u80a1\u6743\u767b\u8bb0\u65e5|\u5b9e\u65bd\u6743\u76ca\u5206\u6d3e|"
    r"\u5177\u4f53\u65e5\u671f\u5c06\u5728.*?\u516c\u544a\u4e2d\u660e\u786e|"
    r"\u5ba1\u6279\u53ca\u5176\u4ed6\u76f8\u5173\u7a0b\u5e8f|\u5c65\u884c.*?\u5ba1\u8bae\u7a0b\u5e8f|"
    r"\u63d0\u4ea4\u516c\u53f8\u80a1\u4e1c(?:\u5927\u4f1a|\u4f1a)\u5ba1\u8bae|"
    r"\u65e0\u987b\u63d0\u4ea4\u516c\u53f8\u80a1\u4e1c(?:\u5927\u4f1a|\u4f1a)\u5ba1\u8bae|"
    r"\u4e0d\u9700\u8981\u7ecf\u8fc7\u6709\u5173\u90e8\u95e8\u6279\u51c6|"
    r"\u8463\u4e8b\u4f1a\u7b2c.+?\u6b21\u4f1a\u8bae|\u76d1\u4e8b\u4f1a\u7b2c.+?\u6b21\u4f1a\u8bae|"
    r"\u9664\u6743\u9664\u606f\u4e8b\u9879|\u56de\u8d2d\u4ef7\u683c\u4e0a\u9650|\u56de\u8d2d\u4e13\u7528\u8bc1\u5238\u8d26\u6237\u671f\u95f4\u4e0d\u4eab\u53d7\u5229\u6da6\u5206\u914d|"
    r"\u53ca\u65f6\u5c65\u884c\u4fe1\u606f\u62ab\u9732\u4e49\u52a1|\u901a\u77e5\u503a\u6743\u4eba"
    r")",
    re.I,
)
_LOW_VALUE_META_RE = re.compile(
    r"(?:"
    r"\u65f6\u95f4\u8303\u56f4|\u4f1a\u8ba1\u6570\u636e\u5dee\u5f02|\u51c0\u8d44\u4ea7\u5dee\u5f02\u60c5\u51b5|"
    r"\u6cd5\u5f8b\u610f\u89c1\u4e66|H\u80a1\u516c\u544a|\u8bc1\u5238\u53d8\u52a8\u6708\u62a5\u8868"
    r")",
    re.I,
)
_GENERIC_PROMO_RE = re.compile(
    r"(?:"
    r"\u5e7f\u6cdb\u5e03\u5c40|\u6253\u9020\u957f\u671f\u53d1\u5c55|\u6218\u7565\u652f\u67f1|"
    r"\u79ef\u6781\u63a8\u52a8.*?\u843d\u5730|\u6301\u7eed\u62d3\u5c55\u5e02\u573a|"
    r"\u5e02\u573a\u53d1\u5c55\u524d\u666f\u5e7f\u9614|\u5de9\u56fa.*?\u7ade\u4e89\u4f18\u52bf"
    r")",
    re.I,
)
_DIVIDEND_SIGNAL_RE = re.compile(
    r"(?:\u6bcf10\u80a1|\u6bcf\u80a1\u6d3e\u53d1|\u6d3e\u53d1\u73b0\u91d1\u7ea2\u5229|\u8f6c\u589e|\u9001\u7ea2\u80a1)",
    re.I,
)
_SECURITY_HEADER_RE = re.compile(
    r"(?:"
    r"\u8bc1\u5238\u4ee3\u7801[:：]?\d{6}|\u8bc1\u5238\u7b80\u79f0[:：]|\u516c\u544a\u7f16\u53f7[:：]|"
    r"(?:\u8463\u4e8b\u4f1a|\u76d1\u4e8b\u4f1a)20\d{2}\u5e74\d{1,2}\u6708\d{1,2}\u65e5|"
    r"\u7edf\u4e00\u793e\u4f1a\u4fe1\u7528\u4ee3\u7801"
    r")",
    re.I,
)
_FUND_META_RE = re.compile(
    r"(?:"
    r"\u52df\u96c6\u8d44\u91d1\u5b58\u653e|\u7ba1\u7406\u4e0e\u5b9e\u9645\u4f7f\u7528\u60c5\u51b5|"
    r"\u4e13\u6237\u5b58\u50a8|\u56db\u65b9\u76d1\u7ba1\u534f\u8bae|\u4e13\u9879\u6838\u67e5\u610f\u89c1|"
    r"\u6301\u7eed\u7763\u5bfc\u5de5\u4f5c\u73b0\u573a\u68c0\u67e5\u62a5\u544a|\u6301\u7eed\u7763\u5bfc\u5de5\u4f5c\u62a5\u544a"
    r")",
    re.I,
)
_AUDIT_ASSURANCE_RE = re.compile(
    r"(?:"
    r"\u6211\u4eec\u63a5\u53d7\u59d4\u6258\uff0c\u5ba1\u8ba1\u4e86|"
    r"\u51fa\u5177.*?(?:\u5ba1\u8ba1\u62a5\u544a|\u9274\u8bc1\u62a5\u544a|\u4e13\u9879\u8bf4\u660e)|"
    r"\u6d89\u53ca\u8d22\u52a1\u516c\u53f8\u5173\u8054\u4ea4\u6613\u7684\u5b58\u6b3e\u3001\u8d37\u6b3e\u7b49\u91d1\u878d\u4e1a\u52a1|"
    r"\u4e13\u9879\u8bf4\u660e"
    r")",
    re.I,
)


def clean_extracted_text(text: str | None) -> str:
    value = html_decode(text).replace("\xa0", " ").replace("\r", " ").strip()
    if not value:
        return ""
    value = _PRIVATE_USE_CHAR_RE.sub("", value)
    value = _CHECKBOX_MARKER_RE.sub("", value)
    value = _LEADING_PAGE_MARKER_RE.sub("", value)
    value = re.sub(r"\s+", " ", value).strip(" \t|_-")

    if "\u6295\u8d44\u8005" in value and "\u8463\u79d8" in value:
        answer_match = re.search(r"\u8463\u79d8(?:\u56de\u590d|\u7b54\u590d)?[:：]\s*", value)
        if answer_match:
            value = value[answer_match.end() :].strip()
    elif value.startswith("\u8463\u79d8\u56de\u7b54"):
        value = re.sub(r"^\u8463\u79d8\u56de\u7b54[:：]\s*", "", value).strip()

    value = re.sub(r"^(?:\u8bc1\u5238\u4e4b\u661f\u6d88\u606f|[\w\u4e00-\u9fff]+(?:\u6d88\u606f|[\u8bb0\u8005]))[，,:： ]*", "", value)

    if _ANNOUNCEMENT_HEADER_TOKEN_RE.search(value[:80]) and "\u5173\u4e8e" in value:
        value = value[value.index("\u5173\u4e8e") :].strip()
    if _SECTION_HEADING_RE.search(value):
        year_match = re.search(r"20\d{2}\u5e74[，,]", value)
        if year_match and year_match.start() > 20:
            value = value[year_match.start() :].strip()
    if "\u516c\u544a" in value and re.search(r"\u516c\u544a(?=(?:\u8fd1\u65e5|\u65e5\u524d|\u516c\u53f8|\u672c\u516c\u53f8|20\d{2}\u5e74))", value):
        value = re.sub(r"(\u516c\u544a)(?=(?:\u8fd1\u65e5|\u65e5\u524d|\u516c\u53f8|\u672c\u516c\u53f8|20\d{2}\u5e74))", r"\1。", value, count=1)

    for marker in ("\u6295\u8d44\u5efa\u8bae", "\u76c8\u5229\u9884\u6d4b", "\u98ce\u9669\u63d0\u793a"):
        position = value.find(marker)
        if position >= 18 and not _OFFICIAL_ACTION_RE.search(value[:position]):
            value = value[:position].rstrip("，,；;：: ")
            break

    fund_match = re.search(r"[，,；;]\s*[^，,；;。]*?(?:ETF|LOF|REIT|\u57fa\u91d1|\u6807\u7684\u6307\u6570|\u7ba1\u7406\u8d39)", value, re.I)
    if fund_match and fund_match.start() >= 12:
        value = value[: fund_match.start()]

    value = re.sub(r"^[,，。；;:：|_-]+", "", value)
    return re.sub(r"\s{2,}", " ", value).strip()


def is_boilerplate_sentence(text: str | None) -> bool:
    value = clean_extracted_text(text)
    if not value:
        return True
    normalized = normalize_text(value)
    if not normalized:
        return True
    if _PAGE_MARKER_ONLY_RE.fullmatch(value):
        return True
    if _DISCLOSURE_DISCLAIMER_RE.match(value):
        return True
    if _SHORT_BOILERPLATE_RE.match(value):
        return True
    if _SIGNATURE_BLOCK_RE.search(value):
        return True
    if _SITE_DISCLAIMER_RE.search(value):
        return True
    if _REPORT_META_RE.search(value):
        return True
    if _GLOSSARY_TABLE_RE.search(value):
        return True
    if _MARKET_WIDGET_RE.search(value):
        return True
    if _SECURITY_HEADER_RE.search(value):
        return True
    if _PAGE_RESIDUE_RE.search(value) and ("\u5e74\u5ea6\u62a5\u544a" in value or "\u516c\u53f8\u516c\u544a_" in value or "\u4f1a\u8ba1\u51c6\u5219" in value):
        return True
    if _LOW_VALUE_META_RE.search(value) and not (_FACT_SIGNAL_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if _FUND_META_RE.search(value) and not (_OFFICIAL_ACTION_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if _AUDIT_ASSURANCE_RE.search(value) and not (_OFFICIAL_ACTION_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if _PROCEDURAL_LEGAL_OPINION_RE.search(value) and not _QUANT_SIGNAL_RE.search(value):
        return True
    if _PROCEDURAL_APPROVAL_RE.search(value) and not _QUANT_SIGNAL_RE.search(value):
        return True
    if _PROCEDURAL_SENTENCE_RE.search(value) and not (_DIVIDEND_SIGNAL_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if _LOW_SIGNAL_EQUITY_SENTENCE_RE.search(value) and not (_DIVIDEND_SIGNAL_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if _ANNOUNCEMENT_HEADER_TOKEN_RE.search(value) and not _FACT_SIGNAL_RE.search(value):
        return True
    if _INTRO_NOISE_RE.match(value) and (len(value) < 40 or not _FACT_SIGNAL_RE.search(value)):
        return True
    if _IR_PLATFORM_NOISE_RE.search(value) and not (_OFFICIAL_ACTION_RE.search(value) or _FACT_SIGNAL_RE.search(value)):
        return True
    if _ANALYST_OPINION_RE.search(value) and not (_OFFICIAL_ACTION_RE.search(value) or _QUANT_SIGNAL_RE.search(value)):
        return True
    if value.startswith("\u60a8\u597d") and len(value) < 16:
        return True
    return False



_HISTORY_AWARD_SENTENCE_RE = re.compile(
    r"(?:20(?:0\d|1\d|2[0-4])年).{0,40}(?:荣获|获评|入选|优秀创新产品奖|交易会|高交会)",
    re.I,
)
_REPORT_INTRO_SENTENCE_RE = re.compile(
    r"(?:技术地位|全球知名|新兴公司|致力于|能够提供|平台化基础系统软件|具备统一生态|发展历程|历程的缩影)",
    re.I,
)
_REPORT_META_SENTENCE_RE = re.compile(
    r"(?:年度报告摘要|母公司存在未弥补亏损|公司基本情况|股票简况)",
    re.I,
)
_CURRENT_OPERATING_SENTENCE_RE = re.compile(
    r"(?:20(?:2[5-9]|[3-9]\d)年).{0,48}(?:营收|营业收入|净利润|归母净利润|研发投入|订单|出货|销量|同比|环比|增长|下降)",
    re.I,
)
_RELATED_PARTY_META_SENTENCE_RE = re.compile(
    r"(?:关联方基本情况|最近一年又一期财务数据|单位：万元|资产总额|负债总额|所有者权益总额|资产负债率)",
    re.I,
)
_CAPITAL_ACTION_SENTENCE_RE = re.compile(
    r"(?:投资金额|同比例增资|增资人民币|货币出资|关联交易|出资方式)",
    re.I,
)
_CORPORATE_CULTURE_SENTENCE_RE = re.compile(
    r"(?:回顾发展初期|发展轨迹|历程的缩影|深刻认识到|以客户为中心)",
    re.I,
)


def evidence_details(profile: dict, text: str, take: int = 4, title_context: str = "") -> list[dict[str, object]]:
    ranked: list[dict[str, object]] = []
    for sentence in sentences_from_text(text):
        if is_boilerplate_sentence(sentence):
            continue
        attribution = sentence_attribution_profile(profile, sentence, title_context=title_context)
        if not attribution["allow"]:
            continue
        type_profile = evidence_type_profile(sentence)
        score = 0
        score += int(attribution["bonus"])
        score += len(keyword_hits([profile.get("name", ""), profile.get("code", "")], sentence)) * 10
        score += len(keyword_hits(profile.get("aliases", []), sentence)) * 8
        score += len(keyword_hits(profile.get("subsidiaries", []), sentence)) * 7
        score += len(keyword_hits(profile.get("products", []), sentence)) * 7
        score += len(keyword_hits(profile.get("themes", []), sentence)) * 4
        score += len(keyword_hits(profile.get("sensitiveFactors", []), sentence)) * 5
        score += len(keyword_hits(profile.get("policyAuthorities", []), sentence)) * 5
        score += len(keyword_hits(MACRO_TERMS, sentence)) * 3
        score += int(type_profile["bonus"])
        if _FACT_SIGNAL_RE.search(sentence):
            score += 7
        if _OFFICIAL_ACTION_RE.search(sentence):
            score += 10
        if _QUANT_SIGNAL_RE.search(sentence):
            score += 3
        if _CURRENT_OPERATING_SENTENCE_RE.search(sentence):
            score += 6
        if _CAPITAL_ACTION_SENTENCE_RE.search(sentence):
            score += 8
        if _WARNING_SENTENCE_RE.search(sentence) or _DIRECT_RISK_RE.search(sentence):
            score -= 12
        if _CONDITIONAL_INFERENCE_RE.search(sentence):
            score -= 8
        if _ANALYST_OPINION_RE.search(sentence):
            score -= 10
        if _IR_PLATFORM_NOISE_RE.search(sentence):
            score -= 8
        if _DIVIDEND_SIGNAL_RE.search(sentence):
            score += 5
        if _PROCEDURAL_SENTENCE_RE.search(sentence):
            score -= 14
        if _LOW_VALUE_META_RE.search(sentence):
            score -= 12
        if _FUND_META_RE.search(sentence):
            score -= 10
        if _AUDIT_ASSURANCE_RE.search(sentence):
            score -= 14
        if _SECURITY_HEADER_RE.search(sentence):
            score -= 20
        if _PAGE_RESIDUE_RE.search(sentence):
            score -= 18
        if _GENERIC_PROMO_RE.search(sentence) and not _QUANT_SIGNAL_RE.search(sentence):
            score -= 6
        if _HISTORY_AWARD_SENTENCE_RE.search(sentence):
            score -= 18
        if _REPORT_META_SENTENCE_RE.search(sentence) and not _CURRENT_OPERATING_SENTENCE_RE.search(sentence):
            score -= 16
        if _REPORT_INTRO_SENTENCE_RE.search(sentence) and not (_QUANT_SIGNAL_RE.search(sentence) or _CURRENT_OPERATING_SENTENCE_RE.search(sentence)):
            score -= 10
        if _RELATED_PARTY_META_SENTENCE_RE.search(sentence) and not _CAPITAL_ACTION_SENTENCE_RE.search(sentence):
            score -= 14
        if _CORPORATE_CULTURE_SENTENCE_RE.search(sentence) and not (_QUANT_SIGNAL_RE.search(sentence) or _CURRENT_OPERATING_SENTENCE_RE.search(sentence)):
            score -= 8
        if type_profile["type"] == "narrative":
            score -= 2
        if len(sentence) > 95:
            score -= 4

        ranked.append(
            {
                "text": sentence,
                "type": type_profile["type"],
                "label": type_profile["label"],
                "lane": type_profile["lane"],
                "score": score,
                "length": len(sentence),
            }
        )

    ranked.sort(key=lambda item: (-int(item["score"]), int(item["length"])))
    return [{k: v for k, v in item.items() if k != "length"} for item in ranked[:take]]


def evidence_profile(details: Iterable[dict[str, object]]) -> dict[str, object]:
    counts = Counter({"fact": 0, "quant": 0, "policy": 0, "path": 0, "impact": 0, "narrative": 0})
    flattened = [detail for detail in details if detail]
    for detail in flattened:
        kind = str(detail.get("type", ""))
        if kind in counts:
            counts[kind] += 1
    total = len(flattened)
    fact_like_count = counts["fact"] + counts["quant"] + counts["policy"]
    inference_count = counts["path"] + counts["impact"]
    narrative_count = counts["narrative"]
    fact_density = round((fact_like_count / total) * 100) if total else 0
    type_counts = [
        {"value": evidence_type_label(kind), "count": counts[kind]}
        for kind in ("fact", "quant", "policy", "path", "impact", "narrative")
        if counts[kind] > 0
    ]
    return {
        "total": total,
        "factLikeCount": fact_like_count,
        "inferenceCount": inference_count,
        "narrativeCount": narrative_count,
        "factDensity": fact_density,
        "typeCounts": type_counts,
        "typeLabels": [item["value"] for item in type_counts],
    }


def unique_evidence_details(values: Iterable[Iterable[dict[str, object]]], take: int = 6) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    seen: set[str] = set()
    for value in values:
        for detail in value or []:
            text = str(detail.get("text", "")).strip()
            if not text:
                continue
            key = normalize_text(text)
            if key and key not in seen:
                seen.add(key)
                results.append(
                    {
                        "text": text,
                        "type": str(detail.get("type", "")),
                        "label": str(detail.get("label", "")),
                        "lane": str(detail.get("lane", "")),
                        "score": int(detail.get("score", 0)),
                    }
                )
                if len(results) >= take:
                    return results
    return results


def title_dedup_key(title: str | None) -> str:
    if not title:
        return ""
    normalized = html_decode(title)
    normalized = re.sub(r"\s*[-—|｜]\s*[^-—|｜]{1,24}$", "", normalized)
    normalized = re.sub(r"\(\d{6}(\.[A-Z]{2})?\)", "", normalized)
    normalized = re.sub(
        r"^[\u4e00-\u9fa5A-Za-z0-9（）()·\-\s]{2,24}[：:]"
        r"\s*(?=关于|20\d{2}年|年度报告|年度报告摘要|半年度报告|季度报告|提质增效|利润分配|现金分红|召开|"
        r"获得药物临床试验批准通知书|增资|关联交易|履职报告|股东)",
        "",
        normalized,
    )
    normalized = re.sub(
        r"^[\u4e00-\u9fa5A-Za-z0-9（）()·\-\s]{2,24}"
        r"(?=关于|20\d{2}年|年度报告|年度报告摘要|半年度报告|季度报告|提质增效|利润分配|现金分红|召开|"
        r"获得药物临床试验批准通知书|增资|关联交易|履职报告|股东)",
        "",
        normalized,
    )
    return normalize_text(normalized)


def top_occurrence_values(values: Iterable[object], take: int = 6) -> list[dict[str, object]]:
    flattened: list[str] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            flattened.extend([str(item).strip() for item in value if str(item).strip()])
        else:
            item = str(value).strip()
            if item:
                flattened.append(item)
    counts = Counter(flattened)
    return [{"value": key, "count": count} for key, count in counts.most_common(take)]
