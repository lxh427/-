from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from .config import (
    MACRO_TERMS,
    SOURCE_FEED_CACHE_MINUTES,
)
from .profiles import profile_match_breakdown, source_candidate_hit_count
from .text_utils import (
    html_decode,
    keyword_hits,
    strip_html,
    url_path_date_to_timestamp,
)

# 这里集中放来源规则、候选过滤和证据标注逻辑。

SCENARIO_CAPABLE_SOURCE_TYPES = {
    "eastmoneyFocus",
    "eastmoneyFastNews",
    "csMarketNews",
    "nmpaOfficial",
    "nhsaOfficial",
    "miitOfficial",
    "ndrcOfficial",
    "search360",
    "sogouSearch",
}

MEDICAL_GENERIC_SIGNAL_TERMS = {"医药", "药品", "医保", "药监", "临床", "审批"}
EMPTY_FEED_MARKER = "__EMPTY_FEED__"
MEDICAL_BROAD_POLICY_NOISE_RE = re.compile(
    r"(?:"
    r"\u6309\u75c5\u79cd\u4ed8\u8d39|\u5206\u7ec4\u65b9\u6848|\u4ed8\u8d39\u65b9\u5f0f|\u5b9e\u5f55|"
    r"\u4e09\u8fdb\u884c\u52a8|\u6751\u536b\u751f\u5ba4|\u533b\u4fdd\u670d\u52a1\u7801|"
    r"\u533b\u4fdd\u7ecf\u529e|\u57fa\u91d1\u76d1\u7ba1|\u98de\u884c\u68c0\u67e5"
    r")",
    re.I,
)
MEDICAL_HARD_POLICY_NOISE_RE = re.compile(
    r"(?:"
    r"\u4e09\u8fdb\u884c\u52a8|\u6751\u536b\u751f\u5ba4|\u5b9e\u5f55|"
    r"\u533b\u4fdd\u670d\u52a1\u7801|\u533b\u4fdd\u7ecf\u529e|\u57fa\u91d1\u76d1\u7ba1|\u98de\u884c\u68c0\u67e5"
    r")",
    re.I,
)
LOW_VALUE_STOCK_WRAPPER_RE = re.compile(
    r"(?:ETF|LOF|REIT|\u51c0\u7533\u8d2d|\u51c0\u6d41\u5165|\u51c0\u8d4e\u56de|"
    r"\u7a7a\u4ed3\u6301\u5355\u7edf\u8ba1|\u6df7\u5408\u5e74\u62a5\u89e3\u8bfb|\u57fa\u91d1\u5e74\u62a5\u89e3\u8bfb|"
    r"\u6807\u7684\u6307\u6570|\u7ba1\u7406\u8d39|\u8054\u63a5\u57fa\u91d1|\u57fa\u91d1\u4efd\u989d|\u573a\u5185\u4efd\u989d)",
    re.I,
)
LOW_VALUE_FUND_HOUSE_RE = re.compile(
    r"(?:"
    r"\u6613\u65b9\u8fbe|\u534e\u590f|\u5609\u5b9e|\u5e7f\u53d1|\u5bcc\u56fd|\u5357\u65b9\u57fa\u91d1|"
    r"\u6c47\u6dfb\u5bcc|\u535a\u65f6|\u56fd\u6cf0|\u5929\u5f18|\u5de5\u94f6\u745e\u4fe1|\u94f6\u534e\u57fa\u91d1|"
    r"\u62db\u5546\u57fa\u91d1|\u666f\u987a\u957f\u57ce|\u9e4f\u534e\u57fa\u91d1|\u4e2d\u6b27\u57fa\u91d1"
    r")",
    re.I,
)
STRONG_EVENT_TITLE_RE = re.compile(
    r"(?:"
    r"\u83b7\u6279|\u6279\u51c6|\u4e2d\u6807|\u8ba2\u5355|\u5408\u540c|\u7b7e\u7f72|\u5408\u4f5c|\u6388\u6743|"
    r"\u6269\u4ea7|\u6295\u4ea7|\u505c\u4ea7|\u56de\u8d2d|\u51cf\u6301|\u8bc9\u8bbc|\u5904\u7f5a|\u5236\u88c1|"
    r"\u5173\u7a0e|\u51b2\u7a81|\u6218\u4e89|\u505c\u706b|\u590d\u822a|\u6062\u590d|\u96c6\u91c7|\u533b\u4fdd|"
    r"\u4e34\u5e8a|\u5ba1\u6279|\u51fa\u53e3|\u8fdb\u53e3|\u8865\u8d34|\u6536\u5165|\u5229\u6da6|\u589e\u957f|"
    r"\u4e0b\u964d|\u4e8f\u635f|\u626d\u4e8f|\u73b0\u91d1\u5206\u7ea2"
    r")",
    re.I,
)
CLEAR_LOW_VALUE_ANNOUNCEMENT_RE = re.compile(
    r"(?:"
    r"\u8bc1\u5238\u53d8\u52a8\u6708\u62a5\u8868|\u6301\u7eed\u7763\u5bfc\u8ddf\u8e2a\u62a5\u544a|\u6301\u7eed\u7763\u5bfc\u5de5\u4f5c\u73b0\u573a\u68c0\u67e5\u62a5\u544a|\u6301\u7eed\u7763\u5bfc\u5de5\u4f5c\u62a5\u544a|"
    r"\u72ec\u7acb\u8d22\u52a1\u987e\u95ee\u62a5\u544a|\u4fdd\u8350\u673a\u6784\u6838\u67e5\u610f\u89c1|\u4e13\u9879\u6838\u67e5\u610f\u89c1|\u6838\u67e5\u610f\u89c1|"
    r"\u6cd5\u5f8b\u610f\u89c1\u4e66|\u52df\u96c6\u8bf4\u660e\u4e66|\u53d7\u6258\u7ba1\u7406\u4e8b\u52a1\u62a5\u544a|"
    r"\u52df\u96c6\u8d44\u91d1\u5b58\u653e|\u7ba1\u7406\u4e0e\u5b9e\u9645\u4f7f\u7528\u60c5\u51b5|"
    r"\u85aa\u916c\u4e0e\u8003\u6838\u59d4\u5458\u4f1a.*?\u6838\u67e5\u610f\u89c1|\u901a\u77e5\u503a\u6743\u4eba|"
    r"\u4e13\u9879\u8bf4\u660e|\u8d22\u52a1\u516c\u53f8\u5173\u8054\u4ea4\u6613\u7684\u5b58\u6b3e\u3001\u8d37\u6b3e\u7b49\u91d1\u878d\u4e1a\u52a1|"
    r"\u81ea\u4e3b\u884c\u6743\u6a21\u5f0f|"
    r"H\u80a1\u516c\u544a|\u8bae\u4e8b\u89c4\u5219|\u5de5\u4f5c\u5236\u5ea6|\u516c\u53f8\u7ae0\u7a0b|\u53ef\u6301\u7eed\u53d1\u5c55\u62a5\u544a"
    r")",
    re.I,
)

CNINFO_PAGE_SIZE = 30
CNINFO_PAGE_LIMIT = 6
CNINFO_ITEM_LIMIT = 80
SINA_SOURCE_PAGE_LIMITS = {"stockNews": 8, "industryNews": 6, "bulletin": 6}
SINA_SOURCE_ITEM_LIMITS = {"stockNews": 48, "industryNews": 24, "bulletin": 24}
EASTMONEY_FOCUS_ITEM_LIMIT = 60
EASTMONEY_FAST_NEWS_ITEM_LIMIT = 48
CS_MARKET_SCAN_LIMIT = 60
NMPA_ITEM_LIMIT = 40
NHSA_ITEM_LIMIT = 48
MIIT_ITEM_LIMIT = 48
NDRC_ITEM_LIMIT = 48
SEARCH_PAGE_COUNT = 1
SEARCH_QUERY_LIMIT = 4
SEARCH_RESULT_LIMIT_PER_ENGINE = 36
SEARCH_HTTP_TIMEOUT_SECONDS = 4
SEARCH_EXPANDED_PAGE_COUNT = 2
SEARCH_EXPANDED_QUERY_LIMIT = 6
SEARCH_EXPANDED_RESULT_LIMIT = 48
SEARCH_EXPANDED_WINDOW_DAYS = 21
SOURCE_CORE_WORKERS = 10
SOURCE_SUPPLEMENTAL_WORKERS = 6
ROUTE_SCENARIO_EVENT_RE = re.compile(
    r"(?:"
    r"伊朗|中东|红海|霍尔木兹|俄乌|乌克兰|关税|制裁|冲突|战争|停火|"
    r"出口管制|供应链|物流|航运|航线|港口|运价|油价|原油|天然气|"
    r"旅游|客流|航班|机场|免签|出入境|消费|节假日|社零|"
    r"芯片|GPU|算力|半导体|国产替代|人工智能|服务器|低空|机器人|"
    r"药监|医保|集采|临床|审批|创新药|疫苗|疫情|DRG|DIP|"
    r"房地产|专项债|基建|投资|开工|建材|"
    r"煤价|电价|电网|锂价|光伏|储能|风电|军工|船舶"
    r")",
    re.I,
)
SEARCH_NOISE_RE = re.compile(
    r"(?:"
    r"股票行情|实时行情|行情走势|个股行情|资金流向|股吧|怎么样|官网|百科|资料|信息|下载|app|软件|开户|"
    r"证券之星|九方智投|同花顺|问董秘|个股日历|投资提醒|机构散户精准买卖点"
    r")",
    re.I,
)
SEARCH_PRIORITY_HOST_RE = re.compile(
    r"(?:gov\.cn|cninfo\.com\.cn|eastmoney\.com|cs\.com\.cn|cnstock\.com|stcn\.com|jrj\.com\.cn|"
    r"news\.cn|people\.com\.cn|cctv\.com|ifeng\.com|163\.com|thepaper\.cn|yicai\.com|caixin\.com|"
    r"eeo\.com\.cn|bjnews\.com\.cn|21jingji\.com|cnr\.cn|sina\.com\.cn)",
    re.I,
)
SEARCH_BLOCKED_HOST_RE = re.compile(
    r"(?:guba\.eastmoney\.com|xueqiu\.com|taoguba\.com\.cn|zhidao\.baidu\.com|baike\.baidu\.com|"
    r"wenku\.baidu\.com|tieba\.baidu\.com|bilibili\.com|douyin\.com|xiaohongshu\.com|weibo\.com)",
    re.I,
)
SEARCH_LOW_VALUE_PATH_RE = re.compile(
    r"(?:/stock|realstock|stockdata|zijinliuxiang|moneyflow|/guba/|/quote/|/gegu/|/stocklist/|"
    r"/company/.*?/nc\.shtml|/fenxi|/yanbao)",
    re.I,
)


def _source_feed_key(key: str) -> str:
    return f"source-feed:v2:{key}"


def get_source_feed_text(
    cache_store: Any,
    http_client: Any,
    key: str,
    url: str,
    referer: str = "",
    timeout: int = 10,
    retries: int = 2,
    allow_curl_fallback: bool = True,
    prefer_curl: bool | None = None,
) -> str:
    # 通用抓取缓存层，避免同一来源页被重复请求。
    cached = cache_store.get("source-feed", _source_feed_key(key))
    if isinstance(cached, str):
        if cached == EMPTY_FEED_MARKER:
            return ""
        if cached:
            return cached
    text = http_client.get_text(
        url,
        referer=referer,
        timeout=timeout,
        retries=retries,
        allow_curl_fallback=allow_curl_fallback,
        prefer_curl=prefer_curl,
    )
    if text:
        cache_store.set(
            "source-feed",
            _source_feed_key(key),
            text,
            datetime.now() + timedelta(minutes=SOURCE_FEED_CACHE_MINUTES),
        )
    else:
        cache_store.set(
            "source-feed",
            _source_feed_key(key),
            EMPTY_FEED_MARKER,
            datetime.now() + timedelta(minutes=3),
        )
    return text or ""


def _unique_terms(*groups: list[str]) -> list[str]:
    results: list[str] = []
    for group in groups:
        for value in group:
            text = str(value).strip()
            if text and text not in results:
                results.append(text)
    return results


def profile_route_tags(profile: dict[str, Any]) -> set[str]:
    return {str(tag).strip().lower() for tag in profile.get("routeTags", []) if str(tag).strip()}


def profile_policy_domains(profile: dict[str, Any]) -> set[str]:
    return {str(domain).strip().lower() for domain in profile.get("policyDomains", []) if str(domain).strip()}


def profile_has_route(profile: dict[str, Any], *tags: str) -> bool:
    routes = profile_route_tags(profile)
    return any(str(tag).strip().lower() in routes for tag in tags)


def profile_has_policy_domain(profile: dict[str, Any], *domains: str) -> bool:
    values = profile_policy_domains(profile)
    return any(str(domain).strip().lower() in values for domain in domains)


def route_trigger_terms(profile: dict[str, Any]) -> list[str]:
    # 给“蝴蝶效应”召回补一层行业路径词。
    terms: list[str] = []

    def add(*values: str) -> None:
        for value in values:
            token = str(value).strip()
            if token and token not in terms:
                terms.append(token)

    if profile_has_route(profile, "shipping", "commodity"):
        add("霍尔木兹", "红海", "运价", "航运", "航线", "海峡", "集运", "港口", "物流", "船舶", "成品油", "原油", "油价")
    if profile_has_route(profile, "travel"):
        add("国际航班", "客流", "出入境", "免签", "旅游", "机场", "航司", "航班", "出行")
    if profile_has_route(profile, "finance"):
        add("降准", "降息", "央行", "流动性", "债券", "汇率", "证券", "资本市场", "两融")
    if profile_has_route(profile, "medical"):
        add(
            "医保",
            "药监",
            "审批",
            "临床",
            "适应症",
            "集采",
            "药品",
            "仿制药",
            "创新药",
            "医保目录",
            "谈判",
            "准入",
            "支付标准",
            "支付方式",
            "病种付费",
            "带量采购",
            "DRG",
            "DIP",
        )
    if profile_has_route(profile, "technology", "semiconductor"):
        add("芯片", "GPU", "算力", "半导体", "出口管制", "国产替代", "先进制程", "集成电路")
    if profile_has_route(profile, "property", "infrastructure"):
        add("房地产", "专项债", "开工", "基建", "投资", "建材", "住房", "项目")
    if profile_has_route(profile, "consumer"):
        add("消费", "社零", "节假日", "提价", "猪肉", "价格", "免税")
    if profile_has_route(profile, "energy", "new_energy"):
        add("煤价", "电价", "能源", "电网", "装机", "锂价", "光伏", "风电", "储能", "原油", "油价")
    return terms


def candidate_text_relevant(profile: dict[str, Any], text: str, context: str = "") -> bool:
    content = f"{text} {context}".strip()
    if not content:
        return False
    if source_candidate_hit_count(profile, content) > 0:
        return True

    route_hits = keyword_hits(route_trigger_terms(profile), content)
    if not route_hits:
        return False
    if len(route_hits) >= 2:
        return True
    if ROUTE_SCENARIO_EVENT_RE.search(content):
        return True
    if profile_has_route(profile, "medical") and medical_policy_relevant(profile, text, context):
        return True
    return False


def _clean_search_result_text(text: str) -> str:
    return re.sub(r"\s+", " ", strip_html(html_decode(text or "")).replace("\xa0", " ")).strip()


def _format_local_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M")


def _date_window_days(from_date: str, to_date: str) -> int:
    try:
        if from_date and to_date:
            start = datetime.strptime(from_date[:10], "%Y-%m-%d")
            end = datetime.strptime(to_date[:10], "%Y-%m-%d")
            return max(1, (end - start).days + 1)
    except ValueError:
        pass
    return 30


def _search_page_count(from_date: str, to_date: str) -> int:
    return SEARCH_EXPANDED_PAGE_COUNT if _date_window_days(from_date, to_date) >= SEARCH_EXPANDED_WINDOW_DAYS else SEARCH_PAGE_COUNT


def _search_query_limit(from_date: str, to_date: str) -> int:
    return SEARCH_EXPANDED_QUERY_LIMIT if _date_window_days(from_date, to_date) >= SEARCH_EXPANDED_WINDOW_DAYS else SEARCH_QUERY_LIMIT


def _search_result_limit(from_date: str, to_date: str) -> int:
    if _date_window_days(from_date, to_date) >= SEARCH_EXPANDED_WINDOW_DAYS:
        return SEARCH_EXPANDED_RESULT_LIMIT
    return SEARCH_RESULT_LIMIT_PER_ENGINE


def _search_result_timestamp(date_text: str, fallback_url: str = "") -> str:
    raw = _clean_search_result_text(date_text)
    now = datetime.now()
    if raw:
        absolute_match = re.search(r"(20\d{2})[年/-](\d{1,2})[月/-](\d{1,2})(?:日)?(?:\s+(\d{1,2}):(\d{2}))?", raw)
        if absolute_match:
            try:
                return _format_local_timestamp(
                    datetime(
                        int(absolute_match.group(1)),
                        int(absolute_match.group(2)),
                        int(absolute_match.group(3)),
                        int(absolute_match.group(4) or 0),
                        int(absolute_match.group(5) or 0),
                    )
                )
            except ValueError:
                pass

        month_day_match = re.search(r"(\d{1,2})月(\d{1,2})日(?:\s+(\d{1,2}):(\d{2}))?", raw)
        if month_day_match:
            try:
                candidate = datetime(
                    now.year,
                    int(month_day_match.group(1)),
                    int(month_day_match.group(2)),
                    int(month_day_match.group(3) or 0),
                    int(month_day_match.group(4) or 0),
                )
                if candidate > now + timedelta(days=1):
                    candidate = candidate.replace(year=candidate.year - 1)
                return _format_local_timestamp(candidate)
            except ValueError:
                pass

        relative_days = re.search(r"(\d+)\s*天前", raw)
        if relative_days:
            return _format_local_timestamp(now - timedelta(days=int(relative_days.group(1))))

        relative_hours = re.search(r"(\d+)\s*小时前", raw)
        if relative_hours:
            return _format_local_timestamp(now - timedelta(hours=int(relative_hours.group(1))))

        yesterday_match = re.search(r"昨天(?:\s+(\d{1,2}):(\d{2}))?", raw)
        if yesterday_match:
            base = now - timedelta(days=1)
            return _format_local_timestamp(
                base.replace(hour=int(yesterday_match.group(1) or 0), minute=int(yesterday_match.group(2) or 0), second=0, microsecond=0)
            )

        today_match = re.search(r"今天(?:\s+(\d{1,2}):(\d{2}))?", raw)
        if today_match:
            return _format_local_timestamp(
                now.replace(hour=int(today_match.group(1) or 0), minute=int(today_match.group(2) or 0), second=0, microsecond=0)
            )

    return url_path_date_to_timestamp(fallback_url)


def _search_result_looks_low_value(title: str, summary: str, url: str, site: str) -> bool:
    text = f"{title} {summary} {site}".strip()
    url_text = str(url or "").lower()
    host = _site_from_url(url_text)
    strong_event = bool(STRONG_EVENT_TITLE_RE.search(text))
    obvious_quote_or_forum = any(
        token in url_text
        for token in (
            "quote.eastmoney.com",
            "data.eastmoney.com/stockdata",
            "guba.eastmoney.com",
            "stock.quote.stockstar.com",
            "finance.sina.com.cn/realstock/company",
            "hq.sinajs.cn",
            "ccht.jl.cn",
            "sports.163.com",
        )
    )
    if SEARCH_BLOCKED_HOST_RE.search(host):
        return True
    if obvious_quote_or_forum and not strong_event:
        return True
    if SEARCH_LOW_VALUE_PATH_RE.search(url_text) and not strong_event:
        return True
    if SEARCH_NOISE_RE.search(text) and not strong_event:
        return True
    if not SEARCH_PRIORITY_HOST_RE.search(host) and SEARCH_NOISE_RE.search(f"{title} {site}") and not strong_event:
        return True
    return False


def _search_query_specs(profile: dict[str, Any], from_date: str = "", to_date: str = "") -> list[dict[str, str]]:
    # 检索词分层构造：先保股票直连，再补场景扩召回。
    specs: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(query: str, mode: str, context: str) -> None:
        text = re.sub(r"\s+", " ", str(query or "")).strip()
        if len(text) < 2 or text in seen:
            return
        seen.add(text)
        specs.append({"query": text, "mode": mode, "context": context})

    name = str(profile.get("name", "")).strip()
    code = str(profile.get("code", "")).strip()
    aliases = [value for value in profile.get("aliases", []) if str(value).strip()]
    entities = _unique_terms(profile.get("subsidiaries", []), profile.get("products", []))
    route_terms = _unique_terms(profile.get("sensitiveFactors", []), profile.get("themes", []), route_trigger_terms(profile))
    scenario_queries = [str(value).strip() for value in profile.get("scenarioQueries", []) if str(value).strip()]
    factor_terms = [
        value
        for value in _unique_terms(profile.get("sensitiveFactors", []), route_trigger_terms(profile))
        if 1 < len(str(value).strip()) <= 16
    ]
    theme_terms = [
        value
        for value in _unique_terms(profile.get("themes", []), profile.get("narrativeHooks", []))
        if 1 < len(str(value).strip()) <= 16
    ]
    max_queries = _search_query_limit(from_date, to_date)

    add(f"{name} {code}".strip(), "direct", "名称代码检索")
    add(name, "direct", "股票名称检索")
    for alias in aliases[:2]:
        add(alias, "direct", "别名检索")
    for entity in entities[:2]:
        add(f"{name} {entity}".strip(), "direct", "实体扩展检索")

    if name and factor_terms:
        add(f"{name} {factor_terms[0]}".strip(), "scenario", "股名+敏感因子")
    if name and theme_terms:
        add(f"{name} {theme_terms[0]}".strip(), "scenario", "股名+主题词")
    if aliases and factor_terms:
        add(f"{aliases[0]} {factor_terms[0]}".strip(), "scenario", "别名+敏感因子")
    for scenario_query in scenario_queries[:2]:
        add(scenario_query, "scenario", "场景补召回")

    scenario_terms = [term for term in route_terms if 1 < len(str(term).strip()) <= 12]
    if len(scenario_terms) >= 2:
        add(" ".join(scenario_terms[:2]), "scenario", "路径词检索")
    if len(scenario_terms) >= 4:
        add(" ".join(scenario_terms[2:4]), "scenario", "扩展路径词检索")
    for entity in entities[:2]:
        add(entity, "direct", "实体检索")
    return specs[:max_queries]


def _build_search_candidate(
    *,
    profile: dict[str, Any],
    title: str,
    url: str,
    summary: str,
    published_at: str,
    source_type: str,
    source_label: str,
    source_site: str,
    query: str,
    query_mode: str,
    query_context: str,
) -> dict[str, Any] | None:
    clean_title = _clean_search_result_text(title)
    clean_summary = _clean_search_result_text(summary)
    clean_site = _clean_search_result_text(source_site)
    clean_url = html_decode(url or "").strip()
    published = _search_result_timestamp(published_at, fallback_url=clean_url)
    if not clean_title or not clean_url or not published:
        return None
    if _search_result_looks_low_value(clean_title, clean_summary, clean_url, clean_site):
        return None
    if not candidate_text_relevant(profile, f"{clean_title} {clean_summary}", clean_site):
        return None
    return {
        "title": clean_title,
        "url": clean_url,
        "publishedAt": published,
        "sourceType": source_type,
        "sourceLabel": source_label,
        "sourceSite": clean_site or source_label,
        "queryMode": query_mode,
        "queryContext": query_context,
        "queryTerm": query,
        "summary": clean_summary or clean_title,
    }


def _site_from_url(url: str) -> str:
    return urlparse(str(url or "").strip()).netloc.lower()


def _specific_medical_hits(values: list[str]) -> list[str]:
    return [value for value in values if value not in MEDICAL_GENERIC_SIGNAL_TERMS]


def medical_policy_relevant(profile: dict[str, Any], title: str, context: str = "") -> bool:
    text = f"{title} {context}".strip()
    breakdown = profile_match_breakdown(profile, text)
    route_hits = keyword_hits(route_trigger_terms(profile), text)
    specific_factor_hits = _specific_medical_hits(breakdown["factorHits"])
    specific_theme_hits = _specific_medical_hits(breakdown["themeHits"])
    specific_route_hits = _specific_medical_hits(route_hits)
    direct_hits = breakdown["directHits"] or breakdown["aliasHits"] or breakdown["subsidiaryHits"] or breakdown["productHits"]
    hard_policy_noise = bool(MEDICAL_HARD_POLICY_NOISE_RE.search(text))
    broad_policy_noise = bool(MEDICAL_BROAD_POLICY_NOISE_RE.search(text))
    strong_policy_terms = {"集采", "准入", "目录", "谈判", "支付标准", "支付方式", "病种付费", "DRG", "DIP"}
    strong_policy_hits = [term for term in specific_route_hits + specific_theme_hits if term in strong_policy_terms]

    if direct_hits:
        return True
    if hard_policy_noise:
        return False
    if len(specific_factor_hits) >= 2:
        return True
    if specific_factor_hits and strong_policy_hits and not broad_policy_noise:
        return True
    if len(strong_policy_hits) >= 2 and (specific_theme_hits or specific_factor_hits) and not broad_policy_noise:
        return True
    return False





def eastmoney_focus_section_ids(profile: dict[str, Any]) -> set[str]:
    selected = {"1"}
    if profile_has_route(profile, "shipping", "travel", "energy", "commodity", "macro_sensitive", "policy", "consumer"):
        selected.update({"2", "3"})
    if profile_has_route(profile, "finance", "property", "infrastructure"):
        selected.update({"2", "4"})
    if profile_has_route(profile, "medical", "technology", "semiconductor", "new_energy", "defense", "industrial", "company_news", "media"):
        selected.add("5")
    if len(selected) == 1:
        selected.update({"2", "5"})
    return selected


def should_keep_sina_title(profile: dict[str, Any], source_type: str, title: str) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    if source_type != "stockNews":
        return True
    if not (LOW_VALUE_STOCK_WRAPPER_RE.search(text) or LOW_VALUE_FUND_HOUSE_RE.search(text) or "\u57fa\u91d1" in text):
        return True

    breakdown = profile_match_breakdown(profile, text)
    has_direct_entity = bool(
        breakdown["directHits"] or breakdown["aliasHits"] or breakdown["subsidiaryHits"] or breakdown["productHits"]
    )
    has_event_signal = bool(STRONG_EVENT_TITLE_RE.search(text))
    if has_direct_entity and has_event_signal:
        return True
    return False


def should_collect_source(source_type: str, profile: dict[str, Any]) -> bool:
    # 不同股票只打到相关路线，避免无效源浪费时间。
    if source_type in {"nmpaOfficial", "nhsaOfficial"}:
        return profile_has_route(profile, "medical")
    if source_type == "miitOfficial":
        return profile_has_route(profile, "technology", "semiconductor", "new_energy", "defense", "industrial")
    if source_type == "ndrcOfficial":
        return (
            profile_has_route(
                profile,
                "shipping",
                "travel",
                "energy",
                "commodity",
                "finance",
                "property",
                "infrastructure",
                "consumer",
                "new_energy",
                "industrial",
                "macro_sensitive",
                "policy",
            )
            or profile_has_policy_domain(profile, "ndrc.gov.cn")
        )
    return True


def eastmoney_fast_news_columns(profile: dict[str, Any]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def add(column_id: str, context: str, page_size: int = 16) -> None:
        if column_id in seen_ids:
            return
        seen_ids.add(column_id)
        selected.append(
            {
                "id": column_id,
                "key": f"eastmoney:fastnews:{column_id}",
                "context": context,
                "pageSize": page_size,
            }
        )

    add("101", "要闻快讯", page_size=18)
    add("102", "7x24 快讯", page_size=18)

    if profile_has_route(profile, "shipping", "energy", "commodity", "property", "infrastructure", "macro_sensitive", "policy"):
        add("106", "商品地缘快讯", page_size=14)
        add("125", "宏观数据快讯", page_size=14)
    if profile_has_route(profile, "finance"):
        add("108", "证券市场快讯", page_size=14)
        add("125", "宏观数据快讯", page_size=14)
    if profile_has_route(profile, "medical", "technology", "semiconductor", "new_energy", "defense", "industrial", "consumer", "travel", "media", "company_news"):
        add("103", "上市公司快讯", page_size=12)
    if profile_has_route(profile, "technology", "semiconductor", "new_energy", "defense", "industrial"):
        add("104", "中国公司快讯", page_size=12)

    return selected


def annotate_candidate_item(profile: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    # 给场景型候选补标签，方便后续评分和聚合。
    source_type = str(item.get("sourceType", ""))
    if source_type not in SCENARIO_CAPABLE_SOURCE_TYPES:
        return item

    text = " ".join(
        [
            str(item.get("title", "")),
            str(item.get("summary", "")),
            str(item.get("queryContext", "")),
            str(item.get("queryTerm", "")),
        ]
    )
    breakdown = profile_match_breakdown(profile, text)
    macro_hits = keyword_hits(MACRO_TERMS, text)
    direct_hits = _unique_terms(
        breakdown["identityHits"],
        breakdown["aliasHits"],
        breakdown["subsidiaryHits"],
        breakdown["productHits"],
    )
    scenario_terms = _unique_terms(
        breakdown["factorHits"],
        breakdown["authorityHits"],
        breakdown["themeHits"],
        breakdown["hookHits"],
        macro_hits,
    )
    route_hits = keyword_hits(route_trigger_terms(profile), text)
    scenario_score = (
        (len(breakdown["factorHits"]) * 3)
        + (len(breakdown["authorityHits"]) * 3)
        + (len(breakdown["themeHits"]) * 2)
        + (len(breakdown["hookHits"]) * 2)
        + len(macro_hits)
        + (len(route_hits) * 2)
    )
    qualifies_as_scenario = (
        not direct_hits
        and (
            (scenario_score >= 6 and route_hits)
            or (route_hits and (breakdown["factorHits"] or breakdown["themeHits"] or macro_hits))
            or (macro_hits and route_hits)
            or (breakdown["authorityHits"] and (breakdown["factorHits"] or breakdown["themeHits"]))
        )
    )

    annotated = dict(item)
    if scenario_terms:
        annotated["scenarioTerms"] = scenario_terms[:6]
    if route_hits:
        annotated["routeTerms"] = route_hits[:6]
    if qualifies_as_scenario:
        base_context = str(item.get("queryContext", "")).strip()
        annotated["queryMode"] = "scenario"
        annotated["queryContext"] = f"{base_context} / 场景扩展" if base_context else "场景扩展"
        annotated["queryTerm"] = " / ".join((_unique_terms(route_hits, scenario_terms))[:4]) or str(item.get("queryTerm", "")).strip()
    return annotated


def annotate_candidate_items(profile: dict[str, Any], items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [annotate_candidate_item(profile, item) for item in items]
