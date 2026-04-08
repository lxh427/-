from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed
import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

from .config import (
    FEED_HTTP_RETRIES,
    FEED_HTTP_TIMEOUT_SECONDS,
    SOURCE_COLLECTION_BUDGET_SECONDS,
    enabled_source_types,
)
from .profiles import get_cninfo_security_snapshot, profile_match_breakdown, profile_signal_hit_count
from .text_utils import (
    date_only_string,
    html_decode,
    in_date_range,
    month_day_time_to_local_timestamp,
    resolve_absolute_url,
    strip_html,
    title_dedup_key,
    unix_ms_to_local_time,
    url_path_date_to_timestamp,
)

from .source_rules import (
    CLEAR_LOW_VALUE_ANNOUNCEMENT_RE,
    CNINFO_ITEM_LIMIT,
    CNINFO_PAGE_LIMIT,
    CNINFO_PAGE_SIZE,
    CS_MARKET_SCAN_LIMIT,
    EASTMONEY_FAST_NEWS_ITEM_LIMIT,
    EASTMONEY_FOCUS_ITEM_LIMIT,
    LOW_VALUE_FUND_HOUSE_RE,
    LOW_VALUE_STOCK_WRAPPER_RE,
    MIIT_ITEM_LIMIT,
    NDRC_ITEM_LIMIT,
    NHSA_ITEM_LIMIT,
    NMPA_ITEM_LIMIT,
    SEARCH_HTTP_TIMEOUT_SECONDS,
    SEARCH_RESULT_LIMIT_PER_ENGINE,
    SINA_SOURCE_ITEM_LIMITS,
    SINA_SOURCE_PAGE_LIMITS,
    SOURCE_CORE_WORKERS,
    SOURCE_SUPPLEMENTAL_WORKERS,
    STRONG_EVENT_TITLE_RE,
    _build_search_candidate,
    _clean_search_result_text,
    _search_page_count,
    _search_query_specs,
    _search_result_limit,
    _site_from_url,
    annotate_candidate_items,
    candidate_text_relevant,
    eastmoney_fast_news_columns,
    eastmoney_focus_section_ids,
    get_source_feed_text,
    profile_has_route,
    medical_policy_relevant,
    should_collect_source,
    should_keep_sina_title,
)

# 这里保留“各来源抓取 + 总调度”，规则判断已经拆到 source_rules.py。

def get_sina_page_url(source_type: str, profile: dict[str, Any], page: int = 1) -> str:
    if source_type == "stockNews":
        if page <= 1:
            return f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllNewsStock/symbol/{profile['symbol']}.phtml"
        return f"https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol={profile['symbol']}&Page={page}"
    if source_type == "industryNews":
        if page <= 1:
            return f"https://vip.stock.finance.sina.com.cn/corp/go.php/stockIndustryNews/symbol/{profile['symbol']}.phtml"
        return f"https://vip.stock.finance.sina.com.cn/corp/view/stockIndustryNews.php?symbol={profile['symbol']}&Page={page}"
    if source_type == "bulletin":
        if page <= 1:
            return f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/{profile['code']}.phtml"
        return f"https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletin.php?stockid={profile['code']}&Page={page}"
    return ""


def parse_sina_list_items(raw_html: str, source_type: str) -> list[dict[str, Any]]:
    if not raw_html:
        return []
    pattern = (
        r'(?is)(\d{4}-\d{2}-\d{2})&nbsp;<a[^>]+href=[\'"]([^\'"]+)[\'"][^>]*>(.*?)</a>'
        if source_type == "bulletin"
        else r'(?is)(\d{4}-\d{2}-\d{2})&nbsp;(\d{2}:\d{2})&nbsp;&nbsp;<a[^>]+href=[\'"]([^\'"]+)[\'"][^>]*>(.*?)</a>'
    )
    items: list[dict[str, Any]] = []
    for match in re.finditer(pattern, raw_html):
        date = match.group(1)
        time = "00:00" if source_type == "bulletin" else match.group(2)
        link_index = 2 if source_type == "bulletin" else 3
        title_index = 3 if source_type == "bulletin" else 4
        link = html_decode(match.group(link_index))
        if link.startswith("/"):
            link = f"https://vip.stock.finance.sina.com.cn{link}"
        title = strip_html(match.group(title_index))
        if not title:
            continue
        items.append(
            {
                "title": title,
                "url": link,
                "publishedAt": f"{date} {time}",
                "sourceType": source_type,
                "sourceLabel": {
                    "stockNews": "新浪个股资讯",
                    "industryNews": "新浪行业资讯",
                    "bulletin": "新浪公司公告",
                }.get(source_type, source_type),
                "sourceSite": "新浪财经",
                "queryMode": "direct",
                "queryContext": "原生资讯页",
                "queryTerm": title,
                "summary": "",
            }
        )
    return items


def get_cninfo_announcement_items(profile: dict[str, Any], from_date: str, to_date: str, http_client: Any) -> list[dict[str, Any]]:
    security = get_cninfo_security_snapshot(profile, http_client)
    if not security or not security.get("orgId"):
        return []
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for page in range(1, CNINFO_PAGE_LIMIT + 1):
        payload = http_client.post_form_json(
            "https://www.cninfo.com.cn/new/hisAnnouncement/query",
            {
                "stock": f"{profile['code']},{security['orgId']}",
                "pageSize": CNINFO_PAGE_SIZE,
                "pageNum": page,
            },
            referer=f"https://www.cninfo.com.cn/new/disclosure/stock?orgId={security['orgId']}&stockCode={profile['code']}",
        )
        announcements = payload.get("announcements") if isinstance(payload, dict) else None
        if not announcements:
            break
        for announcement in announcements:
            title = str(announcement.get("announcementTitle", "")).strip()
            if not title:
                continue
            dedup = title_dedup_key(title)
            if dedup and dedup in seen_keys:
                continue
            published_at = unix_ms_to_local_time(announcement.get("announcementTime"))
            if not in_date_range(published_at, from_date, to_date):
                continue
            pdf_url = ""
            adjunct_url = str(announcement.get("adjunctUrl", "")).strip()
            if adjunct_url:
                pdf_url = f"https://static.cninfo.com.cn/{adjunct_url.lstrip('/')}"
            results.append(
                {
                    "title": title,
                    "url": pdf_url
                    or f"https://www.cninfo.com.cn/new/disclosure/detail?stockCode={profile['code']}&announcementId={announcement.get('announcementId', '')}",
                    "publishedAt": published_at,
                    "sourceType": "cninfoAnnouncement",
                    "sourceLabel": "巨潮资讯公告",
                    "sourceSite": "巨潮资讯",
                    "queryMode": "direct",
                    "queryContext": "官方公告",
                    "queryTerm": profile["code"],
                    "summary": str(announcement.get("shortTitle") or title).strip(),
                }
            )
            if dedup:
                seen_keys.add(dedup)
            if len(results) >= CNINFO_ITEM_LIMIT:
                return results
        oldest = unix_ms_to_local_time(announcements[-1].get("announcementTime"))
        if from_date and date_only_string(oldest) and date_only_string(oldest) < from_date:
            break
    return results


def parse_eastmoney_focus_section_items(section_html: str, context: str, profile: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not section_html:
        return results
    pattern = (
        r'(?is)<li[^>]*id="newsTr\d+"[^>]*>.*?<p class="title"[^>]*><a href="([^"]+)"[^>]*>(.*?)</a></p>'
        r'\s*<p class="info"(?:\s+title="([^"]*)")?[^>]*>(.*?)</p>\s*<p class="time">([^<]+)</p>'
    )
    for match in re.finditer(pattern, section_html):
        url = html_decode(match.group(1))
        if url.startswith("//"):
            url = f"https:{url}"
        elif url.startswith("/"):
            url = f"https://finance.eastmoney.com{url}"
        title = strip_html(match.group(2))
        summary = html_decode(match.group(3)) or strip_html(match.group(4)) or title
        published_at = month_day_time_to_local_timestamp(match.group(5))
        if not title or not url or not published_at:
            continue
        if not candidate_text_relevant(profile, f"{title} {summary}", context):
            continue
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "eastmoneyFocus",
                "sourceLabel": "东方财富焦点资讯",
                "sourceSite": "东方财富网",
                "queryMode": "feed",
                "queryContext": context,
                "queryTerm": context,
                "summary": summary,
            }
        )
    return results



def _eastmoney_fast_news_entries(column: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    req_trace = int(datetime.now().timestamp() * 1000)
    url = (
        "https://np-weblist.eastmoney.com/comm/web/getFastNewsList"
        f"?client=web&biz=web_724&fastColumn={column['id']}&sortEnd=&pageSize={column['pageSize']}&req_trace={req_trace}"
    )
    payload_text = get_source_feed_text(
        cache_store,
        http_client,
        column["key"],
        url,
        referer="https://kuaixun.eastmoney.com/",
        timeout=FEED_HTTP_TIMEOUT_SECONDS,
        retries=FEED_HTTP_RETRIES,
        allow_curl_fallback=False,
        prefer_curl=False,
    )
    try:
        payload = json.loads(payload_text) if payload_text else {}
    except Exception:
        payload = {}
    return (((payload or {}).get("data") or {}).get("fastNewsList") or [])[: int(column.get("pageSize", 16) or 16)]


def get_eastmoney_fast_news_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    columns = eastmoney_fast_news_columns(profile)
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    if not columns:
        return results

    executor = ThreadPoolExecutor(max_workers=min(4, len(columns)))
    futures = {executor.submit(_eastmoney_fast_news_entries, column, cache_store, http_client): column for column in columns}
    try:
        try:
            completed = as_completed(futures, timeout=max(6, FEED_HTTP_TIMEOUT_SECONDS + 2))
            for future in completed:
                column = futures[future]
                try:
                    items = future.result() or []
                except Exception:
                    items = []
                for entry in items:
                    title = str(entry.get("title") or "").strip()
                    summary = str(entry.get("summary") or title).strip()
                    published_at = str(entry.get("showTime") or "").strip()
                    article_code = str(entry.get("code") or "").strip()
                    url = f"https://finance.eastmoney.com/a/{article_code}.html" if article_code else "https://kuaixun.eastmoney.com/"
                    if not title or not published_at:
                        continue
                    if not candidate_text_relevant(profile, f"{title} {summary}", str(column["context"])):
                        continue
                    dedup = title_dedup_key(title)
                    if dedup and dedup in seen_keys:
                        continue
                    if dedup:
                        seen_keys.add(dedup)
                    results.append(
                        {
                            "title": title,
                            "url": url,
                            "publishedAt": published_at,
                            "sourceType": "eastmoneyFastNews",
                            "sourceLabel": "东方财富快讯",
                            "sourceSite": "东方财富",
                            "queryMode": "feed",
                            "queryContext": column["context"],
                            "queryTerm": column["context"],
                            "summary": summary,
                        }
                    )
        except FutureTimeoutError:
            pass
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    results.sort(key=lambda item: str(item.get("publishedAt", "")), reverse=True)
    return results[:EASTMONEY_FAST_NEWS_ITEM_LIMIT]


def get_js_assigned_array(text: str, variable_name: str) -> list[dict[str, Any]]:
    if not text or not variable_name:
        return []
    match = re.search(rf"var\s+{re.escape(variable_name)}\s*=\s*(\[[\s\S]*\])\s*;?\s*$", text)
    if not match:
        return []
    try:
        return json.loads(match.group(1))
    except Exception:
        return []


def get_cs_market_news_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    js = get_source_feed_text(
        cache_store,
        http_client,
        "cs:sy_yw_js",
        "https://www.cs.com.cn/js/mi4rss/mi4_rss_SY_YW.js",
        referer="https://www.cs.com.cn/",
        timeout=FEED_HTTP_TIMEOUT_SECONDS,
        retries=FEED_HTTP_RETRIES,
        allow_curl_fallback=False,
        prefer_curl=False,
    )
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for entry in get_js_assigned_array(js, "SY_YW")[:CS_MARKET_SCAN_LIMIT]:
        url = str(entry.get("externalLink") or entry.get("articleWebUrl") or entry.get("mmInfo_web_url") or "").strip()
        title = str(entry.get("miLtitle") or entry.get("miShortTitle") or strip_html(str(entry.get("richTitle") or ""))).strip()
        summary = str(entry.get("miSummary") or title).strip()
        published_at = unix_ms_to_local_time(entry.get("pubDate")) if entry.get("pubDate") else url_path_date_to_timestamp(url)
        if not title or not url or not published_at:
            continue
        if not candidate_text_relevant(
            profile,
            f"{title} {summary} {entry.get('miOrigin', '')}",
            str(entry.get("subNm", "")),
        ):
            continue
        dedup = title_dedup_key(title)
        if dedup and dedup in seen_keys:
            continue
        if dedup:
            seen_keys.add(dedup)
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "csMarketNews",
                "sourceLabel": "中证网财经要闻",
                "sourceSite": str(entry.get("miOrigin") or "中证网").strip(),
                "queryMode": "feed",
                "queryContext": str(entry.get("subNm") or "财经要闻").strip(),
                "queryTerm": str(entry.get("subNm") or "财经要闻").strip(),
                "summary": summary,
            }
        )
    return results


def get_nmpa_official_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    raw_html = get_source_feed_text(cache_store, http_client, "nmpa:ggtg:index", "https://www.nmpa.gov.cn/xxgk/ggtg/index.html", referer="https://www.nmpa.gov.cn/")
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for match in re.finditer(r'(?is)<li>\s*<a href=[\'"]([^\'"]+)[\'"][^>]*title=[\'"]([^\'"]+)[\'"][^>]*>.*?</a>\s*<span>\((\d{4}-\d{2}-\d{2})\)</span>', raw_html):
        url = resolve_absolute_url("https://www.nmpa.gov.cn/xxgk/ggtg/index.html", match.group(1))
        title = html_decode(match.group(2)).strip()
        published_at = f"{match.group(3)} 00:00"
        if not title or not url:
            continue
        if profile_has_route(profile, "medical") and not medical_policy_relevant(profile, title, "国家药监局 公告通告"):
            continue
        if not profile_has_route(profile, "medical") and profile_signal_hit_count(profile, title) <= 0:
            continue
        dedup = title_dedup_key(title)
        if dedup and dedup in seen_keys:
            continue
        if dedup:
            seen_keys.add(dedup)
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "nmpaOfficial",
                "sourceLabel": "国家药监局公告",
                "sourceSite": "国家药品监督管理局",
                "queryMode": "official",
                "queryContext": "公告通告",
                "queryTerm": "国家药监局",
                "summary": title,
            }
        )
        if len(results) >= NMPA_ITEM_LIMIT:
            return results
    return results


def _nhsa_list_items(raw_html: str, page_url: str, context: str, profile: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for match in re.finditer(r'(?is)<li>\s*<a href="([^"]+)"[^>]*title="([^"]+)"[^>]*>.*?</a>\s*<span>(\d{4}-\d{2}-\d{2})</span>', raw_html):
        url = resolve_absolute_url(page_url, match.group(1))
        title = html_decode(match.group(2)).strip()
        published_at = f"{match.group(3)} 00:00"
        if not title or not url:
            continue
        if profile_has_route(profile, "medical") and not medical_policy_relevant(profile, title, context):
            continue
        if not profile_has_route(profile, "medical") and profile_signal_hit_count(profile, f"{title} {context}") <= 0:
            continue
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "nhsaOfficial",
                "sourceLabel": "国家医保局官方",
                "sourceSite": "国家医疗保障局",
                "queryMode": "official",
                "queryContext": context,
                "queryTerm": "国家医保局",
                "summary": title,
            }
        )
    return results


def _nhsa_policy_items(raw_html: str, page_url: str, profile: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pattern = (
        r'(?is)<li>\s*<span[^>]*>\s*[^<]*\s*</span>\s*<span[^>]*>\s*<a href="([^"]+)"[^>]*title="([^"]+)"[^>]*>'
        r'.*?</a>\s*</span>\s*<span[^>]*>.*?</span>\s*<span[^>]*>(\d{4}-\d{2}-\d{2})</span>'
    )
    for match in re.finditer(pattern, raw_html):
        url = resolve_absolute_url(page_url, match.group(1))
        title = html_decode(match.group(2)).strip()
        published_at = f"{match.group(3)} 00:00"
        if not title or not url:
            continue
        if profile_has_route(profile, "medical") and not medical_policy_relevant(profile, title, "医保政策法规"):
            continue
        if not profile_has_route(profile, "medical") and profile_signal_hit_count(profile, title) <= 0:
            continue
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "nhsaOfficial",
                "sourceLabel": "国家医保局官方",
                "sourceSite": "国家医疗保障局",
                "queryMode": "official",
                "queryContext": "政策法规",
                "queryTerm": "国家医保局",
                "summary": title,
            }
        )
    return results


def get_nhsa_official_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    pages = [
        {"key": "nhsa:col14", "url": "https://www.nhsa.gov.cn/col/col14/index.html", "context": "医保动态", "mode": "list"},
        {"key": "nhsa:col104", "url": "https://www.nhsa.gov.cn/col/col104/index.html", "context": "政策法规", "mode": "policy"},
        {"key": "nhsa:col7", "url": "https://www.nhsa.gov.cn/col/col7/index.html", "context": "统计数据", "mode": "list"},
    ]
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for page in pages:
        raw_html = get_source_feed_text(cache_store, http_client, page["key"], page["url"], referer="https://www.nhsa.gov.cn/")
        if not raw_html:
            continue
        page_items = _nhsa_policy_items(raw_html, page["url"], profile) if page["mode"] == "policy" else _nhsa_list_items(raw_html, page["url"], page["context"], profile)
        for item in page_items:
            dedup = title_dedup_key(item["title"])
            if dedup and dedup in seen_keys:
                continue
            if dedup:
                seen_keys.add(dedup)
            results.append(item)
            if len(results) >= NHSA_ITEM_LIMIT:
                return results
    return results


def get_miit_context_from_url(url: str) -> str:
    text = (url or "").lower()
    if "/gxsj/" in text:
        return "最新数据"
    if "/zwgk/zcwj/" in text or "/zwgk/zcjd/" in text:
        return "最新政策"
    if "/zwgk/wjgs/" in text:
        return "文件公示"
    if "/gzcy/yjzj/" in text:
        return "意见征集"
    if any(part in text for part in ("/xwfb/gxdt/", "/xwfb/bldhd/", "/xwfb/gzdt/", "/xwfb/sjdt/")):
        return "工信动态"
    return ""


def get_miit_official_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    raw_html = get_source_feed_text(cache_store, http_client, "miit:home", "https://www.miit.gov.cn/", referer="https://www.miit.gov.cn/")
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    patterns = [
        r'(?is)<li>\s*<span>(?P<date>\d{4}-\d{2}-\d{2})(?:<font[^>]*>.*?</font>)*</span>\s*<p>\s*<a href="(?P<url>[^"]+)"[^>]*title="(?P<title>[^"]+)"[^>]*>',
        r'(?is)<li>\s*<p>\s*<a href="(?P<url>[^"]+)"[^>]*title="(?P<title>[^"]+)"[^>]*>.*?</a>\s*<span>(?P<date>\d{4}-\d{2}-\d{2})(?:<font[^>]*>.*?</font>)*</span>',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, raw_html):
            url = resolve_absolute_url("https://www.miit.gov.cn/", match.group("url"))
            title = html_decode(match.group("title")).strip()
            published_at = f"{match.group('date')} 00:00"
            context = get_miit_context_from_url(url)
            title_hits = profile_signal_hit_count(profile, title)
            context_hits = profile_signal_hit_count(profile, f"{title} {context}")
            industry_relevant = bool(re.search(r"人工智能|芯片|集成电路|半导体|软件|互联网|通信|汽车|新能源|电池|造船|船舶|化工|纺织|物联网|标准|回收|制造业|电子信息|低空", title))
            if not context or not title or not url or (title_hits <= 0 and not (context_hits > 0 and industry_relevant)):
                continue
            dedup = title_dedup_key(title)
            if dedup and dedup in seen_keys:
                continue
            if dedup:
                seen_keys.add(dedup)
            results.append(
                {
                    "title": title,
                    "url": url,
                    "publishedAt": published_at,
                    "sourceType": "miitOfficial",
                    "sourceLabel": "工信部官方",
                    "sourceSite": "中华人民共和国工业和信息化部",
                    "queryMode": "official",
                    "queryContext": context,
                    "queryTerm": "工信部",
                    "summary": title,
                }
            )
            if len(results) >= MIIT_ITEM_LIMIT:
                return results
    return results


def get_ndrc_context_from_url(url: str) -> str:
    text = (url or "").lower()
    if "/tzgg/" in text:
        return "通知公告"
    if "/xwfb/" in text:
        return "新闻发布"
    if "/wld/" in text:
        return "委领导动态"
    if "/jgsj/" in text or "/sjdt/" in text:
        return "司局动态"
    if "/dfdt/" in text:
        return "地方动态"
    return "新闻动态"


def ndrc_relevance_pattern(profile: dict[str, Any]) -> str:
    patterns: list[str] = []
    if profile_has_route(profile, "shipping", "commodity", "energy", "macro_sensitive"):
        patterns.append(r"价格|成品油|油价|煤炭|天然气|能源|电价|航运|物流|港口|供应链|保供|运价|航线|大宗")
    if profile_has_route(profile, "travel", "consumer"):
        patterns.append(r"客流|旅游|消费|出行|服务业|价格|节假日|机场|航班|免税|猪肉|储备")
    if profile_has_route(profile, "property", "infrastructure"):
        patterns.append(r"项目|投资|基建|房地产|住房|建材|开工|专项债|铁路|城中村")
    if profile_has_route(profile, "finance"):
        patterns.append(r"价格|数据|统计|信用|消费|投资|融资|债券|流动性")
    if profile_has_route(profile, "new_energy", "industrial"):
        patterns.append(r"新能源汽车|充电|电池|电网|制造业|工业|能源|项目|投资")
    return "|".join(patterns or [r"价格|数据|投资|项目"])


def get_ndrc_official_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    raw_html = get_source_feed_text(cache_store, http_client, "ndrc:xwdt", "https://www.ndrc.gov.cn/xwdt/", referer="https://www.ndrc.gov.cn/")
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    macro_profile = should_collect_source("ndrcOfficial", profile)
    relevance_pattern = ndrc_relevance_pattern(profile)
    for match in re.finditer(r'(?is)<li>\s*<a[^>]+href=[\'"]([^\'"]+)[\'"][^>]*title=[\'"]([^\'"]+)[\'"][^>]*>.*?</a>\s*<span>(\d{4}/\d{2}/\d{2})</span>\s*</li>', raw_html):
        url = resolve_absolute_url("https://www.ndrc.gov.cn/xwdt/", match.group(1))
        title = html_decode(match.group(2)).strip()
        published_at = f"{match.group(3).replace('/', '-')} 00:00"
        context = get_ndrc_context_from_url(url)
        title_hits = profile_signal_hit_count(profile, title)
        context_hits = profile_signal_hit_count(profile, f"{title} {context}")
        domain_relevant = bool(re.search(relevance_pattern, title))
        if not title or not url or (title_hits <= 0 and not (macro_profile and (context_hits > 0 or domain_relevant))):
            continue
        dedup = title_dedup_key(title)
        if dedup and dedup in seen_keys:
            continue
        if dedup:
            seen_keys.add(dedup)
        results.append(
            {
                "title": title,
                "url": url,
                "publishedAt": published_at,
                "sourceType": "ndrcOfficial",
                "sourceLabel": "国家发改委官方",
                "sourceSite": "国家发展和改革委员会",
                "queryMode": "official",
                "queryContext": context,
                "queryTerm": "国家发改委",
                "summary": title,
            }
        )
        if len(results) >= NDRC_ITEM_LIMIT:
            return results
    return results



def get_eastmoney_focus_items(profile: dict[str, Any], cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    raw_html = get_source_feed_text(
        cache_store,
        http_client,
        "eastmoney:yaowen",
        "https://finance.eastmoney.com/yaowen.html",
        referer="https://finance.eastmoney.com/",
        timeout=FEED_HTTP_TIMEOUT_SECONDS,
        retries=FEED_HTTP_RETRIES,
        allow_curl_fallback=False,
        prefer_curl=False,
    )
    if not raw_html:
        return []
    section_map = {"1": "资讯精华", "2": "国内经济", "3": "国际经济", "4": "证券聚焦", "5": "公司资讯"}
    selected_ids = eastmoney_focus_section_ids(profile)
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for section in re.finditer(r'(?is)<div class="artitleList2" id="artitileList(?P<id>\d+)">\s*<ul>(?P<body>.*?)</ul>\s*</div>', raw_html):
        section_id = section.group("id")
        if section_id not in selected_ids:
            continue
        context = section_map.get(section_id)
        if not context:
            continue
        for item in parse_eastmoney_focus_section_items(section.group("body"), context, profile):
            dedup = title_dedup_key(item["title"])
            if dedup and dedup in seen_keys:
                continue
            if dedup:
                seen_keys.add(dedup)
            results.append(item)
            if len(results) >= EASTMONEY_FOCUS_ITEM_LIMIT:
                return results
    return results


def get_sina_source_items(source_type: str, profile: dict[str, Any], from_date: str, http_client: Any) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    source_cap = SINA_SOURCE_ITEM_LIMITS[source_type]
    collected = 0
    for page in range(1, SINA_SOURCE_PAGE_LIMITS[source_type] + 1):
        raw_html = http_client.get_text(get_sina_page_url(source_type, profile, page))
        items = parse_sina_list_items(raw_html or "", source_type)
        if not items:
            break
        for item in items:
            if not should_keep_sina_title(profile, source_type, str(item.get("title", ""))):
                continue
            if not from_date or (date_only_string(item["publishedAt"]) and date_only_string(item["publishedAt"]) >= from_date):
                results.append(item)
                collected += 1
            if collected >= source_cap:
                return results
        oldest = items[-1]["publishedAt"]
        if from_date and date_only_string(oldest) and date_only_string(oldest) < from_date:
            break
    return results


def _parse_360_search_items(raw_html: str, profile: dict[str, Any], spec: dict[str, str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not raw_html:
        return results
    for block in re.findall(r'<li class="res-list">([\s\S]*?)</li>', raw_html, re.I):
        title_match = re.search(r'<h3[^>]*>\s*<a[^>]+data-mdurl="([^"]+)"[^>]*>(.*?)</a>', block, re.I)
        if not title_match:
            continue
        actual_url = html_decode(title_match.group(1))
        title = title_match.group(2)
        summary_match = re.search(r'<p class="res-desc">(.*?)</p>', block, re.I)
        rich_match = re.search(r'<div class="res-rich[\s\S]*?<div class="res-comm-con">(.*?)(?:<p class="g-linkinfo"|<div class="g-img-wrap")', block, re.I)
        summary_html = summary_match.group(1) if summary_match else (rich_match.group(1) if rich_match else "")
        site_match = re.search(r'class="g-linkinfo-a"[^>]*>(.*?)</a>', block, re.I)
        site = _clean_search_result_text(site_match.group(1)) if site_match else _site_from_url(actual_url)
        item = _build_search_candidate(
            profile=profile,
            title=title,
            url=actual_url,
            summary=summary_html,
            published_at=summary_html,
            source_type="search360",
            source_label="360 检索补召回",
            source_site=site,
            query=spec["query"],
            query_mode=spec["mode"],
            query_context=f"360 检索 / {spec['context']}",
        )
        if item:
            results.append(item)
            if len(results) >= SEARCH_RESULT_LIMIT_PER_ENGINE:
                return results
    return results


def _parse_sogou_search_items(raw_html: str, profile: dict[str, Any], spec: dict[str, str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not raw_html:
        return results
    for block in re.findall(r'<div class="vrwrap[^"]*"[^>]*>([\s\S]{0,2600}?)</div>\s*</div>', raw_html, re.I):
        data_url_match = re.search(r'data-url="([^"]+)"', block, re.I)
        title_match = re.search(r'<h3 class="vr-title">[\s\S]*?<a[^>]*>(.*?)</a>', block, re.I)
        if not data_url_match or not title_match:
            continue
        actual_url = html_decode(data_url_match.group(1))
        title = title_match.group(1)
        summary_match = re.search(r'id="cacheresult_summary_\d+"[^>]*>(.*?)</div>', block, re.I)
        summary_html = summary_match.group(1) if summary_match else ""
        cite_match = re.search(r'<a class="citeLinkClass"[\s\S]*?>([\s\S]*?)</a>', block, re.I)
        cite_spans = re.findall(r"<span>(.*?)</span>", cite_match.group(1), re.I) if cite_match else []
        date_text = ""
        site = ""
        for value in cite_spans:
            clean_value = _clean_search_result_text(value)
            if not clean_value:
                continue
            if re.search(r"(?:20\d{2}[年/-]\d{1,2}[月/-]\d{1,2}|今天|昨天|\d+\s*天前|\d+\s*小时前)", clean_value):
                date_text = clean_value
            elif not site and not clean_value.startswith("http"):
                site = clean_value
        site = site or _site_from_url(actual_url)
        item = _build_search_candidate(
            profile=profile,
            title=title,
            url=actual_url,
            summary=summary_html,
            published_at=date_text or summary_html,
            source_type="sogouSearch",
            source_label="搜狗检索补召回",
            source_site=site,
            query=spec["query"],
            query_mode=spec["mode"],
            query_context=f"搜狗检索 / {spec['context']}",
        )
        if item:
            results.append(item)
            if len(results) >= SEARCH_RESULT_LIMIT_PER_ENGINE:
                return results
    return results


def get_360_search_items(
    profile: dict[str, Any],
    from_date: str,
    to_date: str,
    cache_store: Any,
    http_client: Any,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    page_count = _search_page_count(from_date, to_date)
    result_limit = _search_result_limit(from_date, to_date)
    specs = _search_query_specs(profile, from_date, to_date)
    jobs = [(spec, page) for spec in specs for page in range(1, page_count + 1)]
    with ThreadPoolExecutor(max_workers=min(6, max(1, len(jobs)))) as executor:
        futures = {}
        for spec, page in jobs:
            query = quote_plus(spec["query"])
            url = f"https://www.so.com/s?q={query}&pn={page}"
            future = executor.submit(
                get_source_feed_text,
                cache_store,
                http_client,
                f"360search:{query}:{page}",
                url,
                "https://www.so.com/",
                SEARCH_HTTP_TIMEOUT_SECONDS,
                FEED_HTTP_RETRIES,
                False,
                False,
            )
            futures[future] = spec
        for future in as_completed(futures):
            spec = futures[future]
            raw_html = future.result() or ""
            for item in _parse_360_search_items(raw_html, profile, spec):
                if not in_date_range(item.get("publishedAt", ""), from_date, to_date):
                    continue
                dedup = title_dedup_key(item["title"]) or item["url"]
                if dedup in seen_keys:
                    continue
                seen_keys.add(dedup)
                results.append(item)
                if len(results) >= result_limit:
                    return results
    return results


def get_sogou_search_items(
    profile: dict[str, Any],
    from_date: str,
    to_date: str,
    cache_store: Any,
    http_client: Any,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    page_count = _search_page_count(from_date, to_date)
    result_limit = _search_result_limit(from_date, to_date)
    specs = _search_query_specs(profile, from_date, to_date)
    jobs = [(spec, page) for spec in specs for page in range(1, page_count + 1)]
    with ThreadPoolExecutor(max_workers=min(6, max(1, len(jobs)))) as executor:
        futures = {}
        for spec, page in jobs:
            query = quote_plus(spec["query"])
            url = f"https://www.sogou.com/web?query={query}&page={page}"
            future = executor.submit(
                get_source_feed_text,
                cache_store,
                http_client,
                f"sogousearch:{query}:{page}",
                url,
                "https://www.sogou.com/",
                SEARCH_HTTP_TIMEOUT_SECONDS,
                FEED_HTTP_RETRIES,
                False,
                False,
            )
            futures[future] = spec
        for future in as_completed(futures):
            spec = futures[future]
            raw_html = future.result() or ""
            for item in _parse_sogou_search_items(raw_html, profile, spec):
                if not in_date_range(item.get("publishedAt", ""), from_date, to_date):
                    continue
                dedup = title_dedup_key(item["title"]) or item["url"]
                if dedup in seen_keys:
                    continue
                seen_keys.add(dedup)
                results.append(item)
                if len(results) >= result_limit:
                    return results
    return results



def should_keep_candidate_item(profile: dict[str, Any], item: dict[str, Any]) -> bool:
    # 进入正文抓取前再做一轮低价值过滤，减少无效文章。
    title = str(item.get("title", "")).strip()
    summary = str(item.get("summary", "")).strip()
    source_type = str(item.get("sourceType", "")).strip()
    text = f"{title} {summary}".strip()
    if not text:
        return False

    if source_type in {"cninfoAnnouncement", "bulletin"} and CLEAR_LOW_VALUE_ANNOUNCEMENT_RE.search(title):
        return False

    if source_type in {"stockNews", "eastmoneyFocus", "eastmoneyFastNews", "csMarketNews"}:
        if LOW_VALUE_STOCK_WRAPPER_RE.search(text) or LOW_VALUE_FUND_HOUSE_RE.search(text) or "\u57fa\u91d1" in text:
            breakdown = profile_match_breakdown(profile, text)
            has_direct_entity = bool(
                breakdown["directHits"] or breakdown["aliasHits"] or breakdown["subsidiaryHits"] or breakdown["productHits"]
            )
            has_strong_event = bool(STRONG_EVENT_TITLE_RE.search(text))
            if not (has_direct_entity and has_strong_event):
                return False
    return True


def _items_in_range(items: list[dict[str, Any]], from_date: str, to_date: str) -> list[dict[str, Any]]:
    return [item for item in items if in_date_range(item.get("publishedAt", ""), from_date, to_date)]


def collect_candidate_items(profile: dict[str, Any], from_date: str, to_date: str, cache_store: Any, http_client: Any) -> list[dict[str, Any]]:
    # 先并发抓候选，再统一做日期过滤和低价值过滤。
    results: list[dict[str, Any]] = []
    tasks: list[tuple[str, Any, tuple[Any, ...]]] = []
    enabled_sources = set(enabled_source_types())

    def add_task(source_type: str, func: Any, *args: Any, guarded: bool = False) -> None:
        if source_type not in enabled_sources:
            return
        if guarded and not should_collect_source(source_type, profile):
            return
        tasks.append((source_type, func, args))

    add_task("cninfoAnnouncement", get_cninfo_announcement_items, profile, from_date, to_date, http_client)
    for source_type in ("stockNews", "industryNews", "bulletin"):
        add_task(source_type, get_sina_source_items, source_type, profile, from_date, http_client)

    add_task("eastmoneyFocus", get_eastmoney_focus_items, profile, cache_store, http_client)
    add_task("eastmoneyFastNews", get_eastmoney_fast_news_items, profile, cache_store, http_client)
    add_task("csMarketNews", get_cs_market_news_items, profile, cache_store, http_client)
    add_task("search360", get_360_search_items, profile, from_date, to_date, cache_store, http_client)
    add_task("sogouSearch", get_sogou_search_items, profile, from_date, to_date, cache_store, http_client)
    add_task("nmpaOfficial", get_nmpa_official_items, profile, cache_store, http_client, guarded=True)
    add_task("nhsaOfficial", get_nhsa_official_items, profile, cache_store, http_client, guarded=True)
    add_task("miitOfficial", get_miit_official_items, profile, cache_store, http_client, guarded=True)
    add_task("ndrcOfficial", get_ndrc_official_items, profile, cache_store, http_client, guarded=True)

    supplemental_sources = {"eastmoneyFocus", "eastmoneyFastNews", "csMarketNews", "search360", "sogouSearch"}
    core_tasks = [task for task in tasks if task[0] not in supplemental_sources]
    supplemental_tasks = [task for task in tasks if task[0] in supplemental_sources]

    if core_tasks:
        with ThreadPoolExecutor(max_workers=min(SOURCE_CORE_WORKERS, max(1, len(core_tasks)))) as executor:
            futures = {executor.submit(func, *args): source_type for source_type, func, args in core_tasks}
            for future in as_completed(futures):
                try:
                    items = future.result() or []
                except Exception:
                    items = []
                results.extend(_items_in_range(items, from_date, to_date))

    if supplemental_tasks:
        futures = {}
        executor = ThreadPoolExecutor(max_workers=min(SOURCE_SUPPLEMENTAL_WORKERS, max(1, len(supplemental_tasks))))
        try:
            for source_type, func, args in supplemental_tasks:
                futures[executor.submit(func, *args)] = source_type
            try:
                for future in as_completed(futures, timeout=SOURCE_COLLECTION_BUDGET_SECONDS):
                    try:
                        items = future.result() or []
                    except Exception:
                        items = []
                    results.extend(_items_in_range(items, from_date, to_date))
            except FutureTimeoutError:
                pass
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    filtered = [item for item in results if should_keep_candidate_item(profile, item)]
    return annotate_candidate_items(profile, filtered)
