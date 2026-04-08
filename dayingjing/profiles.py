from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

from .config import PROFILE_OVERRIDES_PATH
from .text_utils import keyword_hits, normalize_text, strip_html


_AUTO_PROFILE_CACHE: dict[str, dict[str, Any]] = {}
_CNINFO_SECURITY_CACHE: dict[str, dict[str, Any]] = {}


@lru_cache(maxsize=1)
def load_profile_overrides() -> dict[str, dict[str, Any]]:
    if not PROFILE_OVERRIDES_PATH.exists():
        return {}
    try:
        items = json.loads(PROFILE_OVERRIDES_PATH.read_text("utf-8"))
    except Exception:
        return {}
    return {str(item.get("code", "")).strip(): item for item in items if item.get("code")}


def profile_override(code: str) -> dict[str, Any] | None:
    return load_profile_overrides().get(code)


def add_profile_values(profile: dict[str, Any], key: str, values: list[str] | tuple[str, ...] | None) -> None:
    bucket = [str(item).strip() for item in profile.get(key, []) if str(item).strip()]
    seen = set(bucket)
    for value in values or []:
        text = str(value).strip()
        if text and text not in seen:
            bucket.append(text)
            seen.add(text)
    profile[key] = bucket


SECONDARY_BUSINESS_MARKERS = (
    "辅以",
    "兼营",
    "兼有",
    "并涉及",
    "并从事",
    "同时从事",
    "同时涉足",
    "涉足",
    "延伸至",
    "少量涉及",
)


def extract_primary_business_text(text: str | None) -> str:
    value = re.sub(r"\s+", "", text or "").strip()
    if not value:
        return ""
    for marker in SECONDARY_BUSINESS_MARKERS:
        if marker not in value:
            continue
        primary = value.split(marker, 1)[0].strip("，,；;、 ")
        if len(primary) >= 4:
            return primary
    return value


def extract_product_candidates(text: str | None) -> list[str]:
    if not text:
        return []
    results: list[str] = []
    for pattern in (
        r"产品(?:包括|涵盖|主要为|有)([^。；]+)",
        r"业务(?:覆盖|涵盖|包括)([^。；]+)",
        r"专注于([^。；]+)",
        r"应用于([^。；]+)",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        segment = match.group(1)
        for candidate in re.split(r"[、,，/；;]", segment):
            value = re.sub(r"^(应用于|用于|覆盖|包括)", "", candidate).strip()
            if (
                2 <= len(value) <= 18
                and not re.search(r"公司|业务|市场|领域|主要|开展|覆盖|中国", value)
                and value not in results
            ):
                results.append(value)
    return results[:8]


def industry_profile_seed(industry: str, business_text: str) -> dict[str, list[str]]:
    seed = {
        "themes": [],
        "sensitiveFactors": [],
        "narrativeHooks": [],
        "policyAuthorities": [],
        "policyDomains": [],
    }
    combined = f"{industry} {business_text}".strip()
    if industry:
        seed["themes"].append(industry)

    if re.search(r"制药|生物|医疗器械|中药|医疗服务", combined):
        seed["themes"] += ["医药", "创新药", "临床试验", "医保", "仿制药", "原料药"]
        seed["sensitiveFactors"] += ["药品审批", "临床进展", "医保", "集采", "适应症", "参比制剂", "非处方药", "说明书修订", "中药保护"]
        seed["narrativeHooks"] += ["创新药估值", "出海授权"]
        seed["policyAuthorities"] += ["国家药监局", "国家医保局", "国家卫健委"]
        seed["policyDomains"] += ["nmpa.gov.cn", "nhsa.gov.cn", "nhc.gov.cn", "gov.cn"]

    if re.search(r"半导体|电子元件|光学光电子|芯片|集成电路", combined):
        seed["themes"] += ["半导体", "集成电路", "芯片", "国产替代", "算力", "AI", "人工智能"]
        seed["sensitiveFactors"] += ["出口管制", "国产替代", "先进制程", "算力需求", "GPU", "英伟达", "高带宽存储", "物联网"]
        seed["narrativeHooks"] += ["科技突破", "映射交易"]
        seed["policyAuthorities"] += ["工信部", "商务部", "海关总署"]
        seed["policyDomains"] += ["miit.gov.cn", "mofcom.gov.cn", "customs.gov.cn", "gov.cn"]

    if re.search(r"航运|港口|物流|集运|海运|海上集装箱运输|集装箱运输", combined):
        seed["themes"] += ["航运", "运价", "地缘政治", "港口", "船舶"]
        seed["sensitiveFactors"] += ["红海", "霍尔木兹", "伊朗", "运价", "油价", "航线扰动", "造船", "集装箱"]
        seed["narrativeHooks"] += ["地缘冲突映射"]
        seed["policyAuthorities"] += ["交通运输部", "商务部", "海关总署"]
        seed["policyDomains"] += ["mot.gov.cn", "mofcom.gov.cn", "customs.gov.cn", "gov.cn"]

    if re.search(r"机场|航空运输|旅游|酒店|免税|出行", combined):
        seed["themes"] += ["出行", "旅游", "免税", "国际航班"]
        seed["sensitiveFactors"] += ["疫情", "国际航班", "出入境", "客流", "消费复苏"]
        seed["narrativeHooks"] += ["出行修复"]
        seed["policyAuthorities"] += ["民航局", "文化和旅游部", "国家移民管理局"]
        seed["policyDomains"] += ["caac.gov.cn", "mct.gov.cn", "nia.gov.cn", "gov.cn"]

    if re.search(r"电池|光伏|风电|新能源|汽车整车|汽车零部件", combined):
        seed["themes"] += ["新能源", "电动车", "光伏", "储能"]
        seed["sensitiveFactors"] += ["锂价", "补贴政策", "出口", "装机", "碳中和"]
        seed["narrativeHooks"] += ["景气度切换"]
        seed["policyAuthorities"] += ["工信部", "国家发改委", "国家能源局"]
        seed["policyDomains"] += ["miit.gov.cn", "ndrc.gov.cn", "nea.gov.cn", "gov.cn"]

    if re.search(r"石油|煤炭|天然气|有色金属|黄金|铜|铝", combined):
        seed["themes"] += ["资源品", "大宗商品", "周期"]
        seed["sensitiveFactors"] += ["油价", "煤价", "金价", "铜价", "供给扰动", "美元"]
        seed["narrativeHooks"] += ["通胀交易"]
        seed["policyAuthorities"] += ["国家发改委", "国家能源局", "海关总署"]
        seed["policyDomains"] += ["ndrc.gov.cn", "nea.gov.cn", "customs.gov.cn", "gov.cn"]

    if re.search(r"军工|航空装备|航天|船舶|雷达|空管", combined):
        seed["themes"] += ["军工", "航空航天", "军民融合"]
        seed["sensitiveFactors"] += ["军费", "国际局势", "地缘冲突", "订单", "军贸"]
        seed["narrativeHooks"] += ["事件催化"]
        seed["policyAuthorities"] += ["国防部", "工信部", "国务院"]
        seed["policyDomains"] += ["mod.gov.cn", "miit.gov.cn", "gov.cn"]

    if re.search(r"银行|证券|保险|多元金融", combined):
        seed["themes"] += ["金融", "利率", "资本市场"]
        seed["sensitiveFactors"] += ["降准降息", "地产风险", "资本市场改革", "汇率"]
        seed["narrativeHooks"] += ["政策预期"]
        seed["policyAuthorities"] += ["中国人民银行", "国家金融监管总局", "证监会"]
        seed["policyDomains"] += ["pbc.gov.cn", "nfra.gov.cn", "csrc.gov.cn", "gov.cn"]

    if re.search(r"白酒|食品饮料|乳业|调味品|消费", combined):
        seed["themes"] += ["消费", "品牌", "渠道"]
        seed["sensitiveFactors"] += ["消费复苏", "提价", "渠道库存", "节假日需求"]
        seed["narrativeHooks"] += ["消费修复"]
        seed["policyAuthorities"] += ["商务部", "文化和旅游部", "海关总署"]
        seed["policyDomains"] += ["mofcom.gov.cn", "mct.gov.cn", "customs.gov.cn", "gov.cn"]

    if re.search(r"传媒|游戏|影视|广告|教育", combined):
        seed["themes"] += ["传媒", "内容", "流量"]
        seed["sensitiveFactors"] += ["版号", "监管", "广告投放", "热点事件"]
        seed["narrativeHooks"] += ["情绪弹性"]
        seed["policyAuthorities"] += ["国家新闻出版署", "广电总局", "网信办"]
        seed["policyDomains"] += ["nppa.gov.cn", "nrta.gov.cn", "cac.gov.cn", "gov.cn"]

    if re.search(r"地产|建筑|建材|家居", combined):
        seed["themes"] += ["地产链", "基建", "家居"]
        seed["sensitiveFactors"] += ["地产政策", "专项债", "开工率", "竣工"]
        seed["narrativeHooks"] += ["政策刺激"]
        seed["policyAuthorities"] += ["住建部", "国家发改委", "财政部"]
        seed["policyDomains"] += ["mohurd.gov.cn", "ndrc.gov.cn", "mof.gov.cn", "gov.cn"]

    return {key: list(dict.fromkeys(value))[: (12 if "Domains" not in key and "Authorities" not in key else 8)] for key, value in seed.items()}


def _route_text(industry: str, seed: dict[str, list[str]], extra_text: str = "") -> str:
    parts: list[str] = [industry, extra_text]
    for key in ("themes", "sensitiveFactors", "narrativeHooks", "policyAuthorities", "products"):
        parts.extend([str(item).strip() for item in seed.get(key, []) if str(item).strip()])
    return " ".join([part for part in parts if part]).strip()


def infer_route_tags(industry: str, seed: dict[str, list[str]], extra_text: str = "") -> list[str]:
    text = _route_text(industry, seed, extra_text)
    route_tags: list[str] = []

    def add(*tags: str) -> None:
        for tag in tags:
            token = str(tag).strip()
            if token and token not in route_tags:
                route_tags.append(token)

    if re.search(r"制药|生物|医疗器械|中药|医疗服务|创新药|临床|医保|药监", text):
        add("medical", "company_news")
    if re.search(r"半导体|电子元件|光学光电子|芯片|集成电路|人工智能|算力|GPU|国产替代", text):
        add("technology", "semiconductor", "company_news")
    if re.search(r"航运|港口|物流|集运|船舶|运价|航线", text):
        add("shipping", "commodity", "macro_sensitive")
    if re.search(r"机场|航空运输|旅游|酒店|免税|出行|国际航班|客流", text):
        add("travel", "consumer", "macro_sensitive")
    if re.search(r"电池|光伏|风电|新能源|汽车整车|汽车零部件|储能", text):
        add("new_energy", "energy", "technology", "macro_sensitive", "company_news")
    if re.search(r"石油|煤炭|天然气|有色金属|黄金|铜|铝|大宗商品|资源品", text):
        add("energy", "commodity", "macro_sensitive")
    if re.search(r"军工|航空装备|航天|雷达|空管|军民融合", text):
        add("defense", "technology", "company_news")
    if re.search(r"银行|证券|保险|多元金融|资本市场|利率|汇率", text):
        add("finance", "macro_sensitive")
    if re.search(r"白酒|食品饮料|乳业|调味品|消费|品牌|渠道", text):
        add("consumer", "company_news")
    if re.search(r"传媒|游戏|影视|广告|教育|内容|流量", text):
        add("media", "consumer", "company_news")
    if re.search(r"地产|建筑|建材|家居|房地产|基建|开工", text):
        add("property", "infrastructure", "macro_sensitive")
    if re.search(r"制造业|工业互联网|电子信息|造船|装备|标准", text):
        add("industrial", "company_news")
    if any(tag in route_tags for tag in ("shipping", "travel", "energy", "commodity", "finance", "property", "infrastructure", "consumer", "new_energy")):
        add("policy")

    return route_tags[:12]


def build_scenario_queries(profile: dict[str, Any]) -> list[str]:
    scenarios: list[str] = []
    factors = profile.get("sensitiveFactors", [])[:6]
    themes = profile.get("themes", [])[:4]
    route_tags = [str(tag).strip() for tag in profile.get("routeTags", []) if str(tag).strip()]
    name = str(profile.get("name", "")).strip()
    for factor in factors:
        query = " ".join([str(factor).strip(), name]).strip()
        if query:
            scenarios.append(query)
    for theme in themes:
        query = " ".join([str(theme).strip(), "行业事件"]).strip()
        if query:
            scenarios.append(query)

    route_query_map = {
        "shipping": ["红海 航运 运价", "霍尔木兹 油价 航线", "伊朗 停火 集运 运价"],
        "travel": ["国际航班 客流 出入境", "免签 旅游 客流 消费", "机场 航司 航班 恢复"],
        "finance": ["降准降息 资本市场 政策", "央行 流动性 债券 市场", "券商 两融 交易 活跃度"],
        "energy": ["成品油 电价 能源 保供", "煤价 天然气 电力 政策", "新能源 装机 储能 电网"],
        "commodity": ["国际油价 大宗商品 供给", "金价 铜价 锂价 资源品", "PMI PPI 大宗商品"],
        "new_energy": ["新能源汽车 补贴 出口", "光伏 风电 装机", "锂电 储能 产业链"],
        "technology": ["人工智能 算力 国产替代", "出口限制 芯片 GPU", "工业互联网 制造业 升级"],
        "semiconductor": ["先进制程 GPU 出口限制", "国产替代 芯片 算力", "半导体 设备 材料"],
        "medical": ["创新药 审批 医保", "临床进展 适应症 药监", "集采 医保 支付"],
        "property": ["房地产 政策 融资 施工", "专项债 基建 投资", "建材 开工 城中村 改造"],
        "consumer": ["社零 消费 节假日 价格", "促消费 政策 食品 饮料", "猪肉 储备 居民消费"],
        "media": ["版号 热点 内容 传播", "广告 投放 影视 游戏", "舆情 热度 流量"],
        "defense": ["军费 地缘冲突 军贸", "低空 设备 订单 军工", "航空航天 装备"],
    }
    for tag in route_tags:
        scenarios.extend(route_query_map.get(tag, []))
    return list(dict.fromkeys([item for item in scenarios if item]))[:14]


def query_terms(profile: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for bucket in (
        [profile.get("name", ""), profile.get("code", "")],
        profile.get("aliases", []),
        profile.get("subsidiaries", []),
        profile.get("products", []),
        profile.get("themes", []),
        profile.get("sensitiveFactors", []),
        profile.get("narrativeHooks", []),
        profile.get("policyAuthorities", []),
    ):
        for value in bucket:
            text = str(value).strip()
            if text and text not in values:
                values.append(text)
    return values


def profile_match_breakdown(profile: dict[str, Any], text: str) -> dict[str, list[str]]:
    identity_hits = keyword_hits([profile.get("name", ""), profile.get("code", "")], text)
    alias_hits = keyword_hits(profile.get("aliases", []), text)
    subsidiary_hits = keyword_hits(profile.get("subsidiaries", []), text)
    product_hits = keyword_hits(profile.get("products", []), text)
    theme_hits = keyword_hits(profile.get("themes", []), text)
    factor_hits = keyword_hits(profile.get("sensitiveFactors", []), text)
    hook_hits = keyword_hits(profile.get("narrativeHooks", []), text)
    authority_hits = keyword_hits(profile.get("policyAuthorities", []), text)
    return {
        "identityHits": identity_hits,
        "aliasHits": alias_hits,
        "subsidiaryHits": subsidiary_hits,
        "productHits": product_hits,
        "themeHits": theme_hits,
        "factorHits": factor_hits,
        "hookHits": hook_hits,
        "authorityHits": authority_hits,
        "directHits": list(dict.fromkeys(identity_hits + alias_hits + subsidiary_hits + product_hits)),
        "signalHits": list(dict.fromkeys(theme_hits + factor_hits + hook_hits + authority_hits)),
    }


def source_candidate_hit_count(profile: dict[str, Any], text: str) -> int:
    hits: list[str] = []
    for bucket in (
        [profile.get("name", ""), profile.get("code", "")],
        profile.get("aliases", []),
        profile.get("subsidiaries", []),
        profile.get("products", []),
        profile.get("themes", []),
        profile.get("sensitiveFactors", []),
        profile.get("narrativeHooks", []),
        profile.get("policyAuthorities", []),
    ):
        for hit in keyword_hits(bucket, text):
            if hit not in hits:
                hits.append(hit)
    return len(hits)


def profile_signal_hit_count(profile: dict[str, Any], text: str) -> int:
    hits: list[str] = []
    for bucket in (
        [profile.get("name", ""), profile.get("code", "")],
        profile.get("aliases", []),
        profile.get("subsidiaries", []),
        profile.get("products", []),
        profile.get("themes", []),
        profile.get("sensitiveFactors", []),
        profile.get("narrativeHooks", []),
    ):
        for hit in keyword_hits(bucket, text):
            if hit not in hits:
                hits.append(hit)
    return len(hits)


def get_eastmoney_quote_snapshot(stock: dict[str, Any], http_client: Any) -> dict[str, str] | None:
    market_flag = "1" if stock.get("market") == "sh" else "0"
    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={market_flag}.{stock['code']}&fields=f57,f58,f127,f128"
    payload = http_client.get_json(url, referer="https://quote.eastmoney.com/")
    data = payload.get("data") if isinstance(payload, dict) else None
    if not data:
        return None
    return {"industry": str(data.get("f127", "")).strip(), "region": str(data.get("f128", "")).strip()}


def get_sina_company_profile_snapshot(stock: dict[str, Any], http_client: Any) -> dict[str, str] | None:
    url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/{stock['code']}.phtml"
    html = http_client.get_text(url, referer="https://finance.sina.com.cn")
    if not html:
        return None
    intro = ""
    main_business = ""
    match = re.search(r'(?is)<td class="ct">公司简介：</td>\s*<td[^>]*class="ccl"[^>]*>(.*?)</td>', html)
    if match:
        intro = strip_html(match.group(1))
    match = re.search(r'(?is)<td class="ct">主营业务：</td>\s*<td[^>]*class="ccl"[^>]*>(.*?)</td>', html)
    if match:
        main_business = strip_html(match.group(1))
    if not intro and not main_business:
        return None
    return {"intro": intro, "mainBusiness": main_business}


def get_cninfo_security_snapshot(stock: dict[str, Any], http_client: Any) -> dict[str, str] | None:
    cached = _CNINFO_SECURITY_CACHE.get(stock["code"])
    if cached:
        return dict(cached)
    for query in dict.fromkeys([stock["code"], stock.get("name", "")]):
        if not query:
            continue
        payload = http_client.post_form_json(
            "https://www.cninfo.com.cn/new/information/topSearch/query",
            {"keyWord": query, "maxNum": 12},
            referer="https://www.cninfo.com.cn/new/fulltextSearch",
        )
        records = payload if isinstance(payload, list) else []
        if not records:
            continue

        matched = next(
            (
                item
                for item in records
                if str(item.get("code")) == stock["code"]
                and str(item.get("category")) in {"A股", "B股"}
            ),
            None,
        )
        if not matched:
            matched = next((item for item in records if str(item.get("code")) == stock["code"]), None)
        if not matched and stock.get("name"):
            matched = next((item for item in records if str(item.get("zwjc")) == stock["name"]), None)
        if matched:
            result = {
                "orgId": str(matched.get("orgId", "")).strip(),
                "category": str(matched.get("category", "")).strip(),
                "type": str(matched.get("type", "")).strip(),
            }
            _CNINFO_SECURITY_CACHE[stock["code"]] = dict(result)
            return result
    return None


def get_auto_profile_seed(stock: dict[str, Any], http_client: Any) -> dict[str, Any]:
    cached = _AUTO_PROFILE_CACHE.get(stock["code"])
    if cached:
        return dict(cached)

    company_snapshot = get_sina_company_profile_snapshot(stock, http_client) or {}
    business_text = str(company_snapshot.get("mainBusiness", "")).strip()
    intro_text = str(company_snapshot.get("intro", "")).strip()
    primary_business_text = extract_primary_business_text(business_text)
    primary_intro_text = extract_primary_business_text(intro_text)
    industry = str(stock.get("industry") or "").strip()
    if not industry and not (business_text or intro_text):
        industry = str((get_eastmoney_quote_snapshot(stock, http_client) or {}).get("industry") or "").strip()
    signal_text = primary_business_text or primary_intro_text or business_text or intro_text
    combined_text = f"{business_text} {intro_text}".strip()
    seed = industry_profile_seed(industry, signal_text)
    route_tags = infer_route_tags(industry, seed, signal_text or combined_text)

    description = business_text or intro_text
    if not description and industry:
        description = f"{stock['name']}属于{industry}，系统会围绕行业主题、政策因子和跨领域事件自动扩展信号。"
    if not description:
        description = f"{stock['name']}的自动画像已启用，当前按实时新闻、公告、政策和行业映射扩展信号。"
    description = re.sub(r"\s+", " ", description).strip()[:180].rstrip("，。；; ")

    result = {
        "industry": industry,
        "description": description,
        "products": extract_product_candidates(signal_text or combined_text),
        "themes": seed["themes"][:12],
        "sensitiveFactors": seed["sensitiveFactors"][:12],
        "narrativeHooks": seed["narrativeHooks"][:8],
        "policyAuthorities": seed["policyAuthorities"][:8],
        "policyDomains": seed["policyDomains"][:8],
        "routeTags": route_tags,
    }
    _AUTO_PROFILE_CACHE[stock["code"]] = dict(result)
    return result


def get_live_profile(stock: dict[str, Any], http_client: Any) -> dict[str, Any]:
    override = profile_override(stock["code"]) or {}
    auto_seed = get_auto_profile_seed(stock, http_client)
    profile: dict[str, Any] = {
        "code": stock["code"],
        "name": stock["name"],
        "symbol": stock["symbol"],
        "market": stock["market"],
        "industry": auto_seed.get("industry", ""),
        "aliases": [],
        "subsidiaries": [],
        "products": list(auto_seed.get("products", [])),
        "themes": list(auto_seed.get("themes", [])),
        "sensitiveFactors": list(auto_seed.get("sensitiveFactors", [])),
        "narrativeHooks": list(auto_seed.get("narrativeHooks", [])),
        "policyAuthorities": list(auto_seed.get("policyAuthorities", [])),
        "policyDomains": list(auto_seed.get("policyDomains", [])),
        "routeTags": list(auto_seed.get("routeTags", [])),
        "scenarioQueries": [],
        "description": str(auto_seed.get("description", "")),
    }

    if override.get("name"):
        profile["name"] = str(override["name"]).strip()
    for key in (
        "aliases",
        "subsidiaries",
        "products",
        "themes",
        "sensitiveFactors",
        "narrativeHooks",
        "policyAuthorities",
        "policyDomains",
        "routeTags",
    ):
        add_profile_values(profile, key, override.get(key) or [])
    if override.get("description"):
        profile["description"] = str(override["description"]).strip()
    add_profile_values(
        profile,
        "routeTags",
        infer_route_tags(
            str(profile.get("industry", "")),
            {
                "products": list(profile.get("products", [])),
                "themes": list(profile.get("themes", [])),
                "sensitiveFactors": list(profile.get("sensitiveFactors", [])),
                "narrativeHooks": list(profile.get("narrativeHooks", [])),
                "policyAuthorities": list(profile.get("policyAuthorities", [])),
            },
            " ".join(
                [
                    extract_primary_business_text(str(profile.get("description", "")).strip()),
                    " ".join([str(item).strip() for item in profile.get("aliases", []) if str(item).strip()]),
                    " ".join([str(item).strip() for item in profile.get("subsidiaries", []) if str(item).strip()]),
                ]
            ).strip(),
        ),
    )
    profile["scenarioQueries"] = build_scenario_queries(profile)
    return profile


def resolve_override_match(query: str) -> dict[str, Any] | None:
    token = query.strip()
    if not token:
        return None
    overrides = load_profile_overrides()
    code_match = re.fullmatch(r"(?:sh|sz|bj)?(\d{6})", token, re.I)
    if code_match:
        return overrides.get(code_match.group(1))

    normalized_token = normalize_text(token)
    exact_match = None
    fuzzy_matches: list[dict[str, Any]] = []
    for override in overrides.values():
        candidates = [override.get("name", ""), override.get("code", ""), *(override.get("aliases") or [])]
        for candidate in candidates:
            normalized_candidate = normalize_text(str(candidate))
            if not normalized_candidate:
                continue
            if normalized_token == normalized_candidate:
                exact_match = override
                break
            if len(normalized_token) >= 2 and len(normalized_candidate) >= 2:
                if normalized_token in normalized_candidate or normalized_candidate in normalized_token:
                    if not any(item.get("code") == override.get("code") for item in fuzzy_matches):
                        fuzzy_matches.append(override)
        if exact_match:
            break
    if exact_match:
        return exact_match
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0]
    return None
