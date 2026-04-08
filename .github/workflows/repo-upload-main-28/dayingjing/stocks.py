from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

from .config import CACHE_SCHEMA_VERSION, STOCK_UNIVERSE_CACHE_HOURS
from .profiles import load_profile_overrides, resolve_override_match
from .text_utils import normalize_text


_STOCK_UNIVERSE_MEMORY: dict[str, Any] | None = None
_STOCK_UNIVERSE_EXPIRES_AT: datetime | None = None


def market_prefix_from_code(code: str) -> str:
    if re.fullmatch(r"(6|9)\d{5}", code):
        return "sh"
    if re.fullmatch(r"(0|2|3)\d{5}", code):
        return "sz"
    if re.fullmatch(r"(4|8)\d{5}", code):
        return "bj"
    return "sz"


def new_resolved_stock(query: str, code: str, name: str, industry: str = "") -> dict[str, str]:
    market = market_prefix_from_code(code)
    return {
        "query": query,
        "code": code,
        "name": name,
        "symbol": f"{market}{code}",
        "market": market,
        "industry": industry,
    }


def eastmoney_stock_snapshot(code: str, http_client: Any) -> dict[str, str]:
    market = market_prefix_from_code(code)
    market_flag = "1" if market == "sh" else "0"
    payload = http_client.get_json(
        f"https://push2.eastmoney.com/api/qt/stock/get?secid={market_flag}.{code}&fields=f57,f58,f127",
        referer="https://quote.eastmoney.com/",
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    if not data:
        return {}
    return {
        "name": str(data.get("f58", "")).strip(),
        "industry": str(data.get("f127", "")).strip(),
    }


def _stock_universe_cache_key() -> str:
    return f"{CACHE_SCHEMA_VERSION}|stock-universe"


def get_ashare_universe(http_client: Any, cache_store: Any) -> dict[str, Any]:
    global _STOCK_UNIVERSE_MEMORY, _STOCK_UNIVERSE_EXPIRES_AT
    now = datetime.now()
    if (
        _STOCK_UNIVERSE_MEMORY
        and _STOCK_UNIVERSE_EXPIRES_AT
        and _STOCK_UNIVERSE_EXPIRES_AT > now
        and int(_STOCK_UNIVERSE_MEMORY.get("total", 0)) > 1000
    ):
        return {
            "total": _STOCK_UNIVERSE_MEMORY["total"],
            "updatedAt": _STOCK_UNIVERSE_MEMORY["updatedAt"],
            "stocks": list(_STOCK_UNIVERSE_MEMORY["stocks"]),
        }

    cache_key = _stock_universe_cache_key()
    disk_cached = cache_store.get("stock-universe", cache_key)
    if disk_cached and int(disk_cached.get("total", 0)) > 1000:
        _STOCK_UNIVERSE_MEMORY = disk_cached
        _STOCK_UNIVERSE_EXPIRES_AT = now + timedelta(hours=STOCK_UNIVERSE_CACHE_HOURS)
        return disk_cached

    stocks: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    total = 0
    page_size = 100
    page = 1
    total_pages = 1
    while page <= total_pages:
        url = (
            "https://push2.eastmoney.com/api/qt/clist/get"
            f"?pn={page}&pz={page_size}&po=1&np=1&fltt=2&invt=2&fid=f3"
            "&fs=m:0+t:6,m:0+t:80,m:0+t:81+s:2048,m:1+t:2,m:1+t:23"
            "&fields=f12,f14,f100"
        )
        payload = http_client.get_json(url, referer="https://quote.eastmoney.com/center/gridlist.html")
        data = payload.get("data") if isinstance(payload, dict) else None
        rows = data.get("diff") if isinstance(data, dict) else None
        if not rows:
            break
        if page == 1:
            try:
                total = int(data.get("total", 0))
                total_pages = max(1, (total + page_size - 1) // page_size)
            except Exception:
                total_pages = 1
        for item in rows:
            code = str(item.get("f12", "")).strip()
            name = str(item.get("f14", "")).strip()
            industry = str(item.get("f100", "")).strip()
            if re.fullmatch(r"\d{6}", code) and name and code not in seen_codes:
                seen_codes.add(code)
                stocks.append(new_resolved_stock(name, code, name, industry))
        page += 1

    if not stocks:
        for override in load_profile_overrides().values():
            code = str(override.get("code", "")).strip()
            name = str(override.get("name", "")).strip()
            if code and name:
                stocks.append(new_resolved_stock(name, code, name))
        total = len(stocks)

    universe = {
        "total": total or len(stocks),
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stocks": sorted(stocks, key=lambda item: item["code"]),
    }
    _STOCK_UNIVERSE_MEMORY = universe
    _STOCK_UNIVERSE_EXPIRES_AT = now + timedelta(hours=STOCK_UNIVERSE_CACHE_HOURS)
    cache_store.set("stock-universe", cache_key, universe, now + timedelta(hours=STOCK_UNIVERSE_CACHE_HOURS))
    return universe


def stock_search_score(token: str, stock: dict[str, Any], override: dict[str, Any] | None) -> int:
    normalized_token = normalize_text(token)
    if not normalized_token:
        return -1
    normalized_name = normalize_text(stock["name"])
    normalized_code = normalize_text(stock["code"])
    aliases = [normalize_text(item) for item in (override or {}).get("aliases", []) if normalize_text(item)]
    score = 0
    matched = False

    if normalized_token == normalized_code:
        score += 500
        matched = True
    if normalized_token == normalized_name:
        score += 480
        matched = True
    if normalized_token in aliases:
        score += 470
        matched = True
    if normalized_name.startswith(normalized_token):
        score += 320
        matched = True
    if normalized_code.startswith(normalized_token):
        score += 300
        matched = True
    if any(alias.startswith(normalized_token) for alias in aliases):
        score += 280
        matched = True
    if normalized_token in normalized_name:
        score += 180
        matched = True
    if normalized_token in normalized_code:
        score += 160
        matched = True
    if any(normalized_token in alias for alias in aliases):
        score += 150
        matched = True

    if not matched:
        return -1
    if override:
        score += 20
    score += max(0, 12 - abs(len(stock["name"]) - len(token)))
    return score


def resolve_stock_from_suggest_service(query: str, http_client: Any) -> dict[str, str] | None:
    token = (query or "").strip()
    if not token:
        return None
    url = f"https://suggest3.sinajs.cn/suggest/type=11,111&key={quote(token)}&name=suggestdata_codex"
    response = http_client.get_text(url, referer="https://finance.sina.com.cn")
    match = re.search(r'="(.*)";?$', response or "")
    if not match:
        return None

    normalized_token = normalize_text(token)
    best_score = -1
    best_stock = None
    for entry in match.group(1).split(";"):
        parts = entry.split(",")
        if len(parts) < 4:
            continue
        name = str(parts[0]).strip()
        code = str(parts[2]).strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        normalized_name = normalize_text(name)
        score = 0
        if normalized_name == normalized_token:
            score += 220
        if normalized_name.startswith(normalized_token):
            score += 140
        if normalized_token in normalized_name:
            score += 100
        if score > best_score:
            best_score = score
            best_stock = new_resolved_stock(token, code, name)
    return best_stock


def suggest_stock_candidates(query: str, http_client: Any, take: int = 8) -> list[dict[str, Any]]:
    token = (query or "").strip()
    if not token:
        return []
    url = f"https://suggest3.sinajs.cn/suggest/type=11,111&key={quote(token)}&name=suggestdata_codex"
    response = http_client.get_text(url, referer="https://finance.sina.com.cn")
    match = re.search(r'="(.*)";?$', response or "")
    if not match:
        return []

    normalized_token = normalize_text(token)
    ranked: list[dict[str, Any]] = []
    for entry in match.group(1).split(";"):
        parts = entry.split(",")
        if len(parts) < 4:
            continue
        name = str(parts[0]).strip()
        code = str(parts[2]).strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        normalized_name = normalize_text(name)
        score = 0
        if normalize_text(code) == normalized_token:
            score += 260
        if normalized_name == normalized_token:
            score += 220
        if normalized_name.startswith(normalized_token):
            score += 140
        if normalized_token in normalized_name:
            score += 100
        ranked.append(
            {
                "query": token,
                "code": code,
                "name": name,
                "symbol": f"{market_prefix_from_code(code)}{code}",
                "market": market_prefix_from_code(code),
                "industry": "",
                "aliases": [],
                "description": "",
                "score": score,
            }
        )

    ranked.sort(key=lambda item: (-int(item["score"]), len(item["name"]), item["code"]))
    results: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for item in ranked:
        if item["code"] not in seen_codes:
            seen_codes.add(item["code"])
            results.append(item)
        if len(results) >= take:
            break
    return results


def search_stock_candidates(query: str, http_client: Any, cache_store: Any, take: int = 12) -> dict[str, Any]:
    token = (query or "").strip()
    if not token:
        universe = get_ashare_universe(http_client, cache_store)
        return {"total": universe["total"], "updatedAt": universe["updatedAt"], "stocks": []}

    overrides = load_profile_overrides()
    quick_results: list[dict[str, Any]] = []

    override_match = resolve_override_match(token)
    if override_match:
        code = str(override_match.get("code", "")).strip()
        name = str(override_match.get("name", "")).strip()
        if code and name:
            quick_results.append(
                {
                    "query": token,
                    "code": code,
                    "name": name,
                    "symbol": f"{market_prefix_from_code(code)}{code}",
                    "market": market_prefix_from_code(code),
                    "industry": "",
                    "aliases": [item for item in override_match.get("aliases", []) if item][:6],
                    "description": str(override_match.get("description", "")).strip(),
                    "score": 1000,
                }
            )

    for item in suggest_stock_candidates(token, http_client, take=take):
        override = overrides.get(item["code"]) or {}
        item["aliases"] = [alias for alias in override.get("aliases", []) if alias][:6]
        item["description"] = str(override.get("description", "")).strip()
        quick_results.append(item)

    if quick_results:
        deduped_quick: list[dict[str, Any]] = []
        seen_codes: set[str] = set()
        for item in sorted(quick_results, key=lambda entry: (-int(entry["score"]), len(entry["name"]), entry["code"])):
            if item["code"] not in seen_codes:
                seen_codes.add(item["code"])
                deduped_quick.append(item)
            if len(deduped_quick) >= take:
                break
        cached_total = _STOCK_UNIVERSE_MEMORY["total"] if _STOCK_UNIVERSE_MEMORY else len(deduped_quick)
        cached_updated_at = _STOCK_UNIVERSE_MEMORY["updatedAt"] if _STOCK_UNIVERSE_MEMORY else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {"total": cached_total, "updatedAt": cached_updated_at, "stocks": deduped_quick}

    universe = get_ashare_universe(http_client, cache_store)
    ranked: list[dict[str, Any]] = []
    for stock in universe["stocks"]:
        override = overrides.get(stock["code"])
        score = stock_search_score(token, stock, override)
        if score >= 0:
            ranked.append({"stock": stock, "score": score, "override": override})

    ranked.sort(key=lambda item: (-item["score"], len(item["stock"]["name"]), item["stock"]["code"]))
    results: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for entry in ranked:
        stock = entry["stock"]
        if stock["code"] in seen_codes or len(results) >= take:
            continue
        seen_codes.add(stock["code"])
        override = entry["override"] or {}
        results.append(
            {
                "query": token,
                "code": stock["code"],
                "name": stock["name"],
                "symbol": stock["symbol"],
                "market": stock["market"],
                "industry": stock.get("industry", ""),
                "aliases": [item for item in override.get("aliases", []) if item][:6],
                "description": str(override.get("description", "")).strip(),
                "score": entry["score"],
            }
        )

    return {"total": universe["total"], "updatedAt": universe["updatedAt"], "stocks": results}


def resolve_stock_query(query: str, http_client: Any, cache_store: Any) -> dict[str, str] | None:
    token = (query or "").strip()
    if not token:
        return None

    code_match = re.fullmatch(r"(sh|sz|bj)?(\d{6})", token, re.I)
    if code_match:
        code = code_match.group(2)
        symbol = f"{(code_match.group(1) or market_prefix_from_code(code)).lower()}{code}"
        override = resolve_override_match(token)
        override_name = str(override.get("name", "")).strip() if override else ""
        override_industry = str(override.get("industry", "")).strip() if override else ""
        html = http_client.get_text(f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml")
        title_match = re.search(r"<title>\s*([^<(]+)\((\d{6})\)", html or "", re.I)
        if title_match:
            resolved_code = title_match.group(2)
            resolved_name = title_match.group(1).strip()
            if override_name:
                resolved_name = override_name
            return new_resolved_stock(token, resolved_code, resolved_name, override_industry)
        if override:
            return new_resolved_stock(token, code, str(override.get("name", code)))
        eastmoney_snapshot = eastmoney_stock_snapshot(code, http_client)
        if eastmoney_snapshot.get("name"):
            return new_resolved_stock(token, code, eastmoney_snapshot["name"], eastmoney_snapshot.get("industry", ""))
        return new_resolved_stock(token, code, code)

    suggested = resolve_stock_from_suggest_service(token, http_client)
    if suggested:
        return suggested

    lookup_html = http_client.get_text(
        f"https://biz.finance.sina.com.cn/suggest/lookup_n.php?q={quote(token)}",
        referer="https://finance.sina.com.cn",
    )
    title_match = re.search(r"<title>\s*([^<(]+)\((\d{6})\)", lookup_html or "", re.I)
    if title_match:
        code = title_match.group(2)
        return new_resolved_stock(token, code, title_match.group(1).strip())
    list_match = re.search(r"realstock/company/(sz|sh|bj)(\d{6})/nc\.shtml[^>]*>([^<]+)</a>", lookup_html or "", re.I)
    if list_match:
        return new_resolved_stock(token, list_match.group(2), list_match.group(3).strip())

    candidates = search_stock_candidates(token, http_client, cache_store, take=1)["stocks"]
    if candidates:
        candidate = candidates[0]
        return new_resolved_stock(token, candidate["code"], candidate["name"], candidate.get("industry", ""))

    override = resolve_override_match(token)
    if override:
        code = str(override.get("code", "")).strip()
        name = str(override.get("name", token)).strip()
        if code:
            return new_resolved_stock(token, code, name)
    return None
