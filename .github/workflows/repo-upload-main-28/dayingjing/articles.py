from __future__ import annotations

import importlib
import importlib.util
import io
from datetime import datetime, timedelta
from typing import Any

from .config import ARTICLE_CACHE_HOURS, CACHE_SCHEMA_VERSION
from .profiles import query_terms
from .text_utils import (
    clean_extracted_text,
    is_boilerplate_sentence,
    keyword_hits,
    paragraphs_from_html,
    paragraphs_from_plain_text,
    repair_pdf_plain_text,
)


_HAS_PYPDF = importlib.util.find_spec("pypdf") is not None
_HAS_PYMUPDF = importlib.util.find_spec("pymupdf") is not None or importlib.util.find_spec("fitz") is not None


def effective_paragraphs(paragraphs: list[str], profile: dict[str, Any], macro_terms: list[str]) -> list[str]:
    terms = query_terms(profile) + list(macro_terms)
    cleaned_paragraphs: list[str] = []
    for paragraph in paragraphs:
        cleaned = clean_extracted_text(paragraph)
        if not cleaned or is_boilerplate_sentence(cleaned):
            continue
        if cleaned not in cleaned_paragraphs:
            cleaned_paragraphs.append(cleaned)

    effective: list[str] = []
    for paragraph in cleaned_paragraphs:
        if keyword_hits(terms, paragraph) and paragraph not in effective:
            effective.append(paragraph)
    if effective:
        return effective
    if cleaned_paragraphs:
        return cleaned_paragraphs[:6]
    return [clean_extracted_text(paragraph) for paragraph in paragraphs[:6] if clean_extracted_text(paragraph)]


def _article_cache_key(url: str) -> str:
    return f"{CACHE_SCHEMA_VERSION}|article:{url}"


def _fallback_article(fallback_text: str) -> dict[str, Any]:
    return {"rawText": fallback_text, "effectiveText": fallback_text, "contentMode": "fallback"}


def _summary_article(summary_text: str) -> dict[str, Any]:
    return {"rawText": summary_text, "effectiveText": summary_text, "contentMode": "summary"}


def _pdf_text_to_article(
    plain_text: str,
    profile: dict[str, Any],
    macro_terms: list[str],
    fallback_text: str = "",
) -> dict[str, Any]:
    cleaned = repair_pdf_plain_text(plain_text or "")
    if not cleaned:
        return _fallback_article(fallback_text)
    paragraphs = paragraphs_from_plain_text(cleaned)
    effective = effective_paragraphs(paragraphs, profile, macro_terms)
    return {
        "rawText": "\n".join(paragraphs[:60]),
        "effectiveText": "\n".join(effective[:12]),
        "contentMode": "fulltext",
    }


def _pdf_article_content_pypdf(
    pdf_bytes: bytes,
    profile: dict[str, Any],
    macro_terms: list[str],
    fallback_text: str = "",
) -> dict[str, Any]:
    if not _HAS_PYPDF or not pdf_bytes:
        return _fallback_article(fallback_text)
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(pdf_bytes))
        page_texts = [(page.extract_text() or "") for page in reader.pages[:16]]
        return _pdf_text_to_article("\n".join(page_texts), profile, macro_terms, fallback_text=fallback_text)
    except Exception:
        return _fallback_article(fallback_text)


def _pymupdf_module() -> Any | None:
    for module_name in ("pymupdf", "fitz"):
        try:
            return importlib.import_module(module_name)
        except Exception:
            continue
    return None


def _pdf_article_content_pymupdf(
    pdf_bytes: bytes,
    profile: dict[str, Any],
    macro_terms: list[str],
    fallback_text: str = "",
) -> dict[str, Any]:
    if not _HAS_PYMUPDF or not pdf_bytes:
        return _fallback_article(fallback_text)

    try:
        pymupdf = _pymupdf_module()
        if pymupdf is None:
            return _fallback_article(fallback_text)

        document = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        try:
            page_texts = [document.load_page(page_index).get_text("text") for page_index in range(min(len(document), 16))]
        finally:
            document.close()

        return _pdf_text_to_article("\n".join(page_texts), profile, macro_terms, fallback_text=fallback_text)
    except Exception:
        return _fallback_article(fallback_text)


def _fulltext_length(article: dict[str, Any]) -> int:
    if str(article.get("contentMode", "")) != "fulltext":
        return -1
    return len(str(article.get("rawText") or ""))


def _best_pdf_article(*articles: dict[str, Any]) -> dict[str, Any]:
    ranked = sorted(articles, key=_fulltext_length, reverse=True)
    return ranked[0] if ranked else {}


def _pdf_article_content(
    url: str,
    profile: dict[str, Any],
    macro_terms: list[str],
    http_client: Any,
    fallback_text: str = "",
) -> dict[str, Any]:
    pdf_bytes = http_client.get_bytes(url, referer="https://www.cninfo.com.cn/")
    if not pdf_bytes:
        pdf_bytes = http_client.get_bytes(url)
    if not pdf_bytes:
        return _fallback_article(fallback_text)

    pymupdf_result = _pdf_article_content_pymupdf(pdf_bytes, profile, macro_terms, fallback_text=fallback_text)
    if _fulltext_length(pymupdf_result) >= 120:
        return pymupdf_result

    pypdf_result = _pdf_article_content_pypdf(pdf_bytes, profile, macro_terms, fallback_text=fallback_text)
    if _fulltext_length(pypdf_result) >= 120:
        return pypdf_result

    best_result = _best_pdf_article(pymupdf_result, pypdf_result)
    return best_result if _fulltext_length(best_result) >= 0 else _fallback_article(fallback_text)


def get_article_content(
    url: str,
    profile: dict[str, Any],
    macro_terms: list[str],
    cache_store: Any,
    http_client: Any,
    fallback_text: str = "",
    allow_slow_pdf_fallback: bool = True,
) -> dict[str, Any]:
    # 保留 allow_slow_pdf_fallback 参数，兼容旧调用方。
    _ = allow_slow_pdf_fallback

    if not url:
        return _fallback_article(fallback_text)

    cache_key = _article_cache_key(url)
    cached = cache_store.get("article", cache_key)
    if isinstance(cached, dict) and cached.get("contentMode"):
        return cached

    if ".pdf" in url.lower().split("#")[0].split("?")[0]:
        result = _pdf_article_content(url, profile, macro_terms, http_client, fallback_text=fallback_text)
        cache_store.set("article", cache_key, result, datetime.now() + timedelta(hours=ARTICLE_CACHE_HOURS))
        return result

    raw_html = http_client.get_text(url)
    if not raw_html:
        result = _fallback_article(fallback_text)
        cache_store.set("article", cache_key, result, datetime.now() + timedelta(hours=6))
        return result

    paragraphs = paragraphs_from_html(raw_html)
    effective = effective_paragraphs(paragraphs, profile, macro_terms)
    result = {
        "rawText": "\n".join(paragraphs[:24]),
        "effectiveText": "\n".join(effective[:8]),
        "contentMode": "fulltext",
    }
    cache_store.set("article", cache_key, result, datetime.now() + timedelta(hours=ARTICLE_CACHE_HOURS))
    return result


__all__ = ["get_article_content", "effective_paragraphs", "_summary_article"]
