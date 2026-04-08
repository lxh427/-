from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


LOCAL_ROOT_DIR = Path(__file__).resolve().parent.parent
BUNDLED_ROOT_DIR = Path(getattr(sys, "_MEIPASS", LOCAL_ROOT_DIR))
ROOT_DIR = BUNDLED_ROOT_DIR


def _runtime_home_dir() -> Path:
    override = str(os.environ.get("DAYINGJING_HOME", "")).strip()
    if override:
        return Path(override).expanduser()

    if getattr(sys, "frozen", False):
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "DaYingJing"
        if sys.platform.startswith("linux"):
            return Path.home() / ".local" / "share" / "DaYingJing"
        local_appdata = str(os.environ.get("LOCALAPPDATA", "")).strip()
        if local_appdata:
            return Path(local_appdata) / "DaYingJing"
        return Path.home() / "AppData" / "Local" / "DaYingJing"

    return LOCAL_ROOT_DIR


RUNTIME_HOME_DIR = _runtime_home_dir()
DATA_DIR = (RUNTIME_HOME_DIR / "data") if getattr(sys, "frozen", False) else (LOCAL_ROOT_DIR / "data")
CACHE_DIR = DATA_DIR / "cache"
PROFILE_OVERRIDES_PATH = DATA_DIR / "profile-overrides.json"


def _ensure_runtime_layout() -> None:
    # 打包后把可写数据放到用户目录，避免写入安装目录失败。
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    bundled_profile = ROOT_DIR / "data" / "profile-overrides.json"
    if bundled_profile.exists() and not PROFILE_OVERRIDES_PATH.exists():
        shutil.copy2(bundled_profile, PROFILE_OVERRIDES_PATH)


_ensure_runtime_layout()

WEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

CACHE_SCHEMA_VERSION = "2026-04-06-python-domestic-v15-evidence-source-trace"
PRIORITIZED_ITEM_LIMIT = 120
FAST_CONFIRMED_FETCH_LIMIT = 12
QUERY_CACHE_MINUTES = 25
SOURCE_FEED_CACHE_MINUTES = 10
ARTICLE_CACHE_HOURS = 36
STOCK_UNIVERSE_CACHE_HOURS = 6
SOURCE_COLLECTION_BUDGET_SECONDS = 28
FEED_HTTP_TIMEOUT_SECONDS = 6
FEED_HTTP_RETRIES = 1

MACRO_TERMS = [
    "疫情",
    "新冠",
    "伊朗",
    "俄乌",
    "俄乌冲突",
    "中东",
    "红海",
    "霍尔木兹",
    "关税",
    "制裁",
    "大选",
    "特朗普",
    "川普",
    "油价",
    "运价",
    "航运",
    "药监",
    "审批",
    "临床",
    "医保",
    "出口限制",
    "GPU",
    "英伟达",
    "NVIDIA",
    "国产替代",
    "集采",
    "牌照",
    "降准",
    "降息",
    "金价",
    "铜价",
    "锂价",
    "补贴",
    "客流",
    "停火",
    "谈判",
    "军费",
]

SOURCE_CATALOG = {
    "cninfoAnnouncement": {
        "label": "巨潮资讯公告",
        "enabled": True,
        "isOfficial": True,
        "rank": 6,
        "priorityCap": 20,
        "deepFetchCap": 0,
        "fastDeepFetchCap": 2,
        "sourceScore": 24,
        "sourceReason": "来自巨潮资讯官方公告源。",
    },
    "bulletin": {
        "label": "新浪公司公告",
        "enabled": True,
        "rank": 5,
        "priorityCap": 10,
        "deepFetchCap": 0,
        "fastDeepFetchCap": 1,
        "sourceScore": 20,
        "sourceReason": "来自公司公告栏目。",
    },
    "stockNews": {
        "label": "新浪个股资讯",
        "enabled": True,
        "rank": 4,
        "priorityCap": 24,
        "deepFetchCap": 18,
        "fastDeepFetchCap": 4,
        "sourceScore": 12,
        "sourceReason": "来自个股资讯栏目。",
    },
    "industryNews": {
        "label": "新浪行业资讯",
        "enabled": True,
        "rank": 3,
        "priorityCap": 12,
        "deepFetchCap": 10,
        "fastDeepFetchCap": 3,
        "sourceScore": 8,
        "sourceReason": "来自行业资讯栏目。",
    },
    "eastmoneyFocus": {
        "label": "东方财富焦点资讯",
        "enabled": True,
        "rank": 4,
        "priorityCap": 16,
        "deepFetchCap": 14,
        "fastDeepFetchCap": 4,
        "sourceScore": 11,
        "sourceReason": "来自东方财富焦点频道的综合资讯。",
    },
    "eastmoneyFastNews": {
        "label": "东方财富快讯",
        "enabled": True,
        "rank": 5,
        "priorityCap": 18,
        "deepFetchCap": 16,
        "fastDeepFetchCap": 4,
        "sourceScore": 14,
        "sourceReason": "来自东方财富 7x24 与要闻快讯，适合捕捉宏观、地缘、大宗和突发事件。",
    },
    "csMarketNews": {
        "label": "中证网财经要闻",
        "enabled": True,
        "rank": 4,
        "priorityCap": 14,
        "deepFetchCap": 12,
        "fastDeepFetchCap": 4,
        "sourceScore": 10,
        "sourceReason": "来自中证网财经要闻源。",
    },
    "nmpaOfficial": {
        "label": "国家药监局公告",
        "enabled": True,
        "isOfficial": True,
        "rank": 6,
        "priorityCap": 12,
        "deepFetchCap": 10,
        "fastDeepFetchCap": 3,
        "sourceScore": 22,
        "sourceReason": "来自国家药监局官方公告与通告。",
    },
    "nhsaOfficial": {
        "label": "国家医保局官方",
        "enabled": True,
        "isOfficial": True,
        "rank": 6,
        "priorityCap": 12,
        "deepFetchCap": 10,
        "fastDeepFetchCap": 3,
        "sourceScore": 22,
        "sourceReason": "来自国家医保局官方动态、政策与统计数据。",
    },
    "miitOfficial": {
        "label": "工信部官方",
        "enabled": True,
        "isOfficial": True,
        "rank": 5,
        "priorityCap": 14,
        "deepFetchCap": 12,
        "fastDeepFetchCap": 3,
        "sourceScore": 18,
        "sourceReason": "来自工信部官方动态、政策与行业数据。",
    },
    "ndrcOfficial": {
        "label": "国家发改委官方",
        "enabled": True,
        "isOfficial": True,
        "rank": 6,
        "priorityCap": 14,
        "deepFetchCap": 12,
        "fastDeepFetchCap": 3,
        "sourceScore": 20,
        "sourceReason": "来自国家发改委新闻动态、通知公告与司局动态，适合补宏观、价格、项目、运输和消费链条信号。",
    },
    "search360": {
        "label": "360 检索补召回",
        "enabled": True,
        "rank": 3,
        "priorityCap": 18,
        "deepFetchCap": 14,
        "fastDeepFetchCap": 4,
        "sourceScore": 8,
        "sourceReason": "来自 360 检索的历史结果，用于补齐栏目流遗漏的相关原文。",
    },
    "sogouSearch": {
        "label": "搜狗检索补召回",
        "enabled": True,
        "rank": 3,
        "priorityCap": 18,
        "deepFetchCap": 14,
        "fastDeepFetchCap": 4,
        "sourceScore": 8,
        "sourceReason": "来自搜狗检索的历史结果，用于补齐栏目流之下的相关原文。",
    },
}


def enabled_source_types() -> list[str]:
    return [key for key, value in SOURCE_CATALOG.items() if value.get("enabled")]


def enabled_source_labels() -> list[str]:
    return [SOURCE_CATALOG[key]["label"] for key in enabled_source_types()]


def source_setting(source_type: str, key: str, default: object = None) -> object:
    return SOURCE_CATALOG.get(source_type, {}).get(key, default)
